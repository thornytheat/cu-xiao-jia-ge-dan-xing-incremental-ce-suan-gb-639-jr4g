"""增量编排与批次：采信 → 归价 → 质量 → 弹性 → 增量 → 报告，一次事务落库。

报告版本化（促销补录/改价不改写历史）：
- 每个批次绑定当次促销单快照（promo_snapshots），事后再改日历也动不了旧批次；
- 业务口径 series_key = 样例集 + 0 销量处理 + 规则版本；同一口径下促销单变化
  （快照摘要变化）就生成一个新的报告版本，状态 pending（待核），旧版本仍是当前
  报告并按原快照导出；核对通过后 publish 切换 current，旧版本转 archived；
- 幂等键 idem_key = series_key + 促销快照摘要 + 全量输入摘要：同一次补录重复
  重试命中同一条版本记录，绝不新增版本；
- 生成中断（running/failed）后重试：恢复同一条版本记录续算，成功后原地转正；
  失败保留旧版 current 不变，只把该版本标 failed。
"""
from __future__ import annotations

import datetime as dt
import time
import uuid
from collections import defaultdict
from decimal import Decimal

from ... import config, manifest, reasons
from ...store import Store
from ..m1_ingest import service as ingest_service
from ..m2_price import service as price_service
from ..m2_price import snapshot as snapshot_mod
from ..m3_quality import quarantine as quarantine_mod
from ..m3_quality import service as quality_service
from ..m4_elastic import service as elastic_service
from . import baseline, ci, spread

REPLAY_NOTE = "同键批次已存在，复用结果"
FIRST_VERSION_NOTE = "首版报告"
BACKFILL_NOTE = "促销单补录/改价，生成待核新版本"

# 报告版本状态机
STATE_RUNNING = "running"      # 生成中（含中断待恢复）
STATE_PENDING = "pending"      # 生成完成，待人工核对
STATE_PUBLISHED = "published"  # 核对通过，当前报告
STATE_ARCHIVED = "archived"    # 被新版替换的历史版本，仍可按原快照导出
STATE_FAILED = "failed"        # 生成失败，不影响当前报告


class GenerationInterrupted(Exception):
    """生成过程被中断（进程崩溃/外部中止）：版本保留 running，等待重试恢复。

    与普通测算失败区分：普通失败把版本标 failed 并记录原因；中断没有可靠的
    失败结论，按「生成中」挂起，下一次同补录提交原地续算。
    """


def now_iso() -> str:
    return dt.datetime.now(config.UTC).isoformat(timespec="seconds")


def _group_windows(windows) -> dict[str, list]:
    out: dict[str, list] = defaultdict(list)
    for window in windows:
        out[window.promo_id].append(window)
    return out


def promo_dates_of(windows) -> set:
    dates = set()
    for window in windows:
        cursor = window.start
        while cursor < window.end:
            dates.add(cursor)
            cursor += dt.timedelta(days=1)
    return dates


def compute_lift(assessed, elastic_rows) -> dict:
    windows_by_promo = _group_windows(assessed["windows"])
    accounts = {a["promo_id"]: a for a in assessed["accounts"]}
    base_excl = assessed["base_prices_excl"]
    rows: list[dict] = []
    spread_rows: list[dict] = []
    quarantine: list[dict] = []
    measured: list[str] = []

    for promo_id in sorted(windows_by_promo):
        windows = windows_by_promo[promo_id]
        account = accounts.get(promo_id)
        if account is None:
            quarantine.append({
                "code": "Q_PROMO_UNKNOWN_SKU", "stage": "lift", "source": "promo_calendar.csv",
                "line_no": None, "field": "商品编码",
                "raw_value": f"{windows[0].sku}/{windows[0].channel}",
                "detail": f"{promo_id} 没有对应的日销样本",
            })
            continue
        if account["coverage"] < config.MIN_COVERAGE_RATIO:
            continue
        sku, channel = account["sku"], account["channel"]
        slope_row = next((r for r in elastic_rows
                          if r["scope"] == "sku" and r["group_key"] == f"{sku}/{channel}"),
                         None)
        if slope_row is None:
            quarantine.append({
                "code": "L_SAMPLE_TOO_SMALL", "stage": "lift", "source": "daily_sales",
                "line_no": None, "field": "销量",
                "raw_value": f"{sku}/{channel}",
                "detail": f"{promo_id} 没有可用的弹性斜率",
            })
            continue
        dates = promo_dates_of(windows)
        fit = baseline.compute(windows, dates, assessed["sales"], assessed["control"],
                               base_excl)
        if isinstance(fit, str):
            quarantine.append({
                "code": fit, "stage": "lift", "source": "daily_sales",
                "line_no": None, "field": "销量",
                "raw_value": f"{sku}/{channel}",
                "detail": f"{promo_id} 基线不可用",
            })
            continue
        actual_qty, actual_gmv = baseline.actual_totals(
            assessed["sales"], dates, sku, channel)
        dlnp = ci.log_ratio(fit.price_excl, fit.pre_price_excl)
        lift_qty = (actual_qty - fit.base_qty).quantize(Decimal("0.0001"))
        lift_gmv = (actual_gmv - fit.base_gmv).quantize(config.MONEY_QUANT)
        band = ci.interval(lift_gmv=lift_gmv, base_qty=fit.base_qty,
                           base_price=fit.pre_price_excl, dlnp=dlnp,
                           slope=float(slope_row["slope"]),
                           se_slope=float(slope_row["se"]))
        start = min(w.start for w in windows)
        end = max(w.end for w in windows)
        rows.append({
            "promo_id": promo_id, "sku": sku, "channel": channel,
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "days": account["days"], "days_valid": account["days_valid"],
            "days_missing": account["days_missing"], "days_dropped": account["days_dropped"],
            "actual_qty": str(actual_qty), "base_qty": str(fit.base_qty),
            "actual_gmv": str(actual_gmv), "base_gmv": str(fit.base_gmv),
            "lift_qty": str(lift_qty), "lift_gmv": str(lift_gmv),
            "se_gmv": band["se_gmv"], "ci_low": band["ci_low"], "ci_high": band["ci_high"],
            "price_excl": str(fit.price_excl),
            "base_price_excl": str(fit.pre_price_excl),
            "control_sku": config.CONTROL_MAP.get(sku, ""),
            "control_factor": str(fit.control_factor),
            "dlnp": f"{dlnp:.6f}" if dlnp is not None else "0",
            "slope": str(slope_row["slope"]),
            "note": "区间包含 0" if Decimal(band["ci_low"]) <= 0 <= Decimal(band["ci_high"])
            else "区间不含 0",
        })
        measured.append(promo_id)
        channel_gmv = baseline.channel_totals(assessed["sales"], dates, sku)
        shares = spread.allocate(lift_gmv, channel_gmv)
        for share in shares:
            spread_rows.append({
                "promo_id": promo_id, "channel": share["channel"],
                "ratio": share["ratio"], "lift_gmv": share["lift_gmv"],
                "actual_gmv": share["actual_gmv"],
            })
        if spread.remainder_of(lift_gmv, shares) != 0:
            quarantine.append({
                "code": "A_SPREAD_REMAINDER", "stage": "lift",
                "source": "daily_sales", "line_no": None, "field": "增量",
                "raw_value": str(spread.remainder_of(lift_gmv, shares)),
                "detail": f"{promo_id} 分摊尾差已按最大余额法补齐",
            })
    return {"rows": rows, "spread": spread_rows, "quarantine": quarantine,
            "measured": measured}


def _stage(stage: str, in_count: int, out_count: int, quarantine_count: int,
           elapsed: float, status: str = "done", note: str = "") -> dict:
    return {"stage": stage, "in_count": in_count, "out_count": out_count,
            "quarantine_count": quarantine_count, "status": status,
            "elapsed_ms": int(elapsed * 1000), "note": note}


def _count_stage(rows, stage: str) -> int:
    return sum(1 for r in rows if r["stage"] == stage)


def _series_key(dataset: str, mode: str, rule_version: str) -> str:
    """业务口径：同一口径下的促销补录/改价归为同一系列的版本迭代。"""
    return manifest.digest_parts([dataset, mode, rule_version])


def _compute_pipeline(loaded, mode: str, started_at: str):
    """纯计算：采信结果 → 归价 → 质量 → 弹性 → 增量 → 合计。不落库。

    on_stage 钩子仅用于测试中断恢复，在每个阶段结束后回调，抛错即模拟生成中断。
    """
    total_start = time.perf_counter()
    timings: list[tuple[str, int, int, int, float]] = []
    mark = time.perf_counter()
    promo_records = loaded["files"]["promo"].rows
    control_records = loaded["files"]["control"].rows
    sales_records = loaded["files"]["sales"].rows
    timings.append(("ingest", sales_records + promo_records + control_records,
                    sales_records, _count_stage([], "ingest"),
                    time.perf_counter() - mark))

    mark = time.perf_counter()
    normalized = price_service.normalize(loaded)
    price_q = _count_stage(normalized["quarantine"], "price")
    timings.append(("price", sales_records, len(normalized["sales"]), price_q,
                    time.perf_counter() - mark))

    mark = time.perf_counter()
    assessed = quality_service.assess(normalized)
    quality_q = _count_stage(assessed["quarantine"], "quality")
    timings.append(("quality", len(assessed["sales"]), len(assessed["sales"]), quality_q,
                    time.perf_counter() - mark))

    mark = time.perf_counter()
    elastic = elastic_service.run(assessed["sales"], mode)
    elastic_q = len(elastic["quarantine"])
    timings.append(("elastic", len(assessed["sales"]), len(elastic["rows"]),
                    elastic_q, time.perf_counter() - mark))

    mark = time.perf_counter()
    lift = compute_lift(assessed, elastic["rows"])
    lift_q = len(lift["quarantine"])
    timings.append(("lift", len(assessed["accounts"]), len(lift["rows"]),
                    lift_q, time.perf_counter() - mark))

    extra = quarantine_mod.enrich(lift["quarantine"] + elastic["quarantine"])
    all_quarantine = assessed["quarantine"] + extra
    counts = quarantine_mod.level_counts(all_quarantine)

    mark = time.perf_counter()
    totals = build_totals(loaded, assessed, lift, all_quarantine, counts)
    timings.append(("report", len(lift["rows"]), len(all_quarantine), 0,
                    time.perf_counter() - mark))

    elapsed_ms = int((time.perf_counter() - total_start) * 1000)
    stages = [_stage(*item) for item in timings]
    return {
        "assessed": assessed, "elastic": elastic, "lift": lift,
        "all_quarantine": all_quarantine, "totals": totals,
        "stages": stages, "elapsed_ms": elapsed_ms,
    }


def _build_payload(loaded, computed, *, run_id, batch_id, batch_key, settings,
                   dataset, mode, started_at, finished_at, snapshot) -> dict:
    assessed = computed["assessed"]
    elastic = computed["elastic"]
    lift = computed["lift"]
    totals = computed["totals"]
    stages = computed["stages"]
    manifest_doc = manifest.build(
        run_id=run_id, batch_id=batch_id, batch_key=batch_key, settings=settings,
        dataset=dataset, inputs=loaded["sources"], params={"zero_mode": mode},
        started_at=started_at, finished_at=finished_at,
        elapsed_ms=computed["elapsed_ms"], stages=stages, counters=totals,
        totals=totals,
    )
    # 把当次促销单快照写进运行清单：旧版本导出时能自证用的是哪一版促销单
    manifest_doc["promo_snapshot"] = {
        "snapshot_id": snapshot["snapshot_digest"],
        "snapshot_digest": snapshot["snapshot_digest"],
        "source_name": snapshot["source_name"],
        "source_sha256": snapshot["source_sha256"],
        "row_count": snapshot["row_count"],
    }
    return {
        "sources": loaded["sources"],
        "sales": [
            {"line_no": row.line_no, "sale_date": row.sale_date.isoformat(),
             "channel": row.channel, "sku": row.sku, "sku_name": row.sku_name,
             "price_incl": str(row.price_incl), "price_unit": row.price_unit,
             "tax_rate": str(row.tax_rate), "price_excl": str(row.price_excl),
             "qty": row.qty, "promo_id": row.promo_id,
             "amount_excl": str(row.amount_excl), "flags": row.flags}
            for row in assessed["sales"]
        ],
        "promos": [
            {"promo_id": version.promo_id, "sku": version.sku,
             "channel": version.channel, "version": version.version,
             "start_date": version.start.isoformat() if version.start else "",
             "end_date": version.end.isoformat() if version.end else "",
             "activity_price": str(version.price_incl) if version.price_incl else "",
             "status": version.status, "effective": 1 if version.effective else 0,
             "overlap": 1 if version.overlap else 0, "note": version.note,
             "row_no": version.row_no}
            for version in assessed["versions"]
        ],
        "promo_windows": price_service.window_rows(
            assessed["windows"], assessed["base_prices_excl"]),
        "quarantine": [
            {"code": row["code"], "stage": row["stage"], "level": row["level"],
             "source": row["source"], "line_no": row["line_no"], "field": row["field"],
             "raw_value": row["raw_value"], "detail": row["detail"]}
            for row in computed["all_quarantine"]
        ],
        "elasticity": elastic["rows"],
        "lift": lift["rows"],
        "spread": lift["spread"],
        "stages": stages,
        "totals": totals,
        "manifest": manifest_doc,
    }


def run(settings: config.Settings, store: Store, dataset: str = "main",
        zero_mode: str | None = None, on_stage=None) -> dict:
    """跑一次测算并落库；返回结果带报告版本信息。

    on_stage(stage_name)：可选阶段回调，抛异常即模拟生成中断，供恢复测试。
    """
    mode = zero_mode or settings.zero_mode
    if mode not in config.ZERO_MODES:
        mode = config.ZERO_MODE_SHIFT
    started_at = now_iso()

    loaded = ingest_service.load(settings, dataset)
    digests = [source["sha256"] for source in loaded["sources"]]
    batch_key = manifest.digest_parts(
        [dataset, mode, settings.rule_version] + digests)

    # 当次促销单快照（不可变留痕）；同一促销单重试摘要不变
    snapshot = snapshot_mod.build_snapshot(loaded["files"]["promo"])
    series_key = _series_key(dataset, mode, settings.rule_version)
    # 幂等键含促销快照与全量输入：同一补录重试命中同一版本，绝不新增
    idem_key = manifest.digest_parts(
        [series_key, snapshot["snapshot_digest"], batch_key])

    # 1) 幂等/恢复优先：同一补录（idem_key）已有版本记录。
    #    必须先于 batch_key 复用判断——补录改了促销日历会改变 batch_key，
    #    但 idem_key 命中说明就是同一次提交的重试，绝不能当成新补录。
    existing_version = store.get_version_by_idem(idem_key)
    if existing_version is not None and existing_version["state"] in (
            STATE_PENDING, STATE_PUBLISHED, STATE_ARCHIVED):
        existing = store.get_batch(existing_version["batch_id"])
        return {
            "batch": existing, "replay": True, "note": REPLAY_NOTE,
            "batch_key": batch_key,
            "counters": store.totals(existing_version["batch_id"]),
            "report_version": existing_version,
            "snapshot": _snapshot_view(store, existing_version),
        }

    # 2) 已完成同键批次的兼容复用（无版本记录的老数据）：同输入重跑，不新增行。
    #    走到这里说明 idem_key 未命中任何版本；batch_key 含全量输入摘要，
    #    促销补录/改价后必然不同，因此不会误命中。
    existing = store.find_done_by_key(batch_key)
    if existing is not None:
        version = store.get_version_by_batch(existing["batch_id"])
        return {
            "batch": existing, "replay": True, "note": REPLAY_NOTE,
            "batch_key": batch_key, "counters": store.totals(existing["batch_id"]),
            "report_version": version, "snapshot": _snapshot_view(store, version),
        }

    # 3) 新版本，或中断/失败版本的恢复
    version = existing_version
    resumed = False
    if version is None:
        snapshot_id = store.upsert_promo_snapshot(snapshot, started_at)
        with store.connect() as conn:
            version_no = store.next_version_no(conn, series_key)
        is_first = version_no == 1
        batch_id = f"B{dt.datetime.now(config.UTC).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}"
        version_id = store.insert_report_version({
            "series_key": series_key, "version_no": version_no,
            "batch_id": batch_id, "snapshot_id": snapshot_id, "idem_key": idem_key,
            "state": STATE_RUNNING,
            "is_current": 1 if is_first else 0,
            "change_note": FIRST_VERSION_NOTE if is_first else BACKFILL_NOTE,
            "created_at": started_at, "updated_at": started_at,
        })
        version = store.get_version(version_id)
    else:
        # 同一补录重试：running（中断）或 failed 都恢复这一条，不新增版本
        resumed = True
        batch_id = version["batch_id"]
        store.mark_version_state(
            version["version_id"], STATE_RUNNING,
            bump_attempts=True, updated_at=started_at)
        version = store.get_version(version["version_id"])

    # 3) 计算（失败不在原批次上改写：新批次行整体回滚，旧 current 不受影响）
    try:
        if on_stage is not None:
            on_stage("begin")
        computed = _compute_pipeline(loaded, mode, started_at)
        if on_stage is not None:
            on_stage("computed")
        finished_at = now_iso()
        run_id = manifest.new_run_id()
        payload = _build_payload(
            loaded, computed, run_id=run_id, batch_id=batch_id, batch_key=batch_key,
            settings=settings, dataset=dataset, mode=mode,
            started_at=started_at, finished_at=finished_at, snapshot=snapshot)
        store.upsert_batch({
            "batch_id": batch_id, "batch_key": batch_key, "run_id": run_id,
            "dataset": dataset, "status": "done", "created_at": started_at,
            "finished_at": finished_at,
            "input_digest": payload["manifest"]["input_digest"],
            "params": {"zero_mode": mode}, "note": loaded["state_label"],
            "snapshot_id": version["snapshot_id"],
        })
        # write_batch 自身一个事务：中途失败整体回滚，不留半批
        store.write_batch(batch_id, payload)
    except GenerationInterrupted:
        # 中断：保留 running 状态，等待同补录重试恢复（不新增版本、不动旧 current）
        raise
    except Exception as exc:
        # 生成失败：标记该版本 failed，旧版 current 原样保留
        store.mark_version_state(
            version["version_id"], STATE_FAILED,
            error_note=f"{type(exc).__name__}: {exc}"[:500], updated_at=now_iso())
        raise

    # 4) 成功转正：首版直接发布为当前报告；补录版待人工核对，旧版仍 current
    is_first = version["version_no"] == 1
    final_state = STATE_PUBLISHED if is_first else STATE_PENDING
    store.mark_version_state(
        version["version_id"], final_state,
        is_current=1 if is_first else version["is_current"], updated_at=now_iso())
    version = store.get_version(version["version_id"])

    note = "新批次" if is_first else "新版本待核，旧版仍为当前报告"
    if resumed:
        note = "中断恢复完成" if is_first else "中断恢复完成，新版本待核"
    return {
        "batch": store.get_batch(batch_id),
        "replay": False,
        "note": note,
        "batch_key": batch_key,
        "counters": computed["totals"],
        "state": loaded["state"],
        "state_label": loaded["state_label"],
        "report_version": version,
        "snapshot": _snapshot_view(store, version),
        "resumed": resumed,
        "version_state": final_state,
    }


def _snapshot_view(store: Store, version: dict | None) -> dict | None:
    if version is None:
        return None
    snap = store.get_promo_snapshot(version["snapshot_id"])
    if snap is None:
        return None
    return {
        "snapshot_id": snap["snapshot_id"],
        "snapshot_digest": snap["snapshot_digest"],
        "source_name": snap["source_name"],
        "source_sha256": snap["source_sha256"],
        "row_count": snap["row_count"],
        "created_at": snap["created_at"],
    }


def approve_version(store: Store, version_id: int) -> dict:
    """核对通过：把待核版本切换为当前报告，旧当前版本归档。"""
    target = store.get_version(version_id)
    if target is None:
        raise LookupError(version_id)
    return store.publish_version(version_id, now_iso())



def build_totals(loaded, assessed, lift, all_quarantine, counts) -> dict:
    total_records = loaded["rows"]["sales"]
    rows_ok = len(assessed["sales"])
    promos_total = len(assessed["promo_versions"])
    measured = len(lift["rows"])
    pending = len(assessed["pending_promos"])
    blocked = promos_total - measured - pending
    score = assessed["score"]
    actual_gmv = sum((Decimal(r["actual_gmv"]) for r in lift["rows"]), Decimal("0"))
    base_gmv = sum((Decimal(r["base_gmv"]) for r in lift["rows"]), Decimal("0"))
    lift_gmv = sum((Decimal(r["lift_gmv"]) for r in lift["rows"]), Decimal("0"))
    return {
        "records_total": total_records,
        "rows_ok": rows_ok,
        "rows_bad": total_records - rows_ok,
        "dup_header": loaded["dup_header"]["sales"],
        "promos_total": promos_total,
        "promos_measured": measured,
        "promos_blocked": blocked,
        "promos_pending": pending,
        "days_expected": score["expected_days"],
        "days_valid": score["valid_days"],
        "days_missing": score["missing_days"],
        "days_dropped": score["dropped_days"],
        "actual_gmv": str(actual_gmv),
        "base_gmv": str(base_gmv),
        "lift_gmv": str(lift_gmv),
        "actual_qty": str(sum((Decimal(r["actual_qty"]) for r in lift["rows"]),
                              Decimal("0"))),
        "base_qty": str(sum((Decimal(r["base_qty"]) for r in lift["rows"]),
                            Decimal("0"))),
        "lift_qty": str(sum((Decimal(r["lift_qty"]) for r in lift["rows"]),
                            Decimal("0"))),
        "spread_total": str(sum((Decimal(r["lift_gmv"]) for r in lift["spread"]),
                                Decimal("0"))),
        "quarantine_total": len(all_quarantine),
        "quarantine_error": counts[reasons.ERROR],
        "quarantine_warn": counts[reasons.WARN],
        "quarantine_info": counts[reasons.INFO],
        "coverage": score["coverage"],
        "grade": score["grade"],
    }


def latest_or_none(store: Store) -> dict | None:
    return store.latest_batch()
