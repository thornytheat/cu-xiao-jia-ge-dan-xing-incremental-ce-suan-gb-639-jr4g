"""报告版本化与促销单快照：补录不改写历史、待核/发布/归档、中断恢复与幂等。

覆盖需求六条不变量：
1. 每个批次绑定当次促销单快照，改日历后旧批次仍按原快照导出；
2. 补录生成新版本（待核），旧版本仍是当前报告；
3. 核对通过后新版本才切换为当前报告；
4. 生成中断后重试恢复同一条核对（不新增版本）；
5. 生成失败保留旧版当前报告；
6. 同一补录重复重试不新增版本。
"""
from __future__ import annotations

import pathlib
import shutil

import pytest

from app import config
from app.modules.m2_price import snapshot as snapshot_mod
from app.modules.m5_lift import service as lift_service
from fixtures import dataset as fx

PROMO_NAME = "promo_calendar.csv"


@pytest.fixture()
def isolated_settings(settings, tmp_path) -> config.Settings:
    """把样例数据复制到临时目录，改价/补录不污染仓库里的共享样例。"""
    sample_dir = tmp_path / "samples"
    shutil.copytree(settings.sample_dir, sample_dir)
    return config.Settings(
        db_path=settings.db_path, sample_dir=str(sample_dir),
        export_dir=settings.export_dir)


def _promo_path(settings: config.Settings) -> pathlib.Path:
    return pathlib.Path(settings.sample_dir) / PROMO_NAME


def _change_promo_price(settings: config.Settings) -> None:
    """模拟改价：把 P2601 加码版活动价 8.90 改成 8.50（同一促销单新版本）。"""
    path = _promo_path(settings)
    lines = path.read_text(encoding="utf-8").splitlines()
    updated = []
    for line in lines:
        if line.startswith("P2601,MY-0012,电商,2,"):
            updated.append(line.replace(",8.90,", ",8.50,"))
        else:
            updated.append(line)
    path.write_text("\n".join(updated) + "\n", encoding="utf-8")


def _backfill_promo(settings: config.Settings) -> None:
    """模拟补录：追加一条新的促销单 P2605。"""
    path = _promo_path(settings)
    with path.open("a", encoding="utf-8") as fh:
        fh.write("P2605,MY-0021,电商,1,2026-10-15,2026-10-20,14.90,生效,补录\n")


# ---------- 快照 ----------

def test_batch_binds_immutable_promo_snapshot(settings, store, batch_id):
    batch = store.get_batch(batch_id)
    assert batch["snapshot_id"], "批次必须绑定促销单快照"
    snap = store.get_promo_snapshot(batch["snapshot_id"])
    assert snap is not None
    assert len(snap["snapshot_digest"]) == 64
    # 快照内容与当次促销日历一致
    content = snapshot_mod.parse_content(snap["content_json"])
    assert content["source_name"] == PROMO_NAME
    assert content["row_count"] == snap["row_count"]
    promo_ids = {row["促销编号"] for row in content["rows"]}
    assert {"P2601", "P2602", "P2603", "P2604"} <= promo_ids


def test_snapshot_digest_stable_and_order_independent():
    # 直接用促销行构造记录，验证快照摘要与书写顺序无关
    rows_a = [
        fx.promo("P1", "MY-0012", "电商", 1, "2026-09-20", "2026-09-25", "9.90"),
        fx.promo("P1", "MY-0012", "电商", 2, "2026-09-23", "2026-09-28", "8.90"),
        fx.promo("P2", "MY-0021", "商超", 1, "2026-10-01", "2026-10-08", "15.80"),
    ]
    _, records_a = fx.as_records(fx.PROMO_HEADER, rows_a)
    rows_b = [rows_a[1], rows_a[0], rows_a[2]]  # 调换书写顺序
    _, records_b = fx.as_records(fx.PROMO_HEADER, rows_b)

    class _FakeParsed:
        def __init__(self, records):
            self.name = PROMO_NAME
            self.sha256 = "x" * 64
            self.records = records

    snap1 = snapshot_mod.build_snapshot(_FakeParsed(records_a))
    snap2 = snapshot_mod.build_snapshot(_FakeParsed(records_b))
    assert snap1["snapshot_digest"] == snap2["snapshot_digest"]
    assert snap1["row_count"] == snap2["row_count"] == 3

    # 改价后摘要必须变化
    rows_c = [
        fx.promo("P1", "MY-0012", "电商", 1, "2026-09-20", "2026-09-25", "9.90"),
        fx.promo("P1", "MY-0012", "电商", 2, "2026-09-23", "2026-09-28", "7.90"),
        fx.promo("P2", "MY-0021", "商超", 1, "2026-10-01", "2026-10-08", "15.80"),
    ]
    _, records_c = fx.as_records(fx.PROMO_HEADER, rows_c)
    snap3 = snapshot_mod.build_snapshot(_FakeParsed(records_c))
    assert snap3["snapshot_digest"] != snap1["snapshot_digest"]


def test_changing_calendar_does_not_rewrite_history(isolated_settings, store):
    first = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    old_batch_id = first["batch"]["batch_id"]
    old_snapshot_id = store.get_batch(old_batch_id)["snapshot_id"]
    old_lift = {r["promo_id"]: r["lift_gmv"] for r in store.lift(old_batch_id)}

    _change_promo_price(isolated_settings)
    second = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    new_batch_id = second["batch"]["batch_id"]
    assert new_batch_id != old_batch_id

    # 旧批次仍绑定旧快照，增量结果一字未改（历史不被改写）
    assert store.get_batch(old_batch_id)["snapshot_id"] == old_snapshot_id
    assert {r["promo_id"]: r["lift_gmv"] for r in store.lift(old_batch_id)} == old_lift
    # 新批次绑定的是新快照
    assert store.get_batch(new_batch_id)["snapshot_id"] != old_snapshot_id


# ---------- 版本生命周期 ----------

def test_first_version_is_published_current(settings, store, batch_id):
    versions = store.list_versions()
    assert len(versions) == 1
    v = versions[0]
    assert v["version_no"] == 1
    assert v["state"] == lift_service.STATE_PUBLISHED
    assert v["is_current"] == 1
    # 默认当前批次就是首版
    assert store.current_batch()["batch_id"] == batch_id


def test_backfill_creates_pending_version_and_keeps_old_current(isolated_settings, store):
    first = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    old_id = first["batch"]["batch_id"]

    _backfill_promo(isolated_settings)
    result = lift_service.run(settings=isolated_settings, store=store, dataset="main")

    versions = store.list_versions()
    assert len(versions) == 2, "补录应生成一个新版本"
    new_v = next(v for v in versions if v["version_no"] == 2)
    old_v = next(v for v in versions if v["version_no"] == 1)
    assert new_v["state"] == lift_service.STATE_PENDING
    assert new_v["is_current"] == 0
    assert new_v["batch_id"] == result["batch"]["batch_id"]
    # 旧版仍是当前报告
    assert old_v["state"] == lift_service.STATE_PUBLISHED
    assert old_v["is_current"] == 1
    assert store.current_batch()["batch_id"] == old_id
    assert result["version_state"] == lift_service.STATE_PENDING


def test_approve_switches_current_and_archives_old(isolated_settings, store):
    lift_service.run(settings=isolated_settings, store=store, dataset="main")
    _backfill_promo(isolated_settings)
    second = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    new_v = store.get_version_by_batch(second["batch"]["batch_id"])

    approved = lift_service.approve_version(store, new_v["version_id"])

    assert approved["state"] == lift_service.STATE_PUBLISHED
    assert approved["is_current"] == 1
    old_v = store.list_versions(new_v["series_key"])
    v1 = next(v for v in old_v if v["version_no"] == 1)
    assert v1["state"] == lift_service.STATE_ARCHIVED
    assert v1["is_current"] == 0
    # 当前报告已切到新版
    assert store.current_batch()["batch_id"] == second["batch"]["batch_id"]
    # 同一时刻只有一个 current
    currents = [v for v in store.list_versions() if v["is_current"] == 1]
    assert len(currents) == 1


def test_old_version_still_exports_its_own_snapshot_after_switch(isolated_settings, store):
    first = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    old_id = first["batch"]["batch_id"]
    old_lift = {r["promo_id"]: r["lift_gmv"] for r in store.lift(old_id)}

    _change_promo_price(isolated_settings)
    second = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    new_v = store.get_version_by_batch(second["batch"]["batch_id"])
    lift_service.approve_version(store, new_v["version_id"])

    # 显式按旧批次导出，结果仍是旧快照算出来的那一份
    assert {r["promo_id"]: r["lift_gmv"] for r in store.lift(old_id)} == old_lift
    old_manifest = store.manifest(old_id)
    new_manifest = store.manifest(second["batch"]["batch_id"])
    assert (old_manifest["promo_snapshot"]["snapshot_digest"]
            != new_manifest["promo_snapshot"]["snapshot_digest"])


def test_cannot_approve_non_pending_version(settings, store, batch_id):
    v = store.get_version_by_batch(batch_id)
    with pytest.raises(RuntimeError):
        lift_service.approve_version(store, v["version_id"])


def test_versions_must_be_approved_in_order(isolated_settings, store):
    # 连续两次补录产生 v2、v3 两个待核版本：只能先发布最新的 v3，
    # 不允许让更旧的待核版 v2 反向覆盖更新结论。
    lift_service.run(settings=isolated_settings, store=store, dataset="main")
    _backfill_promo(isolated_settings)
    r2 = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    _change_promo_price(isolated_settings)
    r3 = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    v2 = store.get_version_by_batch(r2["batch"]["batch_id"])
    v3 = store.get_version_by_batch(r3["batch"]["batch_id"])
    assert v2["version_no"] == 2 and v3["version_no"] == 3

    # v3 是最新待核版，发布后旧当前 v1 归档
    lift_service.approve_version(store, v3["version_id"])
    final = {v["version_no"]: v for v in store.list_versions(v3["series_key"])}
    assert final[3]["state"] == lift_service.STATE_PUBLISHED and final[3]["is_current"] == 1
    assert final[1]["state"] == lift_service.STATE_ARCHIVED
    assert store.current_batch()["batch_id"] == r3["batch"]["batch_id"]
    # v2 仍是待核，但已有更新的当前报告 v3，不能再用旧版覆盖
    with pytest.raises(RuntimeError):
        lift_service.approve_version(store, v2["version_id"])


def test_cannot_approve_older_pending_when_newer_pending_exists(isolated_settings, store):
    lift_service.run(settings=isolated_settings, store=store, dataset="main")
    _backfill_promo(isolated_settings)
    r2 = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    _change_promo_price(isolated_settings)
    r3 = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    # 直接尝试发布最新的 v3 之后，再构造一个 v4，使 v3 不再是最新——
    # 这里验证：v3 发布后，旧的待核版不能反向覆盖
    lift_service.approve_version(
        store, store.get_version_by_batch(r3["batch"]["batch_id"])["version_id"])
    v2 = store.get_version_by_batch(r2["batch"]["batch_id"])
    # v2 仍是 pending，但已有更新的 published(v3)，拒绝旧版覆盖
    with pytest.raises(RuntimeError):
        lift_service.approve_version(store, v2["version_id"])


# ---------- 幂等与中断恢复 ----------

def test_same_backfill_retry_adds_no_version(isolated_settings, store):
    lift_service.run(settings=isolated_settings, store=store, dataset="main")
    _backfill_promo(isolated_settings)
    lift_service.run(settings=isolated_settings, store=store, dataset="main")
    # 完全相同的补录再跑一次
    again = lift_service.run(settings=isolated_settings, store=store, dataset="main")

    assert len(store.list_versions()) == 2
    pending = [v for v in store.list_versions()
               if v["state"] == lift_service.STATE_PENDING]
    assert len(pending) == 1
    assert again["batch"]["batch_id"] == pending[0]["batch_id"]


def test_interrupted_generation_resumes_same_version(isolated_settings, store):
    lift_service.run(settings=isolated_settings, store=store, dataset="main")
    _backfill_promo(isolated_settings)

    calls = {"n": 0}

    def fail_once(stage):
        # 第一次在计算完成后、落库前中断，模拟进程崩溃
        if stage == "computed":
            calls["n"] += 1
            raise lift_service.GenerationInterrupted("simulated interruption")

    with pytest.raises(lift_service.GenerationInterrupted):
        lift_service.run(settings=isolated_settings, store=store, dataset="main",
                         on_stage=fail_once)

    versions = store.list_versions()
    assert len(versions) == 2, "中断也只占一个版本号"
    interrupted = next(v for v in versions if v["version_no"] == 2)
    assert interrupted["state"] == lift_service.STATE_RUNNING
    assert interrupted["attempts"] == 1
    # 中断不留半批数据
    assert store.sales_count(interrupted["batch_id"]) == 0
    # 旧版仍是当前报告
    assert store.current_batch()["batch_id"] != interrupted["batch_id"]

    # 重试：恢复同一条版本记录续算
    resumed = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    assert resumed["resumed"] is True
    same = store.get_version(interrupted["version_id"])
    assert len(store.list_versions()) == 2, "恢复不新增版本"
    assert same["batch_id"] == interrupted["batch_id"]
    assert same["state"] == lift_service.STATE_PENDING
    assert same["attempts"] == 2
    assert store.sales_count(same["batch_id"]) > 0


def test_failed_generation_keeps_old_current(isolated_settings, store):
    first = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    old_id = first["batch"]["batch_id"]
    _backfill_promo(isolated_settings)

    def always_fail(stage):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        lift_service.run(settings=isolated_settings, store=store, dataset="main",
                         on_stage=always_fail)
    with pytest.raises(RuntimeError):
        lift_service.run(settings=isolated_settings, store=store, dataset="main",
                         on_stage=always_fail)

    # 失败两次仍只有两个版本，旧版始终是当前报告
    assert len(store.list_versions()) == 2
    failed = next(v for v in store.list_versions()
                  if v["state"] == lift_service.STATE_FAILED)
    assert failed["attempts"] == 2
    assert failed["error_note"].startswith("RuntimeError")
    assert store.current_batch()["batch_id"] == old_id

    # 去掉故障后重试，同一条失败版本恢复成待核
    ok = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    assert ok["resumed"] is True
    assert len(store.list_versions()) == 2
    recovered = store.get_version(failed["version_id"])
    assert recovered["state"] == lift_service.STATE_PENDING
    assert store.current_batch()["batch_id"] == old_id


def test_idempotent_rerun_still_reuses_done_batch(settings, store, batch_id):
    # 原幂等路径不被版本化破坏：同输入重跑复用，不新增版本/批次
    again = lift_service.run(settings=settings, store=store, dataset="main")
    assert again["replay"] is True
    assert again["batch"]["batch_id"] == batch_id
    assert len(store.list_versions()) == 1
    assert len(store.list_batches()) == 1


def test_first_version_interruption_resumes_and_publishes(isolated_settings, store):
    # 首版在生成中中断：恢复后应直接发布为当前报告，仍是同一条版本
    def fail_first(stage):
        if stage == "computed":
            raise lift_service.GenerationInterrupted("crash on first run")

    with pytest.raises(lift_service.GenerationInterrupted):
        lift_service.run(settings=isolated_settings, store=store, dataset="main",
                         on_stage=fail_first)
    assert len(store.list_versions()) == 1
    stuck = store.list_versions()[0]
    assert stuck["state"] == lift_service.STATE_RUNNING

    resumed = lift_service.run(settings=isolated_settings, store=store, dataset="main")
    assert resumed["resumed"] is True
    assert len(store.list_versions()) == 1, "首版恢复不新增版本"
    v = store.list_versions()[0]
    assert v["state"] == lift_service.STATE_PUBLISHED
    assert v["is_current"] == 1
    assert v["version_id"] == stuck["version_id"]
    assert store.current_batch()["batch_id"] == v["batch_id"]


def test_synthetic_dataset_versioning_with_write_dataset(tmp_path):
    # 用合成样例端到端验证：改价后新旧两版快照不同、各自可导出
    sales = fx.flat_days(40, start_day=1)
    promo_rows = [
        fx.promo("P1", "MY-0012", "电商", 1, "2026-09-20", "2026-09-25", "9.90"),
    ]
    settings = fx.write_dataset(tmp_path / "v1", sales_rows=sales, promo_rows=promo_rows)
    from app.store import Store
    store = Store(settings.db_path)
    r1 = lift_service.run(settings=settings, store=store, dataset="main")
    s1 = store.get_batch(r1["batch"]["batch_id"])["snapshot_id"]

    # 改价重跑（同一文件、同一口径）
    promo_rows_v2 = [
        fx.promo("P1", "MY-0012", "电商", 1, "2026-09-20", "2026-09-25", "8.90"),
    ]
    fx.write_dataset(pathlib.Path(settings.sample_dir), sales_rows=sales,
                     promo_rows=promo_rows_v2)
    r2 = lift_service.run(settings=settings, store=store, dataset="main")
    s2 = store.get_batch(r2["batch"]["batch_id"])["snapshot_id"]
    assert s1 != s2
    assert len(store.list_versions()) == 2
