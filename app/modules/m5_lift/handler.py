"""M5 入口：增量测算触发、渠道分摊、批次/报告版本历史与增量页。"""
from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from ... import errors, web
from . import service

router = APIRouter()


class LiftIn(BaseModel):
    dataset: str = Field("main", description="样例集键名")
    zero_mode: str = Field("", description="0 销量处理方式：shift / drop")


def _batch_id(request: Request) -> str | None:
    ctx = web.get_ctx(request)
    batch = ctx.current_batch(request.query_params.get("batch_id"))
    return batch["batch_id"] if batch else None


@router.post("/api/lift")
def api_lift(body: LiftIn, request: Request) -> dict:
    settings = request.app.state.settings
    store = request.app.state.store
    result = service.run(settings=settings, store=store,
                         dataset=body.dataset or "main",
                         zero_mode=body.zero_mode or None)
    batch_id = result["batch"]["batch_id"]
    return {
        "status": result["batch"]["status"],
        "batch_id": batch_id,
        "run_id": result["batch"]["run_id"],
        "batch_key": result["batch_key"],
        "replay": result["replay"],
        "counters": result["counters"],
        "lift": store.lift(batch_id),
        "spread": store.spread(batch_id),
        "note": result["note"],
        "report_version": result.get("report_version"),
        "version_state": result.get("version_state"),
        "snapshot": result.get("snapshot"),
        "resumed": result.get("resumed", False),
    }


@router.get("/api/lift")
def api_lift_state(request: Request) -> dict:
    ctx = web.get_ctx(request)
    batch_id = _batch_id(request)
    if batch_id is None:
        return {"batch_id": None, "lift": [], "spread": [], "totals": {}}
    return {
        "batch_id": batch_id,
        "lift": ctx.store.lift(batch_id),
        "spread": ctx.store.spread(batch_id),
        "totals": ctx.store.totals(batch_id),
        "report_version": ctx.store.get_version_by_batch(batch_id),
    }


@router.get("/api/batches")
def api_batches(request: Request) -> dict:
    ctx = web.get_ctx(request)
    batches = ctx.store.list_batches(30)
    # 给每个批次附上报告版本与促销快照，页面可区分当前/待核/历史
    enriched = []
    for batch in batches:
        version = ctx.store.get_version_by_batch(batch["batch_id"])
        item = dict(batch)
        item["report_version"] = version
        item["snapshot_id"] = (version or {}).get("snapshot_id", "")
        item["version_state"] = (version or {}).get("state", "")
        enriched.append(item)
    return {
        "count": len(batches),
        "batches": enriched,
        "keys": [b["batch_key"][:12] for b in batches],
    }


@router.get("/api/report-versions")
def api_report_versions(request: Request) -> dict:
    """报告版本清单：按业务口径分组，含当前指针、待核/失败计数与绑定快照。"""
    ctx = web.get_ctx(request)
    series_key = request.query_params.get("series_key")
    versions = ctx.store.list_versions(series_key)
    out = []
    for version in versions:
        item = dict(version)
        snap = ctx.store.get_promo_snapshot(version["snapshot_id"])
        item["snapshot"] = {
            "snapshot_digest": snap["snapshot_digest"],
            "source_name": snap["source_name"],
            "source_sha256": snap["source_sha256"],
            "row_count": snap["row_count"],
        } if snap else None
        item["batch"] = ctx.store.get_batch(version["batch_id"])
        out.append(item)
    return {"count": len(out), "versions": out, "series": ctx.store.list_series()}


@router.post("/api/report-versions/{version_id}/approve")
def api_approve_version(version_id: int, request: Request) -> dict:
    """核对通过：待核版本切换为当前报告，旧当前版本归档。"""
    ctx = web.get_ctx(request)
    if ctx.store.get_version(version_id) is None:
        raise errors.NotFound(f"报告版本 {version_id} 不存在")
    try:
        version = service.approve_version(ctx.store, version_id)
    except LookupError:
        raise errors.NotFound(f"报告版本 {version_id} 不存在")
    except RuntimeError as exc:
        raise errors.Conflict(str(exc))
    return {
        "status": "ok",
        "report_version": version,
        "batch_id": version["batch_id"],
        "note": "已切换为当前报告，旧版本按原快照保留",
    }


@router.get("/lift")
def page_lift(request: Request):
    ctx = web.get_ctx(request)
    batch_id = _batch_id(request)
    lift_rows = ctx.store.lift(batch_id) if batch_id else []
    spread_rows = ctx.store.spread(batch_id) if batch_id else []
    totals = ctx.store.totals(batch_id) if batch_id else {}
    grouped: dict[str, list[dict]] = {}
    for row in spread_rows:
        grouped.setdefault(row["promo_id"], []).append(row)
    batches = []
    for item in ctx.store.list_batches(12):
        enriched = dict(item)
        version = ctx.store.get_version_by_batch(item["batch_id"])
        enriched["report_version"] = version
        enriched["snapshot_id"] = (version or {}).get("snapshot_id", "")
        batches.append(enriched)
    return web.render(
        request, "lift.html",
        lift=lift_rows,
        spread=grouped,
        totals=totals,
        batches=batches,
        report_versions=ctx.store.list_versions(),
    )
