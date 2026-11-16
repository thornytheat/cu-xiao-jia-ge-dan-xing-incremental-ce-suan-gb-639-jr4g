"""报告版本化的 HTTP 流程：导入 → 补录待核 → 页面展示 → 核对切换 → 旧版导出。"""
from __future__ import annotations

import pathlib
import shutil

import pytest

from app import config


@pytest.fixture()
def isolated_client(client, settings, tmp_path) -> object:
    """客户端指向复制出来的样例目录，改价/补录不污染共享样例。"""
    sample_dir = tmp_path / "samples"
    shutil.copytree(settings.sample_dir, sample_dir)
    isolated = config.Settings(
        db_path=settings.db_path, sample_dir=str(sample_dir),
        export_dir=settings.export_dir)
    client.app.state.settings = isolated
    client.app.state.ctx.settings = isolated
    return client


def _backfill_promo(settings: config.Settings) -> None:
    path = pathlib.Path(settings.sample_dir) / "promo_calendar.csv"
    with path.open("a", encoding="utf-8") as fh:
        fh.write("P2605,MY-0021,电商,1,2026-10-15,2026-10-20,14.90,生效,补录\n")


def _change_price(settings: config.Settings) -> None:
    path = pathlib.Path(settings.sample_dir) / "promo_calendar.csv"
    text = path.read_text(encoding="utf-8").replace(
        "P2601,MY-0012,电商,2,2026-10-12,2026-10-25,8.90,",
        "P2601,MY-0012,电商,2,2026-10-12,2026-10-25,8.50,")
    path.write_text(text, encoding="utf-8")


def test_import_response_carries_snapshot_and_version(isolated_client):
    body = isolated_client.post("/api/import", json={"dataset": "main"}).json()
    assert body["report_version"]["version_no"] == 1
    assert body["report_version"]["state"] == "published"
    assert body["version_state"] == "published"
    assert len(body["snapshot"]["snapshot_digest"]) == 64
    assert body["snapshot"]["row_count"] >= 1


def test_backfill_flow_pending_then_approve(isolated_client):
    settings = isolated_client.app.state.settings
    first = isolated_client.post("/api/import", json={"dataset": "main"}).json()
    old_batch = first["batch_id"]

    _backfill_promo(settings)
    second = isolated_client.post("/api/import", json={"dataset": "main"}).json()
    assert second["replay"] is False
    assert second["batch_id"] != old_batch
    assert second["version_state"] == "pending"

    # 待核期间默认报告仍是旧版
    report = isolated_client.get("/api/report").json()
    assert report["batch_id"] == old_batch
    assert report["report_version"]["state"] == "published"

    versions = isolated_client.get("/api/report-versions").json()
    assert versions["count"] == 2
    pending = next(v for v in versions["versions"] if v["state"] == "pending")
    assert pending["snapshot"]["row_count"] >= 1

    # 同一补录重试不新增版本
    third = isolated_client.post("/api/import", json={"dataset": "main"}).json()
    assert third["batch_id"] == second["batch_id"]
    assert isolated_client.get("/api/report-versions").json()["count"] == 2

    # 核对通过，切换当前报告
    approved = isolated_client.post(
        f"/api/report-versions/{pending['version_id']}/approve").json()
    assert approved["report_version"]["state"] == "published"
    report_after = isolated_client.get("/api/report").json()
    assert report_after["batch_id"] == second["batch_id"]

    # 旧版仍可按原快照显式导出（清单里带各自的促销快照摘要）
    old_export = isolated_client.get(
        "/api/export",
        params={"kind": "manifest", "fmt": "json", "batch_id": old_batch})
    assert old_export.status_code == 200
    import json as _json
    old_manifest = _json.loads(old_export.text)
    new_manifest = _json.loads(isolated_client.get(
        "/api/export",
        params={"kind": "manifest", "fmt": "json",
                "batch_id": second["batch_id"]}).text)
    assert (old_manifest["promo_snapshot"]["snapshot_digest"]
            != new_manifest["promo_snapshot"]["snapshot_digest"])
    assert old_manifest["batch_id"] == old_batch
    assert new_manifest["batch_id"] == second["batch_id"]


def test_approve_unknown_version_is_404(isolated_client):
    isolated_client.post("/api/import", json={"dataset": "main"})
    resp = isolated_client.post("/api/report-versions/9999/approve")
    assert resp.status_code == 404
    assert resp.json()["error"] == "E_NOT_FOUND"


def test_approve_published_version_is_conflict(isolated_client):
    isolated_client.post("/api/import", json={"dataset": "main"})
    versions = isolated_client.get("/api/report-versions").json()["versions"]
    published = versions[0]["version_id"]
    resp = isolated_client.post(f"/api/report-versions/{published}/approve")
    assert resp.status_code == 409
    assert resp.json()["error"] == "E_CONFLICT"


def test_older_pending_cannot_override_newer_report(isolated_client):
    settings = isolated_client.app.state.settings
    isolated_client.post("/api/import", json={"dataset": "main"})
    _backfill_promo(settings)
    second = isolated_client.post("/api/import", json={"dataset": "main"}).json()
    _change_price(settings)
    third = isolated_client.post("/api/import", json={"dataset": "main"}).json()
    versions = {v["version_no"]: v for v in
                isolated_client.get("/api/report-versions").json()["versions"]}
    # 先发布最新的 v3
    ok = isolated_client.post(
        f"/api/report-versions/{versions[3]['version_id']}/approve")
    assert ok.status_code == 200
    # 再发布更旧的待核 v2 被 409 拒绝
    stale = isolated_client.post(
        f"/api/report-versions/{versions[2]['version_id']}/approve")
    assert stale.status_code == 409
    # 当前报告仍是 v3
    assert isolated_client.get("/api/report").json()["batch_id"] == third["batch_id"]
    assert second["batch_id"] != third["batch_id"]


def test_report_page_renders_version_section(isolated_client):
    settings = isolated_client.app.state.settings
    isolated_client.post("/api/import", json={"dataset": "main"})
    _change_price(settings)
    isolated_client.post("/api/import", json={"dataset": "main"})

    page = isolated_client.get("/report")
    assert page.status_code == 200
    assert "报告版本与促销单快照".encode() in page.content
    assert "待核对".encode() in page.content
    assert "核对通过，切换为当前报告".encode() in page.content

    lift_page = isolated_client.get("/lift")
    assert lift_page.status_code == 200
    assert "批次与报告版本".encode() in lift_page.content

    index_page = isolated_client.get("/")
    assert index_page.status_code == 200
    assert "报告版本".encode() in index_page.content


def test_batches_endpoint_enriches_version_state(isolated_client):
    isolated_client.post("/api/import", json={"dataset": "main"})
    payload = isolated_client.get("/api/batches").json()
    item = payload["batches"][0]
    assert item["report_version"]["state"] == "published"
    assert item["snapshot_id"]
