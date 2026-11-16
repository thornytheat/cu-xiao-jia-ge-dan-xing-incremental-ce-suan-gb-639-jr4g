"""SQLite 存储：批次、输入摘要、日销行、促销版本、隔离行、弹性、增量、分摊、阶段、合计、清单。

约定：
- 金额列一律 TEXT（Decimal 字符串），不落 float，避免 2^53 与二进制误差；
- 一次运行的全部行在同一个事务里「先删后插」，失败整体回滚，不留半批数据；
- 读数按 batch_id 过滤，页面永远只展示一个批次。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    batch_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    input_digest TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    note TEXT NOT NULL DEFAULT '',
    snapshot_id TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_batches_key ON batches(batch_key, status);

CREATE TABLE IF NOT EXISTS sources (
    batch_id TEXT NOT NULL, name TEXT NOT NULL, role TEXT NOT NULL,
    encoding TEXT, declared TEXT, bom_stripped INTEGER NOT NULL DEFAULT 0,
    bytes INTEGER NOT NULL DEFAULT 0, sha256 TEXT NOT NULL,
    records INTEGER NOT NULL DEFAULT 0, rows INTEGER NOT NULL DEFAULT 0,
    code TEXT, note TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sales (
    batch_id TEXT NOT NULL, line_no INTEGER NOT NULL, sale_date TEXT NOT NULL,
    channel TEXT NOT NULL, sku TEXT NOT NULL, sku_name TEXT NOT NULL,
    price_incl TEXT, price_unit TEXT, tax_rate TEXT, price_excl TEXT,
    qty INTEGER NOT NULL, promo_id TEXT NOT NULL DEFAULT '',
    amount_excl TEXT NOT NULL DEFAULT '0',
    raw_json TEXT NOT NULL DEFAULT '{}', flags_json TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_sales_batch ON sales(batch_id, sku, channel, sale_date);

CREATE TABLE IF NOT EXISTS promos (
    batch_id TEXT NOT NULL, promo_id TEXT NOT NULL, sku TEXT NOT NULL, channel TEXT NOT NULL,
    version INTEGER NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
    activity_price TEXT, status TEXT NOT NULL, effective INTEGER NOT NULL DEFAULT 0,
    overlap INTEGER NOT NULL DEFAULT 0, note TEXT NOT NULL DEFAULT '', row_no INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS promo_windows (
    batch_id TEXT NOT NULL, promo_id TEXT NOT NULL, sku TEXT NOT NULL, channel TEXT NOT NULL,
    start_date TEXT NOT NULL, end_date TEXT NOT NULL, version INTEGER NOT NULL,
    price_incl TEXT NOT NULL, price_excl TEXT NOT NULL, days INTEGER NOT NULL,
    base_price_excl TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    batch_id TEXT NOT NULL, code TEXT NOT NULL, stage TEXT NOT NULL, level TEXT NOT NULL,
    source TEXT, line_no INTEGER, field TEXT, raw_value TEXT, detail TEXT
);

CREATE TABLE IF NOT EXISTS elasticity (
    batch_id TEXT NOT NULL, scope TEXT NOT NULL, group_key TEXT NOT NULL,
    n INTEGER NOT NULL, zero_count INTEGER NOT NULL, intercept TEXT NOT NULL,
    slope TEXT NOT NULL, se TEXT NOT NULL, tvalue TEXT NOT NULL, r2 TEXT NOT NULL,
    zero_mode TEXT NOT NULL, significant INTEGER NOT NULL, note TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS lift (
    batch_id TEXT NOT NULL, promo_id TEXT NOT NULL, sku TEXT NOT NULL, channel TEXT NOT NULL,
    start_date TEXT NOT NULL, end_date TEXT NOT NULL, days INTEGER NOT NULL,
    days_valid INTEGER NOT NULL, days_missing INTEGER NOT NULL, days_dropped INTEGER NOT NULL,
    actual_qty TEXT NOT NULL, base_qty TEXT NOT NULL, actual_gmv TEXT NOT NULL,
    base_gmv TEXT NOT NULL, lift_qty TEXT NOT NULL, lift_gmv TEXT NOT NULL,
    se_gmv TEXT NOT NULL, ci_low TEXT NOT NULL, ci_high TEXT NOT NULL,
    price_excl TEXT NOT NULL, base_price_excl TEXT NOT NULL,
    control_sku TEXT NOT NULL, control_factor TEXT NOT NULL, dlnp TEXT NOT NULL,
    slope TEXT NOT NULL, note TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS spread (
    batch_id TEXT NOT NULL, promo_id TEXT NOT NULL, channel TEXT NOT NULL,
    ratio TEXT NOT NULL, lift_gmv TEXT NOT NULL, actual_gmv TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stages (
    batch_id TEXT NOT NULL, stage TEXT NOT NULL, in_count INTEGER NOT NULL,
    out_count INTEGER NOT NULL, quarantine_count INTEGER NOT NULL,
    status TEXT NOT NULL, elapsed_ms INTEGER NOT NULL, note TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS totals (
    batch_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS manifests (
    batch_id TEXT PRIMARY KEY, json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS promo_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    snapshot_digest TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    content_json TEXT NOT NULL
);

-- 报告版本：同一业务口径（样例集 + 0 销量处理 + 规则版本）下，促销单补录/改价
-- 生成新版本供核对；旧版本保留原快照与原批次，切换只动 current 指针。
CREATE TABLE IF NOT EXISTS report_versions (
    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_key TEXT NOT NULL,
    version_no INTEGER NOT NULL,
    batch_id TEXT NOT NULL UNIQUE,
    snapshot_id TEXT NOT NULL,
    idem_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    is_current INTEGER NOT NULL DEFAULT 0,
    change_note TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 1,
    error_note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (series_key, version_no)
);
CREATE INDEX IF NOT EXISTS idx_report_series ON report_versions(series_key, state);
CREATE INDEX IF NOT EXISTS idx_report_current ON report_versions(is_current, state);
"""

# 旧库补列：新库建表时直接带上 snapshot_id；老库通过 ALTER 幂等补齐。
_LEGACY_MIGRATIONS = (
    "ALTER TABLE batches ADD COLUMN snapshot_id TEXT NOT NULL DEFAULT ''",
)

BATCH_TABLES = (
    "sources", "sales", "promos", "promo_windows", "quarantine",
    "elasticity", "lift", "spread", "stages", "totals", "manifests",
)


class Store:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._migrate(conn)

    def _migrate(self, conn) -> None:
        """老库幂等补列：列已存在时忽略 OperationalError。"""
        existing = {row[1] for row in conn.execute("PRAGMA table_info(batches)")}
        for statement in _LEGACY_MIGRATIONS:
            column = statement.split("ADD COLUMN", 1)[1].strip().split()[0]
            if column not in existing:
                conn.execute(statement)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------- 批次 ----------
    def upsert_batch(self, batch: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO batches (batch_id, batch_key, run_id, dataset, status,
                        created_at, finished_at, input_digest, params_json, note,
                        snapshot_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     input_digest=excluded.input_digest, note=excluded.note,
                     params_json=excluded.params_json,
                     snapshot_id=excluded.snapshot_id""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"], batch["dataset"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"],
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 batch.get("note", ""), batch.get("snapshot_id", "")),
            )

    def find_done_by_key(self, batch_key: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_key=? AND status='done' "
                "ORDER BY created_at LIMIT 1", (batch_key,)).fetchone()
        return dict(row) if row else None

    def get_batch(self, batch_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM batches WHERE batch_id=?",
                               (batch_id,)).fetchone()
        return dict(row) if row else None

    def latest_batch(self) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM batches ORDER BY created_at DESC, rowid DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def current_batch(self) -> dict | None:
        """当前报告批次：优先取报告版本表标记 current 的已发布批次；
        没有版本记录的老数据回退到最新批次。这样补录生成待核版本时，
        页面与导出默认仍展示旧版（当前报告），核对通过切换后才展示新版。"""
        with self.connect() as conn:
            row = conn.execute(
                """SELECT b.* FROM batches b
                   JOIN report_versions v ON v.batch_id = b.batch_id
                   WHERE v.is_current = 1 AND v.state = 'published'
                   ORDER BY b.created_at DESC, b.rowid DESC LIMIT 1""").fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT * FROM batches ORDER BY created_at DESC, rowid DESC LIMIT 1"
                ).fetchone()
        return dict(row) if row else None

    def list_batches(self, limit: int = 30) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM batches ORDER BY created_at DESC, rowid DESC LIMIT ?",
                (limit,)).fetchall()
        return [dict(r) for r in rows]

    # ---------- 促销单快照 ----------
    def upsert_promo_snapshot(self, snapshot: dict, created_at: str) -> str:
        """按摘要幂等写入快照，返回 snapshot_id（同一促销单重试不新增）。"""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO promo_snapshots (snapshot_id, snapshot_digest, source_name,
                        source_sha256, row_count, created_at, content_json)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(snapshot_digest) DO UPDATE SET
                     source_sha256=excluded.source_sha256,
                     row_count=excluded.row_count""",
                (snapshot["snapshot_digest"], snapshot["snapshot_digest"],
                 snapshot["source_name"], snapshot["source_sha256"],
                 snapshot["row_count"], created_at, snapshot["content_json"]))
        return snapshot["snapshot_digest"]

    def get_promo_snapshot(self, snapshot_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM promo_snapshots WHERE snapshot_id=?",
                (snapshot_id,)).fetchone()
        return dict(row) if row else None

    # ---------- 报告版本 ----------
    def get_version_by_idem(self, idem_key: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM report_versions WHERE idem_key=?",
                (idem_key,)).fetchone()
        return dict(row) if row else None

    def get_version_by_batch(self, batch_id: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM report_versions WHERE batch_id=?",
                (batch_id,)).fetchone()
        return dict(row) if row else None

    def get_version(self, version_id: int) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM report_versions WHERE version_id=?",
                (version_id,)).fetchone()
        return dict(row) if row else None

    def list_versions(self, series_key: str | None = None) -> list[dict]:
        with self.connect() as conn:
            if series_key is None:
                rows = conn.execute(
                    "SELECT * FROM report_versions ORDER BY created_at DESC, version_id DESC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM report_versions WHERE series_key=? "
                    "ORDER BY version_no DESC", (series_key,)).fetchall()
        return [dict(r) for r in rows]

    def list_series(self) -> list[dict]:
        """每个业务口径一行：当前版本号、最新版本状态与绑定的快照。"""
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT series_key,
                          MAX(version_no) AS latest_version_no,
                          SUM(CASE WHEN is_current=1 THEN 1 ELSE 0 END) AS has_current,
                          SUM(CASE WHEN state='pending' THEN 1 ELSE 0 END) AS pending_count,
                          SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) AS failed_count,
                          MAX(updated_at) AS updated_at
                   FROM report_versions GROUP BY series_key
                   ORDER BY updated_at DESC""").fetchall()
        return [dict(r) for r in rows]

    def next_version_no(self, conn, series_key: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(version_no), 0) AS m FROM report_versions "
            "WHERE series_key=?", (series_key,)).fetchone()
        return int(row["m"]) + 1

    def insert_report_version(self, version: dict) -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """INSERT INTO report_versions (series_key, version_no, batch_id,
                        snapshot_id, idem_key, state, is_current, change_note,
                        attempts, error_note, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (version["series_key"], version["version_no"], version["batch_id"],
                 version["snapshot_id"], version["idem_key"], version["state"],
                 int(version.get("is_current", 0)), version.get("change_note", ""),
                 int(version.get("attempts", 1)), version.get("error_note", ""),
                 version["created_at"], version["updated_at"]))
            return int(cur.lastrowid)

    def mark_version_state(self, version_id: int, state: str, *,
                           is_current: int | None = None, error_note: str | None = None,
                           bump_attempts: bool = False, updated_at: str = "") -> None:
        sets = ["state=?", "updated_at=?"]
        args: list = [state, updated_at]
        if is_current is not None:
            sets.append("is_current=?")
            args.append(int(is_current))
        if error_note is not None:
            sets.append("error_note=?")
            args.append(error_note)
        if bump_attempts:
            sets.append("attempts=attempts+1")
        args.append(version_id)
        with self.connect() as conn:
            conn.execute(
                f"UPDATE report_versions SET {', '.join(sets)} WHERE version_id=?",
                args)

    def publish_version(self, version_id: int, updated_at: str) -> dict:
        """核对通过：原子切换当前报告。旧 current 置 0 并归档，目标版本发布。
        任何一步失败整体回滚，保证同一时刻至多一个 current。

        版本必须按号顺序发布：同系列里若已存在更新的待核/已发布版本，
        拒绝把旧版本切成当前报告，避免旧报告反向覆盖更新的结论。
        """
        with self.connect() as conn:
            target = conn.execute(
                "SELECT * FROM report_versions WHERE version_id=?",
                (version_id,)).fetchone()
            if target is None:
                raise LookupError(version_id)
            if target["state"] != "pending":
                raise RuntimeError(f"版本状态为 {target['state']}，不能切换为当前报告")
            newer = conn.execute(
                "SELECT COUNT(*) AS c FROM report_versions "
                "WHERE series_key=? AND version_no>? AND state IN ('pending','published')",
                (target["series_key"], target["version_no"])).fetchone()
            if newer["c"] > 0:
                raise RuntimeError(
                    f"存在更新的报告版本（v{target['version_no']} 之后仍有待核/当前版本），"
                    "请按版本号顺序核对，不能用旧版覆盖新版")
            conn.execute(
                "UPDATE report_versions SET is_current=0, state='archived', updated_at=? "
                "WHERE series_key=? AND is_current=1",
                (updated_at, target["series_key"]))
            conn.execute(
                "UPDATE report_versions SET is_current=1, state='published', "
                "error_note='', updated_at=? WHERE version_id=?",
                (updated_at, version_id))
            row = conn.execute(
                "SELECT * FROM report_versions WHERE version_id=?",
                (version_id,)).fetchone()
            return dict(row)

    def current_version_of_series(self, series_key: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM report_versions WHERE series_key=? AND is_current=1",
                (series_key,)).fetchone()
        return dict(row) if row else None


    def clear_batch_rows(self, batch_id: str, conn) -> None:
        for table in BATCH_TABLES:
            conn.execute(f"DELETE FROM {table} WHERE batch_id=?", (batch_id,))

    # ---------- 写入（一次事务） ----------
    def write_batch(self, batch_id: str, payload: dict) -> None:
        """payload 的每个键对应一张表；整批一次事务，失败回滚不留半批。"""
        with self.connect() as conn:
            self.clear_batch_rows(batch_id, conn)
            for row in payload.get("sources", []):
                conn.execute(
                    """INSERT INTO sources (batch_id, name, role, encoding, declared,
                            bom_stripped, bytes, sha256, records, rows, code, note)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, row["name"], row["role"], row.get("encoding"),
                     row.get("declared"), int(row.get("bom_stripped", 0)),
                     row.get("bytes", 0), row["sha256"], row.get("records", 0),
                     row.get("rows", 0), row.get("code"), row.get("note", "")))
            for row in payload.get("sales", []):
                conn.execute(
                    """INSERT INTO sales (batch_id, line_no, sale_date, channel, sku,
                            sku_name, price_incl, price_unit, tax_rate, price_excl, qty,
                            promo_id, amount_excl, raw_json, flags_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, row["line_no"], row["sale_date"], row["channel"], row["sku"],
                     row["sku_name"], row.get("price_incl"), row.get("price_unit"),
                     row.get("tax_rate"), row.get("price_excl"), row["qty"],
                     row.get("promo_id", ""), row.get("amount_excl", "0"),
                     row.get("raw_json", "{}"),
                     json.dumps(row.get("flags", []), ensure_ascii=False)))
            for row in payload.get("promos", []):
                conn.execute(
                    """INSERT INTO promos (batch_id, promo_id, sku, channel, version,
                            start_date, end_date, activity_price, status, effective,
                            overlap, note, row_no)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, row["promo_id"], row["sku"], row["channel"], row["version"],
                     row["start_date"], row["end_date"], row.get("activity_price"),
                     row["status"], int(row.get("effective", 0)),
                     int(row.get("overlap", 0)), row.get("note", ""), row.get("row_no", 0)))
            for row in payload.get("promo_windows", []):
                conn.execute(
                    """INSERT INTO promo_windows (batch_id, promo_id, sku, channel,
                            start_date, end_date, version, price_incl, price_excl,
                            days, base_price_excl)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, row["promo_id"], row["sku"], row["channel"],
                     row["start_date"], row["end_date"], row["version"],
                     row["price_incl"], row["price_excl"], row["days"],
                     row["base_price_excl"]))
            for row in payload.get("quarantine", []):
                conn.execute(
                    """INSERT INTO quarantine (batch_id, code, stage, level, source,
                            line_no, field, raw_value, detail)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (batch_id, row["code"], row["stage"], row["level"],
                     row.get("source"), row.get("line_no"), row.get("field"),
                     row.get("raw_value"), row.get("detail")))
            for row in payload.get("elasticity", []):
                conn.execute(
                    """INSERT INTO elasticity (batch_id, scope, group_key, n, zero_count,
                            intercept, slope, se, tvalue, r2, zero_mode, significant, note)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, row["scope"], row["group_key"], row["n"],
                     row["zero_count"], row["intercept"], row["slope"], row["se"],
                     row["tvalue"], row["r2"], row["zero_mode"],
                     int(row.get("significant", 0)), row.get("note", "")))
            for row in payload.get("lift", []):
                conn.execute(
                    """INSERT INTO lift (batch_id, promo_id, sku, channel, start_date,
                            end_date, days, days_valid, days_missing, days_dropped,
                            actual_qty, base_qty, actual_gmv, base_gmv, lift_qty,
                            lift_gmv, se_gmv, ci_low, ci_high, price_excl,
                            base_price_excl, control_sku, control_factor, dlnp, slope, note)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, row["promo_id"], row["sku"], row["channel"],
                     row["start_date"], row["end_date"], row["days"],
                     row["days_valid"], row["days_missing"], row["days_dropped"],
                     row["actual_qty"], row["base_qty"], row["actual_gmv"],
                     row["base_gmv"], row["lift_qty"], row["lift_gmv"],
                     row["se_gmv"], row["ci_low"], row["ci_high"],
                     row["price_excl"], row["base_price_excl"], row["control_sku"],
                     row["control_factor"], row["dlnp"], row["slope"],
                     row.get("note", "")))
            for row in payload.get("spread", []):
                conn.execute(
                    """INSERT INTO spread (batch_id, promo_id, channel, ratio,
                            lift_gmv, actual_gmv)
                       VALUES (?,?,?,?,?,?)""",
                    (batch_id, row["promo_id"], row["channel"], row["ratio"],
                     row["lift_gmv"], row["actual_gmv"]))
            for row in payload.get("stages", []):
                conn.execute(
                    """INSERT INTO stages (batch_id, stage, in_count, out_count,
                            quarantine_count, status, elapsed_ms, note)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (batch_id, row["stage"], row["in_count"], row["out_count"],
                     row["quarantine_count"], row["status"], row["elapsed_ms"],
                     row.get("note", "")))
            for key, value in payload.get("totals", {}).items():
                conn.execute("INSERT INTO totals (batch_id, key, value) VALUES (?,?,?)",
                             (batch_id, key, str(value)))
            if payload.get("manifest") is not None:
                conn.execute(
                    "INSERT OR REPLACE INTO manifests (batch_id, json) VALUES (?,?)",
                    (batch_id, json.dumps(payload["manifest"], ensure_ascii=False)))

    # ---------- 读取 ----------
    def _rows(self, sql: str, args=()) -> list[dict]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]

    def sources(self, batch_id: str) -> list[dict]:
        return self._rows("SELECT * FROM sources WHERE batch_id=? ORDER BY role, name",
                          (batch_id,))

    def sales(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM sales WHERE batch_id=? ORDER BY sale_date, channel, sku, line_no",
            (batch_id,))

    def sales_count(self, batch_id: str) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM sales WHERE batch_id=?",
                               (batch_id,)).fetchone()
        return int(row["c"]) if row else 0

    def promos(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM promos WHERE batch_id=? ORDER BY promo_id, version", (batch_id,))

    def promo_windows(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM promo_windows WHERE batch_id=? ORDER BY start_date, promo_id",
            (batch_id,))

    def quarantine(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM quarantine WHERE batch_id=? "
            "ORDER BY CASE level WHEN 'error' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, "
            "code, line_no", (batch_id,))

    def quarantine_by_reason(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT code, level, stage, COUNT(*) AS cnt FROM quarantine WHERE batch_id=? "
            "GROUP BY code, level, stage ORDER BY cnt DESC, code", (batch_id,))

    def quarantine_count(self, batch_id: str) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM quarantine WHERE batch_id=?",
                               (batch_id,)).fetchone()
        return int(row["c"]) if row else 0

    def elasticity(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM elasticity WHERE batch_id=? ORDER BY scope, group_key",
            (batch_id,))

    def lift(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM lift WHERE batch_id=? ORDER BY promo_id, channel", (batch_id,))

    def spread(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM spread WHERE batch_id=? ORDER BY promo_id, channel", (batch_id,))

    def stages(self, batch_id: str) -> list[dict]:
        return self._rows(
            "SELECT * FROM stages WHERE batch_id=? ORDER BY rowid", (batch_id,))

    def totals(self, batch_id: str) -> dict:
        rows = self._rows("SELECT key, value FROM totals WHERE batch_id=?", (batch_id,))
        return {r["key"]: r["value"] for r in rows}

    def manifest(self, batch_id: str) -> dict | None:
        rows = self._rows("SELECT json FROM manifests WHERE batch_id=?", (batch_id,))
        return json.loads(rows[0]["json"]) if rows else None
