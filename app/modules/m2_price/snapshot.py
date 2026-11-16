"""促销单快照：测算批次对当次促销日历的不可变留痕。

需求背景：促销活动常有补录与改价，日历一改，历史测算就被一起改写。
解决办法是每次测算都把当次促销单固化成一份快照：
- 快照内容 = 促销日历的规范化逐行内容（去除书写顺序与首尾空白差异），
  规范化后做 sha256 得到 snapshot_digest；同一份促销单重复补录重试，
  摘要不变，因此不会生成新版本；
- 快照一经写入不再变更，旧批次永远按自己那份快照导出，事后改日历不影响它；
- 批次通过 batches.snapshot_id 绑定快照，报告版本的切换只改「当前批次」指针，
  不动任何历史批次的数据。
"""
from __future__ import annotations

import json

from ... import manifest

# 快照只固化促销业务字段；行号属于物理文件信息，不进摘要（同一补录重新导出
# 行号可能变化，但促销单内容没变，不应被当成新单子）。
SNAPSHOT_FIELDS = (
    "促销编号", "商品编码", "渠道", "版本", "开始日期", "结束日期",
    "活动价", "状态", "备注",
)


def _cell(record, field: str) -> str:
    return (record.data.get(field) or "").strip()


def canonical_rows(records) -> list[dict[str, str]]:
    """把促销记录规范成与书写顺序无关、与首尾空白无关的行集合。"""
    rows = [
        {field: _cell(record, field) for field in SNAPSHOT_FIELDS}
        for record in records
    ]
    # 排序后再摘要：调换两行书写顺序仍是同一张促销单
    return sorted(rows, key=lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True))


def build_snapshot(parsed_promo) -> dict:
    """从采信阶段读出的促销日历 ParsedFile 构造快照文档（不落库）。"""
    rows = canonical_rows(parsed_promo.records)
    payload = {
        "source_name": parsed_promo.name,
        "source_sha256": parsed_promo.sha256,
        "fields": list(SNAPSHOT_FIELDS),
        "row_count": len(rows),
        "rows": rows,
    }
    digest = manifest.digest_parts(
        [json.dumps(payload, ensure_ascii=False, sort_keys=True)])
    return {
        "snapshot_digest": digest,
        "source_name": parsed_promo.name,
        "source_sha256": parsed_promo.sha256,
        "row_count": len(rows),
        "content_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
    }


def parse_content(content_json: str) -> dict:
    return json.loads(content_json)
