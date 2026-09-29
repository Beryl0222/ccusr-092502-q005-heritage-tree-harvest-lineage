"""责任谱系的统一编号。

所有编号均为带前缀的可读字符串，跨模块不混淆：
不同聚合类型前缀不同，同前缀后跟年份与不重复序号。
"""
from __future__ import annotations

import threading
from collections import defaultdict

_PREFIX_TO_KIND = {
    "TREE": "tree_record",
    "CV": "cultivar",
    "CREW": "crew",
    "ASMT": "annual_assessment",
    "WIN": "harvest_window",
    "LOT": "harvest_lot",
    "WGH": "weighing",
    "XFER": "custody_transfer",
    "DAM": "damage_observation",
    "CARE": "care_action",
    "QUAR": "quarantine",
    "EVT": "event",
}
_KIND_TO_PREFIX = {kind: prefix for prefix, kind in _PREFIX_TO_KIND.items()}


def kind_of(identifier: str) -> str | None:
    """由编号反查聚合类型，无法识别返回 None。"""
    if not isinstance(identifier, str) or "-" not in identifier:
        return None
    return _PREFIX_TO_KIND.get(identifier.split("-", 1)[0])


def expect_kind(identifier: str, kind: str) -> bool:
    return kind_of(identifier) == kind


def new_id(kind: str, year: int, seq: int) -> str:
    """生成形如 ``LOT-2026-000042`` 的编号，序号由存储层保证不重复。"""
    if kind not in _KIND_TO_PREFIX:
        raise ValueError(f"未知聚合类型: {kind}")
    return f"{_KIND_TO_PREFIX[kind]}-{year}-{seq:06d}"


class IdSequences:
    """按 (类型, 年份) 维护单调序号，线程安全。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next: dict[tuple[str, int], int] = defaultdict(lambda: 1)

    def seed(self, identifier: str) -> None:
        """回放已有事件时登记编号，推进序号水位。"""
        kind = kind_of(identifier)
        if kind is None:
            return
        prefix, year_text, seq_text = identifier.split("-", 2)
        try:
            year, seq = int(year_text), int(seq_text)
        except ValueError:
            return
        with self._lock:
            bucket = (kind, year)
            self._next[bucket] = max(self._next[bucket], seq + 1)

    def take(self, kind: str, year: int) -> str:
        with self._lock:
            seq = self._next[(kind, year)]
            self._next[(kind, year)] = seq + 1
        return new_id(kind, year, seq)
