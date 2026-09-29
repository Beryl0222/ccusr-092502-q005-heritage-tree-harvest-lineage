"""仅追加的 JSONL 事件存储。

- 事件按写入顺序持久化，重启后原样回放；
- event_id 全局唯一：同编号同内容重放为幂等命中，绝不重复计数；
- (aggregate_id, version) 唯一：版本位已被别的事件占用即冲突，
  调用方必须用隔离等新事件处理，不能覆盖旧记录。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from src.events import Event


class DuplicateEventError(Exception):
    """同一 event_id 以不同内容再次提交。"""


class VersionConflictError(Exception):
    """(aggregate_id, version) 已被另一个事件占用。"""

    def __init__(self, aggregate_id: str, version: int, existing_event_id: str):
        super().__init__(
            f"聚合 {aggregate_id} 版本 {version} 已存在事件 {existing_event_id}"
        )
        self.aggregate_id = aggregate_id
        self.version = version
        self.existing_event_id = existing_event_id


@dataclass(frozen=True)
class AppendResult:
    event: Event
    appended: bool  # False 表示命中幂等，返回的是已存在的同一事件


class EventStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._events: list[Event] = []
        self._by_event_id: dict[str, Event] = {}
        self._by_aggregate_version: dict[tuple[str, int], Event] = {}
        self._load()

    # ---------- 读取 ----------

    def _load(self) -> None:
        if not self.path.exists():
            return
        raw = self.path.read_text(encoding="utf-8")
        lines = raw.splitlines()
        # 崩溃可能导致最后一行截断；逐行解析，跳过末尾不完整行。
        for index, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            try:
                event = Event.from_dict(json.loads(line))
            except json.JSONDecodeError:
                if index == len(lines) - 1 and not raw.endswith("\n"):
                    continue
                raise
            self._index(event)

    def _index(self, event: Event) -> None:
        self._events.append(event)
        self._by_event_id[event.event_id] = event
        self._by_aggregate_version[(event.aggregate_id, event.version)] = event

    def all_events(self) -> list[Event]:
        """按写入顺序返回全部事件。"""
        return list(self._events)

    def events_for(self, aggregate_id: str) -> list[Event]:
        return [e for e in self._events if e.aggregate_id == aggregate_id]

    # ---------- 写入 ----------

    def append(self, event: Event) -> AppendResult:
        existing = self._by_event_id.get(event.event_id)
        if existing is not None:
            if existing.to_dict() == event.to_dict():
                return AppendResult(existing, appended=False)
            raise DuplicateEventError(
                f"event_id {event.event_id} 已存在且内容不同，拒绝改写旧记录"
            )
        owner = self._by_aggregate_version.get((event.aggregate_id, event.version))
        if owner is not None:
            raise VersionConflictError(
                event.aggregate_id, event.version, owner.event_id
            )

        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        self._index(event)
        return AppendResult(event, appended=True)
