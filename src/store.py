"""追加式事件存储。

每个事件一行 JSON，写入即 fsync。重启后整体回放即可恢复全部状态。
去重键：
- ``event_id`` 全局唯一；离线重传复用同一 event_id。
- 每个聚合维护单调 version，旧记录永不改写。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Callable, Iterable


class ConcurrentModificationError(RuntimeError):
    """聚合版本与预期不符（并发或过期写入）。"""


class EventExistsError(RuntimeError):
    """event_id 已存在：调用方据此判断是幂等重传还是标识冲突。"""

    def __init__(self, event_id: str, stored: dict[str, Any]) -> None:
        super().__init__(f"事件编号已存在: {event_id}")
        self.event_id = event_id
        self.stored = stored


class EventStore:
    """JSONL 事件日志，线程安全。"""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._events: list[dict[str, Any]] = []
        self._by_id: dict[str, dict[str, Any]] = {}
        self._versions: dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            self._index(event)

    def _index(self, event: dict[str, Any]) -> None:
        if event["event_id"] in self._by_id:
            # 同一进程内不会发生；日志被外力重复拼接时给出明确错误。
            raise RuntimeError(f"日志中存在重复 event_id: {event['event_id']}")
        self._events.append(event)
        self._by_id[event["event_id"]] = event
        aggregate = event["aggregate_id"]
        self._versions[aggregate] = event["version"]

    @property
    def all_events(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._events)

    def get(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._by_id.get(event_id)

    def version_of(self, aggregate_id: str) -> int:
        with self._lock:
            return self._versions.get(aggregate_id, 0)

    def events_for(self, aggregate_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return [e for e in self._events if e["aggregate_id"] == aggregate_id]

    def append(
        self,
        event: dict[str, Any],
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """追加事件。

        expected_version 为该聚合当前版本（新聚合为 0）；
        若给出的 event_id 已存在则抛出 EventExistsError，不写任何内容。
        """
        event_id = event["event_id"]
        aggregate = event["aggregate_id"]
        with self._lock:
            existing = self._by_id.get(event_id)
            if existing is not None:
                raise EventExistsError(event_id, existing)
            current = self._versions.get(aggregate, 0)
            if expected_version is not None and current != expected_version:
                raise ConcurrentModificationError(
                    f"{aggregate} 版本 {current} 与预期 {expected_version} 不符"
                )
            if event["version"] != current + 1:
                raise ConcurrentModificationError(
                    f"{aggregate} 事件版本必须为 {current + 1}，实际 {event['version']}"
                )
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=False) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            self._index(event)
            return event

    def replay(self, sink: Callable[[dict[str, Any]], None]) -> None:
        """按写入顺序把全部事件投递给投影/状态装配器。"""
        with self._lock:
            events: Iterable[dict[str, Any]] = list(self._events)
        for event in events:
            sink(event)
