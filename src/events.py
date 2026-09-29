"""领域事件构造与基础校验。

事件是不可变事实：一旦落库，只能由后续事件（隔离、纠正、修复等）
表达新的结论，绝不原地改写。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.envelope import validate_event

_ROOT = Path(__file__).resolve().parents[1]
_CONTRACT = json.loads((_ROOT / "contracts" / "domain.json").read_text(encoding="utf-8"))

EVENT_TYPES: frozenset[str] = frozenset(_CONTRACT["events"])
DESTINATION_CHANNELS: frozenset[str] = frozenset(_CONTRACT["destination_channels"])


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    occurred_at: str
    aggregate_id: str
    version: int
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "occurred_at": self.occurred_at,
            "aggregate_id": self.aggregate_id,
            "version": self.version,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Event":
        return cls(
            event_id=raw["event_id"],
            event_type=raw["event_type"],
            occurred_at=raw["occurred_at"],
            aggregate_id=raw["aggregate_id"],
            version=raw["version"],
            payload=raw["payload"],
        )


def now_iso() -> str:
    """带时区的当前时间（ISO 8601）。"""
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def build_event(
    event_id: str,
    event_type: str,
    aggregate_id: str,
    version: int,
    payload: dict[str, Any],
    occurred_at: str | None = None,
) -> Event:
    """构造一条事件并用共享信封规则校验。"""
    event = Event(
        event_id=event_id,
        event_type=event_type,
        occurred_at=occurred_at or now_iso(),
        aggregate_id=aggregate_id,
        version=version,
        payload=payload,
    )
    errors = validate_event(event.to_dict(), set(EVENT_TYPES))
    if errors:
        raise ValueError("事件校验失败: " + "; ".join(errors))
    return event
