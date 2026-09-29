"""领域事件构造与指纹。

业务事实一经接收即不可变；更正只能通过后续事件表达。
离线称重以秤端事件号去重：同号同内容视为重传，同号不同内容视为冲突。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from src.envelope import validate_event


class DomainError(ValueError):
    """业务规则被违反。"""


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def build_event(
    event_type: str,
    aggregate_id: str,
    version: int,
    payload: dict[str, Any],
    event_id: str,
    allowed_events: set[str],
    occurred_at: str | None = None,
) -> dict[str, Any]:
    """构造并校验一条事件信封。"""
    event = {
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": occurred_at or now_iso(),
        "aggregate_id": aggregate_id,
        "version": version,
        "payload": payload,
    }
    errors = validate_event(event, allowed_events)
    if errors:
        raise DomainError("; ".join(errors))
    return event


def event_fingerprint(event: dict[str, Any]) -> str:
    """事件业务内容指纹：排除 event_id 与 version。

    重传的同一秤端事件号若内容（含称量数值与发生时间）完全一致，
    指纹相同；任何一字节不同即构成标识冲突。
    """
    basis = {
        "event_type": event["event_type"],
        "aggregate_id": event["aggregate_id"],
        "occurred_at": event["occurred_at"],
        "payload": event["payload"],
    }
    encoded = json.dumps(
        basis, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
