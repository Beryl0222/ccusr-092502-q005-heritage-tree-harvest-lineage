"""责任谱系应用服务。

关键规则：
- 编号不可混淆：不同聚合使用不同前缀，序号由事件存储回放后单调续发。
- 离线称重重传以秤端称量号去重：同号同内容幂等返回，同号异内容先隔离批次。
- 业务事实只追加；纠正以新事件表达，不覆盖旧记录。
- 树势暂停只阻断之后的采收作业，历史交接与责任不变。
- 观察被纠正时仅重算该树该年度的负荷投影与修复计划。
- 待处理的隔离与修复全部能从事件流恢复，重启不丢。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from src.events import DomainError, build_event, event_fingerprint, now_iso
from src.ids import IdSequences, kind_of
from src.projection import LineageView
from src.store import EventStore

SEVERITIES = ("LIGHT", "MODERATE", "SEVERE")
DESTINATIONS = ("VISITOR_PICK", "RESEARCH_SAMPLE", "DONATION", "SALVAGE_DISCARD")
VIGOR_LEVELS = ("WEAK", "NORMAL", "STRONG")


class QuarantineConflict(DomainError):
    """标识冲突或越界，批次已被隔离。"""

    def __init__(self, quarantine_id: str, reason: str, detail: str) -> None:
        super().__init__(f"批次已隔离 {quarantine_id}: {reason} {detail}")
        self.quarantine_id = quarantine_id
        self.reason = reason
        self.detail = detail


def load_contract() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[1] / "contracts" / "domain.json"
    return json.loads(path.read_text(encoding="utf-8"))


class LineageService:
    def __init__(self, store: EventStore) -> None:
        self.store = store
        contract = load_contract()
        self.allowed_events = set(contract["events"])
        self.ids = IdSequences()
        self.view = LineageView()
        self._lock = threading.RLock()
        store.replay(self._ingest)

    # ---- 启动恢复 -------------------------------------------------------

    def _ingest(self, event: dict[str, Any]) -> None:
        self.ids.seed(event["event_id"])
        self.ids.seed(event["aggregate_id"])
        for nested_kind in ("quarantine_id", "weighing_id", "window_id",
                            "care_id", "observation_id", "transfer_id"):
            value = event.get("payload", {}).get(nested_kind)
            if isinstance(value, str):
                self.ids.seed(value)
        self.view.apply(event)

    def pending_quarantines(self) -> list[dict[str, Any]]:
        """重启后仍待处理的隔离批次。"""
        return [
            {"lot_id": lot.id, "tree_id": lot.tree_id, "year": lot.year,
             "quarantine": lot.quarantines[-1]}
            for lot in self.view.lots.values()
            if lot.status == "quarantined"
        ]

    def pending_care(self) -> list[dict[str, Any]]:
        """重启后仍待完成的修复计划。"""
        return [
            {"care_id": care.id, "tree_id": care.tree_id, "year": care.year,
             "severity": care.severity, "basis_observation_ids": care.basis_observation_ids}
            for care in self.view.care.values() if care.status == "planned"
        ]

    # ---- 内部工具 -------------------------------------------------------

    @staticmethod
    def _year_now() -> int:
        return datetime.now().astimezone().year

    def _append(self, event_type: str, aggregate_id: str, payload: dict[str, Any],
                event_id: str | None = None, occurred_at: str | None = None) -> dict[str, Any]:
        version = self.store.version_of(aggregate_id) + 1
        eid = event_id or self.ids.take("event", self._year_now())
        event = build_event(
            event_type, aggregate_id, version, payload, eid,
            self.allowed_events, occurred_at=occurred_at,
        )
        self.store.append(event, expected_version=version - 1)
        self._ingest(event)
        return event

    def _require_tree(self, tree_id: str) -> dict[str, Any]:
        tree = self.view.trees.get(tree_id)
        if tree is None:
            raise DomainError(f"未知树木: {tree_id}")
        return tree

    def _require_lot(self, lot_id: str):
        lot = self.view.lots.get(lot_id)
        if lot is None:
            raise DomainError(f"未知批次: {lot_id}")
        return lot

    def _assert_active(self, tree_id: str) -> None:
        if self.view.is_suspended(tree_id):
            raise DomainError(f"树木 {tree_id} 已暂停作业，需先恢复树势")

    def _quarantine(self, lot, reason: str, detail: str,
                    conflicts: list[dict[str, Any]] | None = None) -> str:
        quarantine_id = self.ids.take("quarantine", lot.year)
        self._append("LOT_QUARANTINED", lot.id, {
            "quarantine_id": quarantine_id,
            "reason": reason,
            "detail": detail,
            "conflicts": conflicts or [],
        })
        return quarantine_id

    # ---- 基础登记 -------------------------------------------------------

    def register_cultivar(self, name: str) -> str:
        with self._lock:
            for cv in self.view.cultivars.values():
                if cv["name"] == name:
                    return cv["id"]
            cid = self.ids.take("cultivar", self._year_now())
            self._append("CULTIVAR_REGISTERED", cid, {"name": name})
            return cid

    def register_crew(self, name: str, contact: str | None = None) -> str:
        with self._lock:
            for crew in self.view.crews.values():
                if crew["name"] == name:
                    return crew["id"]
            crew_id = self.ids.take("crew", self._year_now())
            self._append("CREW_REGISTERED", crew_id, {"name": name, "contact": contact})
            return crew_id

    def register_tree(self, cultivar_id: str, location: str,
                      planted_year: int | None = None) -> str:
        with self._lock:
            if cultivar_id not in self.view.cultivars:
                raise DomainError(f"未知品种: {cultivar_id}")
            for tree in self.view.trees.values():
                if tree["location"] == location and tree["cultivar_id"] == cultivar_id:
                    return tree["id"]
            tree_id = self.ids.take("tree_record", self._year_now())
            self._append("TREE_REGISTERED", tree_id, {
                "cultivar_id": cultivar_id,
                "location": location,
                "planted_year": planted_year,
            })
            return tree_id

    # ---- 年度评估与树势 -------------------------------------------------

    def assess_tree(self, tree_id: str, year: int, vigor: str,
                    capacity_kg: float, note: str = "") -> str:
        with self._lock:
            self._require_tree(tree_id)
            if vigor not in VIGOR_LEVELS:
                raise DomainError(f"树势等级非法: {vigor}")
            if capacity_kg < 0:
                raise DomainError("年度负荷容量不能为负")
            assessment_id = self.ids.take("annual_assessment", year)
            self._append("TREE_ASSESSED", assessment_id, {
                "tree_id": tree_id, "year": year, "vigor": vigor,
                "capacity_kg": round(float(capacity_kg), 3), "note": note,
            })
            return assessment_id

    def set_suspension(self, tree_id: str, suspended: bool, reason: str) -> None:
        with self._lock:
            self._require_tree(tree_id)
            if self.view.is_suspended(tree_id) == suspended:
                return  # 状态已一致，幂等不产生事件
            self._append("TREE_SUSPENSION_TOGGLED", tree_id, {
                "tree_id": tree_id, "suspended": suspended, "reason": reason,
            })

    def open_window(self, tree_id: str, year: int, authorized_kg: float,
                    ends_on: str | None = None) -> str:
        with self._lock:
            self._require_tree(tree_id)
            self._assert_active(tree_id)
            existing = [w for w in self.view.windows.values()
                        if w["tree_id"] == tree_id and w["year"] == year]
            if existing:
                raise DomainError(f"{tree_id} {year} 年已存在可采范围 {existing[0]['id']}")
            assessment = self.view.annual_assessment(tree_id, year)
            if assessment is None:
                raise DomainError(f"{tree_id} {year} 年尚未评估，不能划定可采范围")
            authorized_kg = round(float(authorized_kg), 3)
            if authorized_kg <= 0:
                raise DomainError("可采量必须为正数")
            if authorized_kg > assessment["capacity_kg"] + 1e-9:
                raise DomainError(
                    f"可采量 {authorized_kg}kg 超出年度负荷容量 {assessment['capacity_kg']}kg"
                )
            window_id = self.ids.take("harvest_window", year)
            self._append("HARVEST_WINDOW_OPENED", window_id, {
                "tree_id": tree_id, "year": year,
                "authorized_kg": authorized_kg, "ends_on": ends_on,
            })
            return window_id

    # ---- 批次与称重 -----------------------------------------------------

    def create_lot(self, tree_id: str, crew_id: str, year: int,
                   window_id: str, note: str = "") -> str:
        with self._lock:
            self._require_tree(tree_id)
            self._assert_active(tree_id)
            if crew_id not in self.view.crews:
                raise DomainError(f"未知作业小组: {crew_id}")
            window = self.view.windows.get(window_id)
            if window is None:
                raise DomainError(f"未知可采范围: {window_id}")
            if window["tree_id"] != tree_id or window["year"] != year:
                raise DomainError("可采范围与树木/年度不符")
            lot_id = self.ids.take("harvest_lot", year)
            self._append("LOT_CREATED", lot_id, {
                "tree_id": tree_id, "crew_id": crew_id, "year": year,
                "window_id": window_id, "note": note,
            })
            return lot_id

    def record_weighing(self, lot_id: str, weighing_id: str, weight_kg: float,
                        device_id: str | None = None,
                        weighed_at: str | None = None) -> dict[str, Any]:
        """离线称重上报。

        weighing_id 由秤端生成，重传时必须保持不变：
        - 同号同内容：幂等返回首次事件；
        - 同号不同内容：隔离本批次并抛出 QuarantineConflict。
        """
        with self._lock:
            lot = self._require_lot(lot_id)
            if kind_of(weighing_id) != "weighing":
                raise DomainError("称量编号必须使用 WGH 前缀")
            weight_kg = round(float(weight_kg), 3)
            if weight_kg <= 0:
                raise DomainError("称重数值必须为正数")
            at = weighed_at or now_iso()

            existing = self.store.get(weighing_id)
            if existing is not None:
                incoming = {
                    "event_type": "LOT_WEIGHED",
                    "aggregate_id": lot_id,
                    "occurred_at": at,
                    "payload": {
                        "weighing_id": weighing_id,
                        "weight_kg": weight_kg,
                        "device_id": device_id,
                        "weighed_at": at,
                    },
                }
                if event_fingerprint(incoming) == event_fingerprint(existing):
                    return existing  # 幂等重传
                # 同号不同内容：不允许改写，先隔离批次。
                quarantine_id = self._quarantine(
                    lot, "ID_FINGERPRINT_CONFLICT",
                    f"称量号 {weighing_id} 重传内容与首次记录不一致",
                    conflicts=[{
                        "weighing_id": weighing_id,
                        "stored_fingerprint": event_fingerprint(existing),
                        "incoming_fingerprint": event_fingerprint(incoming),
                    }],
                )
                raise QuarantineConflict(
                    quarantine_id, "ID_FINGERPRINT_CONFLICT",
                    f"称量号 {weighing_id} 出现两个不同内容",
                )

            self._assert_active(lot.tree_id)
            if lot.status == "discarded":
                raise DomainError(f"批次 {lot_id} 已废弃，不能继续称重")
            if lot.status == "quarantined":
                raise DomainError(f"批次 {lot_id} 处于隔离中，需先放行或废弃")

            event = self._append("LOT_WEIGHED", lot_id, {
                "weighing_id": weighing_id,
                "weight_kg": weight_kg,
                "device_id": device_id,
                "weighed_at": at,
            }, event_id=weighing_id, occurred_at=at)

            window = self.view.windows.get(lot.window_id) if lot.window_id else None
            if window and lot.net_weight_kg > window["authorized_kg"] + 1e-9:
                quarantine_id = self._quarantine(
                    lot, "OUT_OF_WINDOW_LIMIT",
                    f"累计净重 {lot.net_weight_kg}kg 超出可采范围 "
                    f"{window['authorized_kg']}kg",
                )
                raise QuarantineConflict(
                    quarantine_id, "OUT_OF_WINDOW_LIMIT",
                    f"批次 {lot_id} 累计称重超出可采范围",
                )
            return event

    def release_lot(self, lot_id: str, reason: str) -> None:
        """隔离批次经人工核查后放行。"""
        with self._lock:
            lot = self._require_lot(lot_id)
            if lot.status != "quarantined":
                raise DomainError(f"批次 {lot_id} 不在隔离中")
            self._append("LOT_RELEASED", lot_id, {"reason": reason})

    def discard_lot(self, lot_id: str, reason: str) -> None:
        """隔离批次核查后废弃（计入损耗守恒，不进入任何去向）。"""
        with self._lock:
            lot = self._require_lot(lot_id)
            if lot.status not in ("quarantined", "created", "released"):
                raise DomainError(f"批次 {lot_id} 状态 {lot.status} 不可废弃")
            if lot.transfers:
                raise DomainError("已发生交接的批次不能废弃，责任链必须保留")
            self._append("LOT_DISCARDED", lot_id, {"reason": reason})

    # ---- 去向与交接 -----------------------------------------------------

    def assign_destination(self, lot_id: str, destination: str) -> None:
        with self._lock:
            lot = self._require_lot(lot_id)
            if destination not in DESTINATIONS:
                raise DomainError(f"未知去向: {destination}")
            if destination == "SALVAGE_DISCARD":
                raise DomainError("废弃去向请通过 discard_lot 处理")
            if lot.status == "discarded":
                raise DomainError("已废弃批次不能指定去向")
            if lot.status == "quarantined":
                raise DomainError("隔离批次放行前不能指定去向")
            self._assert_active(lot.tree_id)
            if lot.destination is not None:
                if lot.destination == destination:
                    return  # 幂等
                raise DomainError(
                    f"批次已确定去向 {lot.destination}，同一批果实不能再记为 {destination}"
                )
            self._append("DESTINATION_ASSIGNED", lot_id, {"destination": destination})

    def transfer_custody(self, lot_id: str, to_party: str, quantity_kg: float,
                         note: str = "", from_party: str | None = None) -> str:
        with self._lock:
            lot = self._require_lot(lot_id)
            self._assert_active(lot.tree_id)
            if lot.status in ("quarantined", "discarded"):
                raise DomainError(f"批次状态 {lot.status}，不能交接")
            if lot.destination is None:
                raise DomainError("交接前必须确定唯一去向")
            quantity_kg = round(float(quantity_kg), 3)
            if quantity_kg <= 0:
                raise DomainError("交接数量必须为正数")
            if quantity_kg > lot.remaining_kg + 1e-9:
                raise DomainError(
                    f"交接 {quantity_kg}kg 超过批次剩余 {lot.remaining_kg}kg"
                )
            current_holder = lot.transfers[-1]["to_party"] if lot.transfers else lot.crew_id
            if from_party is not None and from_party != current_holder:
                raise DomainError(
                    f"当前责任人是 {current_holder}，不能以 {from_party} 名义交接"
                )
            transfer_id = self.ids.take("custody_transfer", lot.year)
            self._append("CUSTODY_TRANSFERRED", transfer_id, {
                "lot_id": lot_id,
                "from_party": current_holder,
                "to_party": to_party,
                "quantity_kg": quantity_kg,
                "weight_snapshot_kg": lot.net_weight_kg,
                "destination": lot.destination,
                "note": note,
            })
            return transfer_id

    # ---- 损伤观察、纠正与修复 -------------------------------------------

    def observe_damage(self, lot_id: str, branch_code: str, severity: str,
                       dropped_loss_kg: float = 0.0, observed_by: str | None = None,
                       note: str = "") -> str:
        with self._lock:
            lot = self._require_lot(lot_id)
            if lot.status == "discarded":
                raise DomainError("已废弃批次不能再登记观察")
            if severity not in SEVERITIES:
                raise DomainError(f"损伤等级非法: {severity}")
            if dropped_loss_kg < 0:
                raise DomainError("落果损失不能为负")
            obs_id = self.ids.take("damage_observation", lot.year)
            self._append("DAMAGE_OBSERVED", obs_id, {
                "lot_id": lot_id,
                "tree_id": lot.tree_id,
                "year": lot.year,
                "branch_code": branch_code,
                "severity": severity,
                "dropped_loss_kg": round(float(dropped_loss_kg), 3),
                "observed_by": observed_by,
                "note": note,
            })
            self._replan_care(lot.tree_id, lot.year, "DAMAGE_OBSERVED")
            return obs_id

    def correct_observation(self, observation_id: str, reason: str,
                            severity: str | None = None,
                            dropped_loss_kg: float | None = None,
                            note: str | None = None) -> None:
        """纠正一次观察：只重算相关树木的年度负荷与修复计划。"""
        with self._lock:
            obs = self.view.observations.get(observation_id)
            if obs is None:
                raise DomainError(f"未知观察: {observation_id}")
            if severity is not None and severity not in SEVERITIES:
                raise DomainError(f"损伤等级非法: {severity}")
            if dropped_loss_kg is not None and dropped_loss_kg < 0:
                raise DomainError("落果损失不能为负")
            payload: dict[str, Any] = {"reason": reason}
            if severity is not None:
                payload["severity"] = severity
            if dropped_loss_kg is not None:
                payload["dropped_loss_kg"] = round(float(dropped_loss_kg), 3)
            if note is not None:
                payload["note"] = note
            self._append("OBSERVATION_CORRECTED", observation_id, payload)
            self._replan_care(obs.tree_id, obs.year, "OBSERVATION_CORRECTED")

    def _replan_care(self, tree_id: str, year: int, trigger: str) -> None:
        """依据当前（含纠正后的）观察重算单树年度修复计划。"""
        basis, severity = self.view.required_care_basis(tree_id, year)
        open_care = self.view.open_care_for(tree_id, year)
        if open_care is not None:
            current = set(open_care.basis_observation_ids)
            if current == set(basis) and open_care.severity == severity:
                return
            if not basis:
                # 纠正后观察不再达到修复门槛：取消待处理计划，历史计划仍在。
                self._append("CARE_CANCELLED", open_care.id, {
                    "reason": f"{trigger}: 纠正后无达到修复门槛的观察"
                })
                return
            payload: dict[str, Any] = {
                "tree_id": tree_id, "year": year,
                "basis_observation_ids": list(basis),
                "severity": severity,
                "supersedes_care_id": open_care.id,
                "supersede_reason": trigger,
            }
            new_id = self.ids.take("care_action", year)
            self._append("CARE_PLANNED", new_id, payload)
            return
        if basis:
            care_id = self.ids.take("care_action", year)
            self._append("CARE_PLANNED", care_id, {
                "tree_id": tree_id, "year": year,
                "basis_observation_ids": list(basis),
                "severity": severity,
            })

    def complete_care(self, care_id: str, summary: str) -> None:
        with self._lock:
            care = self.view.care.get(care_id)
            if care is None:
                raise DomainError(f"未知修复计划: {care_id}")
            if care.status != "planned":
                raise DomainError(f"修复计划状态为 {care.status}，不能完工登记")
            self._append("CARE_COMPLETED", care_id, {"summary": summary})

    # ---- 查询 -----------------------------------------------------------

    def annual_load(self, tree_id: str, year: int) -> dict[str, Any]:
        with self._lock:
            return self.view.annual_load(tree_id, year)

    def tree_lineage(self, tree_id: str) -> dict[str, Any]:
        with self._lock:
            return self.view.tree_lineage(tree_id)

    def conservation_report(self, year: int | None = None) -> dict[str, Any]:
        with self._lock:
            return self.view.conservation_report(year)

    def lot_closure(self, lot_id: str) -> dict[str, Any]:
        with self._lock:
            return self.view.lot_closure(lot_id)
