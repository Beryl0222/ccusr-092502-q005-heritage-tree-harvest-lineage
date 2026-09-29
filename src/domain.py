"""采后责任谱系领域服务。

所有写操作都产出仅追加事件；关键规则：

- 离线称重以 (device_id, client_record_id) 为幂等键：完全重复直接返回
  原事件；同一键却声称是另一个批次，属于标识冲突，新批次先隔离，
  旧称重记录原样保留。
- 果品去向三选一：批次准入后只能登记一次去向；交接重量必须等于
  批次重量，保证同一批果不可能同时进入两个渠道。
- 树势异常暂停该树之后的作业；已完成的交接与责任人不受影响。
- 损伤观察被纠正时，追加纠正事件，并只重算该树当年的年度负荷与
  尚未完成的修复计划，其他树木不动。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.events import DESTINATION_CHANNELS, Event, now_iso
from src.ids import make_id
from src.projections import Lineage, fold
from src.store import EventStore

SEVERITY_ORDER = {"LIGHT": 1, "MODERATE": 2, "SEVERE": 3}
_WEIGHT_TOL = 1e-6


class DomainError(ValueError):
    """业务规则被违反。"""


@dataclass
class CommandResult:
    event: Event
    appended: bool
    side_effects: list[Event]

    @property
    def events(self) -> list[Event]:
        return [self.event, *self.side_effects]


class LineageService:
    def __init__(self, store: EventStore):
        self.store = store
        self._rebuild()

    # ---------- 内部工具 ----------

    def _rebuild(self) -> None:
        self.lineage: Lineage = fold(self.store.all_events())

    def _next_version(self, aggregate_id: str) -> int:
        return 1 + max(
            (e.version for e in self.store.all_events()
             if e.aggregate_id == aggregate_id),
            default=0,
        )

    def _append(
        self,
        event_type: str,
        aggregate_id: str,
        payload: dict,
        *,
        event_token: str | None = None,
        occurred_at: str | None = None,
    ) -> Event:
        token = event_token or uuid.uuid4().hex
        event_id = f"evt-{event_type.lower()}-{token}"
        event = self._build(event_id, event_type, aggregate_id, payload, occurred_at)
        result = self.store.append(event)
        self.lineage.apply(result.event)
        return result.event

    def _build(self, event_id, event_type, aggregate_id, payload, occurred_at) -> Event:
        from src.events import build_event
        version = self._next_version(aggregate_id)
        return build_event(event_id, event_type, aggregate_id, version, payload, occurred_at)

    def _require_tree(self, tree_id: str):
        tree = self.lineage.trees.get(tree_id)
        if tree is None:
            raise DomainError(f"树木不存在: {tree_id}")
        return tree

    def _require_active_tree(self, tree_id: str):
        tree = self._require_tree(tree_id)
        if tree.status == "ANOMALOUS":
            raise DomainError(f"树木 {tree_id} 树势异常，已暂停后续作业: {tree.anomaly_reason}")
        return tree

    def _range_weighed(self, range_id: str) -> float:
        total = 0.0
        for op in self.lineage.operations.values():
            if op.range_id == range_id:
                total += sum(
                    b.weight_kg for b in self.lineage.batches.values() if b.op_id == op.id
                )
        return total

    # ---------- 基础档案 ----------

    def register_cultivar(self, token: str, name: str) -> Event:
        cid = str(make_id("cultivar", token))
        if cid in self.lineage.cultivars:
            raise DomainError(f"品种已登记: {cid}")
        return self._append("CULTIVAR_REGISTERED", cid, {"name": name}, event_token=token)

    def register_crew(self, token: str, name: str, leader: str) -> Event:
        crew_id = str(make_id("crew", token))
        if crew_id in self.lineage.crews:
            raise DomainError(f"作业小组已登记: {crew_id}")
        return self._append("CREW_REGISTERED", crew_id,
                            {"name": name, "leader": leader}, event_token=token)

    def register_tree(self, token: str, cultivar_token: str, name: str) -> Event:
        cultivar_id = str(make_id("cultivar", cultivar_token))
        if cultivar_id not in self.lineage.cultivars:
            raise DomainError(f"品种不存在: {cultivar_id}")
        tree_id = str(make_id("tree_record", token))
        if tree_id in self.lineage.trees:
            raise DomainError(f"树木已登记: {tree_id}")
        return self._append("TREE_REGISTERED", tree_id,
                            {"cultivar_id": cultivar_id, "name": name}, event_token=token)

    def assess_tree(self, token: str, tree_token: str, year: str,
                    load_capacity_kg: float, assessor: str,
                    occurred_at: str | None = None) -> Event:
        tree_id = str(make_id("tree_record", tree_token))
        self._require_tree(tree_id)
        if load_capacity_kg <= 0:
            raise DomainError("年度负荷必须为正数")
        assess_id = str(make_id("annual_assessment", token))
        if year in self.lineage.trees[tree_id].assessments:
            raise DomainError(f"树木 {tree_id} 的 {year} 年度评估已存在")
        return self._append("TREE_ASSESSED", assess_id, {
            "tree_id": tree_id, "year": year,
            "load_capacity_kg": float(load_capacity_kg), "assessor": assessor,
        }, event_token=token, occurred_at=occurred_at)

    def grant_picking_range(self, token: str, tree_token: str, year: str,
                            valid_from: str, valid_to: str, pickable_kg: float,
                            occurred_at: str | None = None) -> Event:
        tree_id = str(make_id("tree_record", tree_token))
        tree = self._require_tree(tree_id)
        if year not in tree.assessments:
            raise DomainError(f"必须先完成 {year} 年度评估才能划定可采范围")
        if pickable_kg <= 0 or pickable_kg > tree.assessments[year].load_capacity_kg + _WEIGHT_TOL:
            raise DomainError("可采量必须为正且不超过年度负荷")
        range_id = str(make_id("picking_range", token))
        return self._append("PICKING_RANGE_GRANTED", range_id, {
            "tree_id": tree_id, "year": year,
            "valid_from": valid_from, "valid_to": valid_to,
            "pickable_kg": float(pickable_kg),
        }, event_token=token, occurred_at=occurred_at)

    # ---------- 树势 ----------

    def flag_tree_anomaly(self, tree_token: str, reason: str,
                          occurred_at: str | None = None) -> list[Event]:
        """标记树势异常；该树所有进行中的作业一并暂停，已完成交接保持原样。"""
        tree_id = str(make_id("tree_record", tree_token))
        self._require_tree(tree_id)
        events = [self._append("TREE_ANOMALY_FLAGGED", tree_id, {"reason": reason},
                               occurred_at=occurred_at)]
        for op in self.lineage.operations.values():
            if op.tree_id == tree_id and op.status == "STARTED":
                events.append(self._append("OPERATION_PAUSED", op.id,
                                           {"reason": f"树木异常联动暂停: {reason}"},
                                           occurred_at=occurred_at))
        return events

    def return_tree_to_service(self, tree_token: str, note: str,
                               occurred_at: str | None = None) -> Event:
        tree_id = str(make_id("tree_record", tree_token))
        self._require_tree(tree_id)
        return self._append("TREE_RETURNED_TO_SERVICE", tree_id, {"note": note},
                            occurred_at=occurred_at)

    # ---------- 作业 ----------

    def start_operation(self, token: str, tree_token: str, crew_token: str,
                        range_token: str, year: str, planned_kg: float,
                        occurred_at: str | None = None) -> Event:
        tree_id = str(make_id("tree_record", tree_token))
        crew_id = str(make_id("crew", crew_token))
        range_id = str(make_id("picking_range", range_token))
        tree = self._require_active_tree(tree_id)
        if crew_id not in self.lineage.crews:
            raise DomainError(f"作业小组不存在: {crew_id}")
        rng = next((r for r in tree.ranges
                    if r.id == range_id and r.year == year and r.active), None)
        if rng is None:
            raise DomainError(f"可采范围不存在或不适用于 {tree_id} {year}")
        if planned_kg <= 0:
            raise DomainError("计划采收量必须为正数")
        remaining = rng.pickable_kg - self._range_weighed(range_id)
        if planned_kg > remaining + _WEIGHT_TOL:
            raise DomainError(
                f"计划采收 {planned_kg}kg 超出可采范围剩余 {remaining:.2f}kg"
            )
        op_id = str(make_id("harvest_operation", token))
        return self._append("OPERATION_STARTED", op_id, {
            "tree_id": tree_id, "crew_id": crew_id, "range_id": range_id,
            "year": year, "planned_kg": float(planned_kg),
        }, event_token=token, occurred_at=occurred_at)

    def pause_operation(self, op_token: str, reason: str,
                        occurred_at: str | None = None) -> Event:
        op_id = str(make_id("harvest_operation", op_token))
        op = self.lineage.operations.get(op_id)
        if op is None or op.status != "STARTED":
            raise DomainError("只有进行中的作业可以暂停")
        return self._append("OPERATION_PAUSED", op_id, {"reason": reason},
                            occurred_at=occurred_at)

    def complete_operation(self, op_token: str,
                           occurred_at: str | None = None) -> Event:
        op_id = str(make_id("harvest_operation", op_token))
        op = self.lineage.operations.get(op_id)
        if op is None or op.status not in ("STARTED", "PAUSED"):
            raise DomainError("作业不存在或已结束")
        return self._append("OPERATION_COMPLETED", op_id, {}, occurred_at=occurred_at)

    def record_dropped_fruit(self, op_token: str, weight_kg: float, note: str,
                             event_token: str | None = None,
                             occurred_at: str | None = None) -> Event:
        op_id = str(make_id("harvest_operation", op_token))
        op = self.lineage.operations.get(op_id)
        if op is None:
            raise DomainError(f"作业不存在: {op_id}")
        if weight_kg <= 0:
            raise DomainError("落果重量必须为正数")
        return self._append("DROPPED_FRUIT_RECORDED", op_id, {
            "op_id": op_id, "weight_kg": float(weight_kg), "note": note,
        }, event_token=event_token, occurred_at=occurred_at)

    # ---------- 离线称重重传：幂等 + 冲突隔离 ----------

    def _find_idempotent_batch(self, device_id: str, client_record_id: str):
        for b in self.lineage.batches.values():
            if b.device_id == device_id and b.client_record_id == client_record_id:
                return b
        return None

    def weigh_lot(self, batch_token: str, op_token: str, weight_kg: float,
                  device_id: str, client_record_id: str,
                  occurred_at: str | None = None) -> CommandResult:
        """离线秤上送称重。

        同一 (device_id, client_record_id) 重传：
        - 指向同一批次且内容一致 → 幂等返回，不重复计重；
        - 指向另一个批次号 → 标识冲突，新批次登记后立即隔离。
        """
        op_id = str(make_id("harvest_operation", op_token))
        batch_id = str(make_id("harvest_lot", batch_token))
        op = self.lineage.operations.get(op_id)
        if op is None:
            raise DomainError(f"作业不存在: {op_id}")
        if weight_kg <= 0:
            raise DomainError("称重重量必须为正数")

        prior = self._find_idempotent_batch(device_id, client_record_id)
        if prior is not None:
            if prior.id == batch_id and abs(prior.weight_kg - weight_kg) <= _WEIGHT_TOL:
                # 完整重传：取回原事件，由存储层保证不重复落库。
                original = next(
                    e for e in self.store.events_for(batch_id) if e.event_type == "LOT_WEIGHED"
                )
                return CommandResult(original, appended=False, side_effects=[])
            # 同一离线记录却换了批次号（或重量）：标识冲突。
            if batch_id in self.lineage.batches:
                raise DomainError(
                    f"批次号 {batch_id} 已被占用且离线记录不一致，拒绝改写；"
                    "请更换批次号后重新上送"
                )
            weighed = self._append("LOT_WEIGHED", batch_id, {
                "op_id": op_id, "tree_id": op.tree_id, "crew_id": op.crew_id,
                "year": op.year, "weight_kg": float(weight_kg),
                "device_id": device_id, "client_record_id": client_record_id,
            }, event_token=f"weigh-{batch_token}", occurred_at=occurred_at)
            quarantined = self.quarantine_batch(
                batch_token,
                reason=f"离线记录 {device_id}/{client_record_id} 已归属批次 {prior.id}，"
                       "标识冲突待核查",
                claimed_batch_id=prior.id, occurred_at=occurred_at,
            )
            return CommandResult(weighed, appended=True, side_effects=[quarantined])

        if batch_id in self.lineage.batches:
            raise DomainError(f"批次号 {batch_id} 已存在，不能以新离线记录覆盖")

        # 树势异常或作业暂停时不得继续称重入库。
        self._require_active_tree(op.tree_id)
        if op.status != "STARTED":
            raise DomainError(f"作业 {op_id} 当前状态 {op.status}，不能称重")
        tree = self.lineage.trees[op.tree_id]
        rng = next(r for r in tree.ranges if r.id == op.range_id)
        already = self._range_weighed(op.range_id)
        if already + weight_kg > rng.pickable_kg + _WEIGHT_TOL:
            raise DomainError(
                f"本次称重将使 {op.tree_id} 累计 {already + weight_kg:.2f}kg "
                f"超过可采范围 {rng.pickable_kg}kg"
            )
        assessment = tree.assessments.get(op.year)
        load = self.lineage.annual_load(op.tree_id, op.year)
        if assessment and load["weighed_kg"] + weight_kg > assessment.load_capacity_kg + _WEIGHT_TOL:
            raise DomainError(
                f"本次称重将超过 {op.tree_id} {op.year} 年度负荷 "
                f"{assessment.load_capacity_kg}kg"
            )

        weighed = self._append("LOT_WEIGHED", batch_id, {
            "op_id": op_id, "tree_id": op.tree_id, "crew_id": op.crew_id,
            "year": op.year, "weight_kg": float(weight_kg),
            "device_id": device_id, "client_record_id": client_record_id,
        }, event_token=f"weigh-{batch_token}", occurred_at=occurred_at)
        return CommandResult(weighed, appended=True, side_effects=[])

    # ---------- 隔离处置 ----------

    def quarantine_batch(self, batch_token: str, reason: str,
                         claimed_batch_id: str | None = None,
                         event_token: str | None = None,
                         occurred_at: str | None = None) -> Event:
        batch_id = str(make_id("harvest_lot", batch_token))
        batch = self.lineage.batches.get(batch_id)
        if batch is None:
            raise DomainError(f"批次不存在: {batch_id}")
        if batch.status in ("ADMITTED", "REJECTED"):
            raise DomainError(f"批次已处置为 {batch.status}，不能再隔离")
        return self._append("BATCH_QUARANTINED", batch_id, {
            "reason": reason, "claimed_batch_id": claimed_batch_id,
        }, event_token=event_token or f"quar-{uuid.uuid4().hex}",
                            occurred_at=occurred_at)

    def admit_batch(self, batch_token: str, note: str,
                    event_token: str | None = None,
                    occurred_at: str | None = None) -> Event:
        batch_id = str(make_id("harvest_lot", batch_token))
        batch = self.lineage.batches.get(batch_id)
        if batch is None or batch.status not in ("HELD", "QUARANTINED"):
            raise DomainError("只有待处置或隔离中的批次可以核查准入")
        return self._append("BATCH_ADMITTED", batch_id, {"note": note},
                            event_token=event_token or f"admit-{uuid.uuid4().hex}",
                            occurred_at=occurred_at)

    def reject_batch(self, batch_token: str, reason: str,
                     event_token: str | None = None,
                     occurred_at: str | None = None) -> Event:
        batch_id = str(make_id("harvest_lot", batch_token))
        batch = self.lineage.batches.get(batch_id)
        if batch is None or batch.status not in ("HELD", "QUARANTINED"):
            raise DomainError("只有待处置或隔离中的批次可以拒收")
        return self._append("BATCH_REJECTED", batch_id, {"reason": reason},
                            event_token=event_token or f"reject-{uuid.uuid4().hex}",
                            occurred_at=occurred_at)

    # ---------- 去向与交接 ----------

    def assign_destination(self, batch_token: str, channel: str,
                           occurred_at: str | None = None) -> Event:
        if channel not in DESTINATION_CHANNELS:
            raise DomainError(f"未知去向渠道: {channel}")
        batch_id = str(make_id("harvest_lot", batch_token))
        batch = self.lineage.batches.get(batch_id)
        if batch is None:
            raise DomainError(f"批次不存在: {batch_id}")
        if batch.status != "ADMITTED":
            raise DomainError("只有核查准入的批次才能登记去向")
        if batch.destination is not None:
            # 同一批果不能同时记为游客采摘、科研留样和公益赠送。
            raise DomainError(
                f"批次已登记去向 {batch.destination}，去向互斥，不能改记 {channel}"
            )
        return self._append("DESTINATION_ASSIGNED", batch_id, {"channel": channel},
                            occurred_at=occurred_at)

    def transfer_custody(self, token: str, batch_token: str, from_party: str,
                         to_party: str, custodian: str, weight_kg: float,
                         occurred_at: str | None = None) -> Event:
        batch_id = str(make_id("harvest_lot", batch_token))
        xfer_id = str(make_id("custody_transfer", token))
        batch = self.lineage.batches.get(batch_id)
        if batch is None:
            raise DomainError(f"批次不存在: {batch_id}")
        if batch.status != "ADMITTED":
            raise DomainError("批次未准入，不能交接")
        if batch.destination is None:
            raise DomainError("交接前必须先登记唯一去向")
        if abs(weight_kg - batch.weight_kg) > _WEIGHT_TOL:
            raise DomainError(
                f"交接重量 {weight_kg}kg 与批次称重 {batch.weight_kg}kg 不一致，"
                "禁止整批转手时缺斤短两"
            )
        if not batch.transfers:
            if from_party != batch.crew_id:
                raise DomainError(
                    f"首次交接必须由采收班组 {batch.crew_id} 交出，原责任人不可跳过"
                )
        else:
            last = batch.transfers[-1]
            if from_party != last.to_party:
                raise DomainError(
                    f"交接链断裂：上一手接收方是 {last.to_party}，本次交出方却是 {from_party}"
                )
        return self._append("CUSTODY_TRANSFERRED", xfer_id, {
            "batch_id": batch_id, "from_party": from_party,
            "to_party": to_party, "custodian": custodian,
            "weight_kg": float(weight_kg),
        }, event_token=token, occurred_at=occurred_at)

    # ---------- 损伤观察与修复 ----------

    def observe_damage(self, token: str, tree_token: str, branch_code: str,
                       severity: str, fruit_loss_kg: float, year: str,
                       op_token: str | None = None,
                       occurred_at: str | None = None) -> Event:
        tree_id = str(make_id("tree_record", tree_token))
        self._require_tree(tree_id)
        if severity not in SEVERITY_ORDER:
            raise DomainError(f"损伤等级非法: {severity}")
        if fruit_loss_kg < 0:
            raise DomainError("损失重量不能为负")
        op_id = str(make_id("harvest_operation", op_token)) if op_token else None
        if op_id and op_id not in self.lineage.operations:
            raise DomainError(f"作业不存在: {op_id}")
        obs_id = str(make_id("damage_observation", token))
        return self._append("DAMAGE_OBSERVED", obs_id, {
            "tree_id": tree_id, "op_id": op_id, "branch_code": branch_code,
            "severity": severity, "fruit_loss_kg": float(fruit_loss_kg),
            "year": year,
        }, event_token=token, occurred_at=occurred_at)

    def _care_severity(self, observation_ids: list[str]) -> str:
        levels = [self.lineage.observations[oid].severity for oid in observation_ids]
        return max(levels, key=lambda s: SEVERITY_ORDER[s])

    def plan_care(self, token: str, tree_token: str, year: str,
                  observation_tokens: list[str],
                  occurred_at: str | None = None) -> Event:
        tree_id = str(make_id("tree_record", tree_token))
        self._require_tree(tree_id)
        observation_ids = [str(make_id("damage_observation", t)) for t in observation_tokens]
        for oid in observation_ids:
            obs = self.lineage.observations.get(oid)
            if obs is None or obs.tree_id != tree_id or obs.year != year:
                raise DomainError(f"观察 {oid} 不属于该树 {year} 年度")
        care_id = str(make_id("care_action", token))
        return self._append("CARE_PLANNED", care_id, {
            "tree_id": tree_id, "year": year,
            "observation_ids": observation_ids,
            "severity": self._care_severity(observation_ids),
        }, event_token=token, occurred_at=occurred_at)

    def correct_observation(self, obs_token: str, branch_code: str, severity: str,
                            fruit_loss_kg: float, reason: str,
                            occurred_at: str | None = None) -> list[Event]:
        """纠正一次观察：只追加纠正事件，并重算该树当年的负荷与修复计划。"""
        obs_id = str(make_id("damage_observation", obs_token))
        obs = self.lineage.observations.get(obs_id)
        if obs is None:
            raise DomainError(f"观察不存在: {obs_id}")
        if severity not in SEVERITY_ORDER:
            raise DomainError(f"损伤等级非法: {severity}")
        if fruit_loss_kg < 0:
            raise DomainError("损失重量不能为负")
        events = [self._append("DAMAGE_OBSERVATION_CORRECTED", obs_id, {
            "branch_code": branch_code, "severity": severity,
            "fruit_loss_kg": float(fruit_loss_kg), "reason": reason,
        }, event_token=f"corr-{uuid.uuid4().hex}", occurred_at=occurred_at)]

        # 年度负荷由投影自动重算；这里只需联动修订该树尚未完成的修复计划。
        open_care = self.lineage.open_care(obs.tree_id, obs.year)
        if open_care is not None and obs_id in open_care.observation_ids:
            events.append(self._append("CARE_PLAN_REVISED", open_care.id, {
                "observation_ids": list(open_care.observation_ids),
                "severity": self._care_severity(open_care.observation_ids),
                "reason": f"观察 {obs_id} 已纠正，修复计划随之重算",
            }, event_token=f"rev-{uuid.uuid4().hex}", occurred_at=occurred_at))
        return events

    def complete_care(self, care_token: str, crew_token: str, summary: str,
                      occurred_at: str | None = None) -> Event:
        care_id = str(make_id("care_action", care_token))
        crew_id = str(make_id("crew", crew_token))
        care = self.lineage.care_actions.get(care_id)
        if care is None:
            raise DomainError(f"修复计划不存在: {care_id}")
        if care.status != "PLANNED":
            raise DomainError("修复计划已完成")
        if crew_id not in self.lineage.crews:
            raise DomainError(f"作业小组不存在: {crew_id}")
        return self._append("CARE_COMPLETED", care_id, {
            "crew_id": crew_id, "summary": summary,
        }, event_token=f"done-{care_token}", occurred_at=occurred_at)
