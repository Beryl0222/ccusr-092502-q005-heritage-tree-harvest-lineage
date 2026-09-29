"""把仅追加事件折叠成当前状态与责任谱系视图。

投影随时可以从事件日志整体重建：更正事件不会删除旧事实，
折叠时以最新结论为准，因此“纠正一次观察”只需重新折叠
相关树木的年度负荷与修复计划，不影响其他树木。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from src.events import DESTINATION_CHANNELS, Event


def _year_of(event: Event) -> str:
    return str(event.occurred_at)[:4]


@dataclass
class CultivarView:
    id: str
    name: str


@dataclass
class CrewView:
    id: str
    name: str
    leader: str


@dataclass
class AssessmentView:
    id: str
    tree_id: str
    year: str
    load_capacity_kg: float
    assessor: str


@dataclass
class RangeView:
    id: str
    tree_id: str
    year: str
    valid_from: str
    valid_to: str
    pickable_kg: float
    active: bool = True


@dataclass
class OperationView:
    id: str
    tree_id: str
    crew_id: str
    range_id: str
    year: str
    planned_kg: float
    status: str = "STARTED"  # STARTED / PAUSED / COMPLETED
    dropped_kg: float = 0.0
    drop_events: int = 0


@dataclass
class TransferView:
    id: str
    seq: int
    from_party: str
    to_party: str
    custodian: str
    weight_kg: float


@dataclass
class BatchView:
    id: str
    op_id: str
    tree_id: str
    crew_id: str
    year: str
    weight_kg: float
    device_id: str
    client_record_id: str
    status: str = "HELD"  # HELD / QUARANTINED / ADMITTED / REJECTED
    quarantine_reason: str | None = None
    claimed_batch_id: str | None = None
    destination: str | None = None
    transfers: list[TransferView] = field(default_factory=list)


@dataclass
class ObservationView:
    id: str
    tree_id: str
    op_id: str | None
    branch_code: str
    severity: str
    fruit_loss_kg: float
    year: str
    corrected: bool = False
    correction_note: str | None = None


@dataclass
class CareView:
    id: str
    tree_id: str
    year: str
    observation_ids: list[str]
    severity: str
    status: str = "PLANNED"  # PLANNED / COMPLETED
    revisions: int = 0
    completed_by: str | None = None
    summary: str | None = None


@dataclass
class TreeView:
    id: str
    cultivar_id: str
    name: str
    status: str = "REGISTERED"  # REGISTERED / ANOMALOUS
    anomaly_reason: str | None = None
    assessments: dict[str, AssessmentView] = field(default_factory=dict)
    ranges: list[RangeView] = field(default_factory=list)


@dataclass
class ConservationReport:
    year: str
    weighed_kg: float
    held_kg: float
    quarantined_kg: float
    admitted_kg: float
    rejected_kg: float
    assigned_kg: float
    stock_kg: float
    by_channel: dict[str, float]
    balanced: bool
    discrepancies: list[str]


@dataclass
class ClosureItem:
    batch_id: str
    tree_id: str
    crew_id: str
    destination: str | None
    custodian_chain: list[str]
    closed: bool
    gaps: list[str]


class Lineage:
    """折叠后的全量状态。"""

    def __init__(self) -> None:
        self.cultivars: dict[str, CultivarView] = {}
        self.crews: dict[str, CrewView] = {}
        self.trees: dict[str, TreeView] = {}
        self.operations: dict[str, OperationView] = {}
        self.batches: dict[str, BatchView] = {}
        self.observations: dict[str, ObservationView] = {}
        self.care_actions: dict[str, CareView] = {}
        self.tree_care: dict[tuple[str, str], list[str]] = {}

    # ---------- 折叠 ----------

    def apply(self, event: Event) -> None:
        p = event.payload
        et = event.event_type
        if et == "CULTIVAR_REGISTERED":
            self.cultivars[event.aggregate_id] = CultivarView(event.aggregate_id, p["name"])
        elif et == "CREW_REGISTERED":
            self.crews[event.aggregate_id] = CrewView(event.aggregate_id, p["name"], p["leader"])
        elif et == "TREE_REGISTERED":
            self.trees[event.aggregate_id] = TreeView(
                event.aggregate_id, p["cultivar_id"], p["name"]
            )
        elif et == "TREE_ASSESSED":
            view = AssessmentView(
                event.aggregate_id, p["tree_id"], p["year"],
                float(p["load_capacity_kg"]), p["assessor"],
            )
            self.trees[p["tree_id"]].assessments[p["year"]] = view
        elif et == "PICKING_RANGE_GRANTED":
            self.trees[p["tree_id"]].ranges.append(RangeView(
                event.aggregate_id, p["tree_id"], p["year"],
                p["valid_from"], p["valid_to"], float(p["pickable_kg"]),
            ))
        elif et == "TREE_ANOMALY_FLAGGED":
            tree = self.trees[event.aggregate_id]
            tree.status = "ANOMALOUS"
            tree.anomaly_reason = p["reason"]
        elif et == "TREE_RETURNED_TO_SERVICE":
            tree = self.trees[event.aggregate_id]
            tree.status = "REGISTERED"
            tree.anomaly_reason = None
        elif et == "OPERATION_STARTED":
            self.operations[event.aggregate_id] = OperationView(
                event.aggregate_id, p["tree_id"], p["crew_id"], p["range_id"],
                p["year"], float(p["planned_kg"]),
            )
        elif et == "OPERATION_PAUSED":
            self.operations[event.aggregate_id].status = "PAUSED"
        elif et == "OPERATION_COMPLETED":
            self.operations[event.aggregate_id].status = "COMPLETED"
        elif et == "DROPPED_FRUIT_RECORDED":
            op = self.operations[p["op_id"]]
            op.dropped_kg += float(p["weight_kg"])
            op.drop_events += 1
        elif et == "LOT_WEIGHED":
            self.batches[event.aggregate_id] = BatchView(
                event.aggregate_id, p["op_id"], p["tree_id"], p["crew_id"],
                p["year"], float(p["weight_kg"]),
                p["device_id"], p["client_record_id"],
            )
        elif et == "BATCH_QUARANTINED":
            batch = self.batches[event.aggregate_id]
            batch.status = "QUARANTINED"
            batch.quarantine_reason = p["reason"]
            batch.claimed_batch_id = p.get("claimed_batch_id")
        elif et == "BATCH_ADMITTED":
            batch = self.batches[event.aggregate_id]
            batch.status = "ADMITTED"
            batch.quarantine_reason = None
        elif et == "BATCH_REJECTED":
            batch = self.batches[event.aggregate_id]
            batch.status = "REJECTED"
            batch.quarantine_reason = None
        elif et == "DESTINATION_ASSIGNED":
            batch = self.batches[event.aggregate_id]
            batch.destination = p["channel"]
        elif et == "DAMAGE_OBSERVED":
            self.observations[event.aggregate_id] = ObservationView(
                event.aggregate_id, p["tree_id"], p.get("op_id"),
                p["branch_code"], p["severity"], float(p["fruit_loss_kg"]),
                p["year"],
            )
        elif et == "DAMAGE_OBSERVATION_CORRECTED":
            obs = self.observations[event.aggregate_id]
            obs.severity = p["severity"]
            obs.branch_code = p["branch_code"]
            obs.fruit_loss_kg = float(p["fruit_loss_kg"])
            obs.corrected = True
            obs.correction_note = p["reason"]
        elif et == "CUSTODY_TRANSFERRED":
            p2 = event.payload
            batch = self.batches[p2["batch_id"]]
            batch.transfers.append(TransferView(
                event.aggregate_id, len(batch.transfers) + 1,
                p2["from_party"], p2["to_party"], p2["custodian"],
                float(p2["weight_kg"]),
            ))
        elif et == "CARE_PLANNED":
            care = CareView(
                event.aggregate_id, p["tree_id"], p["year"],
                list(p["observation_ids"]), p["severity"],
            )
            self.care_actions[event.aggregate_id] = care
            self.tree_care.setdefault((p["tree_id"], p["year"]), []).append(event.aggregate_id)
        elif et == "CARE_PLAN_REVISED":
            care = self.care_actions[event.aggregate_id]
            care.severity = p["severity"]
            care.observation_ids = list(p["observation_ids"])
            care.revisions += 1
        elif et == "CARE_COMPLETED":
            care = self.care_actions[event.aggregate_id]
            care.status = "COMPLETED"
            care.completed_by = p["crew_id"]
            care.summary = p.get("summary")

    # ---------- 查询 ----------

    def annual_load(self, tree_id: str, year: str) -> dict[str, float]:
        """某棵树某年的年度负荷构成（果实已离树即计入，与行政状态无关）。"""
        weighed = sum(
            b.weight_kg for b in self.batches.values()
            if b.tree_id == tree_id and b.year == year
        )
        dropped = sum(
            op.dropped_kg for op in self.operations.values()
            if op.tree_id == tree_id and op.year == year
        )
        damage = sum(
            o.fruit_loss_kg for o in self.observations.values()
            if o.tree_id == tree_id and o.year == year
        )
        return {
            "weighed_kg": weighed,
            "dropped_kg": dropped,
            "damage_loss_kg": damage,
            "total_kg": weighed + dropped + damage,
        }

    def open_care(self, tree_id: str, year: str) -> CareView | None:
        for care_id in self.tree_care.get((tree_id, year), []):
            care = self.care_actions[care_id]
            if care.status == "PLANNED":
                return care
        return None

    def tree_lineage(self, tree_id: str) -> dict:
        """从一棵树追到各次采收、批次、去向、交接、观察与修复。"""
        tree = self.trees[tree_id]
        ops = [op for op in self.operations.values() if op.tree_id == tree_id]
        result: dict = {"tree": tree, "assessments": list(tree.assessments.values()),
                        "ranges": tree.ranges, "operations": []}
        for op in sorted(ops, key=lambda o: o.id):
            op_batches = [b for b in self.batches.values() if b.op_id == op.id]
            result["operations"].append({
                "operation": op,
                "dropped_kg": op.dropped_kg,
                "batches": sorted(op_batches, key=lambda b: b.id),
            })
        result["observations"] = sorted(
            (o for o in self.observations.values() if o.tree_id == tree_id),
            key=lambda o: o.id,
        )
        result["care"] = [
            self.care_actions[cid]
            for key, ids in self.tree_care.items()
            if key[0] == tree_id for cid in ids
        ]
        result["loads"] = {
            year: self.annual_load(tree_id, year)
            for year in tree.assessments
        }
        return result

    def pending_work(self) -> dict:
        """重启后继续待处理的隔离与修复。"""
        return {
            "quarantined_batches": [
                b for b in self.batches.values() if b.status == "QUARANTINED"
            ],
            "planned_care": [
                c for c in self.care_actions.values() if c.status == "PLANNED"
            ],
        }

    def conservation_report(self, year: str) -> ConservationReport:
        """果品数量守恒核对：称重总量在各状态间不重不漏。"""
        batches = [b for b in self.batches.values() if b.year == year]
        weighed = sum(b.weight_kg for b in batches)
        held = sum(b.weight_kg for b in batches if b.status == "HELD")
        quarantined = sum(b.weight_kg for b in batches if b.status == "QUARANTINED")
        admitted = sum(b.weight_kg for b in batches if b.status == "ADMITTED")
        rejected = sum(b.weight_kg for b in batches if b.status == "REJECTED")
        assigned_batches = [b for b in batches if b.destination is not None]
        assigned = sum(b.weight_kg for b in assigned_batches)
        stock = admitted - assigned
        by_channel = {c: 0.0 for c in sorted(DESTINATION_CHANNELS)}
        for b in assigned_batches:
            by_channel[b.destination] += b.weight_kg

        discrepancies: list[str] = []
        if abs((held + quarantined + admitted + rejected) - weighed) > 1e-9:
            discrepancies.append("按状态汇总与称重总量不平")
        if stock < -1e-9:
            discrepancies.append("去向分配重量超过准入重量")
        bad = [b.id for b in assigned_batches if b.status != "ADMITTED"]
        if bad:
            discrepancies.append(f"非准入批次被分配去向: {bad}")
        return ConservationReport(
            year=year, weighed_kg=weighed, held_kg=held,
            quarantined_kg=quarantined, admitted_kg=admitted,
            rejected_kg=rejected, assigned_kg=assigned, stock_kg=stock,
            by_channel=by_channel, balanced=not discrepancies,
            discrepancies=discrepancies,
        )

    def responsibility_report(self, year: str) -> list[ClosureItem]:
        """责任闭环：批次的作业班组、交接链与最终去向是否闭合。"""
        items: list[ClosureItem] = []
        for b in sorted(self.batches.values(), key=lambda x: x.id):
            if b.year != year:
                continue
            gaps: list[str] = []
            chain = [f"{t.from_party}→{t.to_party}（{t.custodian}）" for t in b.transfers]
            if not b.crew_id:
                gaps.append("缺少作业班组")
            if b.status == "HELD":
                gaps.append("批次尚未完成准入处置")
            elif b.status == "QUARANTINED":
                gaps.append(f"批次仍在隔离中待核查：{b.quarantine_reason}")
            elif b.status == "ADMITTED":
                if b.destination is None:
                    gaps.append("已准入但未登记去向")
                if not b.transfers:
                    gaps.append("缺少交接记录")
            if b.transfers:
                first_from = b.transfers[0].from_party
                if first_from != b.crew_id:
                    gaps.append("交接起点不是采收班组，原责任人断链")
                for prev, nxt in zip(b.transfers, b.transfers[1:]):
                    if prev.to_party != nxt.from_party:
                        gaps.append(f"交接链在 {prev.id} 与 {nxt.id} 之间断裂")
                if b.destination and b.transfers[-1].to_party != b.destination:
                    gaps.append("交接终点与登记去向不一致")
            items.append(ClosureItem(
                batch_id=b.id, tree_id=b.tree_id, crew_id=b.crew_id,
                destination=b.destination, custodian_chain=chain,
                closed=not gaps, gaps=gaps,
            ))
        return items


def fold(events: list[Event]) -> Lineage:
    lineage = Lineage()
    for event in events:
        lineage.apply(event)
    return lineage
