"""读模型：由事件流装配，随时可整体重放重建。

所有数字都来自不可变事件；纠正事件只会把观察的“当前值”切换到新版本，
历史值仍保留在事件流中，可从树木谱系一路追溯。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

SEVERITY_ORDER = {"LIGHT": 1, "MODERATE": 2, "SEVERE": 3}
CARE_THRESHOLD = "MODERATE"


def _kg(value: Any) -> float:
    return round(float(value), 3)


@dataclass
class LotView:
    id: str
    tree_id: str
    crew_id: str
    year: int
    window_id: str | None = None
    note: str = ""
    created_at: str = ""
    status: str = "created"  # created / quarantined / released / discarded
    weighings: list[dict[str, Any]] = field(default_factory=list)
    quarantines: list[dict[str, Any]] = field(default_factory=list)
    released_at: str | None = None
    discarded_at: str | None = None
    discard_reason: str = ""
    destination: str | None = None
    destination_history: list[dict[str, Any]] = field(default_factory=list)
    transfers: list[dict[str, Any]] = field(default_factory=list)
    damage_ids: list[str] = field(default_factory=list)

    @property
    def net_weight_kg(self) -> float:
        return _kg(sum(w["weight_kg"] for w in self.weighings))

    @property
    def transferred_kg(self) -> float:
        return _kg(sum(t["quantity_kg"] for t in self.transfers))

    @property
    def remaining_kg(self) -> float:
        return _kg(self.net_weight_kg - self.transferred_kg)

    @property
    def handed_over(self) -> bool:
        return bool(self.transfers)


@dataclass
class ObservationView:
    id: str
    lot_id: str
    tree_id: str
    year: int
    branch_code: str
    severity: str
    dropped_loss_kg: float
    observed_by: str | None
    note: str
    occurred_at: str
    corrections: list[dict[str, Any]] = field(default_factory=list)

    @property
    def corrected(self) -> bool:
        return bool(self.corrections)

    def needs_care(self) -> bool:
        return SEVERITY_ORDER[self.severity] >= SEVERITY_ORDER[CARE_THRESHOLD]


@dataclass
class CareView:
    id: str
    tree_id: str
    year: int
    basis_observation_ids: list[str]
    severity: str
    planned_at: str
    note: str = ""
    status: str = "planned"  # planned / completed / superseded / cancelled
    superseded_by: str | None = None
    supersede_reason: str = ""
    completed_at: str | None = None
    completion_summary: str = ""
    cancelled_at: str | None = None
    cancel_reason: str = ""


class LineageView:
    """事件流的只读装配结果。"""

    def __init__(self) -> None:
        self.cultivars: dict[str, dict[str, Any]] = {}
        self.crews: dict[str, dict[str, Any]] = {}
        self.trees: dict[str, dict[str, Any]] = {}
        self.assessments: dict[str, dict[str, Any]] = {}
        self.assessment_by_tree_year: dict[tuple[str, int], str] = {}
        self.windows: dict[str, dict[str, Any]] = {}
        self.lots: dict[str, LotView] = {}
        self.observations: dict[str, ObservationView] = {}
        self.care: dict[str, CareView] = {}
        # tree_id -> 是否暂停（按事件顺序折叠）
        self.suspended: dict[str, bool] = {}
        self.suspension_history: dict[str, list[dict[str, Any]]] = defaultdict(list)

    # ---- 事件折叠 -------------------------------------------------------

    def apply(self, event: dict[str, Any]) -> None:
        etype = event["event_type"]
        payload = event["payload"]
        handler = getattr(self, f"_on_{etype.lower()}", None)
        if handler is not None:
            handler(event, payload)

    def _on_cultivar_registered(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        self.cultivars[e["aggregate_id"]] = {
            "id": e["aggregate_id"], "name": p["name"], "registered_at": e["occurred_at"]
        }

    def _on_crew_registered(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        self.crews[e["aggregate_id"]] = {
            "id": e["aggregate_id"],
            "name": p["name"],
            "contact": p.get("contact"),
            "registered_at": e["occurred_at"],
        }

    def _on_tree_registered(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        self.trees[e["aggregate_id"]] = {
            "id": e["aggregate_id"],
            "cultivar_id": p["cultivar_id"],
            "location": p["location"],
            "planted_year": p.get("planted_year"),
            "registered_at": e["occurred_at"],
        }

    def _on_tree_assessed(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        assessment = {
            "id": e["aggregate_id"],
            "tree_id": p["tree_id"],
            "year": p["year"],
            "vigor": p["vigor"],
            "capacity_kg": _kg(p["capacity_kg"]),
            "note": p.get("note", ""),
            "occurred_at": e["occurred_at"],
        }
        self.assessments[e["aggregate_id"]] = assessment
        # 同一年度多次评估，以最新一条为准（旧事件仍保留）。
        self.assessment_by_tree_year[(p["tree_id"], p["year"])] = e["aggregate_id"]

    def _on_tree_suspension_toggled(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        self.suspended[p["tree_id"]] = bool(p["suspended"])
        self.suspension_history[p["tree_id"]].append(
            {"suspended": bool(p["suspended"]), "reason": p["reason"], "occurred_at": e["occurred_at"]}
        )

    def _on_harvest_window_opened(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        self.windows[e["aggregate_id"]] = {
            "id": e["aggregate_id"],
            "tree_id": p["tree_id"],
            "year": p["year"],
            "authorized_kg": _kg(p["authorized_kg"]),
            "ends_on": p.get("ends_on"),
            "opened_at": e["occurred_at"],
        }

    def _on_lot_created(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        self.lots[e["aggregate_id"]] = LotView(
            id=e["aggregate_id"],
            tree_id=p["tree_id"],
            crew_id=p["crew_id"],
            year=p["year"],
            window_id=p.get("window_id"),
            note=p.get("note", ""),
            created_at=e["occurred_at"],
        )

    def _on_lot_weighed(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        lot = self.lots[e["aggregate_id"]]
        lot.weighings.append({
            "id": p["weighing_id"],
            "event_id": e["event_id"],
            "weight_kg": _kg(p["weight_kg"]),
            "device_id": p.get("device_id"),
            "weighed_at": p.get("weighed_at", e["occurred_at"]),
            "recorded_at": e["occurred_at"],
        })

    def _on_lot_quarantined(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        lot = self.lots[e["aggregate_id"]]
        lot.status = "quarantined"
        lot.quarantines.append({
            "id": p["quarantine_id"],
            "reason": p["reason"],
            "detail": p.get("detail", ""),
            "conflicts": p.get("conflicts", []),
            "occurred_at": e["occurred_at"],
            "resolved": False,
        })

    def _on_lot_released(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        lot = self.lots[e["aggregate_id"]]
        lot.status = "released"
        lot.released_at = e["occurred_at"]
        # 一次放行结论关闭该批次全部未决隔离。
        for quarantine in lot.quarantines:
            if not quarantine["resolved"]:
                quarantine["resolved"] = True
                quarantine["resolution"] = p.get("reason", "")
                quarantine["resolved_at"] = e["occurred_at"]

    def _on_lot_discarded(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        lot = self.lots[e["aggregate_id"]]
        lot.status = "discarded"
        lot.discarded_at = e["occurred_at"]
        lot.discard_reason = p.get("reason", "")
        for quarantine in lot.quarantines:
            if not quarantine["resolved"]:
                quarantine["resolved"] = True
                quarantine["resolution"] = "LOT_DISCARDED"
                quarantine["resolved_at"] = e["occurred_at"]

    def _on_destination_assigned(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        lot = self.lots[e["aggregate_id"]]
        lot.destination = p["destination"]
        lot.destination_history.append(
            {"destination": p["destination"], "occurred_at": e["occurred_at"]}
        )

    def _on_custody_transferred(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        lot = self.lots[p["lot_id"]]
        lot.transfers.append({
            "id": e["aggregate_id"],
            "from_party": p.get("from_party"),
            "to_party": p["to_party"],
            "quantity_kg": _kg(p["quantity_kg"]),
            "weight_snapshot_kg": _kg(p["weight_snapshot_kg"]),
            "destination": p["destination"],
            "note": p.get("note", ""),
            "occurred_at": e["occurred_at"],
        })

    def _on_damage_observed(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        observation = ObservationView(
            id=e["aggregate_id"],
            lot_id=p["lot_id"],
            tree_id=p["tree_id"],
            year=p["year"],
            branch_code=p["branch_code"],
            severity=p["severity"],
            dropped_loss_kg=_kg(p["dropped_loss_kg"]),
            observed_by=p.get("observed_by"),
            note=p.get("note", ""),
            occurred_at=e["occurred_at"],
        )
        self.observations[e["aggregate_id"]] = observation
        self.lots[p["lot_id"]].damage_ids.append(e["aggregate_id"])

    def _on_observation_corrected(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        observation = self.observations[e["aggregate_id"]]
        observation.corrections.append({
            "severity": p.get("severity", observation.severity),
            "dropped_loss_kg": _kg(
                p["dropped_loss_kg"] if p.get("dropped_loss_kg") is not None
                else observation.dropped_loss_kg
            ),
            "note": p.get("note", observation.note),
            "reason": p.get("reason", ""),
            "occurred_at": e["occurred_at"],
        })
        latest = observation.corrections[-1]
        observation.severity = latest["severity"]
        observation.dropped_loss_kg = latest["dropped_loss_kg"]
        observation.note = latest["note"]

    def _on_care_planned(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        care = CareView(
            id=e["aggregate_id"],
            tree_id=p["tree_id"],
            year=p["year"],
            basis_observation_ids=list(p["basis_observation_ids"]),
            severity=p["severity"],
            planned_at=e["occurred_at"],
            note=p.get("note", ""),
        )
        self.care[e["aggregate_id"]] = care
        superseded = p.get("supersedes_care_id")
        if superseded and superseded in self.care:
            old = self.care[superseded]
            old.status = "superseded"
            old.superseded_by = e["aggregate_id"]
            old.supersede_reason = p.get("supersede_reason", "REPLANNED")

    def _on_care_completed(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        care = self.care[e["aggregate_id"]]
        care.status = "completed"
        care.completed_at = e["occurred_at"]
        care.completion_summary = p.get("summary", "")

    def _on_care_cancelled(self, e: dict[str, Any], p: dict[str, Any]) -> None:
        care = self.care[e["aggregate_id"]]
        care.status = "cancelled"
        care.cancelled_at = e["occurred_at"]
        care.cancel_reason = p.get("reason", "")

    # ---- 查询 -----------------------------------------------------------

    def is_suspended(self, tree_id: str) -> bool:
        return self.suspended.get(tree_id, False)

    def lot_ids_for_tree_year(self, tree_id: str, year: int) -> list[str]:
        return [lid for lid, lot in self.lots.items()
                if lot.tree_id == tree_id and lot.year == year]

    def harvested_kg(self, tree_id: str, year: int) -> float:
        """年度采收净重：隔离批次同样计入，果实确实已离开树体。"""
        return _kg(sum(
            lot.net_weight_kg for lot in self.lots.values()
            if lot.tree_id == tree_id and lot.year == year
        ))

    def annual_assessment(self, tree_id: str, year: int) -> dict[str, Any] | None:
        aid = self.assessment_by_tree_year.get((tree_id, year))
        return self.assessments.get(aid) if aid else None

    def open_care_for(self, tree_id: str, year: int) -> CareView | None:
        for care in self.care.values():
            if (care.tree_id == tree_id and care.year == year
                    and care.status == "planned"):
                return care
        return None

    def required_care_basis(self, tree_id: str, year: int) -> tuple[list[str], str]:
        """根据当前（含纠正后）观察计算修复依据。"""
        basis = sorted(
            oid for oid, obs in self.observations.items()
            if obs.tree_id == tree_id and obs.year == year and obs.needs_care()
        )
        severity = "SEVERE" if any(
            self.observations[oid].severity == "SEVERE" for oid in basis
        ) else ("MODERATE" if basis else "NONE")
        return basis, severity

    def annual_load(self, tree_id: str, year: int) -> dict[str, Any]:
        """单树年度负荷：只汇总该树本年数据，供局部重算。"""
        assessment = self.annual_assessment(tree_id, year)
        capacity = assessment["capacity_kg"] if assessment else None
        harvested = self.harvested_kg(tree_id, year)
        lot_ids = self.lot_ids_for_tree_year(tree_id, year)
        dropped = _kg(sum(
            obs.dropped_loss_kg for obs in self.observations.values()
            if obs.tree_id == tree_id and obs.year == year
        ))
        suspended = self.is_suspended(tree_id)
        result = {
            "tree_id": tree_id,
            "year": year,
            "capacity_kg": capacity,
            "harvested_kg": harvested,
            "remaining_capacity_kg": _kg(capacity - harvested) if capacity is not None else None,
            "dropped_loss_kg": dropped,
            "lot_ids": lot_ids,
            "suspended": suspended,
            "within_capacity": None if capacity is None else harvested <= capacity + 1e-9,
        }
        return result

    def tree_lineage(self, tree_id: str) -> dict[str, Any]:
        """养护员视角：从一棵树追到各次采收的影响与责任。"""
        if tree_id not in self.trees:
            raise KeyError(f"未知树木: {tree_id}")
        years = sorted({lot.year for lot in self.lots.values() if lot.tree_id == tree_id}
                       | {year for (t, year) in self.assessment_by_tree_year if t == tree_id})
        annual: list[dict[str, Any]] = []
        for year in years:
            aid = self.assessment_by_tree_year.get((tree_id, year))
            annual.append({
                "year": year,
                "assessment": self.assessments.get(aid) if aid else None,
                "load": self.annual_load(tree_id, year),
                "lots": [self._lot_lineage(lid) for lid in self.lot_ids_for_tree_year(tree_id, year)],
                "care": [self._care_lineage(cid) for cid, c in self.care.items()
                         if c.tree_id == tree_id and c.year == year],
            })
        return {
            "tree": self.trees[tree_id],
            "cultivar": self.cultivars.get(self.trees[tree_id]["cultivar_id"]),
            "suspended": self.is_suspended(tree_id),
            "suspension_history": self.suspension_history.get(tree_id, []),
            "annual": annual,
        }

    def _lot_lineage(self, lot_id: str) -> dict[str, Any]:
        lot = self.lots[lot_id]
        return {
            "lot": {
                "id": lot.id,
                "year": lot.year,
                "status": lot.status,
                "crew": self.crews.get(lot.crew_id),
                "window_id": lot.window_id,
                "created_at": lot.created_at,
                "note": lot.note,
            },
            "weighings": list(lot.weighings),
            "quarantines": list(lot.quarantines),
            "destination": lot.destination,
            "destination_history": list(lot.destination_history),
            "transfers": list(lot.transfers),
            "current_custodian": lot.transfers[-1]["to_party"] if lot.transfers else lot.crew_id,
            "damage": [self._observation_lineage(oid) for oid in lot.damage_ids],
            "accounting": {
                "net_weight_kg": lot.net_weight_kg,
                "transferred_kg": lot.transferred_kg,
                "remaining_kg": lot.remaining_kg,
            },
        }

    def _observation_lineage(self, obs_id: str) -> dict[str, Any]:
        obs = self.observations[obs_id]
        return {
            "id": obs.id,
            "branch_code": obs.branch_code,
            "severity": obs.severity,
            "dropped_loss_kg": obs.dropped_loss_kg,
            "observed_by": obs.observed_by,
            "note": obs.note,
            "occurred_at": obs.occurred_at,
            "corrected": obs.corrected,
            "corrections": list(obs.corrections),
        }

    def _care_lineage(self, care_id: str) -> dict[str, Any]:
        care = self.care[care_id]
        return {
            "id": care.id,
            "status": care.status,
            "severity": care.severity,
            "basis_observation_ids": list(care.basis_observation_ids),
            "planned_at": care.planned_at,
            "completed_at": care.completed_at,
            "superseded_by": care.superseded_by,
            "summary": care.completion_summary,
        }

    def conservation_report(self, year: int | None = None) -> dict[str, Any]:
        """管理者视角：果品数量守恒与责任闭环核对。"""
        rows: list[dict[str, Any]] = []
        for tree_id in sorted(self.trees):
            years = {lot.year for lot in self.lots.values() if lot.tree_id == tree_id}
            if year is not None:
                years = {y for y in years if y == year}
            for y in sorted(years):
                rows.append(self._tree_year_conservation(tree_id, y))
        totals = {"harvested_kg": 0.0, "transferred_kg": 0.0,
                  "discarded_kg": 0.0, "isolated_kg": 0.0,
                  "dropped_loss_kg": 0.0}
        by_destination: dict[str, float] = defaultdict(float)
        for row in rows:
            for key in ("harvested_kg", "transferred_kg", "discarded_kg",
                        "isolated_kg", "dropped_loss_kg"):
                totals[key] = _kg(totals[key] + row[key])
            for dest, qty in row["by_destination"].items():
                by_destination[dest] = _kg(by_destination[dest] + qty)
        return {
            "year": year,
            "tree_years": rows,
            "totals": totals,
            "by_destination": dict(by_destination),
            "all_balanced": all(r["balanced"] for r in rows),
            "open_items": {
                "quarantined_lots": [
                    lot.id for lot in self.lots.values() if lot.status == "quarantined"
                    and (year is None or lot.year == year)
                ],
                "unclosed_lots": [
                    lot.id for lot in self.lots.values()
                    if lot.status != "discarded"
                    and (year is None or lot.year == year)
                    and abs(lot.remaining_kg) > 1e-9
                ],
                "open_care": [
                    cid for cid, care in self.care.items()
                    if care.status == "planned" and (year is None or care.year == year)
                ],
            },
        }

    def _tree_year_conservation(self, tree_id: str, year: int) -> dict[str, Any]:
        lots = [self.lots[lid] for lid in self.lot_ids_for_tree_year(tree_id, year)]
        harvested = _kg(sum(lot.net_weight_kg for lot in lots))
        transferred = _kg(sum(lot.transferred_kg for lot in lots))
        discarded = _kg(sum(lot.net_weight_kg for lot in lots if lot.status == "discarded"))
        isolated = _kg(sum(
            lot.net_weight_kg for lot in lots if lot.status == "quarantined"
        ))
        dropped = _kg(sum(
            obs.dropped_loss_kg for obs in self.observations.values()
            if obs.tree_id == tree_id and obs.year == year
        ))
        by_destination: dict[str, float] = defaultdict(float)
        for lot in lots:
            if lot.status == "discarded":
                continue
            for transfer in lot.transfers:
                by_destination[transfer["destination"]] = _kg(
                    by_destination[transfer["destination"]] + transfer["quantity_kg"]
                )
        balanced = abs(harvested - (transferred + discarded + isolated)) <= 1e-6
        return {
            "tree_id": tree_id,
            "year": year,
            "harvested_kg": harvested,
            "transferred_kg": transferred,
            "discarded_kg": discarded,
            "isolated_kg": isolated,
            "dropped_loss_kg": dropped,
            "by_destination": dict(by_destination),
            "balanced": balanced,
            "lot_ids": [lot.id for lot in lots],
        }

    def lot_closure(self, lot_id: str) -> dict[str, Any]:
        """单批次责任闭环核对。"""
        lot = self.lots[lot_id]
        unresolved_damage = []
        open_care_ids = []
        for oid in lot.damage_ids:
            obs = self.observations[oid]
            if not obs.needs_care():
                continue
            covering = [
                cid for cid, care in self.care.items()
                if care.tree_id == obs.tree_id and care.year == obs.year
                and care.status == "completed" and oid in care.basis_observation_ids
            ]
            if not covering:
                unresolved_damage.append(oid)
        for care in self.care.values():
            if care.tree_id == lot.tree_id and care.year == lot.year and care.status == "planned":
                open_care_ids.append(care.id)
        if lot.status == "discarded":
            closed = True
        else:
            closed = (
                lot.destination is not None
                and abs(lot.remaining_kg) <= 1e-9
                and not unresolved_damage
            )
        return {
            "lot_id": lot_id,
            "status": lot.status,
            "destination": lot.destination,
            "net_weight_kg": lot.net_weight_kg,
            "transferred_kg": lot.transferred_kg,
            "remaining_kg": lot.remaining_kg,
            "custody_chain": [
                {"from_party": t["from_party"], "to_party": t["to_party"],
                 "quantity_kg": t["quantity_kg"], "occurred_at": t["occurred_at"]}
                for t in lot.transfers
            ],
            "unresolved_damage_ids": unresolved_damage,
            "open_care_ids": open_care_ids,
            "closed": closed,
        }
