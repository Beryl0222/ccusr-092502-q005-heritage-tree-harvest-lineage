"""采后责任谱系端到端行为测试。"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.domain import DomainError, LineageService
from src.ids import IdError, make_id, parse_id
from src.projections import fold
from src.store import DuplicateEventError, EventStore, VersionConflictError

T = "2026-09-25T10:00:00+08:00"


def make_service(path: str | Path) -> LineageService:
    return LineageService(EventStore(path))


def seed_season(svc: LineageService) -> None:
    """铺好一个成熟季：三个品种、若干树、两个班组、评估与可采范围。"""
    svc.register_cultivar("bianshi", "扁柿")
    svc.register_cultivar("huoshi", "火柿")
    svc.register_cultivar("fangshi", "方柿")
    svc.register_crew("team-a", "采护一组", "周组长")
    svc.register_crew("team-b", "采护二组", "吴组长")
    svc.register_tree("0001", "bianshi", "东区1号扁柿")
    svc.register_tree("0002", "huoshi", "中区2号火柿")
    svc.register_tree("0003", "fangshi", "西区3号方柿")
    for tree, cap in (("0001", 100.0), ("0002", 80.0), ("0003", 60.0)):
        svc.assess_tree(f"{tree}-a2026", tree, "2026", cap, "林技师", occurred_at=T)
        svc.grant_picking_range(f"{tree}-r2026", tree, "2026",
                                "2026-09-20", "2026-10-20", cap * 0.8,
                                occurred_at=T)


class FullSeasonTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = make_service(Path(self.tmp.name) / "events.jsonl")
        seed_season(self.svc)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_full_flow_lineage_and_conservation(self) -> None:
        svc = self.svc
        svc.start_operation("op-1", "0001", "team-a", "0001-r2026",
                            "2026", 50.0, occurred_at=T)

        # 首次称重 + 离线重传：幂等，不重复计重。
        first = svc.weigh_lot("b001", "op-1", 20.0, "scale-7", "rec-1001",
                              occurred_at=T)
        self.assertTrue(first.appended)
        again = svc.weigh_lot("b001", "op-1", 20.0, "scale-7", "rec-1001",
                              occurred_at=T)
        self.assertFalse(again.appended)
        self.assertEqual(again.event.event_id, first.event.event_id)
        load = svc.lineage.annual_load("tree-0001", "2026")
        self.assertEqual(load["weighed_kg"], 20.0)

        # 同一离线记录被另一台流程冒认为新批次 b002：先隔离，不改旧记录。
        clash = svc.weigh_lot("b002", "op-1", 19.5, "scale-7", "rec-1001",
                              occurred_at=T)
        self.assertEqual(clash.side_effects[0].event_type, "BATCH_QUARANTINED")
        self.assertEqual(svc.lineage.batches["batch-b001"].status, "HELD")
        self.assertEqual(svc.lineage.batches["batch-b002"].status, "QUARANTINED")
        # 隔离批次不计入准入，但仍计入该树年度负荷（果实已离树）。
        self.assertEqual(svc.lineage.annual_load("tree-0001", "2026")["weighed_kg"],
                         39.5)

        # 核查后准入并登记去向；去向互斥。
        svc.admit_batch("b001", "复核秤量与封签一致", occurred_at=T)
        svc.assign_destination("b001", "VISITOR_PICKING", occurred_at=T)
        with self.assertRaises(DomainError):
            svc.assign_destination("b001", "RESEARCH_SAMPLE", occurred_at=T)

        # 交接链：采收班组交出，重量必须等于批次重量。
        with self.assertRaises(DomainError):
            svc.transfer_custody("x1-bad", "b001", "team-b", "VISITOR_BAY",
                                 "郑游客", 18.0, occurred_at=T)
        svc.transfer_custody("x1", "b001", "crew-team-a", "VISITOR_BAY", "郑接待",
                             20.0, occurred_at=T)
        with self.assertRaises(DomainError):
            svc.transfer_custody("x2-bad", "b001", "team-a", "STALL-2",
                                 "钱摊主", 20.0, occurred_at=T)
        svc.transfer_custody("x2", "b001", "VISITOR_BAY", "VISITOR_PICKING",
                             "钱摊主", 20.0, occurred_at=T)

        # 落果与枝条损伤观察。
        svc.record_dropped_fruit("op-1", 3.0, "风雨后自然落果",
                                 event_token="drop-1", occurred_at=T)
        svc.observe_damage("obs-1", "0001", "branch-A2", "MODERATE", 2.0,
                           "2026", op_token="op-1", occurred_at=T)
        svc.plan_care("care-1", "0001", "2026", ["obs-1"], occurred_at=T)

        lineage = svc.lineage.tree_lineage("tree-0001")
        self.assertEqual(lineage["loads"]["2026"]["total_kg"], 39.5 + 3.0 + 2.0)
        self.assertEqual(len(lineage["operations"][0]["batches"]), 2)

        # 数量守恒：39.5 称重 = 隔离 19.5 + 准入 20.0；准入 20 已全部分配。
        report = svc.lineage.conservation_report("2026")
        self.assertTrue(report.balanced, report.discrepancies)
        self.assertEqual(report.weighed_kg, 39.5)
        self.assertEqual(report.quarantined_kg, 19.5)
        self.assertEqual(report.admitted_kg, 20.0)
        self.assertEqual(report.stock_kg, 0.0)
        self.assertEqual(report.by_channel["VISITOR_PICKING"], 20.0)

        # 责任闭环：b001 链路闭合，b002 仍隔离无去向。
        closure = {c.batch_id: c for c in svc.lineage.responsibility_report("2026")}
        self.assertTrue(closure["batch-b001"].closed, closure["batch-b001"].gaps)
        self.assertFalse(closure["batch-b002"].closed)

    def test_anomaly_pauses_future_work_but_keeps_history(self) -> None:
        svc = self.svc
        svc.start_operation("op-1", "0002", "team-a", "0002-r2026",
                            "2026", 30.0, occurred_at=T)
        svc.weigh_lot("b100", "op-1", 15.0, "scale-1", "rec-1", occurred_at=T)
        svc.admit_batch("b100", "无误", occurred_at=T)
        svc.assign_destination("b100", "PUBLIC_DONATION", occurred_at=T)
        svc.transfer_custody("x100", "b100", "crew-team-a", "PUBLIC_DONATION", "孙公益",
                             15.0, occurred_at=T)

        events = svc.flag_tree_anomaly("0002", "主干流胶，树势异常",
                                       occurred_at=T)
        self.assertEqual(events[1].event_type, "OPERATION_PAUSED")
        self.assertEqual(svc.lineage.operations["op-op-1"].status, "PAUSED")

        # 之后的作业与称重全部被拦下。
        with self.assertRaises(DomainError):
            svc.start_operation("op-2", "0002", "team-b",
                                "0002-r2026", "2026", 10.0, occurred_at=T)
        with self.assertRaises(DomainError):
            svc.weigh_lot("b101", "op-1", 5.0, "scale-1", "rec-2", occurred_at=T)

        # 已完成的交接仍指向原责任人。
        batch = svc.lineage.batches["batch-b100"]
        self.assertEqual(batch.transfers[0].from_party, "crew-team-a")
        self.assertEqual(batch.transfers[0].custodian, "孙公益")

    def test_correction_only_recomputes_related_tree(self) -> None:
        svc = self.svc
        svc.start_operation("op-1", "0003", "team-b", "0003-r2026",
                            "2026", 20.0, occurred_at=T)
        svc.weigh_lot("b200", "op-1", 10.0, "scale-2", "rec-9", occurred_at=T)
        svc.observe_damage("obs-9", "0003", "branch-B1", "SEVERE", 6.0,
                           "2026", op_token="op-1", occurred_at=T)
        svc.plan_care("care-9", "0003", "2026", ["obs-9"], occurred_at=T)
        before = svc.lineage.annual_load("tree-0003", "2026")
        self.assertEqual(before["damage_loss_kg"], 6.0)
        self.assertEqual(svc.lineage.care_actions["care-care-9"].severity, "SEVERE")

        # 纠正：原观察事件保留，新事件覆盖结论；修复计划随之降级重算。
        svc.correct_observation("obs-9", "branch-B1", "LIGHT", 1.0,
                                "现场复核为表皮擦伤", occurred_at=T)
        after = svc.lineage.annual_load("tree-0003", "2026")
        self.assertEqual(after["damage_loss_kg"], 1.0)
        self.assertEqual(after["total_kg"], 11.0)
        care = svc.lineage.care_actions["care-care-9"]
        self.assertEqual(care.severity, "LIGHT")
        self.assertEqual(care.revisions, 1)

        # 旧事实未被改写：日志里仍能找到原始的 SEVERE 观察。
        raw = fold(svc.store.all_events())
        original = next(
            e for e in svc.store.all_events()
            if e.event_type == "DAMAGE_OBSERVED" and e.aggregate_id == "obs-obs-9"
        )
        self.assertEqual(original.payload["severity"], "SEVERE")
        self.assertEqual(raw.observations["obs-obs-9"].severity, "LIGHT")

        # 完成修复后再纠正，不改变已闭环修复的责任人。
        svc.complete_care("care-9", "team-b", "支护修剪完成", occurred_at=T)
        svc.correct_observation("obs-9", "branch-B1", "MODERATE", 2.0,
                                "秋后复查", occurred_at=T)
        done = svc.lineage.care_actions["care-care-9"]
        self.assertEqual(done.status, "COMPLETED")
        self.assertEqual(done.completed_by, "crew-team-b")
        self.assertEqual(done.revisions, 1)

    def test_caps_block_overload(self) -> None:
        svc = self.svc
        svc.start_operation("op-1", "0003", "team-a", "0003-r2026",
                            "2026", 48.0, occurred_at=T)
        svc.weigh_lot("b300", "op-1", 47.0, "scale-3", "rec-a", occurred_at=T)
        # 可采范围 48kg，再称 2kg 超范围。
        with self.assertRaises(DomainError):
            svc.weigh_lot("b301", "op-1", 2.0, "scale-3", "rec-b", occurred_at=T)

    def test_typed_ids_reject_cross_kind_use(self) -> None:
        with self.assertRaises(IdError):
            parse_id("tree-0001", "harvest_lot")
        with self.assertRaises(IdError):
            make_id("harvest_lot", "bad token!")
        self.assertEqual(str(make_id("tree_record", "7")), "tree-7")


class RestartRecoveryTest(unittest.TestCase):
    def test_pending_quarantine_and_care_resume_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            svc = make_service(path)
            seed_season(svc)
            svc.start_operation("op-1", "0001", "team-a",
                                "0001-r2026", "2026", 40.0, occurred_at=T)
            # 冲突批次进入隔离；另有一条观察产生待修复计划。
            svc.weigh_lot("b001", "op-1", 10.0, "scale-7", "rec-1", occurred_at=T)
            svc.weigh_lot("b002", "op-1", 10.0, "scale-7", "rec-1", occurred_at=T)
            svc.observe_damage("obs-1", "0001", "branch-C", "MODERATE",
                               1.5, "2026", op_token="op-1", occurred_at=T)
            svc.plan_care("care-1", "0001", "2026", ["obs-1"], occurred_at=T)

            # 服务重启：从日志完整重建，待处理事项原样找回。
            restarted = make_service(path)
            pending = restarted.lineage.pending_work()
            self.assertEqual([b.id for b in pending["quarantined_batches"]],
                             ["batch-b002"])
            self.assertEqual([c.id for c in pending["planned_care"]],
                             ["care-care-1"])

            # 幂等键也随状态恢复：重传仍不重复计数。
            again = restarted.weigh_lot("b001", "op-1", 10.0, "scale-7",
                                        "rec-1", occurred_at=T)
            self.assertFalse(again.appended)

            # 继续处理隔离与修复，闭环。
            restarted.reject_batch("b002", "确认为重复冒认，拒收", occurred_at=T)
            restarted.admit_batch("b001", "核查无误", occurred_at=T)
            restarted.assign_destination("b001", "RESEARCH_SAMPLE", occurred_at=T)
            restarted.transfer_custody("x1", "b001", "crew-team-a", "RESEARCH_LAB",
                                       "林博士", 10.0, occurred_at=T)
            restarted.complete_care("care-1", "team-a", "枝条修剪支护完成",
                                    occurred_at=T)
            report = restarted.lineage.conservation_report("2026")
            self.assertTrue(report.balanced, report.discrepancies)
            self.assertEqual(report.rejected_kg, 10.0)
            self.assertEqual(restarted.lineage.pending_work()["planned_care"], [])

            # 日志行均为合法 JSON，且可再次重建。
            lines = [l for l in path.read_text(encoding="utf-8").splitlines() if l]
            self.assertTrue(lines)
            for line in lines:
                json.loads(line)


class StoreGuardTest(unittest.TestCase):
    def test_duplicate_event_id_with_other_content_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = EventStore(Path(tmp) / "e.jsonl")
            from src.events import build_event
            e1 = build_event("evt-x", "TREE_REGISTERED", "tree-1", 1,
                             {"cultivar_id": "cultivar-c", "name": "甲"})
            store.append(e1)
            e2 = build_event("evt-x", "TREE_REGISTERED", "tree-1", 2,
                             {"cultivar_id": "cultivar-c", "name": "乙"})
            with self.assertRaises(DuplicateEventError):
                store.append(e2)

    def test_aggregate_version_slot_is_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = EventStore(Path(tmp) / "e.jsonl")
            from src.events import build_event
            store.append(build_event("evt-a", "TREE_REGISTERED", "tree-1", 1,
                                     {"cultivar_id": "cultivar-c", "name": "甲"}))
            with self.assertRaises(VersionConflictError):
                store.append(build_event("evt-b", "TREE_ASSESSED", "tree-1", 1,
                                         {"tree_id": "tree-1", "year": "2026",
                                          "load_capacity_kg": 1.0,
                                          "assessor": "z"}))


if __name__ == "__main__":
    unittest.main()
