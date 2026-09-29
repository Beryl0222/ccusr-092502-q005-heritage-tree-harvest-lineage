"""责任谱系端到端行为测试。

只使用标准库与临时目录，覆盖：
编号不混淆、离线称重重传幂等、标识冲突隔离、去向唯一、
暂停不影响历史交接、观察纠正只局部重算、重启恢复、守恒与责任闭环。
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.events import DomainError
from src.service import LineageService, QuarantineConflict
from src.store import EventStore


YEAR = 2026


def build_season(service: LineageService) -> dict[str, str]:
    """准备一个成熟季：品种、三棵树、小组、评估、范围、一个批次。"""
    cv_flat = service.register_cultivar("扁柿")
    cv_fire = service.register_cultivar("火柿")
    cv_square = service.register_cultivar("方柿")
    crew_a = service.register_crew("甲组")
    crew_b = service.register_crew("乙组")
    t1 = service.register_tree(cv_flat, "梅竹-01")
    t2 = service.register_tree(cv_fire, "梅竹-02")
    t3 = service.register_tree(cv_square, "烟水渔庄-07")
    service.assess_tree(t1, YEAR, "NORMAL", 120.0, "壮年树")
    service.assess_tree(t2, YEAR, "WEAK", 40.0, "偏弱")
    service.assess_tree(t3, YEAR, "STRONG", 150.0)
    w1 = service.open_window(t1, YEAR, 60.0, ends_on="2026-11-10")
    w2 = service.open_window(t2, YEAR, 20.0)
    w3 = service.open_window(t3, YEAR, 80.0)
    lot1 = service.create_lot(t1, crew_a, YEAR, w1, note="国庆体验场")
    lot2 = service.create_lot(t2, crew_b, YEAR, w2)
    lot3 = service.create_lot(t3, crew_a, YEAR, w3)
    return {
        "cv_flat": cv_flat, "cv_fire": cv_fire, "cv_square": cv_square,
        "crew_a": crew_a, "crew_b": crew_b,
        "t1": t1, "t2": t2, "t3": t3,
        "w1": w1, "w2": w2, "w3": w3,
        "lot1": lot1, "lot2": lot2, "lot3": lot3,
    }


class LineageServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.service = LineageService(EventStore(self.tmp / "events.jsonl"))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def restart(self) -> LineageService:
        return LineageService(EventStore(self.tmp / "events.jsonl"))

    # ---- 编号 -----------------------------------------------------------

    def test_ids_are_unambiguous_and_monotonic(self) -> None:
        ctx = build_season(self.service)
        for identifier, prefix in (
            (ctx["t1"], "TREE"), (ctx["cv_flat"], "CV"), (ctx["crew_a"], "CREW"),
            (ctx["w1"], "WIN"), (ctx["lot1"], "LOT"),
        ):
            self.assertTrue(identifier.startswith(prefix + "-"), identifier)
        wgh = "WGH-2026-000001"
        self.service.record_weighing(ctx["lot1"], wgh, 12.5,
                                     device_id="scale-3", weighed_at="2026-10-01T09:00:00+08:00")
        # 重启后序号继续单调，不与已有编号冲突。
        restarted = self.restart()
        new_lot = restarted.create_lot(
            ctx["t1"], ctx["crew_a"], YEAR, ctx["w1"])
        self.assertNotEqual(new_lot, ctx["lot1"])
        self.assertTrue(new_lot.startswith("LOT-2026-"))

    # ---- 离线称重幂等与冲突 ---------------------------------------------

    def test_offline_weighing_reupload_is_idempotent(self) -> None:
        ctx = build_season(self.service)
        kwargs = dict(weight_kg=18.0, device_id="scale-3",
                      weighed_at="2026-10-01T09:00:00+08:00")
        first = self.service.record_weighing(ctx["lot1"], "WGH-2026-000101", **kwargs)
        # 网络抖动重传：同样的秤端编号与内容，不产生第二条事件。
        second = self.service.record_weighing(ctx["lot1"], "WGH-2026-000101", **kwargs)
        self.assertIs(first, second)
        lot = self.service.view.lots[ctx["lot1"]]
        self.assertEqual(len(lot.weighings), 1)
        self.assertEqual(lot.net_weight_kg, 18.0)

    def test_same_id_different_content_quarantines_without_rewrite(self) -> None:
        ctx = build_season(self.service)
        wgh = "WGH-2026-000202"
        self.service.record_weighing(
            ctx["lot1"], wgh, 15.0, weighed_at="2026-10-02T08:30:00+08:00")
        original = self.service.store.get(wgh)
        with self.assertRaises(QuarantineConflict) as raised:
            self.service.record_weighing(
                ctx["lot1"], wgh, 51.0, weighed_at="2026-10-02T08:30:00+08:00")
        self.assertEqual(raised.exception.reason, "ID_FINGERPRINT_CONFLICT")
        lot = self.service.view.lots[ctx["lot1"]]
        self.assertEqual(lot.status, "quarantined")
        self.assertTrue(lot.quarantines[-1]["id"].startswith("QUAR-"))
        # 首次记录原封不动。
        self.assertEqual(self.service.store.get(wgh), original)
        self.assertEqual(lot.net_weight_kg, 15.0)
        # 隔离期间禁止任何新称重与去向。
        with self.assertRaises(DomainError):
            self.service.record_weighing(
                ctx["lot1"], "WGH-2026-000203", 1.0,
                weighed_at="2026-10-02T09:00:00+08:00")
        with self.assertRaises(DomainError):
            self.service.assign_destination(ctx["lot1"], "VISITOR_PICK")

    def test_exceeding_window_limit_quarantines_lot(self) -> None:
        ctx = build_season(self.service)
        # 弱树 t2 可采范围只有 20kg。
        self.service.record_weighing(
            ctx["lot2"], "WGH-2026-000301", 12.0,
            weighed_at="2026-10-03T09:00:00+08:00")
        with self.assertRaises(QuarantineConflict) as raised:
            self.service.record_weighing(
                ctx["lot2"], "WGH-2026-000302", 10.0,
                weighed_at="2026-10-03T09:10:00+08:00")
        self.assertEqual(raised.exception.reason, "OUT_OF_WINDOW_LIMIT")
        self.assertEqual(self.service.view.lots[ctx["lot2"]].status, "quarantined")
        pending = {p["lot_id"] for p in self.service.pending_quarantines()}
        self.assertIn(ctx["lot2"], pending)

    def test_quarantine_release_resumes_flow(self) -> None:
        ctx = build_season(self.service)
        # 弱树 t2 可采范围只有 20kg：21kg 触发隔离。
        with self.assertRaises(QuarantineConflict):
            self.service.record_weighing(
                ctx["lot2"], "WGH-2026-000401", 21.0,
                weighed_at="2026-10-04T09:00:00+08:00")
        self.service.release_lot(ctx["lot2"], "核查秤具误差，确认实际 19.5kg 内")
        lot = self.service.view.lots[ctx["lot2"]]
        self.assertEqual(lot.status, "released")
        self.assertTrue(lot.quarantines[-1]["resolved"])
        self.service.assign_destination(ctx["lot2"], "VISITOR_PICK")

    # ---- 去向唯一与守恒 -------------------------------------------------

    def test_one_lot_cannot_have_two_destinations(self) -> None:
        ctx = build_season(self.service)
        self.service.record_weighing(
            ctx["lot1"], "WGH-2026-000501", 20.0,
            weighed_at="2026-10-05T09:00:00+08:00")
        self.service.assign_destination(ctx["lot1"], "VISITOR_PICK")
        with self.assertRaises(DomainError):
            self.service.assign_destination(ctx["lot1"], "RESEARCH_SAMPLE")
        with self.assertRaises(DomainError):
            self.service.assign_destination(ctx["lot1"], "DONATION")

    def test_custody_quantities_cannot_exceed_weight(self) -> None:
        ctx = build_season(self.service)
        self.service.record_weighing(
            ctx["lot1"], "WGH-2026-000601", 30.0,
            weighed_at="2026-10-06T09:00:00+08:00")
        self.service.assign_destination(ctx["lot1"], "DONATION")
        self.service.transfer_custody(ctx["lot1"], "公益接收点甲", 18.0)
        with self.assertRaises(DomainError):
            self.service.transfer_custody(ctx["lot1"], "公益接收点乙", 13.0)
        # 错误的交出人不被接受：当前责任人已是接收点甲。
        with self.assertRaises(DomainError):
            self.service.transfer_custody(
                ctx["lot1"], "公益接收点乙", 5.0, from_party=ctx["crew_a"])
        xfer = self.service.transfer_custody(
            ctx["lot1"], "公益接收点乙", 12.0, from_party="公益接收点甲")
        self.assertTrue(xfer.startswith("XFER-"))
        closure = self.service.lot_closure(ctx["lot1"])
        self.assertAlmostEqual(closure["remaining_kg"], 0.0, places=6)

    def test_conservation_report_balances(self) -> None:
        ctx = build_season(self.service)
        # lot1 游客采摘 25kg，分两次交清。
        self.service.record_weighing(
            ctx["lot1"], "WGH-2026-000701", 25.0,
            weighed_at="2026-10-07T09:00:00+08:00")
        self.service.assign_destination(ctx["lot1"], "VISITOR_PICK")
        self.service.transfer_custody(ctx["lot1"], "游客体验台", 25.0)
        # lot3 科研留样 8kg。
        self.service.record_weighing(
            ctx["lot3"], "WGH-2026-000702", 8.0,
            weighed_at="2026-10-07T10:00:00+08:00")
        self.service.assign_destination(ctx["lot3"], "RESEARCH_SAMPLE")
        self.service.transfer_custody(ctx["lot3"], "科研合作组", 8.0)

        report = self.service.conservation_report(YEAR)
        self.assertTrue(report["all_balanced"])
        self.assertEqual(report["totals"]["harvested_kg"], 33.0)
        self.assertEqual(report["totals"]["transferred_kg"], 33.0)
        self.assertEqual(report["by_destination"]["VISITOR_PICK"], 25.0)
        self.assertEqual(report["by_destination"]["RESEARCH_SAMPLE"], 8.0)
        self.assertNotIn("DONATION", report["by_destination"])
        self.assertEqual(report["open_items"]["unclosed_lots"], [])

    # ---- 暂停与历史责任 -------------------------------------------------

    def test_suspension_blocks_future_work_but_keeps_history(self) -> None:
        ctx = build_season(self.service)
        self.service.record_weighing(
            ctx["lot2"], "WGH-2026-000801", 10.0,
            weighed_at="2026-10-08T09:00:00+08:00")
        self.service.assign_destination(ctx["lot2"], "DONATION")
        self.service.transfer_custody(ctx["lot2"], "社区敬老点", 10.0)
        chain_before = self.service.lot_closure(ctx["lot2"])["custody_chain"]

        # 弱树出现异常，暂停之后的作业。
        self.service.set_suspension(ctx["t2"], True, "叶片黄化，树势异常")
        with self.assertRaises(DomainError):
            self.service.create_lot(ctx["t2"], ctx["crew_b"], YEAR, ctx["w2"])
        with self.assertRaises(DomainError):
            self.service.assign_destination(ctx["lot2"], "RESEARCH_SAMPLE")

        # 已完成交接的责任链原样保留，原责任人仍可追溯。
        chain_after = self.service.lot_closure(ctx["lot2"])["custody_chain"]
        self.assertEqual(chain_before, chain_after)
        self.assertEqual(chain_after[0]["from_party"], ctx["crew_b"])
        self.assertEqual(chain_after[0]["to_party"], "社区敬老点")

        # 恢复后可继续作业。
        self.service.set_suspension(ctx["t2"], False, "复评恢复")
        self.assertFalse(self.service.view.is_suspended(ctx["t2"]))

    # ---- 观察纠正与局部重算 ---------------------------------------------

    def test_correction_only_recomputes_related_tree(self) -> None:
        ctx = build_season(self.service)
        self.service.record_weighing(
            ctx["lot1"], "WGH-2026-000901", 14.0,
            weighed_at="2026-10-09T09:00:00+08:00")
        self.service.record_weighing(
            ctx["lot3"], "WGH-2026-000902", 9.0,
            weighed_at="2026-10-09T10:00:00+08:00")
        obs1 = self.service.observe_damage(
            ctx["lot1"], "B-12", "MODERATE", dropped_loss_kg=3.0,
            observed_by=ctx["crew_a"], note="主枝劈裂")
        obs3 = self.service.observe_damage(
            ctx["lot3"], "B-03", "SEVERE", dropped_loss_kg=5.0)

        care_t1 = self.service.view.open_care_for(ctx["t1"], YEAR)
        care_t3 = self.service.view.open_care_for(ctx["t3"], YEAR)
        self.assertIsNotNone(care_t1)
        self.assertEqual(care_t1.severity, "MODERATE")
        self.assertEqual(care_t3.severity, "SEVERE")

        # 纠正 t1 的观察为轻微且无落果：t1 待处理修复计划被取消，t3 不受影响。
        self.service.correct_observation(
            obs1, "现场复核仅为表皮擦伤",
            severity="LIGHT", dropped_loss_kg=0.0)
        self.assertIsNone(self.service.view.open_care_for(ctx["t1"], YEAR))
        self.assertEqual(self.service.view.care[care_t1.id].status, "cancelled")
        self.assertEqual(self.service.view.open_care_for(ctx["t3"], YEAR).id, care_t3.id)

        load_t1 = self.service.annual_load(ctx["t1"], YEAR)
        self.assertEqual(load_t1["harvested_kg"], 14.0)
        self.assertEqual(load_t1["dropped_loss_kg"], 0.0)
        self.assertTrue(load_t1["within_capacity"])
        load_t3 = self.service.annual_load(ctx["t3"], YEAR)
        self.assertEqual(load_t3["dropped_loss_kg"], 5.0)

        # 纠正后的观察仍保留原始记录与更正轨迹。
        obs_view = self.service.view.observations[obs1]
        self.assertEqual(obs_view.severity, "LIGHT")
        self.assertTrue(obs_view.corrected)
        self.assertEqual(len(obs_view.corrections), 1)

    def test_correction_replans_care_basis(self) -> None:
        ctx = build_season(self.service)
        self.service.record_weighing(
            ctx["lot1"], "WGH-2026-001001", 6.0,
            weighed_at="2026-10-10T09:00:00+08:00")
        obs = self.service.observe_damage(ctx["lot1"], "B-01", "MODERATE")
        first_care = self.service.view.open_care_for(ctx["t1"], YEAR)
        self.assertEqual(first_care.basis_observation_ids, [obs])
        # 升级为严重：原待处理计划被新计划取代，依据链可追溯。
        self.service.correct_observation(obs, "复查发现木质部受损", severity="SEVERE")
        new_care = self.service.view.open_care_for(ctx["t1"], YEAR)
        self.assertNotEqual(new_care.id, first_care.id)
        self.assertEqual(new_care.severity, "SEVERE")
        self.assertEqual(self.service.view.care[first_care.id].status, "superseded")
        self.assertEqual(self.service.view.care[first_care.id].superseded_by, new_care.id)
        # 完成新计划后，损伤未决项清零；整批闭环还需果品交接完毕。
        self.service.complete_care(new_care.id, "已做伤口封堵与支撑")
        closure = self.service.lot_closure(ctx["lot1"])
        self.assertEqual(closure["unresolved_damage_ids"], [])
        self.assertFalse(closure["closed"])
        self.service.assign_destination(ctx["lot1"], "VISITOR_PICK")
        self.service.transfer_custody(ctx["lot1"], "游客体验台", 6.0)
        self.assertTrue(self.service.lot_closure(ctx["lot1"])["closed"])

    # ---- 重启恢复 -------------------------------------------------------

    def test_restart_resumes_pending_quarantine_and_care(self) -> None:
        ctx = build_season(self.service)
        self.service.record_weighing(
            ctx["lot1"], "WGH-2026-001101", 7.0,
            weighed_at="2026-10-11T09:00:00+08:00")
        self.service.observe_damage(ctx["lot1"], "B-09", "SEVERE", dropped_loss_kg=2.0)
        with self.assertRaises(QuarantineConflict):
            self.service.record_weighing(
                ctx["lot1"], "WGH-2026-001102", 999.0,
                weighed_at="2026-10-11T09:05:00+08:00")  # 超范围隔离
        pending_quar = self.service.pending_quarantines()
        pending_care = self.service.pending_care()
        self.assertEqual(len(pending_quar), 1)
        self.assertEqual(len(pending_care), 1)

        restarted = self.restart()
        self.assertEqual({p["lot_id"] for p in restarted.pending_quarantines()},
                         {ctx["lot1"]})
        self.assertEqual({p["care_id"] for p in restarted.pending_care()},
                         {p["care_id"] for p in pending_care})
        # 重启后继续处理隔离：放行、完成修复。
        restarted.release_lot(ctx["lot1"], "复核为误读数值")
        restarted.assign_destination(ctx["lot1"], "VISITOR_PICK")
        care_id = restarted.pending_care()[0]["care_id"]
        restarted.complete_care(care_id, "枝条修剪与伤口处理")
        self.assertEqual(restarted.pending_quarantines(), [])
        self.assertEqual(restarted.pending_care(), [])

    # ---- 树木谱系 -------------------------------------------------------

    def test_tree_lineage_traces_every_harvest_impact(self) -> None:
        ctx = build_season(self.service)
        self.service.record_weighing(
            ctx["lot1"], "WGH-2026-001201", 22.0,
            weighed_at="2026-10-12T09:00:00+08:00")
        self.service.observe_damage(
            ctx["lot1"], "B-05", "LIGHT", dropped_loss_kg=1.0,
            observed_by=ctx["crew_a"])
        self.service.assign_destination(ctx["lot1"], "VISITOR_PICK")
        self.service.transfer_custody(ctx["lot1"], "游客体验台", 22.0)

        lineage = self.service.tree_lineage(ctx["t1"])
        self.assertEqual(lineage["tree"]["id"], ctx["t1"])
        self.assertEqual(lineage["cultivar"]["name"], "扁柿")
        annual = lineage["annual"][0]
        self.assertEqual(annual["load"]["harvested_kg"], 22.0)
        lot_line = annual["lots"][0]
        self.assertEqual(lot_line["accounting"]["net_weight_kg"], 22.0)
        self.assertEqual(lot_line["current_custodian"], "游客体验台")
        self.assertEqual(lot_line["damage"][0]["branch_code"], "B-05")
        self.assertEqual(lot_line["lot"]["crew"]["name"], "甲组")

    def test_discard_counts_as_loss_and_cannot_transfer(self) -> None:
        ctx = build_season(self.service)
        self.service.record_weighing(
            ctx["lot3"], "WGH-2026-001301", 4.0,
            weighed_at="2026-10-13T09:00:00+08:00")
        with self.assertRaises(QuarantineConflict):
            self.service.record_weighing(
                ctx["lot3"], "WGH-2026-001302", 80.0,
                weighed_at="2026-10-13T09:05:00+08:00")  # 累计 84kg 超出 80kg 范围
        self.service.discard_lot(ctx["lot3"], "冲突无法核查，整批废弃")
        with self.assertRaises(DomainError):
            self.service.transfer_custody(ctx["lot3"], "任何人", 1.0)
        report = self.service.conservation_report(YEAR)
        row = next(r for r in report["tree_years"] if r["tree_id"] == ctx["t3"])
        self.assertEqual(row["discarded_kg"], 84.0)
        self.assertTrue(row["balanced"])


if __name__ == "__main__":
    unittest.main()
