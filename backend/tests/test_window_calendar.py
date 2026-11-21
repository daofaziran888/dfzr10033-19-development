"""窗口日历交接测试：夏令时回拨/春拨、幂等导出、时区变更重叠归属与调度批次定位。"""
import csv
import io

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store
from app.services import calendar_engine as ce
from app.services.time_window import TimeWindowService
from app.services.schedule_job import ScheduleJobService

client = TestClient(app)
window_svc = TimeWindowService()
job_svc = ScheduleJobService()

FALLBACK_DATE = "2026-11-01"   # 美国夏令时回拨：本地 01:00 出现两次
SPRING_DATE = "2026-03-08"     # 美国春拨：本地 02:00 不存在


def _make_window(width="3600", step="3600", align="local"):
    entry, missing = window_svc.create_entry({
        "窗口编号": f"TIME-T{len(store.rows('time_window')):04d}",
        "窗口类型": "滑动", "滑动宽度": width, "滑动步长": step,
        "对齐基准": align, "缺值策略": "fill_prev", "窗口别名": "dst_test", "最后计算": "-"})
    assert entry is not None, missing
    return entry["id"]


def _make_job(tz_name):
    entry, missing = job_svc.create_entry({
        "任务编号": f"SCHE-T{len(store.rows('schedule_job')):04d}",
        "目标立方": "1", "Cron表达式": "0 * * * *", "时区设置": tz_name,
        "依赖检查": "none", "超时上限": "600", "失败通知": "email", "下次执行": "-"})
    assert entry is not None, missing
    return entry["id"]


def _export(window_id, job_id, start=FALLBACK_DATE, count=24):
    result = window_svc.export_computation_calendar(
        window_id, job_id, start_date=start, count=count)
    assert result["ok"], result["message"]
    return result


class TestFallbackCalendar:
    def test_repeated_local_hour_distinguished_and_unique_utc(self):
        """回拨日：重复本地小时拆成两行，UTC 边界各自唯一。"""
        window_id = _make_window()
        job_id = _make_job("America/New_York")
        result = _export(window_id, job_id)
        assert result["created"] and result["schedule"]["version"] == 1

        entries = result["entries"]
        repeated = [e for e in entries if e["local_start"] == "2026-11-01 01:00:00"]
        assert len(repeated) == 2
        assert {e["occurrence"] for e in repeated} == {"第1次", "第2次"}
        first, second = sorted(repeated, key=lambda e: e["utc_start"])
        assert first["utc_start"] == "2026-11-01T05:00:00+00:00"
        assert second["utc_start"] == "2026-11-01T06:00:00+00:00"

        executable = [e for e in entries if e["entry_status"] == "待领取"]
        starts = [e["utc_start"] for e in executable]
        assert len(starts) == len(set(starts)) == 25  # 24 个本地步长 + 1 次重复
        assert result["stats"]["repeated"] == 2

    def test_download_file_distinguishes_repeated_hours(self):
        """下载的日历文件用「本地出现」列区分重复小时。"""
        window_id = _make_window()
        job_id = _make_job("America/New_York")
        result = _export(window_id, job_id)
        sid = result["schedule"]["id"]
        resp = client.get(f"/api/pipelines/window-calendar/download?schedule_id={sid}")
        assert resp.status_code == 200
        assert "window_calendar" in resp.headers["content-disposition"]
        rows = list(csv.reader(io.StringIO(resp.text)))
        header, data = rows[0], rows[1:]
        assert header[6:9] == ["本地开始", "本地结束", "本地出现"]
        repeated = [r for r in data if r[6] == "2026-11-01 01:00:00"]
        assert len(repeated) == 2
        assert {r[8] for r in repeated} == {"第1次", "第2次"}
        assert repeated[0][9] != repeated[1][9]  # UTC 开始各自唯一


class TestSpringForwardGap:
    def test_missing_local_period_has_no_fake_utc(self):
        """春拨日：缺失的本地时段不生成虚假的 UTC 区间。"""
        window_id = _make_window()
        job_id = _make_job("America/New_York")
        result = _export(window_id, job_id, start=SPRING_DATE)
        entries = result["entries"]
        missing = [e for e in entries if e["period_status"] == "本地缺失"]
        assert len(missing) == 1
        assert missing[0]["local_start"] == "2026-03-08 02:00:00"
        assert missing[0]["utc_start"] == "" and missing[0]["utc_end"] == ""
        assert missing[0]["entry_status"] == "不可调度"
        executable = [e for e in entries if e["entry_status"] == "待领取"]
        assert len(executable) == 23
        assert not any(e["local_start"] == "2026-03-08 02:00:00" and e["utc_start"]
                       for e in entries)


class TestIdempotentExport:
    def test_duplicate_submission_creates_single_schedule(self):
        """重复提交只创建一份日历安排。"""
        window_id = _make_window()
        job_id = _make_job("America/New_York")
        first = _export(window_id, job_id)
        before = len(ce.schedules())
        second = _export(window_id, job_id)
        assert first["created"] and not second["created"]
        assert second["schedule"]["id"] == first["schedule"]["id"]
        assert len(ce.schedules()) == before
        resp = client.post("/api/pipelines/window-calendar-export", params={
            "window_id": window_id, "job_id": job_id,
            "start_date": FALLBACK_DATE, "count": 24})
        assert resp.status_code == 200
        assert resp.json()["created"] is False
        assert len(ce.schedules()) == before


class TestTimezoneUpdateOverlap:
    def test_overlap_ownership_and_history_preserved(self):
        """时区更新后新旧日历重叠：归属可解释，历史条目与批次保留。"""
        window_id = _make_window()
        job_id = _make_job("America/New_York")
        v1 = _export(window_id, job_id)
        s1 = v1["schedule"]["id"]
        claim = job_svc.claim_from_calendar(job_id, s1, claim_count=2)
        assert claim["ok"] and len(claim["claimed"]) == 2
        run = job_svc.run_claimed_batches(job_id)
        assert len(run["batches"]) == 2
        v1_entries = {e["id"]: dict(e) for e in ce.calendar_entries(s1)}
        batch_count = len(ce.batches())

        # 窗口所属时区更新，历史窗口仍有任务尚未完成 → 新旧两版日历重叠
        store.find("schedule_job", job_id)["时区设置"] = "Asia/Shanghai"
        v2 = _export(window_id, job_id)
        assert v2["created"] and v2["schedule"]["version"] == 2
        assert v2["schedule"]["supersedes"] == s1

        # 与 v1 已完成时段重叠的 v2 条目让渡给历史版本
        yielded = [e for e in v2["entries"] if e["entry_status"] == "已让渡"]
        assert len(yielded) == 2
        for entry in yielded:
            assert entry["owner_schedule_id"] == s1
            owner = v1_entries[entry["owner_entry_id"]]
            assert owner["entry_status"] == "已完成"
            assert ce.intervals_overlap(entry["utc_start"], entry["utc_end"],
                                        owner["utc_start"], owner["utc_end"])
        assert any(note["resolution"] == "让渡历史版本" for note in v2["overlaps"])

        # 历史兼容：v1 已领取/已完成条目与实际运行批次全部保留
        for entry_id, before in v1_entries.items():
            after = next(e for e in ce.calendar_entries(s1) if e["id"] == entry_id)
            assert after["entry_status"] == before["entry_status"]
            assert after["batch_no"] == before["batch_no"]
        assert len(ce.batches()) == batch_count

        # 领取 v2：让渡条目不会被再次领取，领取区间不与历史批次重叠
        claim2 = job_svc.claim_from_calendar(job_id, v2["schedule"]["id"], claim_count=50)
        assert claim2["claimed"]
        for entry in claim2["claimed"]:
            for batch in ce.batches():
                if batch["window_id"] == window_id:
                    assert not ce.intervals_overlap(
                        entry["utc_start"], entry["utc_end"],
                        batch["utc_start"], batch["utc_end"])
        job_svc.run_claimed_batches(job_id)
        # 全局不变量：同一窗口不存在跨版本重叠的已完成批次
        done = [b for b in ce.batches() if b["window_id"] == window_id]
        for i, a in enumerate(done):
            for b in done[i + 1:]:
                if a["schedule_id"] == b["schedule_id"]:
                    continue
                assert not ce.intervals_overlap(a["utc_start"], a["utc_end"],
                                                b["utc_start"], b["utc_end"])


class TestNoDoubleCounting:
    def test_same_result_cannot_count_for_two_periods(self):
        """同一份计算结果不可被当成两个时段的已完成产出。"""
        window_id = _make_window()
        job_id = _make_job("America/New_York")
        v1 = _export(window_id, job_id)
        s1 = v1["schedule"]["id"]
        job_svc.claim_from_calendar(job_id, s1, claim_count=1)
        run = job_svc.run_claimed_batches(job_id)
        assert len(run["batches"]) == 1

        store.find("schedule_job", job_id)["时区设置"] = "Asia/Shanghai"
        v2 = _export(window_id, job_id)
        yielded = [e for e in v2["entries"] if e["entry_status"] == "已让渡"]
        assert len(yielded) == 1
        target = yielded[0]

        # 模拟脏数据：让渡条目被强行置为已领取，调度器仍不得重复写批
        target["entry_status"] = "已领取"
        target["claimed_by"] = job_id
        run2 = job_svc.run_claimed_batches(job_id)
        assert len(run2["batches"]) == 0
        assert any(s["entry_id"] == target["id"] for s in run2["skipped"])
        assert target["entry_status"] == "已让渡"
        assert not any(b["entry_id"] == target["id"] for b in ce.batches())
        assert len([b for b in ce.batches() if b["window_id"] == window_id]) == 1


class TestClaimRunPipeline:
    def test_api_export_claim_run_and_locate_batches(self):
        """导出 → 领取 → 写批 全管道：执行起点、清单同步、调度记录定位。"""
        window_id = _make_window()
        job_id = _make_job("America/New_York")
        resp = client.post("/api/pipelines/window-calendar-export", params={
            "window_id": window_id, "job_id": job_id,
            "start_date": FALLBACK_DATE, "count": 24})
        assert resp.status_code == 200
        exported = resp.json()
        assert exported["ok"] and exported["created"]
        sid = exported["schedule"]["id"]

        resp = client.post("/api/pipelines/schedule-claim-run", params={
            "job_id": job_id, "schedule_id": sid, "claim_count": 3})
        assert resp.status_code == 200
        payload = resp.json()
        claim, run = payload["claim"], payload["run"]
        # 定时计算读入日历文件确定执行起点 = 首个待领取时段
        assert claim["start"]["local_start"] == "2026-11-01 00:00:00"
        assert len(claim["claimed"]) == 3
        # 领取结果同步到日历清单，调度器按时段写入实际计算批
        manifest = window_svc.calendar_manifest(sid)
        done = [e for e in manifest["entries"] if e["entry_status"] == "已完成"]
        assert len(done) == 3 and all(e["batch_no"] for e in done)
        assert len(run["batches"]) == 3
        # 从调度记录定位执行批次
        records = job_svc.dispatch_records(job_id=job_id)
        assert records["total"] == 3
        assert {r["batch_no"] for r in records["records"]} == \
               {b["batch_no"] for b in run["batches"]}
        audits = [a for a in store.rows("query_audit")
                  if a.get("数据来源") == "schedule_job"
                  and f"job={job_id}" in a.get("查询语句", "")]
        assert len(audits) == 3
        # 下次执行推进到第 4 个时段（回拨日 02:00 本地 = UTC 07:00）
        assert store.find("schedule_job", job_id)["下次执行"] == "2026-11-01T07:00:00+00:00"


class TestTwoFallbackCalendars:
    def test_two_calendars_unique_utc_and_locatable_batches(self):
        """两份回拨日历：重复本地时间可区分、UTC 边界唯一、批次各自可定位。"""
        w1 = _make_window()
        j1 = _make_job("America/New_York")
        w2 = _make_window()
        j2 = _make_job("America/Chicago")
        r1 = _export(w1, j1)
        r2 = _export(w2, j2)
        s1, s2 = r1["schedule"]["id"], r2["schedule"]["id"]
        assert s1 != s2

        for result in (r1, r2):
            repeated = [e for e in result["entries"] if e["occurrence"] in ("第1次", "第2次")]
            assert len(repeated) == 2
            executable = [e for e in result["entries"] if e["entry_status"] == "待领取"]
            starts = [e["utc_start"] for e in executable]
            assert len(starts) == len(set(starts))
        # 同一本地重复小时，两个时区映射到不同的 UTC 边界
        ny_first = next(e for e in r1["entries"] if e["occurrence"] == "第1次")
        chi_first = next(e for e in r2["entries"] if e["occurrence"] == "第1次")
        assert ny_first["local_start"] == chi_first["local_start"] == "2026-11-01 01:00:00"
        assert ny_first["utc_start"] != chi_first["utc_start"]

        job_svc.claim_from_calendar(j1, s1, claim_count=4)
        run1 = job_svc.run_claimed_batches(j1)
        job_svc.claim_from_calendar(j2, s2, claim_count=4)
        run2 = job_svc.run_claimed_batches(j2)
        assert len(run1["batches"]) == 4 and len(run2["batches"]) == 4

        rec1 = job_svc.dispatch_records(schedule_id=s1)
        rec2 = job_svc.dispatch_records(schedule_id=s2)
        assert rec1["total"] == 4 and rec2["total"] == 4
        assert all(r["schedule_id"] == s1 for r in rec1["records"])
        assert all(r["schedule_id"] == s2 for r in rec2["records"])
        assert {r["batch_no"] for r in rec1["records"]}.isdisjoint(
            {r["batch_no"] for r in rec2["records"]})
