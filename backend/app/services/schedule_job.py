"""定时计算领域服务：预计算任务维护 + 窗口日历消费。

定时计算读入可交接的窗口日历文件确定执行起点，领取结果同步回日历清单，
再由调度器按时段写入实际计算批；跨版本重叠时段不得重复计入产出。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services import calendar_engine as ce
from app.services.base import BaseModuleService
from app.store import store


class ScheduleJobService(BaseModuleService):
    MODULE = "schedule_job"
    ENTITY = "预计算任务"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["任务编号", "目标立方", "Cron表达式", "时区设置", "依赖检查", "超时上限", "失败通知", "下次执行"]
    REQUIRED_FIELDS = ["任务编号", "目标立方", "Cron表达式"]
    STATUS_ORDER = ["待激活", "计算中", "已完成", "已停用"]
    ACTION_RULES = {"激活任务": "计算中", "立即执行": "已完成", "停用任务": "已停用"}
    NEGATIVE_ACTIONS = ["停用任务"]

    def claim_from_calendar(
        self,
        job_id: int,
        schedule_id: int | None = None,
        claim_count: int = 1,
    ) -> dict[str, Any]:
        """读入可交接的窗口日历文件，确定执行起点并领取时段。

        执行起点 = 日历中首个「待领取」的可执行时段；与其他版本已领取/
        已完成条目重叠的时段直接让渡，领取结果同步回日历清单。
        """
        job = store.find(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": f"预计算任务 {job_id} 不存在或已归档"}
        if schedule_id is not None:
            schedule = ce.find_schedule(schedule_id)
            if schedule is None:
                return {"ok": False, "message": f"窗口日历 {schedule_id} 不存在"}
            if int(schedule["job_id"]) != int(job_id):
                return {"ok": False, "message": f"窗口日历 {schedule_id} 不属于任务 {job_id}，不能交接"}
        else:
            candidates = [s for s in ce.schedules() if int(s["job_id"]) == int(job_id)]
            if not candidates:
                return {"ok": False, "message": "没有可交接的窗口日历文件，请先从时序窗口导出"}
            schedule = max(candidates, key=lambda s: (int(s.get("version", 0)), int(s["id"])))

        entries = sorted(ce.calendar_entries(schedule["id"]),
                         key=lambda e: (e.get("utc_start") or "9999", e["seq"]))
        claimed: list[dict[str, Any]] = []
        yielded: list[dict[str, Any]] = []
        for entry in entries:
            if len(claimed) >= max(1, int(claim_count)):
                break
            if entry.get("entry_status") != "待领取":
                continue
            owner = ce.find_claimed_owner(entry)
            if owner is not None:
                entry["entry_status"] = "已让渡"
                entry["owner_entry_id"] = owner["id"]
                entry["owner_schedule_id"] = owner["schedule_id"]
                yielded.append(entry)
                continue
            entry["entry_status"] = "已领取"
            entry["claimed_by"] = job_id
            entry["claimed_at"] = ce.now_iso()
            claimed.append(entry)

        nxt = next((e for e in entries if e.get("entry_status") == "待领取"), None)
        job["下次执行"] = nxt["utc_start"] if nxt else "-"
        if claimed and job.get("status") == "待激活":
            job["status"] = "计算中"
            job["pending"] = True
        message = f"已领取 {len(claimed)} 个执行时段"
        if yielded:
            message += f"，{len(yielded)} 个重叠时段让渡历史版本"
        return {
            "ok": True,
            "message": message,
            "schedule_id": schedule["id"],
            "version": schedule["version"],
            "start": claimed[0] if claimed else None,
            "claimed": claimed,
            "yielded": yielded,
        }

    def run_claimed_batches(self, job_id: int) -> dict[str, Any]:
        """调度器按已领取时段写入实际计算批。

        同一窗口其他版本日历若已有覆盖该时段的完成批次，则本条目让渡，
        同一份计算结果不可被当成两个时段的已完成产出。
        """
        job = store.find(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": f"预计算任务 {job_id} 不存在或已归档"}
        claimed = [e for e in ce.calendar_entries()
                   if e.get("claimed_by") == job_id and e.get("entry_status") == "已领取"]
        claimed.sort(key=lambda e: (e.get("utc_start") or "", e["seq"]))
        made: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for entry in claimed:
            conflict = _find_completed_conflict(entry)
            if conflict is not None:
                entry["entry_status"] = "已让渡"
                entry["owner_entry_id"] = conflict["entry_id"]
                entry["owner_schedule_id"] = conflict["schedule_id"]
                skipped.append({
                    "entry_id": entry["id"],
                    "reason": f"时段已被批次 {conflict['batch_no']} 覆盖，"
                              "同一计算结果不可当成两个时段的已完成产出",
                })
                continue
            batch_no = f"BAT-{entry['schedule_id']:04d}-{entry['id']:05d}"
            if any(b["batch_no"] == batch_no for b in ce.batches()):
                continue  # 该时段已写批，重复触发幂等跳过
            batch = {
                "id": ce.next_id(ce.batches()),
                "batch_no": batch_no,
                "job_id": job_id,
                "schedule_id": entry["schedule_id"],
                "entry_id": entry["id"],
                "window_id": entry["window_id"],
                "utc_start": entry["utc_start"],
                "utc_end": entry["utc_end"],
                "result_hash": hashlib.md5(
                    f"{entry['window_id']}|{entry['utc_start']}|{entry['utc_end']}".encode("utf-8")
                ).hexdigest()[:16],
                "status": "已完成",
                "created_at": ce.now_iso(),
            }
            ce.batches().append(batch)
            entry["entry_status"] = "已完成"
            entry["batch_no"] = batch_no
            made.append(batch)
            _audit_schedule_batch(batch)
        if made:
            window = store.find("time_window", made[-1]["window_id"])
            if window is not None:
                window["最后计算"] = made[-1]["utc_end"]
            job_schedule_ids = {s["id"] for s in ce.schedules() if int(s["job_id"]) == int(job_id)}
            remaining = [e for e in ce.calendar_entries()
                         if e["schedule_id"] in job_schedule_ids
                         and e.get("entry_status") in ("待领取", "已领取")]
            remaining.sort(key=lambda e: (e.get("utc_start") or "", e["seq"]))
            job["status"] = "计算中" if remaining else "已完成"
            job["pending"] = bool(remaining)
            job["下次执行"] = remaining[0]["utc_start"] if remaining else "-"
        return {"ok": True, "message": f"写入 {len(made)} 个实际计算批",
                "batches": made, "skipped": skipped}

    def dispatch_records(
        self,
        job_id: int | None = None,
        schedule_id: int | None = None,
    ) -> dict[str, Any]:
        """调度记录：按任务或日历版本定位各自的实际计算批。"""
        versions = {s["id"]: s for s in ce.schedules()}
        records: list[dict[str, Any]] = []
        for batch in ce.batches():
            if job_id is not None and batch["job_id"] != job_id:
                continue
            if schedule_id is not None and batch["schedule_id"] != schedule_id:
                continue
            schedule = versions.get(batch["schedule_id"], {})
            records.append({
                **batch,
                "version": schedule.get("version"),
                "timezone": schedule.get("timezone"),
                "window_no": schedule.get("window_no"),
                "job_no": schedule.get("job_no"),
            })
        records.sort(key=lambda r: r["id"])
        return {"ok": True, "total": len(records), "records": records}


def _find_completed_conflict(entry: dict[str, Any]) -> dict[str, Any] | None:
    """同一窗口其他版本日历中，已有完成批次覆盖该时段则冲突（防重复计入产出）。"""
    for batch in ce.batches():
        if batch["window_id"] != entry["window_id"] or batch["entry_id"] == entry["id"]:
            continue
        if batch["schedule_id"] == entry["schedule_id"]:
            continue  # 同一份日历内部的滑动窗口允许相邻重叠
        if batch.get("status") != "已完成":
            continue
        if ce.intervals_overlap(entry["utc_start"], entry["utc_end"],
                                batch["utc_start"], batch["utc_end"]):
            return batch
    return None


def _audit_schedule_batch(batch: dict[str, Any]) -> None:
    """把实际计算批写进查询审计，作为可检索的调度记录。"""
    rows = store.rows("query_audit")
    rows.append({
        "id": ce.next_id(rows),
        "记录编号": f"AUDIT-{batch['batch_no']}",
        "查询语句": f"SCHEDULE job={batch['job_id']} batch={batch['batch_no']} entry={batch['entry_id']}",
        "执行耗时": "0ms",
        "扫描行数": "1",
        "返回行数": "1",
        "数据来源": "schedule_job",
        "操作用户": "scheduler",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    })
