"""时序窗口领域服务：窗口定义维护 + 可交接计算日历的导出。

日历导出用时序窗口配置（宽度/步长/对齐基准）和定时任务时区数据，
生成带每个执行时段起止时间的窗口日历文件，交接给定时计算消费。
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.services import calendar_engine as ce
from app.services.base import BaseModuleService
from app.store import store


class TimeWindowService(BaseModuleService):
    MODULE = "time_window"
    ENTITY = "窗口定义"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["窗口编号", "窗口类型", "滑动宽度", "滑动步长", "对齐基准", "缺值策略", "窗口别名", "最后计算"]
    REQUIRED_FIELDS = ["窗口编号", "窗口类型", "滑动宽度"]
    STATUS_ORDER = ["草稿", "启用中", "已暂停", "已废弃"]
    ACTION_RULES = {"启用窗口": "启用中", "暂停窗口": "已暂停", "废弃窗口": "已废弃"}
    NEGATIVE_ACTIONS = []

    def export_computation_calendar(
        self,
        window_id: int,
        job_id: int,
        start_date: str | None = None,
        count: int = 24,
        align: str | None = None,
    ) -> dict[str, Any]:
        """用时序窗口配置和定时任务时区导出可交接的计算日历。

        重复提交（同一窗口、任务、时区与展开参数）只保留一份日历安排；
        窗口所属时区更新后再次导出会生成新版本，与历史版本重叠的时段
        按「已领取/已完成者优先」裁定实际归属，历史条目与批次全部保留。
        """
        window = store.find(self.MODULE, window_id)
        if window is None:
            return {"ok": False, "message": f"窗口定义 {window_id} 不存在或已归档"}
        job = store.find("schedule_job", job_id)
        if job is None:
            return {"ok": False, "message": f"预计算任务 {job_id} 不存在或已归档"}
        tz_name = str(job.get("时区设置") or "").strip() or "UTC"
        try:
            ZoneInfo(tz_name)
        except Exception:
            return {"ok": False, "message": f"定时任务时区「{tz_name}」无法识别"}
        width = ce.parse_seconds(window.get("滑动宽度"), 3600)
        step = ce.parse_seconds(window.get("滑动步长"), width)
        align_mode = (align or str(window.get("对齐基准") or "epoch")).strip()
        if start_date:
            try:
                datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                return {"ok": False, "message": f"起始日期「{start_date}」应为 YYYY-MM-DD"}
        else:
            start_date = datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")
        count = max(1, min(int(count), 500))

        key = ce.schedule_key(window_id, job_id, tz_name, start_date, count, width, step, align_mode)
        for existing in ce.schedules():
            if existing.get("schedule_key") == key:
                return {
                    "ok": True,
                    "created": False,
                    "message": "相同参数的日历安排已存在，重复提交不再创建",
                    "schedule": existing,
                    "entries": ce.calendar_entries(existing["id"]),
                    "overlaps": self.explain_overlaps(existing["id"]),
                }

        siblings = [s for s in ce.schedules()
                    if s["window_id"] == window_id and s["job_id"] == job_id]
        version = max((int(s.get("version", 0)) for s in siblings), default=0) + 1
        supersedes = max(siblings, key=lambda s: int(s.get("version", 0)))["id"] if siblings else None

        schedule_id = ce.next_id(ce.schedules())
        schedule = {
            "id": schedule_id,
            "安排编号": f"CAL-{schedule_id:04d}",
            "schedule_key": key,
            "window_id": window_id,
            "window_no": window.get("窗口编号", ""),
            "job_id": job_id,
            "job_no": job.get("任务编号", ""),
            "timezone": tz_name,
            "version": version,
            "supersedes": supersedes,
            "start_date": start_date,
            "period_count": count,
            "width_seconds": width,
            "step_seconds": step,
            "align": align_mode,
            "created_at": ce.now_iso(),
            "status": "生效中",
        }
        ce.schedules().append(schedule)
        if supersedes is not None:
            prev = ce.find_schedule(supersedes)
            if prev is not None:
                prev["被取代"] = f"v{version}"

        periods = ce.build_periods(
            width=width, step=step, align=align_mode,
            tz_name=tz_name, start_date=start_date, count=count,
        )
        rows = ce.calendar_entries()
        new_entries: list[dict[str, Any]] = []
        for period in periods:
            executable = period["period_status"] in ce.EXECUTABLE_PERIODS
            entry = {
                "id": ce.next_id(rows),
                "schedule_id": schedule_id,
                "window_id": window_id,
                "job_id": job_id,
                **period,
                "entry_status": "待领取" if executable else "不可调度",
                "owner_entry_id": None,
                "owner_schedule_id": None,
                "claimed_by": None,
                "claimed_at": "",
                "batch_no": "",
            }
            if executable:
                owner = ce.find_claimed_owner(entry)
                if owner is not None:
                    entry["entry_status"] = "已让渡"
                    entry["owner_entry_id"] = owner["id"]
                    entry["owner_schedule_id"] = owner["schedule_id"]
            rows.append(entry)
            new_entries.append(entry)

        return {
            "ok": True,
            "created": True,
            "message": f"计算日历 v{version} 已导出，可交接给定时计算",
            "schedule": schedule,
            "entries": new_entries,
            "overlaps": self.explain_overlaps(schedule_id),
            "stats": {
                "total": len(new_entries),
                "executable": sum(1 for e in new_entries if e["entry_status"] == "待领取"),
                "repeated": sum(1 for e in new_entries if e["period_status"] in ("重复首次", "重复再次")),
                "missing": sum(1 for e in new_entries if e["period_status"] == "本地缺失"),
                "yielded": sum(1 for e in new_entries if e["entry_status"] == "已让渡"),
            },
        }

    def calendar_manifest(self, schedule_id: int) -> dict[str, Any] | None:
        """日历清单：一份日历安排的全部执行时段及其领取/完成状态。"""
        schedule = ce.find_schedule(schedule_id)
        if schedule is None:
            return None
        return {
            "schedule": schedule,
            "entries": ce.calendar_entries(schedule_id),
            "overlaps": self.explain_overlaps(schedule_id),
        }

    def explain_overlaps(self, schedule_id: int) -> list[dict[str, Any]]:
        """解释该版本日历中重叠时段的实际归属。

        已裁定的给出归属条目与归属版本；历史版本尚未领取的重叠时段
        标记为「归属待领取时裁定」，交接双方都能看清时段归谁。
        """
        schedule = ce.find_schedule(schedule_id)
        if schedule is None:
            return []
        versions = {s["id"]: s.get("version") for s in ce.schedules()}
        notes: list[dict[str, Any]] = []
        for entry in ce.calendar_entries(schedule_id):
            if entry["period_status"] not in ce.EXECUTABLE_PERIODS:
                continue
            if entry.get("owner_entry_id"):
                notes.append({
                    "entry_id": entry["id"],
                    "utc_start": entry["utc_start"],
                    "utc_end": entry["utc_end"],
                    "owner_entry_id": entry["owner_entry_id"],
                    "owner_schedule_id": entry["owner_schedule_id"],
                    "owner_version": versions.get(entry["owner_schedule_id"]),
                    "resolution": "让渡历史版本",
                })
                continue
            if entry.get("entry_status") != "待领取":
                continue
            for other in ce.calendar_entries():
                if other["schedule_id"] == schedule_id or other["window_id"] != entry["window_id"]:
                    continue
                if other.get("entry_status") not in ("待领取", "已领取", "已完成"):
                    continue
                if ce.intervals_overlap(entry["utc_start"], entry["utc_end"],
                                        other.get("utc_start", ""), other.get("utc_end", "")):
                    notes.append({
                        "entry_id": entry["id"],
                        "utc_start": entry["utc_start"],
                        "utc_end": entry["utc_end"],
                        "owner_entry_id": other["id"] if other["entry_status"] != "待领取" else None,
                        "owner_schedule_id": other["schedule_id"],
                        "owner_version": versions.get(other["schedule_id"]),
                        "resolution": "归属历史版本" if other["entry_status"] != "待领取" else "归属待领取时裁定",
                    })
                    break
        return notes

    def calendar_csv(self, schedule_id: int) -> str | None:
        """生成可交接的窗口日历文件。

        重复出现的本地小时用「本地出现」列区分（第1次/第2次），
        缺失的本地时段不留 UTC 起止，不生成虚假区间。
        """
        schedule = ce.find_schedule(schedule_id)
        if schedule is None:
            return None
        versions = {s["id"]: s.get("version") for s in ce.schedules()}
        output = io.StringIO()
        output.write("\ufeff")  # BOM，保证 Excel 打开日历文件不乱码
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow([
            "安排编号", "版本", "窗口编号", "任务编号", "时区", "序号",
            "本地开始", "本地结束", "本地出现", "UTC开始", "UTC结束",
            "时段状态", "条目状态", "实际归属条目", "归属版本", "领取任务", "批次编号",
        ])
        for entry in sorted(ce.calendar_entries(schedule_id), key=lambda e: e["seq"]):
            owner_version = ""
            if entry.get("owner_schedule_id") is not None:
                owner_version = f"v{versions.get(entry['owner_schedule_id'], '')}"
            writer.writerow([
                schedule["安排编号"], f"v{schedule['version']}",
                schedule["window_no"], schedule["job_no"], schedule["timezone"],
                entry["seq"], entry["local_start"], entry["local_end"], entry["occurrence"],
                entry["utc_start"], entry["utc_end"],
                entry["period_status"], entry["entry_status"],
                entry.get("owner_entry_id") or "", owner_version,
                entry.get("claimed_by") or "", entry.get("batch_no") or "",
            ])
        return output.getvalue()
