"""窗口日历引擎：把时序窗口配置按定时任务时区展开为可交接的执行时段。

职责边界：
- 纯函数负责本地墙钟 → UTC 的换算与分类（夏令时回拨重复、春拨缺失）；
- 存储助手维护三张内部表：日历安排、日历清单条目、实际计算批；
- 状态流转（导出、领取、让渡、写批）在 time_window / schedule_job 服务里完成。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.store import store

UTC = timezone.utc
LOCAL_FMT = "%Y-%m-%d %H:%M:%S"

SCHEDULE_TABLE = "window_calendar"       # 日历安排：一次导出一份，重复提交复用同一份
ENTRY_TABLE = "window_calendar_entry"    # 日历清单：每个执行时段一行，领取结果同步回写
BATCH_TABLE = "schedule_batch"           # 实际计算批：调度器按时段写入

EXECUTABLE_PERIODS = ("正常", "重复首次", "重复再次")
LOCAL_ALIGNMENTS = ("local", "local_midnight", "wall", "本地", "本地对齐")


def parse_seconds(raw: Any, default: int = 3600) -> int:
    """把「3600」「10m」「1h」这类宽度/步长配置解析成秒。"""
    text = str(raw or "").strip().lower()
    if not text:
        return default
    factor = 1
    if text[-1:] == "s":
        text = text[:-1]
    elif text[-1:] == "m":
        text, factor = text[:-1], 60
    elif text[-1:] == "h":
        text, factor = text[:-1], 3600
    elif text[-1:] == "d":
        text, factor = text[:-1], 86400
    try:
        return max(1, int(float(text) * factor))
    except ValueError:
        return default


def classify_local(tz: ZoneInfo, naive: datetime) -> list[datetime]:
    """本地墙钟时间映射到合法 UTC 时刻。

    返回 0 个 → 该本地时段不存在（春拨跳过）；1 个 → 正常；
    2 个 → 夏令时回拨导致的重复本地小时，按 UTC 先后排列。
    """
    found: list[datetime] = []
    for fold in (0, 1):
        utc_dt = naive.replace(tzinfo=tz, fold=fold).astimezone(UTC)
        if utc_dt.astimezone(tz).replace(tzinfo=None) == naive and utc_dt not in found:
            found.append(utc_dt)
    return found


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def parse_utc(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


def now_iso() -> str:
    return iso_utc(datetime.now(UTC))


def intervals_overlap(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    """半开区间 [start, end) 重叠判定；任一端缺失（本地时段无 UTC）不算重叠。"""
    if not all((a_start, a_end, b_start, b_end)):
        return False
    s1, e1 = parse_utc(a_start), parse_utc(a_end)
    s2, e2 = parse_utc(b_start), parse_utc(b_end)
    return s1 < e2 and s2 < e1


def build_periods(
    *,
    width: int,
    step: int,
    align: str,
    tz_name: str,
    start_date: str,
    count: int,
) -> list[dict[str, Any]]:
    """按窗口配置和任务时区展开执行时段。

    本地对齐时：重复本地小时展开为两行（第1次/第2次，UTC 边界各自唯一）；
    缺失本地时段只登记「本地缺失」行，不生成虚假的 UTC 区间。
    """
    tz = ZoneInfo(tz_name)
    anchor = datetime.strptime(start_date, "%Y-%m-%d")
    local_mode = align.strip().lower() in LOCAL_ALIGNMENTS
    rows: list[dict[str, Any]] = []
    if local_mode:
        for i in range(count):
            local_start = anchor + timedelta(seconds=step * i)
            local_end = local_start + timedelta(seconds=width)
            base = {
                "local_start": local_start.strftime(LOCAL_FMT),
                "local_end": local_end.strftime(LOCAL_FMT),
            }
            candidates = classify_local(tz, local_start)
            if not candidates:
                rows.append({**base, "occurrence": "缺失", "utc_start": "", "utc_end": "",
                             "period_status": "本地缺失"})
                continue
            repeated = len(candidates) > 1
            for order, utc_start in enumerate(candidates):
                rows.append({
                    **base,
                    "occurrence": ("第1次" if order == 0 else "第2次") if repeated else "",
                    "utc_start": iso_utc(utc_start),
                    "utc_end": iso_utc(utc_start + timedelta(seconds=width)),
                    "period_status": ("重复首次" if order == 0 else "重复再次") if repeated else "正常",
                })
    else:
        anchor_utc = anchor.replace(tzinfo=UTC)
        for i in range(count):
            utc_start = anchor_utc + timedelta(seconds=step * i)
            utc_end = utc_start + timedelta(seconds=width)
            rows.append({
                "local_start": utc_start.astimezone(tz).strftime(LOCAL_FMT),
                "local_end": utc_end.astimezone(tz).strftime(LOCAL_FMT),
                "occurrence": "",
                "utc_start": iso_utc(utc_start),
                "utc_end": iso_utc(utc_end),
                "period_status": "正常",
            })
    for seq, row in enumerate(rows):
        row["seq"] = seq
    return rows


def schedule_key(
    window_id: int, job_id: int, tz_name: str, start_date: str,
    count: int, width: int, step: int, align: str,
) -> str:
    """幂等键：同一窗口、任务、时区与展开参数的重复提交只保留一份日历安排。"""
    raw = f"{window_id}|{job_id}|{tz_name}|{start_date}|{count}|{width}|{step}|{align}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def find_claimed_owner(entry: dict[str, Any]) -> dict[str, Any] | None:
    """在其他版本日历中查找已领取/已完成且 UTC 区间重叠的条目（实际归属方）。"""
    for row in store.rows(ENTRY_TABLE):
        if row["schedule_id"] == entry["schedule_id"] or row["window_id"] != entry["window_id"]:
            continue
        if row.get("entry_status") not in ("已领取", "已完成"):
            continue
        if intervals_overlap(entry["utc_start"], entry["utc_end"],
                             row.get("utc_start", ""), row.get("utc_end", "")):
            return row
    return None


# -- 内部表访问 --

def schedules() -> list[dict[str, Any]]:
    return store.rows(SCHEDULE_TABLE)


def find_schedule(schedule_id: int) -> dict[str, Any] | None:
    for row in schedules():
        if int(row.get("id", 0)) == int(schedule_id):
            return row
    return None


def calendar_entries(schedule_id: int | None = None) -> list[dict[str, Any]]:
    rows = store.rows(ENTRY_TABLE)
    if schedule_id is None:
        return rows
    return [row for row in rows if int(row.get("schedule_id", 0)) == int(schedule_id)]


def batches() -> list[dict[str, Any]]:
    return store.rows(BATCH_TABLE)


def next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1
