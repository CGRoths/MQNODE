from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

SATOSHI_PER_BTC = 100_000_000


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_bucket_start_30m(ts: datetime) -> datetime:
    ts = ts.astimezone(timezone.utc)
    minute = 30 if ts.minute >= 30 else 0
    return ts.replace(minute=minute, second=0, microsecond=0)


def to_open_time_ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def safe_div(num: Optional[float], den: Optional[float]) -> Optional[float]:
    if num is None or den in (None, 0):
        return None
    return num / den


def median(values: Iterable[float]) -> Optional[float]:
    s = sorted(values)
    if not s:
        return None
    n = len(s)
    mid = n // 2
    return float(s[mid]) if n % 2 else float((s[mid - 1] + s[mid]) / 2)


def hour_bounds(ts: datetime) -> tuple[datetime, datetime]:
    start = ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return start, start + timedelta(hours=1)
