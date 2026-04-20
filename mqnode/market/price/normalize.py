from __future__ import annotations

from datetime import datetime
from typing import Any

from mqnode.core.utils import to_bucket_start_10m


def normalize_ohlcv_bucket(
    source_name: str,
    bucket_start_utc: datetime,
    *,
    open_price_usd: float | int | None,
    high_price_usd: float | int | None,
    low_price_usd: float | int | None,
    close_price_usd: float | int | None,
    volume_btc: float | int | None = None,
    volume_usd: float | int | None = None,
    trade_count: int | None = None,
    raw_payload: dict[str, Any] | None = None,
    source_updated_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        'source_name': source_name,
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'open_price_usd': open_price_usd,
        'high_price_usd': high_price_usd,
        'low_price_usd': low_price_usd,
        'close_price_usd': close_price_usd,
        'volume_btc': volume_btc,
        'volume_usd': volume_usd,
        'trade_count': trade_count,
        'raw_payload': raw_payload,
        'source_updated_at': source_updated_at,
    }
