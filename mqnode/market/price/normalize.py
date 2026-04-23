from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from mqnode.core.utils import to_bucket_start_10m


def normalize_ohlcv_bucket(
    source_name: str,
    bucket_start_utc: datetime,
    *,
    symbol: str,
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
    derived_volume_usd = volume_usd
    if derived_volume_usd is None and volume_btc is not None and close_price_usd is not None:
        derived_volume_usd = float(volume_btc) * float(close_price_usd)
    return {
        'source_name': source_name,
        'symbol': symbol,
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'open_price_usd': open_price_usd,
        'high_price_usd': high_price_usd,
        'low_price_usd': low_price_usd,
        'close_price_usd': close_price_usd,
        'volume_btc': volume_btc,
        'volume_usd': derived_volume_usd,
        'trade_count': trade_count,
        'raw_payload': raw_payload,
        'source_updated_at': source_updated_at,
    }


def aggregate_small_candles_to_10m(
    source_name: str,
    symbol: str,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[to_bucket_start_10m(row['bucket_start_utc'])].append(row)

    aggregated_rows: list[dict[str, Any]] = []
    for bucket_start_utc, items in sorted(grouped.items()):
        items.sort(key=lambda row: row['bucket_start_utc'])
        volume_btc = sum(float(item['volume_btc'] or 0) for item in items)
        volume_usd = sum(float(item['volume_usd'] or 0) for item in items)
        trade_counts = [item['trade_count'] for item in items if item.get('trade_count') is not None]
        high_price_usd = max(
            float(item['high_price_usd']) for item in items if item.get('high_price_usd') is not None
        )
        low_price_usd = min(
            float(item['low_price_usd']) for item in items if item.get('low_price_usd') is not None
        )
        aggregated_rows.append(
            normalize_ohlcv_bucket(
                source_name,
                bucket_start_utc,
                symbol=symbol,
                open_price_usd=items[0]['open_price_usd'],
                high_price_usd=high_price_usd,
                low_price_usd=low_price_usd,
                close_price_usd=items[-1]['close_price_usd'],
                volume_btc=volume_btc,
                volume_usd=volume_usd if volume_usd > 0 else None,
                trade_count=sum(trade_counts) if trade_counts else None,
                raw_payload={'aggregated_from': '5m', 'candles': [item.get('raw_payload') for item in items]},
                source_updated_at=max(
                    (item['source_updated_at'] for item in items if item.get('source_updated_at') is not None),
                    default=None,
                ),
            )
        )
    return aggregated_rows
