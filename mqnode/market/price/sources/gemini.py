from __future__ import annotations

from datetime import datetime, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import aggregate_small_candles_to_10m, normalize_ohlcv_bucket
from mqnode.market.price.source_support import default_db, get_ingestion_window, request_json, upsert_source_rows

API_URL = 'https://api.gemini.com/v2/candles/btcusd/5m'
SOURCE_NAME = 'gemini'
TABLE_NAME = 'gemini_price_10m'
SYMBOL = 'BTCUSD'


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch 5-minute Gemini candles and aggregate them into 10-minute buckets."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    payload = request_json(
        API_URL,
        timeout=getattr(settings, 'price_request_timeout_seconds', 30),
    )

    rows = []
    for candle in sorted(payload, key=lambda row: int(row[0])):
        open_time = datetime.fromtimestamp(int(candle[0]) / 1000, tz=timezone.utc)
        if open_time < start_bucket or open_time >= end_bucket:
            continue
        rows.append(
            normalize_ohlcv_bucket(
                SOURCE_NAME,
                open_time,
                symbol=SYMBOL,
                open_price_usd=float(candle[1]),
                high_price_usd=float(candle[2]),
                low_price_usd=float(candle[3]),
                close_price_usd=float(candle[4]),
                volume_btc=float(candle[5]),
                raw_payload={'candle': candle},
                source_updated_at=datetime.now(timezone.utc),
            )
        )

    aggregated_rows = aggregate_small_candles_to_10m(SOURCE_NAME, SYMBOL, rows)
    return upsert_source_rows(db, SOURCE_NAME, TABLE_NAME, aggregated_rows)
