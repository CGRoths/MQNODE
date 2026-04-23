from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import aggregate_small_candles_to_10m, normalize_ohlcv_bucket
from mqnode.market.price.source_support import default_db, get_ingestion_window, request_json, upsert_source_rows

API_URL = 'https://api.exchange.coinbase.com/products/BTC-USD/candles'
FIVE_MINUTE_CHUNK = timedelta(minutes=5 * 300)
SOURCE_NAME = 'coinbase'
TABLE_NAME = 'coinbase_price_10m'
SYMBOL = 'BTC-USD'


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch 5-minute Coinbase candles and aggregate them into 10-minute buckets."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    rows = []
    chunk_start = start_bucket

    while chunk_start < end_bucket:
        chunk_end = min(chunk_start + FIVE_MINUTE_CHUNK, end_bucket)
        candles = request_json(
            API_URL,
            params={
                'start': chunk_start.isoformat(),
                'end': chunk_end.isoformat(),
                'granularity': 300,
            },
            timeout=getattr(settings, 'price_request_timeout_seconds', 30),
        )
        for candle in sorted(candles, key=lambda row: int(row[0])):
            open_time = datetime.fromtimestamp(int(candle[0]), tz=timezone.utc)
            rows.append(
                normalize_ohlcv_bucket(
                    SOURCE_NAME,
                    open_time,
                    symbol=SYMBOL,
                    open_price_usd=float(candle[3]),
                    high_price_usd=float(candle[2]),
                    low_price_usd=float(candle[1]),
                    close_price_usd=float(candle[4]),
                    volume_btc=float(candle[5]),
                    raw_payload={'candle': candle},
                    source_updated_at=datetime.now(timezone.utc),
                )
            )
        if not candles:
            break
        chunk_start = chunk_end

    aggregated_rows = aggregate_small_candles_to_10m(SOURCE_NAME, SYMBOL, rows)
    return upsert_source_rows(db, SOURCE_NAME, TABLE_NAME, aggregated_rows)
