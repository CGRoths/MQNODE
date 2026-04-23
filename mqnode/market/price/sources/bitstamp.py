from __future__ import annotations

from datetime import datetime, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import normalize_ohlcv_bucket
from mqnode.market.price.source_support import default_db, get_ingestion_window, request_json, upsert_source_rows

API_URL = 'https://www.bitstamp.net/api/v2/ohlc/btcusd/'
SOURCE_NAME = 'bitstamp'
TABLE_NAME = 'bitstamp_price_10m'
SYMBOL = 'BTCUSD'


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch direct 10-minute Bitstamp candles and upsert them idempotently."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    rows = []

    payload = request_json(
        API_URL,
        params={
            'step': 600,
            'limit': 1000,
            'start': int(start_bucket.timestamp()),
            'end': int(end_bucket.timestamp()),
        },
        timeout=getattr(settings, 'price_request_timeout_seconds', 30),
    )
    candles = payload.get('data', {}).get('ohlc', [])
    for candle in sorted(candles, key=lambda row: int(row['timestamp'])):
        bucket_time = datetime.fromtimestamp(int(candle['timestamp']), tz=timezone.utc)
        rows.append(
            normalize_ohlcv_bucket(
                SOURCE_NAME,
                bucket_time,
                symbol=SYMBOL,
                open_price_usd=float(candle['open']),
                high_price_usd=float(candle['high']),
                low_price_usd=float(candle['low']),
                close_price_usd=float(candle['close']),
                volume_btc=float(candle['volume']),
                trade_count=int(candle['trades']) if candle.get('trades') is not None else None,
                raw_payload=candle,
                source_updated_at=datetime.now(timezone.utc),
            )
        )

    return upsert_source_rows(db, SOURCE_NAME, TABLE_NAME, rows)
