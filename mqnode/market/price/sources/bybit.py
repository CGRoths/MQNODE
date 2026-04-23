from __future__ import annotations

from datetime import datetime, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import normalize_ohlcv_bucket
from mqnode.market.price.source_support import default_db, get_ingestion_window, request_json, upsert_source_rows

API_URL = 'https://api.bybit.com/v5/market/kline'
SOURCE_NAME = 'bybit'
TABLE_NAME = 'bybit_price_10m'
SYMBOL = 'BTCUSDT'


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch direct 10-minute Bybit spot candles and upsert them idempotently."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    start_ms = int(start_bucket.timestamp() * 1000)
    end_ms = int(end_bucket.timestamp() * 1000)
    rows = []

    while start_ms < end_ms:
        payload = request_json(
            API_URL,
            params={
                'category': 'spot',
                'symbol': SYMBOL,
                'interval': '10',
                'start': start_ms,
                'end': end_ms,
                'limit': 1000,
            },
            timeout=getattr(settings, 'price_request_timeout_seconds', 30),
        )
        candles = sorted(payload.get('result', {}).get('list', []), key=lambda row: int(row[0]))
        if not candles:
            break

        for candle in candles:
            open_time_ms = int(candle[0])
            rows.append(
                normalize_ohlcv_bucket(
                    SOURCE_NAME,
                    datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc),
                    symbol=SYMBOL,
                    open_price_usd=float(candle[1]),
                    high_price_usd=float(candle[2]),
                    low_price_usd=float(candle[3]),
                    close_price_usd=float(candle[4]),
                    volume_btc=float(candle[5]),
                    volume_usd=float(candle[6]),
                    raw_payload={'kline': candle},
                    source_updated_at=datetime.fromtimestamp(int(payload['time']) / 1000, tz=timezone.utc),
                )
            )

        if len(candles) < 1000:
            break
        start_ms = int(candles[-1][0]) + 600_000

    return upsert_source_rows(db, SOURCE_NAME, TABLE_NAME, rows)
