from __future__ import annotations

from datetime import datetime, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import normalize_ohlcv_bucket
from mqnode.market.price.source_support import default_db, get_ingestion_window, request_json, upsert_source_rows

API_URL = 'https://api.kraken.com/0/public/OHLC'
SOURCE_NAME = 'kraken'
TABLE_NAME = 'kraken_price_10m'
SYMBOL = 'XBT/USD'


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch direct 10-minute Kraken spot OHLC candles and upsert them idempotently."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    payload = request_json(
        API_URL,
        params={
            'pair': 'XBTUSD',
            'interval': 10,
            'since': int(start_bucket.timestamp()),
        },
        timeout=getattr(settings, 'price_request_timeout_seconds', 30),
    )

    result = payload.get('result', {})
    pair_key = next((key for key in result if key != 'last'), None)
    candles = result.get(pair_key, []) if pair_key else []
    rows = []
    for candle in candles:
        open_time = datetime.fromtimestamp(int(float(candle[0])), tz=timezone.utc)
        if open_time >= end_bucket:
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
                volume_btc=float(candle[6]),
                trade_count=int(float(candle[7])),
                raw_payload={'candle': candle},
                source_updated_at=datetime.now(timezone.utc),
            )
        )

    return upsert_source_rows(db, SOURCE_NAME, TABLE_NAME, rows)
