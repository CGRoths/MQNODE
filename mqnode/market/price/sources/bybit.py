from __future__ import annotations

from mqnode.market.price.normalize import normalize_ohlcv_bucket

SOURCE_NAME = 'bybit'
TABLE_NAME = 'bybit_price_10m'


def normalize_bucket(bucket_start_utc, **kwargs):
    return normalize_ohlcv_bucket(SOURCE_NAME, bucket_start_utc, **kwargs)
