from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PriceSourceSpec:
    source_name: str
    table_name: str
    priority_rank: int
    notes: str
    asset_symbol: str
    base_asset: str = 'BTC'
    quote_asset: str = 'USD'
    interval: str = '10m'


PRICE_SOURCES: tuple[PriceSourceSpec, ...] = (
    PriceSourceSpec('bitstamp', 'bitstamp_price_10m', 1, 'Long-history anchor venue.', asset_symbol='BTCUSD'),
    PriceSourceSpec('coinbase', 'coinbase_price_10m', 2, 'USD spot benchmark venue.', asset_symbol='BTC-USD'),
    PriceSourceSpec('binance', 'binance_price_10m', 3, 'Major global liquidity source.', asset_symbol='BTCUSDT'),
    PriceSourceSpec('bybit', 'bybit_price_10m', 4, 'Crypto-native spot venue.', asset_symbol='BTCUSDT'),
    PriceSourceSpec('okx', 'okx_price_10m', 5, 'Supplementary major spot venue.', asset_symbol='BTC-USDT'),
    PriceSourceSpec('kraken', 'kraken_price_10m', 6, 'USD spot venue with direct 10m OHLC.', asset_symbol='XBT/USD'),
    PriceSourceSpec(
        'bitfinex',
        'bitfinex_price_10m',
        7,
        'Supplementary BTC/USD liquidity source.',
        asset_symbol='tBTCUSD',
    ),
    PriceSourceSpec(
        'gemini',
        'gemini_price_10m',
        8,
        'US spot venue using 5m candle aggregation.',
        asset_symbol='BTCUSD',
    ),
)
PRICE_SOURCE_MAP = {source.source_name: source for source in PRICE_SOURCES}


def get_price_sources() -> tuple[PriceSourceSpec, ...]:
    return PRICE_SOURCES


def get_price_source(source_name: str) -> PriceSourceSpec:
    return PRICE_SOURCE_MAP[source_name]


def get_enabled_price_sources(cur=None) -> tuple[PriceSourceSpec, ...]:
    if cur is None:
        return PRICE_SOURCES
    try:
        cur.execute(
            '''
            SELECT source_name
            FROM mq_price_source_registry
            WHERE enabled = true
            ORDER BY priority_rank ASC, source_name ASC
            '''
        )
        rows: list[dict[str, Any]] = cur.fetchall()
    except Exception:
        return PRICE_SOURCES

    enabled = tuple(PRICE_SOURCE_MAP[row['source_name']] for row in rows if row['source_name'] in PRICE_SOURCE_MAP)
    return enabled or PRICE_SOURCES
