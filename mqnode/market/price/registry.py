from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PriceSourceSpec:
    source_name: str
    table_name: str
    priority_rank: int
    notes: str
    asset_symbol: str = 'BTCUSD'
    base_asset: str = 'BTC'
    quote_asset: str = 'USD'
    interval: str = '10m'


PRICE_SOURCES: tuple[PriceSourceSpec, ...] = (
    PriceSourceSpec('bitstamp', 'bitstamp_price_10m', 1, 'Long-history anchor venue.'),
    PriceSourceSpec('bybit', 'bybit_price_10m', 2, 'Crypto-native spot venue.'),
    PriceSourceSpec('binance', 'binance_price_10m', 3, 'Major global liquidity source.'),
    PriceSourceSpec('okx', 'okx_price_10m', 4, 'Supplementary major spot venue.'),
)


def get_price_sources() -> tuple[PriceSourceSpec, ...]:
    return PRICE_SOURCES
