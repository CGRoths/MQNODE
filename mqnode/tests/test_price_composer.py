from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mqnode.market.price.composer import compose_canonical_price, compose_price_details, estimate_price_completeness
from mqnode.market.price.normalize import aggregate_small_candles_to_10m


def test_compose_canonical_price_uses_volume_weighted_fair_price():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    payload = compose_canonical_price(
        bucket,
        [
            {
                'source_name': 'bitstamp',
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 10,
                'volume_usd': 880000,
            },
            {
                'source_name': 'bybit',
                'open_price_usd': 87980,
                'high_price_usd': 88200,
                'low_price_usd': 87890,
                'close_price_usd': 88040,
                'volume_btc': 12,
                'volume_usd': 1056000,
            },
            {
                'source_name': 'binance',
                'open_price_usd': 88020,
                'high_price_usd': 88120,
                'low_price_usd': 87920,
                'close_price_usd': 88060,
                'volume_btc': 20,
                'volume_usd': 1760000,
            },
        ],
    )

    assert payload is not None
    assert payload['source_count'] == 3
    assert payload['source_names'] == ['bitstamp', 'binance', 'bybit']
    assert payload['composition_method'] == 'volume_weighted_ohlc_v1'
    assert payload['open_price_usd'] == pytest.approx((88000 * 10 + 87980 * 12 + 88020 * 20) / 42)
    assert payload['close_price_usd'] == pytest.approx((88050 * 10 + 88040 * 12 + 88060 * 20) / 42)
    assert payload['volume_btc'] == 42
    assert payload['volume_usd'] == 3696000


def test_compose_price_details_keeps_exchange_level_fields():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    payload = compose_price_details(
        bucket,
        [
            {
                'source_name': 'bitstamp',
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 10,
                'volume_usd': 880000,
            },
            {
                'source_name': 'coinbase',
                'open_price_usd': 88010,
                'high_price_usd': 88110,
                'low_price_usd': 87910,
                'close_price_usd': 88020,
                'volume_btc': 5,
                'volume_usd': 440100,
            },
        ],
    )

    assert payload is not None
    assert payload['bitstamp_close_price'] == 88050
    assert payload['coinbase_close_price'] == 88020
    assert payload['fair_close_price'] == pytest.approx((88050 * 10 + 88020 * 5) / 15)
    assert payload['total_volume_btc'] == 15


def test_aggregate_small_candles_to_10m_rolls_5m_rows_up_cleanly():
    rows = [
        {
            'bucket_start_utc': datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc),
            'open_price_usd': 88000,
            'high_price_usd': 88100,
            'low_price_usd': 87950,
            'close_price_usd': 88020,
            'volume_btc': 3,
            'volume_usd': 264060,
            'trade_count': 10,
            'raw_payload': {'id': 1},
            'source_updated_at': datetime(2026, 4, 20, 0, 5, tzinfo=timezone.utc),
        },
        {
            'bucket_start_utc': datetime(2026, 4, 20, 0, 5, tzinfo=timezone.utc),
            'open_price_usd': 88020,
            'high_price_usd': 88200,
            'low_price_usd': 88000,
            'close_price_usd': 88150,
            'volume_btc': 4,
            'volume_usd': 352600,
            'trade_count': 8,
            'raw_payload': {'id': 2},
            'source_updated_at': datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc),
        },
    ]

    aggregated = aggregate_small_candles_to_10m('coinbase', 'BTC-USD', rows)

    assert len(aggregated) == 1
    assert aggregated[0]['bucket_start_utc'] == datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    assert aggregated[0]['open_price_usd'] == 88000
    assert aggregated[0]['close_price_usd'] == 88150
    assert aggregated[0]['high_price_usd'] == 88200
    assert aggregated[0]['low_price_usd'] == 87950
    assert aggregated[0]['volume_btc'] == 7
    assert aggregated[0]['trade_count'] == 18


def test_price_completeness_is_none_without_sources():
    assert estimate_price_completeness([]) is None
