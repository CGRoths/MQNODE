from __future__ import annotations

from datetime import datetime, timezone

from mqnode.market.price.composer import compose_canonical_price, estimate_price_completeness


def test_compose_canonical_price_uses_median_across_available_sources():
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
    assert payload['source_names'] == ['binance', 'bitstamp', 'bybit']
    assert payload['open_price_usd'] == 88000.0
    assert payload['close_price_usd'] == 88050.0
    assert payload['volume_btc'] == 42
    assert payload['volume_usd'] == 3696000


def test_price_completeness_is_none_without_sources():
    assert estimate_price_completeness([]) is None
