from __future__ import annotations

from mqnode.checkpoints.checkpoint_service import checkpoint_error, checkpoint_ok

PRICE_CANONICAL_COMPONENT = 'btc_price_canonical_composer'


def price_checkpoint_ok(cur, component: str = PRICE_CANONICAL_COMPONENT, last_bucket_time=None):
    checkpoint_ok(cur, 'BTC', component, '10m', last_bucket_time=last_bucket_time)


def price_checkpoint_error(cur, error_message: str, component: str = PRICE_CANONICAL_COMPONENT, last_bucket_time=None):
    checkpoint_error(cur, 'BTC', component, '10m', error_message, last_bucket_time=last_bucket_time)
