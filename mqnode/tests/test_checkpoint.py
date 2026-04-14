from mqnode.core.utils import to_bucket_start_30m
from datetime import datetime, timezone


def test_bucket_rounding():
    dt = datetime(2025, 1, 1, 12, 44, 4, tzinfo=timezone.utc)
    assert to_bucket_start_30m(dt).minute == 30
