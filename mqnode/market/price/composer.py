from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from mqnode.core.utils import iter_bucket_range, median, safe_div, to_bucket_start_10m
from mqnode.db.repositories import get_checkpoint
from mqnode.market.price.checkpoints import PRICE_CANONICAL_COMPONENT, price_checkpoint_error, price_checkpoint_ok
from mqnode.market.price.registry import get_price_sources

logger = logging.getLogger(__name__)


def _fetch_source_rows_for_bucket(cur, bucket_start_utc: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in get_price_sources():
        cur.execute(
            f'''
            SELECT
              %s AS source_name,
              bucket_start_utc,
              open_price_usd,
              high_price_usd,
              low_price_usd,
              close_price_usd,
              volume_btc,
              volume_usd
            FROM {source.table_name}
            WHERE bucket_start_utc = %s
            ''',
            (source.source_name, bucket_start_utc),
        )
        row = cur.fetchone()
        if row:
            rows.append(row)
    return rows


def compose_canonical_price(bucket_start_utc: datetime, source_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not source_rows:
        return None

    def _median_for(field: str) -> float | None:
        values = [float(row[field]) for row in source_rows if row.get(field) is not None]
        return median(values)

    volume_btc = sum(float(row['volume_btc'] or 0) for row in source_rows)
    volume_usd = sum(float(row['volume_usd'] or 0) for row in source_rows)
    source_names = sorted(row['source_name'] for row in source_rows)
    return {
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'source_count': len(source_rows),
        'source_names': source_names,
        'composition_method': 'median',
        'open_price_usd': _median_for('open_price_usd'),
        'high_price_usd': _median_for('high_price_usd'),
        'low_price_usd': _median_for('low_price_usd'),
        'close_price_usd': _median_for('close_price_usd'),
        'volume_btc': volume_btc if volume_btc > 0 else None,
        'volume_usd': volume_usd if volume_usd > 0 else None,
    }


def _upsert_canonical_price(cur, payload: dict[str, Any]) -> None:
    cur.execute(
        '''
        INSERT INTO mq_btc_price_10m(
          bucket_start_utc,
          source_count,
          source_names,
          composition_method,
          open_price_usd,
          high_price_usd,
          low_price_usd,
          close_price_usd,
          volume_btc,
          volume_usd,
          updated_at
        ) VALUES (
          %(bucket_start_utc)s,
          %(source_count)s,
          %(source_names)s::jsonb,
          %(composition_method)s,
          %(open_price_usd)s,
          %(high_price_usd)s,
          %(low_price_usd)s,
          %(close_price_usd)s,
          %(volume_btc)s,
          %(volume_usd)s,
          now()
        )
        ON CONFLICT (bucket_start_utc) DO UPDATE SET
          source_count = EXCLUDED.source_count,
          source_names = EXCLUDED.source_names,
          composition_method = EXCLUDED.composition_method,
          open_price_usd = EXCLUDED.open_price_usd,
          high_price_usd = EXCLUDED.high_price_usd,
          low_price_usd = EXCLUDED.low_price_usd,
          close_price_usd = EXCLUDED.close_price_usd,
          volume_btc = EXCLUDED.volume_btc,
          volume_usd = EXCLUDED.volume_usd,
          updated_at = now()
        ''',
        {
            **payload,
            'source_names': json.dumps(payload['source_names']),
        },
    )


def rebuild_canonical_price_bucket(db, bucket_start_utc: datetime) -> dict[str, Any] | None:
    bucket_start_utc = to_bucket_start_10m(bucket_start_utc)
    try:
        with db.cursor() as cur:
            source_rows = _fetch_source_rows_for_bucket(cur, bucket_start_utc)
            payload = compose_canonical_price(bucket_start_utc, source_rows)
            if payload:
                _upsert_canonical_price(cur, payload)
            else:
                cur.execute('DELETE FROM mq_btc_price_10m WHERE bucket_start_utc = %s', (bucket_start_utc,))
            price_checkpoint_ok(cur, last_bucket_time=bucket_start_utc)
            return payload
    except Exception as exc:
        logger.exception('canonical_price_rebuild_failed bucket=%s error=%s', bucket_start_utc.isoformat(), exc)
        with db.cursor() as cur:
            price_checkpoint_error(cur, str(exc), last_bucket_time=bucket_start_utc)
        raise


def catch_up_canonical_price_from_checkpoint(db, end_bucket: datetime | None = None) -> int:
    with db.cursor() as cur:
        checkpoint = get_checkpoint(cur, 'BTC', PRICE_CANONICAL_COMPONENT, '10m')
        replay_start = checkpoint.get('last_bucket_time')

        cur.execute(
            '''
            SELECT MIN(bucket_start_utc) AS min_bucket, MAX(bucket_start_utc) AS max_bucket
            FROM (
              SELECT bucket_start_utc FROM bitstamp_price_10m
              UNION ALL
              SELECT bucket_start_utc FROM bybit_price_10m
              UNION ALL
              SELECT bucket_start_utc FROM binance_price_10m
              UNION ALL
              SELECT bucket_start_utc FROM okx_price_10m
            ) price_buckets
            '''
        )
        bounds = cur.fetchone() or {}
    min_bucket = bounds.get('min_bucket')
    max_bucket = bounds.get('max_bucket')
    if min_bucket is None or max_bucket is None:
        return 0

    start_bucket = replay_start or min_bucket
    final_bucket = min(end_bucket, max_bucket) if end_bucket is not None else max_bucket
    rebuilt = 0
    for bucket in iter_bucket_range(start_bucket, final_bucket, 10):
        rebuild_canonical_price_bucket(db, bucket)
        rebuilt += 1
    return rebuilt


def estimate_price_completeness(source_rows: list[dict[str, Any]]) -> float | None:
    if not source_rows:
        return None
    return safe_div(len(source_rows), len(get_price_sources()))
