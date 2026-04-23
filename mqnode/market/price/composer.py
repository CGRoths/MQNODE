from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any

from mqnode.core.utils import iter_bucket_range, safe_div, to_bucket_start_10m
from mqnode.db.repositories import get_checkpoint
from mqnode.market.price.checkpoints import PRICE_CANONICAL_COMPONENT, price_checkpoint_error, price_checkpoint_ok
from mqnode.market.price.registry import get_enabled_price_sources, get_price_sources

logger = logging.getLogger(__name__)

COMPOSITION_METHOD = 'volume_weighted_ohlc_v1'
DETAIL_METRICS = ('open_price', 'high_price', 'low_price', 'close_price', 'volume_btc')
IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _fetch_source_rows_for_bucket(cur, bucket_start_utc: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in get_enabled_price_sources(cur):
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
              volume_usd,
              trade_count
            FROM {source.table_name}
            WHERE bucket_start_utc = %s
            ''',
            (source.source_name, bucket_start_utc),
        )
        row = cur.fetchone()
        if row:
            rows.append(row)
    return rows


def _is_valid_source_row(row: dict[str, Any]) -> bool:
    required_fields = ('open_price_usd', 'high_price_usd', 'low_price_usd', 'close_price_usd')
    values = [row.get(field) for field in required_fields]
    if any(value is None or float(value) <= 0 for value in values):
        return False

    open_price = float(row['open_price_usd'])
    high_price = float(row['high_price_usd'])
    low_price = float(row['low_price_usd'])
    close_price = float(row['close_price_usd'])
    if high_price < max(open_price, close_price, low_price):
        return False
    if low_price > min(open_price, close_price, high_price):
        return False
    return True


def _weight_for_row(row: dict[str, Any], rows: list[dict[str, Any]]) -> float:
    volume_btc = float(row.get('volume_btc') or 0)
    if any(float(candidate.get('volume_btc') or 0) > 0 for candidate in rows):
        return max(volume_btc, 0.0)
    return 1.0


def _weighted_aggregate(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [
        (float(row[field]), _weight_for_row(row, rows))
        for row in rows
        if row.get(field) is not None
    ]
    if not values:
        return None
    total_weight = sum(weight for _, weight in values)
    if total_weight <= 0:
        return None
    return sum(value * weight for value, weight in values) / total_weight


def _ordered_source_names(rows: list[dict[str, Any]]) -> list[str]:
    available = {row['source_name'] for row in rows}
    return [source.source_name for source in get_price_sources() if source.source_name in available]


def compose_canonical_price(bucket_start_utc: datetime, source_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid_rows = [row for row in source_rows if _is_valid_source_row(row)]
    if not valid_rows:
        return None

    source_names = _ordered_source_names(valid_rows)
    total_volume_btc = sum(float(row['volume_btc'] or 0) for row in valid_rows)
    total_volume_usd = sum(float(row['volume_usd'] or 0) for row in valid_rows)
    return {
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'source_count': len(valid_rows),
        'source_names': source_names,
        'composition_method': COMPOSITION_METHOD,
        'open_price_usd': _weighted_aggregate(valid_rows, 'open_price_usd'),
        'high_price_usd': _weighted_aggregate(valid_rows, 'high_price_usd'),
        'low_price_usd': _weighted_aggregate(valid_rows, 'low_price_usd'),
        'close_price_usd': _weighted_aggregate(valid_rows, 'close_price_usd'),
        'volume_btc': total_volume_btc if total_volume_btc > 0 else None,
        'volume_usd': total_volume_usd if total_volume_usd > 0 else None,
    }


def compose_price_details(bucket_start_utc: datetime, source_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    canonical_payload = compose_canonical_price(bucket_start_utc, source_rows)
    if canonical_payload is None:
        return None

    row_map = {row['source_name']: row for row in source_rows if _is_valid_source_row(row)}
    payload: dict[str, Any] = {
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'fair_open_price': canonical_payload['open_price_usd'],
        'fair_high_price': canonical_payload['high_price_usd'],
        'fair_low_price': canonical_payload['low_price_usd'],
        'fair_close_price': canonical_payload['close_price_usd'],
        'total_volume_btc': canonical_payload['volume_btc'],
        'source_count': canonical_payload['source_count'],
        'source_names': canonical_payload['source_names'],
        'composition_method': canonical_payload['composition_method'],
    }
    for source in get_price_sources():
        row = row_map.get(source.source_name)
        payload[f'{source.source_name}_open_price'] = row.get('open_price_usd') if row else None
        payload[f'{source.source_name}_high_price'] = row.get('high_price_usd') if row else None
        payload[f'{source.source_name}_low_price'] = row.get('low_price_usd') if row else None
        payload[f'{source.source_name}_close_price'] = row.get('close_price_usd') if row else None
        payload[f'{source.source_name}_volume_btc'] = row.get('volume_btc') if row else None
    return payload


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


def _upsert_price_details(cur, payload: dict[str, Any]) -> None:
    columns = ['bucket_start_utc']
    values = ['%(bucket_start_utc)s']
    updates = []

    for source in get_price_sources():
        for metric in DETAIL_METRICS:
            column = f'{source.source_name}_{metric}'
            columns.append(column)
            values.append(f'%({column})s')
            updates.append(f'{column} = EXCLUDED.{column}')

    for column in (
        'fair_open_price',
        'fair_high_price',
        'fair_low_price',
        'fair_close_price',
        'total_volume_btc',
        'source_count',
        'source_names',
        'composition_method',
    ):
        columns.append(column)
        values.append(f'%({column})s')
        updates.append(f'{column} = EXCLUDED.{column}')

    cur.execute(
        f'''
        INSERT INTO mq_btc_price_fair_10m_details(
          {', '.join(columns)},
          updated_at
        ) VALUES (
          {', '.join(values)},
          now()
        )
        ON CONFLICT (bucket_start_utc) DO UPDATE SET
          {', '.join(updates)},
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
            canonical_payload = compose_canonical_price(bucket_start_utc, source_rows)
            detail_payload = compose_price_details(bucket_start_utc, source_rows)
            if canonical_payload is None or detail_payload is None:
                cur.execute('DELETE FROM mq_btc_price_10m WHERE bucket_start_utc = %s', (bucket_start_utc,))
                cur.execute(
                    'DELETE FROM mq_btc_price_fair_10m_details WHERE bucket_start_utc = %s',
                    (bucket_start_utc,),
                )
            else:
                _upsert_canonical_price(cur, canonical_payload)
                _upsert_price_details(cur, detail_payload)
            price_checkpoint_ok(cur, last_bucket_time=bucket_start_utc)
            return canonical_payload
    except Exception as exc:
        logger.exception('canonical_price_rebuild_failed bucket=%s error=%s', bucket_start_utc.isoformat(), exc)
        with db.cursor() as cur:
            price_checkpoint_error(cur, str(exc), last_bucket_time=bucket_start_utc)
        raise


def _price_bounds_query(table_names: list[str]) -> str:
    selects = [f'SELECT bucket_start_utc FROM {table_name}' for table_name in table_names]
    return ' UNION ALL '.join(selects)


def catch_up_canonical_price_from_checkpoint(db, end_bucket: datetime | None = None) -> int:
    with db.cursor() as cur:
        checkpoint = get_checkpoint(cur, 'BTC', PRICE_CANONICAL_COMPONENT, '10m')
        replay_start = checkpoint.get('last_bucket_time')
        enabled_sources = get_enabled_price_sources(cur)
        table_names = [source.table_name for source in enabled_sources if IDENTIFIER_RE.fullmatch(source.table_name)]
        if not table_names:
            return 0

        cur.execute(
            f'''
            SELECT MIN(bucket_start_utc) AS min_bucket, MAX(bucket_start_utc) AS max_bucket
            FROM (
              {_price_bounds_query(table_names)}
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
    return safe_div(len([row for row in source_rows if _is_valid_source_row(row)]), len(get_price_sources()))
