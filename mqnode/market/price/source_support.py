from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

import requests

from mqnode.config.settings import get_settings
from mqnode.core.utils import to_bucket_start_10m, utc_now
from mqnode.db.connection import DB
from mqnode.market.price.checkpoints import (
    price_source_checkpoint_error,
    price_source_checkpoint_ok,
    price_source_replay_start,
)

IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def request_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> Any:
    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_ingestion_window(db, source_name: str, *, lookback_hours: int = 48) -> tuple[datetime, datetime]:
    with db.cursor() as cur:
        start_bucket = price_source_replay_start(cur, source_name, lookback_hours=lookback_hours)
    return start_bucket, to_bucket_start_10m(utc_now())


def filter_closed_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    current_bucket = to_bucket_start_10m(utc_now())
    deduped = {
        row['bucket_start_utc']: row
        for row in rows
        if row.get('bucket_start_utc') is not None and row['bucket_start_utc'] < current_bucket
    }
    return [deduped[bucket] for bucket in sorted(deduped)]


def upsert_source_rows(db, source_name: str, table_name: str, rows: list[dict[str, Any]]) -> int:
    if not IDENTIFIER_RE.fullmatch(table_name):
        raise ValueError(f'Invalid price table name: {table_name}')

    rows = filter_closed_rows(rows)
    if not rows:
        return 0

    last_bucket_time = max(row['bucket_start_utc'] for row in rows)
    try:
        with db.cursor() as cur:
            for row in rows:
                cur.execute(
                    f'''
                    INSERT INTO {table_name}(
                      bucket_start_utc,
                      symbol,
                      open_price_usd,
                      high_price_usd,
                      low_price_usd,
                      close_price_usd,
                      volume_btc,
                      volume_usd,
                      trade_count,
                      raw_payload,
                      source_updated_at,
                      updated_at
                    ) VALUES (
                      %(bucket_start_utc)s,
                      %(symbol)s,
                      %(open_price_usd)s,
                      %(high_price_usd)s,
                      %(low_price_usd)s,
                      %(close_price_usd)s,
                      %(volume_btc)s,
                      %(volume_usd)s,
                      %(trade_count)s,
                      %(raw_payload)s::jsonb,
                      %(source_updated_at)s,
                      now()
                    )
                    ON CONFLICT (bucket_start_utc) DO UPDATE SET
                      symbol = EXCLUDED.symbol,
                      open_price_usd = EXCLUDED.open_price_usd,
                      high_price_usd = EXCLUDED.high_price_usd,
                      low_price_usd = EXCLUDED.low_price_usd,
                      close_price_usd = EXCLUDED.close_price_usd,
                      volume_btc = EXCLUDED.volume_btc,
                      volume_usd = EXCLUDED.volume_usd,
                      trade_count = EXCLUDED.trade_count,
                      raw_payload = EXCLUDED.raw_payload,
                      source_updated_at = EXCLUDED.source_updated_at,
                      updated_at = now()
                    ''',
                    {
                        **row,
                        'raw_payload': json.dumps(row.get('raw_payload')),
                    },
                )
            price_source_checkpoint_ok(cur, source_name, last_bucket_time=last_bucket_time)
    except Exception as exc:
        with db.cursor() as cur:
            price_source_checkpoint_error(cur, source_name, str(exc), last_bucket_time=last_bucket_time)
        raise

    return len(rows)


def default_db(db=None):
    return db or DB(get_settings())
