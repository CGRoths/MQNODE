from __future__ import annotations

import logging
from datetime import datetime

from mqnode.core.utils import SATOSHI_PER_BTC, hour_bounds, safe_div

logger = logging.getLogger(__name__)


def _calc_row(price_usd, supply_total_sat, transferred_sat):
    if price_usd is None:
        return None, None, None
    transferred_value_usd = (transferred_sat / SATOSHI_PER_BTC) * price_usd
    market_cap_usd = (supply_total_sat / SATOSHI_PER_BTC) * price_usd
    nvt_raw = safe_div(market_cap_usd, transferred_value_usd)
    return transferred_value_usd, market_cap_usd, nvt_raw


def calculate_nvt(db, bucket_start_utc: datetime, interval: str) -> None:
    with db.cursor() as cur:
        if interval == '10m':
            cur.execute('SELECT * FROM btc_primitive_10m WHERE bucket_start_utc = %s', (bucket_start_utc,))
            row = cur.fetchone()
            if not row:
                return
            transferred_sat = row['transferred_sat_10m'] or 0
            supply_total_sat = row['supply_total_sat'] or 0
            price_usd = row['close_price_usd']
            table = 'btc_nvt_10m'
            source_start = row['first_height']
            source_end = row['last_height']
        elif interval == '1h':
            h_start, h_end = hour_bounds(bucket_start_utc)
            cur.execute(
                '''
                SELECT * FROM btc_primitive_10m
                WHERE bucket_start_utc >= %s AND bucket_start_utc < %s
                ORDER BY bucket_start_utc ASC
                ''',
                (h_start, h_end),
            )
            rows = cur.fetchall()
            if len(rows) == 0:
                return
            transferred_sat = sum(r['transferred_sat_10m'] or 0 for r in rows)
            supply_total_sat = rows[-1]['supply_total_sat'] or 0
            price_usd = rows[-1]['close_price_usd']
            table = 'btc_nvt_1h'
            source_start = min(r['first_height'] for r in rows if r['first_height'] is not None)
            source_end = max(r['last_height'] for r in rows if r['last_height'] is not None)
            bucket_start_utc = h_start
        else:
            raise ValueError(f'Unsupported interval for NVT: {interval}')

        transferred_value_usd, market_cap_usd, nvt_raw = _calc_row(price_usd, supply_total_sat, transferred_sat)
        if price_usd is None:
            logger.warning('nvt_missing_price bucket=%s interval=%s', bucket_start_utc, interval)

        cur.execute(
            f'''
            INSERT INTO {table}(
              bucket_start_utc, price_usd, supply_total_sat, market_cap_usd,
              transferred_sat, transferred_value_usd, nvt_raw,
              source_start_height, source_end_height, version, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (bucket_start_utc) DO UPDATE SET
              price_usd = EXCLUDED.price_usd,
              supply_total_sat = EXCLUDED.supply_total_sat,
              market_cap_usd = EXCLUDED.market_cap_usd,
              transferred_sat = EXCLUDED.transferred_sat,
              transferred_value_usd = EXCLUDED.transferred_value_usd,
              nvt_raw = EXCLUDED.nvt_raw,
              source_start_height = EXCLUDED.source_start_height,
              source_end_height = EXCLUDED.source_end_height,
              version = EXCLUDED.version,
              updated_at = now()
            ''',
            (bucket_start_utc, price_usd, supply_total_sat, market_cap_usd, transferred_sat, transferred_value_usd, nvt_raw, source_start, source_end, 'v1'),
        )
