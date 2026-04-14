from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from mqnode.core.utils import SATOSHI_PER_BTC, median, to_bucket_start_30m, to_open_time_ms
from mqnode.db.repositories import upsert_checkpoint
from mqnode.queue.producer import enqueue_primitive_ready

logger = logging.getLogger(__name__)


def rebuild_30m_bucket(db, height: int) -> datetime | None:
    with db.cursor() as cur:
        cur.execute('SELECT COALESCE(median_time, block_time) AS t FROM btc_primitive_block WHERE height = %s', (height,))
        row = cur.fetchone()
        if not row or not row['t']:
            return None
        bucket = to_bucket_start_30m(row['t'])
        bucket_end = bucket + timedelta(minutes=30)

        cur.execute(
            '''
            SELECT * FROM btc_primitive_block
            WHERE COALESCE(median_time, block_time) >= %s
              AND COALESCE(median_time, block_time) < %s
            ORDER BY height ASC
            ''',
            (bucket, bucket_end),
        )
        blocks = cur.fetchall()
        if not blocks:
            return None

        first, last = blocks[0], blocks[-1]
        block_times = [b['block_time'].timestamp() for b in blocks if b['block_time']]
        intervals = [j - i for i, j in zip(block_times, block_times[1:])]

        issued = sum(b['issued_sat'] or 0 for b in blocks)
        fees = sum(b['total_fee_sat'] or 0 for b in blocks)
        total_out = sum(b['total_out_sat'] or 0 for b in blocks)
        tx_count = sum(b['tx_count'] or 0 for b in blocks)
        input_count = sum(b['input_count'] or 0 for b in blocks)
        output_count = sum(b['output_count'] or 0 for b in blocks)

        close_price = None
        market_cap = (last['cumulative_supply_sat'] / SATOSHI_PER_BTC) * close_price if close_price else None
        onchain_vol_usd = ((total_out + fees) / SATOSHI_PER_BTC) * close_price if close_price else None

        cur.execute(
            '''
            INSERT INTO btc_primitive_30m(
              bucket_start_utc, open_time_ms, first_height, last_height, block_count,
              first_block_time_utc, last_block_time_utc, issued_sat_30m, fees_sat_30m,
              miner_revenue_sat_30m, supply_total_sat, block_reward_sat_avg, halving_epoch,
              total_out_sat_30m, total_fee_sat_30m, transferred_sat_30m, transferred_btc_30m,
              tx_count_30m, input_count_30m, output_count_30m, tx_rate_per_sec_30m,
              block_size_total_bytes_30m, block_size_mean_bytes_30m, block_weight_total_wu_30m,
              block_weight_mean_wu_30m, block_vsize_total_vb_30m,
              block_interval_mean_sec_30m, block_interval_median_sec_30m, difficulty_last,
              chainwork_last, best_block_height_last, close_price_usd, market_cap_usd,
              onchain_volume_usd_raw_30m, updated_at
            ) VALUES (
              %(bucket_start_utc)s, %(open_time_ms)s, %(first_height)s, %(last_height)s, %(block_count)s,
              %(first_block_time_utc)s, %(last_block_time_utc)s, %(issued_sat_30m)s, %(fees_sat_30m)s,
              %(miner_revenue_sat_30m)s, %(supply_total_sat)s, %(block_reward_sat_avg)s, %(halving_epoch)s,
              %(total_out_sat_30m)s, %(total_fee_sat_30m)s, %(transferred_sat_30m)s, %(transferred_btc_30m)s,
              %(tx_count_30m)s, %(input_count_30m)s, %(output_count_30m)s, %(tx_rate_per_sec_30m)s,
              %(block_size_total_bytes_30m)s, %(block_size_mean_bytes_30m)s, %(block_weight_total_wu_30m)s,
              %(block_weight_mean_wu_30m)s, %(block_vsize_total_vb_30m)s,
              %(block_interval_mean_sec_30m)s, %(block_interval_median_sec_30m)s, %(difficulty_last)s,
              %(chainwork_last)s, %(best_block_height_last)s, %(close_price_usd)s, %(market_cap_usd)s,
              %(onchain_volume_usd_raw_30m)s, %(updated_at)s
            )
            ON CONFLICT (bucket_start_utc) DO UPDATE SET
              last_height = EXCLUDED.last_height,
              block_count = EXCLUDED.block_count,
              issued_sat_30m = EXCLUDED.issued_sat_30m,
              fees_sat_30m = EXCLUDED.fees_sat_30m,
              miner_revenue_sat_30m = EXCLUDED.miner_revenue_sat_30m,
              supply_total_sat = EXCLUDED.supply_total_sat,
              total_out_sat_30m = EXCLUDED.total_out_sat_30m,
              total_fee_sat_30m = EXCLUDED.total_fee_sat_30m,
              transferred_sat_30m = EXCLUDED.transferred_sat_30m,
              tx_count_30m = EXCLUDED.tx_count_30m,
              difficulty_last = EXCLUDED.difficulty_last,
              chainwork_last = EXCLUDED.chainwork_last,
              best_block_height_last = EXCLUDED.best_block_height_last,
              close_price_usd = EXCLUDED.close_price_usd,
              market_cap_usd = EXCLUDED.market_cap_usd,
              onchain_volume_usd_raw_30m = EXCLUDED.onchain_volume_usd_raw_30m,
              updated_at = EXCLUDED.updated_at
            ''',
            {
                'bucket_start_utc': bucket,
                'open_time_ms': to_open_time_ms(bucket),
                'first_height': first['height'],
                'last_height': last['height'],
                'block_count': len(blocks),
                'first_block_time_utc': first['block_time'],
                'last_block_time_utc': last['block_time'],
                'issued_sat_30m': issued,
                'fees_sat_30m': fees,
                'miner_revenue_sat_30m': issued + fees,
                'supply_total_sat': last['cumulative_supply_sat'],
                'block_reward_sat_avg': issued / len(blocks),
                'halving_epoch': int((first['height'] or 0) // 210000),
                'total_out_sat_30m': total_out,
                'total_fee_sat_30m': fees,
                'transferred_sat_30m': total_out + fees,
                'transferred_btc_30m': (total_out + fees) / SATOSHI_PER_BTC,
                'tx_count_30m': tx_count,
                'input_count_30m': input_count,
                'output_count_30m': output_count,
                'tx_rate_per_sec_30m': tx_count / 1800,
                'block_size_total_bytes_30m': sum(b['block_size_bytes'] or 0 for b in blocks),
                'block_size_mean_bytes_30m': sum(b['block_size_bytes'] or 0 for b in blocks) / len(blocks),
                'block_weight_total_wu_30m': sum(b['block_weight_wu'] or 0 for b in blocks),
                'block_weight_mean_wu_30m': sum(b['block_weight_wu'] or 0 for b in blocks) / len(blocks),
                'block_vsize_total_vb_30m': sum(b['block_vsize_vb'] or 0 for b in blocks),
                'block_interval_mean_sec_30m': sum(intervals) / len(intervals) if intervals else None,
                'block_interval_median_sec_30m': median(intervals),
                'difficulty_last': last['difficulty'],
                'chainwork_last': last['chainwork'],
                'best_block_height_last': last['height'],
                'close_price_usd': close_price,
                'market_cap_usd': market_cap,
                'onchain_volume_usd_raw_30m': onchain_vol_usd,
                'updated_at': datetime.now(timezone.utc),
            },
        )
        upsert_checkpoint(cur, 'BTC', 'btc_primitive_30m_builder', '30m', last_height=last['height'], last_bucket_time=bucket)

    enqueue_primitive_ready(bucket, '30m')
    return bucket
