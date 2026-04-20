from __future__ import annotations

from datetime import datetime, timezone

from mqnode.chains.btc.block_parser import parse_block
from mqnode.chains.btc.rpc import BitcoinRPC


def _to_dt(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc) if ts is not None else None


def ingest_block(cur, rpc: BitcoinRPC, height: int, last_supply_sat: int) -> int:
    block_hash = rpc.get_block_hash(height)
    block = rpc.get_block(block_hash)
    raw, primitive = parse_block(block, cumulative_supply_sat_prev=last_supply_sat)

    cur.execute(
        '''
        INSERT INTO btc_blocks_raw(
          height, block_hash, previous_block_hash, block_time, median_time, tx_count, size,
          stripped_size, weight, difficulty, chainwork, version, merkle_root, raw_json
        ) VALUES (%(height)s, %(block_hash)s, %(previous_block_hash)s, %(block_time)s, %(median_time)s, %(tx_count)s,
                  %(size)s, %(stripped_size)s, %(weight)s, %(difficulty)s, %(chainwork)s, %(version)s,
                  %(merkle_root)s, %(raw_json)s)
        ON CONFLICT (height) DO UPDATE SET
          block_hash = EXCLUDED.block_hash,
          previous_block_hash = EXCLUDED.previous_block_hash,
          block_time = EXCLUDED.block_time,
          median_time = EXCLUDED.median_time,
          tx_count = EXCLUDED.tx_count,
          size = EXCLUDED.size,
          stripped_size = EXCLUDED.stripped_size,
          weight = EXCLUDED.weight,
          difficulty = EXCLUDED.difficulty,
          chainwork = EXCLUDED.chainwork,
          version = EXCLUDED.version,
          merkle_root = EXCLUDED.merkle_root,
          raw_json = EXCLUDED.raw_json
        ''',
        {**raw, 'block_time': _to_dt(raw['block_time']), 'median_time': _to_dt(raw['median_time'])},
    )

    cur.execute(
        '''
        INSERT INTO btc_primitive_block(
          height, block_hash, block_time, median_time, tx_count, non_coinbase_tx_count,
          total_out_sat, total_fee_sat, subsidy_sat, issued_sat, miner_revenue_sat,
          input_count, output_count, block_size_bytes, block_weight_wu, block_vsize_vb,
          tx_size_total_bytes, tx_vsize_total_vb, avg_fee_sat, min_feerate_sat_vb,
          max_feerate_sat_vb, segwit_tx_count, sw_total_size_bytes, sw_total_weight_wu,
          difficulty, chainwork, cumulative_supply_sat
        ) VALUES (
          %(height)s, %(block_hash)s, %(block_time)s, %(median_time)s, %(tx_count)s, %(non_coinbase_tx_count)s,
          %(total_out_sat)s, %(total_fee_sat)s, %(subsidy_sat)s, %(issued_sat)s, %(miner_revenue_sat)s,
          %(input_count)s, %(output_count)s, %(block_size_bytes)s, %(block_weight_wu)s, %(block_vsize_vb)s,
          %(tx_size_total_bytes)s, %(tx_vsize_total_vb)s, %(avg_fee_sat)s, %(min_feerate_sat_vb)s,
          %(max_feerate_sat_vb)s, %(segwit_tx_count)s, %(sw_total_size_bytes)s, %(sw_total_weight_wu)s,
          %(difficulty)s, %(chainwork)s, %(cumulative_supply_sat)s
        )
        ON CONFLICT (height) DO UPDATE SET
          block_hash = EXCLUDED.block_hash,
          block_time = EXCLUDED.block_time,
          median_time = EXCLUDED.median_time,
          tx_count = EXCLUDED.tx_count,
          non_coinbase_tx_count = EXCLUDED.non_coinbase_tx_count,
          total_out_sat = EXCLUDED.total_out_sat,
          total_fee_sat = EXCLUDED.total_fee_sat,
          subsidy_sat = EXCLUDED.subsidy_sat,
          issued_sat = EXCLUDED.issued_sat,
          miner_revenue_sat = EXCLUDED.miner_revenue_sat,
          input_count = EXCLUDED.input_count,
          output_count = EXCLUDED.output_count,
          block_size_bytes = EXCLUDED.block_size_bytes,
          block_weight_wu = EXCLUDED.block_weight_wu,
          block_vsize_vb = EXCLUDED.block_vsize_vb,
          tx_size_total_bytes = EXCLUDED.tx_size_total_bytes,
          tx_vsize_total_vb = EXCLUDED.tx_vsize_total_vb,
          avg_fee_sat = EXCLUDED.avg_fee_sat,
          min_feerate_sat_vb = EXCLUDED.min_feerate_sat_vb,
          max_feerate_sat_vb = EXCLUDED.max_feerate_sat_vb,
          segwit_tx_count = EXCLUDED.segwit_tx_count,
          sw_total_size_bytes = EXCLUDED.sw_total_size_bytes,
          sw_total_weight_wu = EXCLUDED.sw_total_weight_wu,
          difficulty = EXCLUDED.difficulty,
          chainwork = EXCLUDED.chainwork,
          cumulative_supply_sat = EXCLUDED.cumulative_supply_sat
        ''',
        {**primitive, 'block_time': _to_dt(primitive['block_time']), 'median_time': _to_dt(primitive['median_time'])},
    )
    return int(primitive['cumulative_supply_sat'])
