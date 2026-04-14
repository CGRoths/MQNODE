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
        ON CONFLICT (height) DO UPDATE SET raw_json=EXCLUDED.raw_json
        ''',
        {**raw, 'block_time': _to_dt(raw['block_time']), 'median_time': _to_dt(raw['median_time'])},
    )

    cur.execute(
        '''
        INSERT INTO btc_primitive_block(
          height, block_hash, block_time, median_time, tx_count, non_coinbase_tx_count,
          total_out_sat, total_fee_sat, subsidy_sat, issued_sat, miner_revenue_sat,
          input_count, output_count, block_size_bytes, block_weight_wu, block_vsize_vb,
          difficulty, chainwork, cumulative_supply_sat
        ) VALUES (
          %(height)s, %(block_hash)s, %(block_time)s, %(median_time)s, %(tx_count)s, %(non_coinbase_tx_count)s,
          %(total_out_sat)s, %(total_fee_sat)s, %(subsidy_sat)s, %(issued_sat)s, %(miner_revenue_sat)s,
          %(input_count)s, %(output_count)s, %(block_size_bytes)s, %(block_weight_wu)s, %(block_vsize_vb)s,
          %(difficulty)s, %(chainwork)s, %(cumulative_supply_sat)s
        )
        ON CONFLICT (height) DO UPDATE SET
          total_out_sat = EXCLUDED.total_out_sat,
          total_fee_sat = EXCLUDED.total_fee_sat,
          cumulative_supply_sat = EXCLUDED.cumulative_supply_sat
        ''',
        {**primitive, 'block_time': _to_dt(primitive['block_time']), 'median_time': _to_dt(primitive['median_time'])},
    )
    return int(primitive['cumulative_supply_sat'])
