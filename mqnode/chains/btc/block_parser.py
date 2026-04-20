from __future__ import annotations

from statistics import mean
from typing import Any


def parse_block(block: dict[str, Any], cumulative_supply_sat_prev: int) -> tuple[dict[str, Any], dict[str, Any]]:
    txs = block.get('tx', [])
    tx_count = len(txs)
    non_coinbase_count = max(0, tx_count - 1)
    total_out_sat = 0
    total_fee_sat = 0
    input_count = 0
    output_count = 0
    tx_size_total_bytes = 0
    tx_vsize_total_vb = 0
    segwit_tx_count = 0
    sw_total_size_bytes = 0
    sw_total_weight_wu = 0
    fee_values_sat: list[int] = []
    feerate_values_sat_vb: list[float] = []

    subsidy_sat = 0
    if txs:
        coinbase_vout = txs[0].get('vout', [])
        subsidy_sat = int(sum((v.get('value', 0) or 0) for v in coinbase_vout) * 100_000_000)

    for tx in txs:
        input_count += len(tx.get('vin', []))
        vout = tx.get('vout', [])
        output_count += len(vout)
        total_out_sat += int(sum((v.get('value', 0) or 0) for v in vout) * 100_000_000)
        tx_size = int(tx.get('size') or 0)
        tx_weight = int(tx.get('weight') or 0)
        tx_vsize = int(tx.get('vsize') or ((tx_weight + 3) // 4 if tx_weight else 0))
        tx_size_total_bytes += tx_size
        tx_vsize_total_vb += tx_vsize

        if any(vin.get('txinwitness') for vin in tx.get('vin', [])):
            segwit_tx_count += 1
            sw_total_size_bytes += tx_size
            sw_total_weight_wu += tx_weight

        fee = tx.get('fee')
        if fee is not None:
            fee_sat = int(fee * 100_000_000)
            total_fee_sat += fee_sat
            fee_values_sat.append(fee_sat)
            if tx_vsize > 0:
                feerate_values_sat_vb.append(fee_sat / tx_vsize)

    issued_sat = subsidy_sat
    cumulative_supply_sat = cumulative_supply_sat_prev + issued_sat
    block_weight_wu = int(block.get('weight') or 0)
    primitive = {
        'height': block['height'],
        'block_hash': block['hash'],
        'block_time': block.get('time'),
        'median_time': block.get('mediantime'),
        'tx_count': tx_count,
        'non_coinbase_tx_count': non_coinbase_count,
        'total_out_sat': total_out_sat,
        'total_fee_sat': total_fee_sat,
        'subsidy_sat': subsidy_sat,
        'issued_sat': issued_sat,
        'miner_revenue_sat': issued_sat + total_fee_sat,
        'input_count': input_count,
        'output_count': output_count,
        'block_size_bytes': block.get('size', 0),
        'block_weight_wu': block_weight_wu,
        'block_vsize_vb': int(block.get('vsize') or ((block_weight_wu + 3) // 4 if block_weight_wu else block.get('strippedsize', 0) or 0)),
        'tx_size_total_bytes': tx_size_total_bytes,
        'tx_vsize_total_vb': tx_vsize_total_vb,
        'avg_fee_sat': mean(fee_values_sat) if fee_values_sat else None,
        'min_feerate_sat_vb': min(feerate_values_sat_vb) if feerate_values_sat_vb else None,
        'max_feerate_sat_vb': max(feerate_values_sat_vb) if feerate_values_sat_vb else None,
        'segwit_tx_count': segwit_tx_count,
        'sw_total_size_bytes': sw_total_size_bytes,
        'sw_total_weight_wu': sw_total_weight_wu,
        'difficulty': block.get('difficulty'),
        'chainwork': block.get('chainwork'),
        'cumulative_supply_sat': cumulative_supply_sat,
    }
    raw = {
        'height': block['height'],
        'block_hash': block['hash'],
        'previous_block_hash': block.get('previousblockhash'),
        'block_time': block.get('time'),
        'median_time': block.get('mediantime'),
        'tx_count': tx_count,
        'size': block.get('size', 0),
        'stripped_size': block.get('strippedsize', 0),
        'weight': block.get('weight', 0),
        'difficulty': block.get('difficulty'),
        'chainwork': block.get('chainwork'),
        'version': block.get('version'),
        'merkle_root': block.get('merkleroot'),
        'raw_json': block,
    }
    return raw, primitive
