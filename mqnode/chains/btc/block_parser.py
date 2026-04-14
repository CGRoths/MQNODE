from __future__ import annotations

from typing import Any


def parse_block(block: dict[str, Any], cumulative_supply_sat_prev: int) -> tuple[dict[str, Any], dict[str, Any]]:
    txs = block.get('tx', [])
    tx_count = len(txs)
    non_coinbase_count = max(0, tx_count - 1)
    total_out_sat = 0
    total_fee_sat = 0
    input_count = 0
    output_count = 0

    subsidy_sat = 0
    if txs:
        coinbase_vout = txs[0].get('vout', [])
        subsidy_sat = int(sum((v.get('value', 0) or 0) for v in coinbase_vout) * 100_000_000)

    for tx in txs:
        input_count += len(tx.get('vin', []))
        vout = tx.get('vout', [])
        output_count += len(vout)
        total_out_sat += int(sum((v.get('value', 0) or 0) for v in vout) * 100_000_000)
        fee = tx.get('fee')
        if fee is not None:
            total_fee_sat += int(fee * 100_000_000)

    issued_sat = subsidy_sat
    cumulative_supply_sat = cumulative_supply_sat_prev + issued_sat
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
        'block_weight_wu': block.get('weight', 0),
        'block_vsize_vb': block.get('strippedsize', 0),
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
