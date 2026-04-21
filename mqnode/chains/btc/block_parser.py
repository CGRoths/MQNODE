from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from statistics import mean
from typing import Any

SATOSHIS_PER_BTC = Decimal('100000000')
HALVING_INTERVAL = 210_000
INITIAL_SUBSIDY_SAT = 50 * 100_000_000


def _btc_to_sat(value: Any) -> int:
    """Convert a BTC-denominated RPC value into satoshis without float drift."""
    if value is None:
        return 0
    return int((Decimal(str(value)) * SATOSHIS_PER_BTC).quantize(Decimal('1'), rounding=ROUND_HALF_UP))


def _block_subsidy_sat(height: int) -> int:
    """Return the consensus max subsidy for a block height."""
    halvings = height // HALVING_INTERVAL
    if halvings >= 64:
        return 0
    return INITIAL_SUBSIDY_SAT >> halvings


def _is_coinbase_tx(tx: dict[str, Any]) -> bool:
    vin = tx.get('vin') or []
    return bool(vin and vin[0].get('coinbase'))


def parse_block(block: dict[str, Any], cumulative_supply_sat_prev: int) -> tuple[dict[str, Any], dict[str, Any]]:
    """Normalize a verbose Bitcoin Core block into raw and primitive block records."""
    txs = block.get('tx', [])
    tx_count = len(txs)
    non_coinbase_count = max(0, tx_count - 1)
    total_out_sat = 0
    input_count = 0
    output_count = 0
    tx_size_total_bytes = 0
    tx_vsize_total_vb = 0
    segwit_tx_count = 0
    sw_total_size_bytes = 0
    sw_total_weight_wu = 0
    fee_values_sat: list[int] = []
    feerate_values_sat_vb: list[float] = []
    observed_fee_sat_total = 0

    max_subsidy_sat = _block_subsidy_sat(int(block['height'])) if txs else 0
    coinbase_reward_sat = 0
    if txs:
        coinbase_vout = txs[0].get('vout', [])
        coinbase_reward_sat = sum(_btc_to_sat(v.get('value', 0) or 0) for v in coinbase_vout)

    for tx in txs:
        is_coinbase = _is_coinbase_tx(tx)
        vin = tx.get('vin', [])
        if not is_coinbase:
            input_count += len(vin)
        vout = tx.get('vout', [])
        output_count += len(vout)
        total_out_sat += sum(_btc_to_sat(v.get('value', 0) or 0) for v in vout)
        tx_size = int(tx.get('size') or 0)
        tx_weight = int(tx.get('weight') or 0)
        tx_vsize = int(tx.get('vsize') or ((tx_weight + 3) // 4 if tx_weight else 0))
        tx_size_total_bytes += tx_size
        tx_vsize_total_vb += tx_vsize

        if not is_coinbase and any(vin_row.get('txinwitness') for vin_row in vin):
            segwit_tx_count += 1
            sw_total_size_bytes += tx_size
            sw_total_weight_wu += tx_weight

        if is_coinbase:
            continue

        fee = tx.get('fee')
        if fee is not None:
            fee_sat = _btc_to_sat(fee)
            observed_fee_sat_total += fee_sat
            fee_values_sat.append(fee_sat)
            if tx_vsize > 0:
                feerate_values_sat_vb.append(fee_sat / tx_vsize)

    fee_observations_complete = len(fee_values_sat) == non_coinbase_count
    total_fee_sat = (
        observed_fee_sat_total
        if fee_observations_complete
        else max(coinbase_reward_sat - max_subsidy_sat, 0)
    )
    # Supply should follow the subsidy actually created on-chain, not the headline reward cap.
    issued_sat = min(max(coinbase_reward_sat - total_fee_sat, 0), max_subsidy_sat)
    subsidy_sat = issued_sat
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
        'miner_revenue_sat': coinbase_reward_sat,
        'input_count': input_count,
        'output_count': output_count,
        'block_size_bytes': block.get('size', 0),
        'block_weight_wu': block_weight_wu,
        'block_vsize_vb': int(
            block.get('vsize')
            or ((block_weight_wu + 3) // 4 if block_weight_wu else block.get('strippedsize', 0) or 0)
        ),
        'tx_size_total_bytes': tx_size_total_bytes,
        'tx_vsize_total_vb': tx_vsize_total_vb,
        'avg_fee_sat': mean(fee_values_sat) if fee_observations_complete and fee_values_sat else None,
        'min_feerate_sat_vb': (
            min(feerate_values_sat_vb)
            if fee_observations_complete and len(feerate_values_sat_vb) == non_coinbase_count
            else None
        ),
        'max_feerate_sat_vb': (
            max(feerate_values_sat_vb)
            if fee_observations_complete and len(feerate_values_sat_vb) == non_coinbase_count
            else None
        ),
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
