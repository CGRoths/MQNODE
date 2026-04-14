from __future__ import annotations

import argparse

from mqnode.chains.btc.ingest import ingest_block
from mqnode.chains.btc.rpc import BitcoinRPC
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.db.repositories import upsert_checkpoint
from mqnode.queue.producer import enqueue_raw_block_ready


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-height', type=int, required=True)
    parser.add_argument('--end-height', type=int, required=True)
    args = parser.parse_args()

    settings = get_settings()
    db = DB(settings)
    rpc = BitcoinRPC(settings)

    supply = 0
    with db.cursor() as cur:
        cur.execute('SELECT COALESCE(MAX(cumulative_supply_sat), 0) AS s FROM btc_primitive_block WHERE height < %s', (args.start_height,))
        supply = int(cur.fetchone()['s'])

    for h in range(args.start_height, args.end_height + 1):
        with db.cursor() as cur:
            supply = ingest_block(cur, rpc, h, supply)
            upsert_checkpoint(cur, 'BTC', 'btc_raw_block_ingestion', 'block', last_height=h)
        enqueue_raw_block_ready(h)
        print(f'Backfilled block {h}')
