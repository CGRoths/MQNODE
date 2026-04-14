from __future__ import annotations

import logging
import time

from mqnode.chains.btc.ingest import ingest_block
from mqnode.chains.btc.rpc import BitcoinRPC
from mqnode.config.logging_config import configure_logging
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.db.repositories import get_checkpoint, upsert_checkpoint
from mqnode.queue.producer import enqueue_raw_block_ready

logger = logging.getLogger(__name__)


def run_listener() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    db = DB(settings)
    rpc = BitcoinRPC(settings)

    while True:
        try:
            with db.cursor() as cur:
                checkpoint = get_checkpoint(cur, 'BTC', 'btc_raw_block_ingestion', 'block')
                last_height = int(checkpoint.get('last_height') or 0)
                cur.execute('SELECT COALESCE(MAX(cumulative_supply_sat), 0) AS s FROM btc_primitive_block WHERE height <= %s', (last_height,))
                last_supply_sat = int(cur.fetchone()['s'])

            node_height = rpc.get_block_count()
            if node_height <= last_height:
                logger.info('listener_no_new_block last_height=%s node_height=%s', last_height, node_height)
                time.sleep(settings.btc_listener_sleep_seconds)
                continue

            for h in range(last_height + 1, node_height + 1):
                try:
                    with db.cursor() as cur:
                        last_supply_sat = ingest_block(cur, rpc, h, last_supply_sat)
                        upsert_checkpoint(cur, 'BTC', 'btc_raw_block_ingestion', 'block', last_height=h, status='ok')
                    enqueue_raw_block_ready(h)
                    logger.info('ingested_block height=%s', h)
                except Exception as exc:
                    logger.exception('ingest_failed height=%s err=%s', h, exc)
                    with db.cursor() as cur:
                        upsert_checkpoint(cur, 'BTC', 'btc_raw_block_ingestion', 'block', last_height=h - 1, status='error', error_message=str(exc))
                    continue
        except Exception as exc:
            logger.exception('listener_loop_error err=%s', exc)
            time.sleep(settings.btc_listener_sleep_seconds)


if __name__ == '__main__':
    run_listener()
