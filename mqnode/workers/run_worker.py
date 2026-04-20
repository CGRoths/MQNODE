from __future__ import annotations

import argparse
import logging
import threading
import time

from rq import Connection, Worker

from mqnode.checkpoints.checkpoint_service import checkpoint_error, checkpoint_ok
from mqnode.config.logging_config import configure_logging
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.queue.jobs import BTC_MARKET_QUEUE, BTC_MINER_QUEUE, BTC_NETWORK_QUEUE, BTC_PRIMITIVE_QUEUE
from mqnode.queue.redis_conn import get_redis
from mqnode.workers.btc_market_worker import replay_market_startup
from mqnode.workers.btc_miner_worker import replay_miner_startup
from mqnode.workers.btc_network_worker import replay_network_startup
from mqnode.workers.btc_primitive_worker import replay_primitive_startup

logger = logging.getLogger(__name__)

REPLAY_HANDLERS = {
    BTC_PRIMITIVE_QUEUE: replay_primitive_startup,
    BTC_NETWORK_QUEUE: replay_network_startup,
    BTC_MINER_QUEUE: replay_miner_startup,
    BTC_MARKET_QUEUE: replay_market_startup,
}


def _worker_component(queue_name: str) -> str:
    return f'worker_{queue_name}'


def _start_worker_heartbeat(queue_name: str, stop_event: threading.Event) -> threading.Thread:
    settings = get_settings()
    db = DB(settings)

    def _beat() -> None:
        while not stop_event.is_set():
            try:
                with db.cursor() as cur:
                    checkpoint_ok(cur, 'BTC', _worker_component(queue_name), 'heartbeat')
            except Exception as exc:
                logger.exception('worker_heartbeat_failed queue=%s error=%s', queue_name, exc)
            stop_event.wait(settings.worker_heartbeat_seconds)

    thread = threading.Thread(target=_beat, name=f'{queue_name}-heartbeat', daemon=True)
    thread.start()
    return thread


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--queue', required=True)
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)

    while True:
        stop_event = threading.Event()
        heartbeat_thread = _start_worker_heartbeat(args.queue, stop_event)
        try:
            if settings.worker_startup_replay and args.queue in REPLAY_HANDLERS:
                replayed = REPLAY_HANDLERS[args.queue]()
                logger.info('worker_startup_replay queue=%s replayed=%s', args.queue, replayed)

            redis = get_redis(settings)
            with Connection(redis):
                worker = Worker([args.queue], connection=redis)
                with DB(settings).cursor() as cur:
                    checkpoint_ok(cur, 'BTC', _worker_component(args.queue), 'heartbeat')
                worker.work()
            return
        except Exception as exc:
            logger.exception('worker_runtime_error queue=%s error=%s', args.queue, exc)
            with DB(settings).cursor() as cur:
                checkpoint_error(cur, 'BTC', _worker_component(args.queue), 'heartbeat', str(exc))
            time.sleep(settings.worker_retry_sleep_seconds)
        finally:
            stop_event.set()
            heartbeat_thread.join(timeout=1)


if __name__ == '__main__':
    main()
