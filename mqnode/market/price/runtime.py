from __future__ import annotations

import argparse
import logging
import time
from importlib import import_module

from mqnode.config.logging_config import configure_logging
from mqnode.config.settings import Settings, get_settings
from mqnode.db.connection import DB
from mqnode.market.price.composer import catch_up_canonical_price_from_checkpoint
from mqnode.market.price.registry import get_price_source

logger = logging.getLogger(__name__)


def compose_prices_once(db) -> int:
    rebuilt = catch_up_canonical_price_from_checkpoint(db)
    logger.info('price_composer_once rebuilt_buckets=%s', rebuilt)
    return rebuilt


def run_price_composer(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    db = DB(settings)

    while True:
        try:
            compose_prices_once(db)
        except Exception as exc:
            logger.exception('price_composer_loop_error error=%s', exc)
        time.sleep(settings.price_composer_sleep_seconds)


def run_price_source_ingestion_once(source_name: str, settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    db = DB(settings)
    get_price_source(source_name)
    module = import_module(f'mqnode.market.price.sources.{source_name}')
    fetch_buckets = getattr(module, 'fetch_buckets', None)
    if fetch_buckets is None:
        raise NotImplementedError(
            f'Price source {source_name} has no fetch_buckets() implementation yet. '
            'Use this runtime boundary to add source-specific ingestion without changing the rest of MQNODE.'
        )
    return int(fetch_buckets(db=db, settings=settings))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['compose', 'ingest-source'])
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--source')
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)

    if args.mode == 'compose':
        if args.once:
            compose_prices_once(DB(settings))
            return
        run_price_composer(settings)
        return

    if not args.source:
        raise ValueError('--source is required for ingest-source mode')
    processed = run_price_source_ingestion_once(args.source, settings=settings)
    logger.info('price_source_ingestion_once source=%s processed=%s', args.source, processed)


if __name__ == '__main__':
    main()
