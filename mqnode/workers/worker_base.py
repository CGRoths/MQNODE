from __future__ import annotations

import logging
from datetime import datetime

from mqnode.checkpoints.checkpoint_service import checkpoint_error, checkpoint_ok
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.registry.dynamic_loader import load_function
from mqnode.registry.metric_registry import get_enabled_metrics

logger = logging.getLogger(__name__)


class WorkerBase:
    chain = 'BTC'
    factor = 'NETWORK'

    def __init__(self):
        self.settings = get_settings()
        self.db = DB(self.settings)

    def execute_metrics(self, bucket_start_utc: datetime, interval: str) -> None:
        with self.db.cursor() as cur:
            metrics = get_enabled_metrics(self.db, self.chain, self.factor)
            if not metrics:
                logger.info('no_enabled_metrics chain=%s factor=%s', self.chain, self.factor)
                return
            for metric in metrics:
                component = f"btc_metric_{metric['metric_name']}_{interval}"
                try:
                    fn = load_function(metric['module_path'], metric['function_name'])
                    fn(self.db, bucket_start_utc, interval)
                    checkpoint_ok(cur, self.chain, component, interval, last_bucket_time=bucket_start_utc)
                    logger.info('metric_ok metric=%s bucket=%s interval=%s', metric['metric_name'], bucket_start_utc, interval)
                except Exception as exc:
                    logger.exception('metric_fail metric=%s bucket=%s interval=%s err=%s', metric['metric_name'], bucket_start_utc, interval, exc)
                    checkpoint_error(cur, self.chain, component, interval, str(exc), last_bucket_time=bucket_start_utc)
