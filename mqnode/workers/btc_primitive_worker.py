from __future__ import annotations

from mqnode.chains.btc.primitive_builder import rebuild_30m_bucket
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB


def process_raw_block_job(payload: dict):
    height = int(payload['height'])
    db = DB(get_settings())
    return rebuild_30m_bucket(db, height)
