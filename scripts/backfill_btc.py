from mqnode.scripts.init_db import run_migrations
from mqnode.db.connection import DB
from mqnode.config.settings import get_settings


if __name__ == '__main__':
    import runpy
    runpy.run_module('mqnode.scripts.backfill_btc', run_name='__main__')
