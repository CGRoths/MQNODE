from mqnode.scripts.init_db import run_migrations
from mqnode.db.connection import DB
from mqnode.config.settings import get_settings

if __name__ == '__main__':
    from mqnode.scripts.seed_registry import DB, PRICE_SOURCE_SQL, SQL, get_settings
    with DB(get_settings()).cursor() as cur:
        cur.execute(SQL)
        cur.execute(PRICE_SOURCE_SQL)
    print('Metric registry seeded.')
