from mqnode.scripts.seed_registry import *

if __name__ == '__main__':
    from mqnode.scripts.seed_registry import DB, PRICE_SOURCE_SQL, SQL, get_settings
    with DB(get_settings()).cursor() as cur:
        cur.execute(SQL)
        cur.execute(PRICE_SOURCE_SQL)
    print('Metric registry seeded.')
