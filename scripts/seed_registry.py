from mqnode.scripts.seed_registry import *

if __name__ == '__main__':
    from mqnode.scripts.seed_registry import DB, SQL, get_settings
    with DB(get_settings()).cursor() as cur:
        cur.execute(SQL)
    print('Metric registry seeded.')
