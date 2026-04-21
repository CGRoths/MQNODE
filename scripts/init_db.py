from mqnode.scripts.init_db import run_migrations
from mqnode.db.connection import DB
from mqnode.config.settings import get_settings

if __name__ == '__main__':
    from mqnode.scripts.init_db import run_migrations, DB, get_settings
    run_migrations(DB(get_settings()))
    print('Migrations applied.')
