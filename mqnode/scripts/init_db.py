from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.db.migrations import run_schema


if __name__ == '__main__':
    run_schema(DB(get_settings()))
    print('Schema initialized.')
