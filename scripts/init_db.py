from mqnode.scripts.init_db import *

if __name__ == '__main__':
    from mqnode.scripts.init_db import run_schema, DB, get_settings
    run_schema(DB(get_settings()))
    print('Schema initialized.')
