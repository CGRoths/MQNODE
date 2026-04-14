from fastapi import APIRouter

from mqnode.config.settings import get_settings
from mqnode.db.connection import DB

router = APIRouter(prefix='/api/v1/btc')


@router.get('/registry')
def registry():
    with DB(get_settings()).cursor() as cur:
        cur.execute('SELECT metric_name, chain, factor, interval, version, output_table FROM metric_registry WHERE enabled = true ORDER BY id ASC')
        rows = cur.fetchall()
    return {'count': len(rows), 'items': rows}
