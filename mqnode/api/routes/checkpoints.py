from fastapi import APIRouter

from mqnode.config.settings import get_settings
from mqnode.db.connection import DB

router = APIRouter(prefix='/api/v1/btc')


@router.get('/checkpoints')
def checkpoints():
    with DB(get_settings()).cursor() as cur:
        cur.execute('SELECT * FROM sync_checkpoints WHERE chain = %s ORDER BY component, interval', ('BTC',))
        rows = cur.fetchall()
    return {'count': len(rows), 'items': rows}
