from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Query

from mqnode.config.settings import get_settings
from mqnode.db.connection import DB

router = APIRouter(prefix='/api/v1/btc')


@router.get('/metrics/nvt')
def nvt(
    interval: Literal['30m', '1h'] = Query('30m'),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
):
    table = 'btc_nvt_30m' if interval == '30m' else 'btc_nvt_1h'
    start = start or datetime.now(timezone.utc) - timedelta(days=1)
    end = end or datetime.now(timezone.utc)
    with DB(get_settings()).cursor() as cur:
        cur.execute(
            f'''SELECT * FROM {table} WHERE bucket_start_utc >= %s AND bucket_start_utc <= %s ORDER BY bucket_start_utc''',
            (start, end),
        )
        rows = cur.fetchall()
    return {'interval': interval, 'count': len(rows), 'items': rows}


@router.get('/primitive')
def primitive(
    interval: Literal['30m'] = Query('30m'),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
):
    start = start or datetime.now(timezone.utc) - timedelta(days=1)
    end = end or datetime.now(timezone.utc)
    with DB(get_settings()).cursor() as cur:
        cur.execute(
            '''SELECT * FROM btc_primitive_30m WHERE bucket_start_utc >= %s AND bucket_start_utc <= %s ORDER BY bucket_start_utc''',
            (start, end),
        )
        rows = cur.fetchall()
    return {'interval': interval, 'count': len(rows), 'items': rows}
