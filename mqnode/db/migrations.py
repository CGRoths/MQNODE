from __future__ import annotations

from pathlib import Path

from mqnode.db.connection import DB


def run_schema(db: DB) -> None:
    schema_path = Path(__file__).with_name('schema.sql')
    sql = schema_path.read_text(encoding='utf-8')
    with db.cursor() as cur:
        cur.execute(sql)
