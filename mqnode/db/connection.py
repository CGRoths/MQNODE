from __future__ import annotations

from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from mqnode.config.settings import Settings


class DB:
    def __init__(self, settings: Settings):
        self.dsn = settings.postgres_dsn

    @contextmanager
    def cursor(self):
        conn = psycopg2.connect(self.dsn)
        try:
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    yield cur
        finally:
            conn.close()
