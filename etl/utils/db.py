from contextlib import contextmanager
from dataclasses import dataclass
import os
import psycopg2


@dataclass
class DBConnection:
    db: str
    user: str
    password: str
    host: str
    port: int = 5432


class PostgresConnection:
    def __init__(self, db_conn: DBConnection):
        self.conn_url = (
            f'postgresql://{db_conn.user}:{db_conn.password}@'
            f'{db_conn.host}:{db_conn.port}/{db_conn.db}'
        )

    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        self.conn = psycopg2.connect(self.conn_url)
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()


def get_postgis_creds() -> DBConnection:
    return DBConnection(
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'password'),
        db=os.getenv('POSTGRES_DB', 'bc_str'),
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', 5433)),
    )