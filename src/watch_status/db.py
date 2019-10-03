import json
import logging
import select

import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from watch_status import sig_handler, cfg

logger = logging.getLogger('watch_status.db_event_loop')

class Proxy:
    """
    TODO:
    self db_instance - readonly
    make db_fields private

    """

    @classmethod
    def __init__(self, DSN, schema, isolation_level):
        self.DSN = DSN
        self.schema = schema
        self.isolation_level = isolation_level

        conn = psycopg2.connect(DSN)
        conn.set_isolation_level(isolation_level)
        set_active_schema(conn, schema)
        self._conn = conn


    def __getattr__(self, item):
            return getattr(self._conn, item)



class RetryConnection(psycopg2.extensions.connection):
    pass


def get_connection(DSN, schema, isolation_level):
    conn = psycopg2.connect(DSN)
    conn.set_isolation_level(isolation_level)
    set_active_schema(conn, schema)
    return conn


def db_event_loop(conn, channel, stable_heap) -> None:
    """

    Args:
        conn: connection object
        stable_heap: StableHeap

    Returns:
        None

    """
    curs = conn.cursor()
    curs.execute("LISTEN {};".format(channel))

    logger.debug('db_event_loop started, listening channel {} notifications'.format(cfg.db.channel))
    while not sig_handler.finish:
        if select.select([conn], [], [], 5) == ([], [], []):
            logger.debug('listening ...')
        else:
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                logger.debug('event catched: {}'.format(notify))
                payload_dict = json.loads(notify.payload)
                stable_heap.push(payload_dict['data']['id'], payload_dict)


def set_active_schema(conn, schema) -> None:
    """
    Args:
        conn: connection object
        schema: str

    Returns:
        None
    """
    with conn.cursor() as curs:
        query = 'SET search_path TO %s'
        values = (schema,)
        curs.execute(query, values)
    conn.commit()


def update_column(conn, table, column, pk_column, pk, value) -> None:
    """
        Updates column based on arguments
    Args:
        conn: connection object
        table: table name
        column: column name
        pk_column: pk column name
        pk: pk
        value: value

    Returns:
        None

    """
    with conn.cursor() as curs:
        query = sql.SQL('UPDATE {} SET {} = %s WHERE {} = %s').format(sql.Identifier(table), sql.Identifier(column),
                                                                      sql.Identifier(pk_column))
        values = (value, pk)
        curs.execute(query, values)
    conn.commit()

