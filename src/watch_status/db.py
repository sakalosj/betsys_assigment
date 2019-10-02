import json
import logging
import select

import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from watch_status import cfg

logger = logging.getLogger('watch_status.event_listener')


def get_connection(DSN, schema, isolation_level):
    conn = psycopg2.connect(DSN)
    conn.set_isolation_level(isolation_level)
    set_active_schema(conn, schema)
    return conn


def event_listener(conn, channel, stable_heap) -> None:
    """

    Args:
        conn: connection object
        stable_heap: StableHeap

    Returns:
        None

    """
    curs = conn.cursor()
    curs.execute("LISTEN {};".format(channel))

    logger.debug('event loop started, listening channel {}'.format(cfg.db.channel))
    while True:
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
        # print(query)
        values = (value, pk)
        curs.execute(query, values)
    conn.commit()

