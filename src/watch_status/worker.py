import json
import logging
import threading
from datetime import datetime
from time import sleep

from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

from watch_status import cfg
from watch_status.db import get_connection, set_active_schema, update_column

logger = logging.getLogger('watch_status.event_listener')
file_lock = threading.Lock()


def worker(conn, stab_heap, id, file_name=cfg.main.workers_output_file):
    while True:
        logger.debug('{}: checking heap for entries'.format(id))
        while not stab_heap.empty():
            event_data = stab_heap.pop()
            logger.debug('{}: processing {}'.format(id, event_data))
            table = event_data['table']
            column = 'logged_at'
            pk_column = 'id'
            pk = event_data['data']['id']
            value = datetime.utcnow()
            update_column(conn, table, column, pk_column, pk, value)
            log_action(file_name, str(id) + ': ' + json.dumps(event_data))
        sleep(1)


def log_action(file_name, data):
    with file_lock:
        with open(file_name, 'a') as file:
            file.write(data+'\n')

