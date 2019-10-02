"""
Main module responsible for execution and config initialization
"""
import configparser
import logging
import threading
from concurrent.futures.thread import ThreadPoolExecutor as thread_pool
from time import sleep

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED

import watch_status
from watch_status import cfg, event_listener
from watch_status.db import get_connection
from watch_status.stable_heap import StableHeap

logger = logging.getLogger('watch_status')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(cfg.main.log_file)
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

def init_config() -> None:
    """
        initializes configuration
    """

    config = configparser.ConfigParser()
    if config.read(cfg.main.cfg_file):
        logger.info('config found')
        cfg.DSN = config['main']['DSN']
        cfg.log_file = config['main']['log']
    else:
        logger.info('config not found')


if __name__ == "__main__":
    stable_heap = StableHeap()
    conn_evnt = get_connection(cfg.db.DSN, cfg.db.schema, ISOLATION_LEVEL_AUTOCOMMIT)
    threading.Thread(target=event_listener, args=(conn_evnt, cfg.db.channel, stable_heap), name='event_listener').start()

    conn = get_connection(cfg.db.DSN, cfg.db.schema, ISOLATION_LEVEL_READ_COMMITTED)

    with thread_pool(max_workers=cfg.main.max_workers, thread_name_prefix='watch_status_worker') as executor:
        futures = {executor.submit(watch_status.worker, conn, stable_heap, id): id for id in range(cfg.main.max_workers)}
        while True:
            done = [future_id for future_id in futures.items() if future_id[0].done()]
            logger.info('threads done: {}'.format(done))
            for future, id in done:
                try:
                    future.result()
                except BaseException:
                    logger.exception('worker id {} thread raised exception'.format(id))
                else:
                    logger.error('worker id {} thread finished'.format(id))
                finally:
                    del(futures[future])
                    futures[executor.submit(watch_status.worker, conn, stable_heap, id)] = id
                    logger.debug('worker id {} restarted'.format(id))
            sleep(1)
