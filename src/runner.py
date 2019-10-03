"""
Main module responsible for execution and config initialization
"""
import configparser
import logging
from concurrent.futures.thread import ThreadPoolExecutor as thread_pool
from functools import partial
from time import sleep

from psycopg2._psycopg import InterfaceError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED

from watch_status import cfg,  sig_handler
from watch_status.db import get_connection, db_event_loop
from watch_status.utils import StableHeap
from watch_status.worker import worker_loop

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
        exit(1)


if __name__ == "__main__":

    stb_heap = StableHeap()
    try:
        conn_evnt = get_connection(cfg.db.DSN, cfg.db.schema, ISOLATION_LEVEL_AUTOCOMMIT)
        conn = get_connection(cfg.db.DSN, cfg.db.schema, ISOLATION_LEVEL_READ_COMMITTED)

        db_event_loop_part = partial(db_event_loop, conn_evnt, cfg.db.channel, stb_heap)
        worker_loop_part = partial(worker_loop, conn, stb_heap)

        with thread_pool(max_workers=cfg.main.max_workers, thread_name_prefix='main_loop') as executor:
            futures = {executor.submit(db_event_loop_part): db_event_loop_part,
                       executor.submit(worker_loop_part): worker_loop_part
                       }

            while not sig_handler.finish:
                for future in [future for future in futures if future.done()]:
                    try:
                        future.result()
                    except InterfaceError:
                       sig_handler.finish = True
                    except Exception:
                        logger.exception(' {} raised exception'.format(futures[future].func.__name__))
                    else:
                        logger.error(' {} finished - BAD'.format(futures[future].func.__name__))
                    finally:
                        futures[executor.submit(futures[future])] = futures[future]
                        logger.debug(' {}  restarted'.format(futures[future].func.__name__))
                        del (futures[future])
                sleep(1)

    except InterfaceError:
        logger.exception()




