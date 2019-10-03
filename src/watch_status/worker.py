import json
import logging
import threading
from datetime import datetime
from concurrent.futures.thread import ThreadPoolExecutor as thread_pool
from time import sleep


from watch_status import cfg, sig_handler
from watch_status.db import update_column

logger = logging.getLogger('watch_status.db_event_loop')
file_lock = threading.Lock()


def worker(conn, stable_heap, id, file_name=cfg.main.workers_output_file) -> None:
    """
    worker for processing db nootifications via StableHeap

    Args:
        conn: connsction object
        stable_heap: StableHeap
        id: worker id
        file_name: output file

    Returns:
        None

    """
    logger.debug('starting worker {}'.format(id))
    while not sig_handler.finish:
        while not stable_heap.empty():
            event_data = stable_heap.pop()
            try:
                logger.debug('{}: processing {}'.format(id, event_data))
                table = event_data['table']
                column = 'logged_at'
                pk_column = 'id'
                pk = event_data['data']['id']
                value = datetime.utcnow()
                update_column(conn, table, column, pk_column, pk, value)
                log_action(file_name, str(id) + ': ' + json.dumps(event_data))
            except:
                stable_heap.push(event_data)
                raise
        sleep(5)


def log_action(file_name, data) -> None:
    """
    Logs results into file - thread safe
    Args:
        file_name: file name
        data: data to be written

    Returns:

    """
    with file_lock:
        with open(file_name, 'a') as file:
            file.write(data+'\n')


def worker_loop(conn, stable_heap, max_workers=cfg.main.max_workers) -> None:
    """
    worker loop, responsible for starting worker threads and restart in case of failure

    Args:
        conn: connection object
        stable_heap: StableHeap
        max_workers: max workers

    Returns:
        None

    """
    with thread_pool(max_workers=cfg.main.max_workers, thread_name_prefix='watch_status_worker') as executor:
        logger.debug('worker_loop started')
        futures = {executor.submit(worker, conn, stable_heap, id): id for id in range(max_workers)}
        while not sig_handler.finish:
            logger.debug('worker_loop checking workers status ')
            done = [future_id for future_id in futures.items() if future_id[0].done()]
            for future, id in done:
                try:
                    future.result()
                except Exception:
                    logger.exception('worker id {} thread raised exception'.format(id))
                else:
                    logger.error('worker id {} thread finished'.format(id))
                finally:
                    del(futures[future])
                    futures[executor.submit(worker, conn, stable_heap, id)] = id
                    logger.debug('worker id {} restarted'.format(id))
            sleep(5)
