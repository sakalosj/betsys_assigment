import heapq
import logging
import queue
import signal
import threading
from collections import defaultdict


logger = logging.getLogger('watch_status.db_event_loop')


class StableHeap:
    """
    Class implements stable heap behavior using heap for getting lowest index and dictionary with queues(FIFO)

    """

    def __init__(self):
        self._dict_queue = defaultdict(queue.Queue)
        self._id_heap = []
        self._lock = threading.Lock()

    def push(self, key, value):
        with self._lock:
            if key not in self._id_heap:
                heapq.heappush(self._id_heap, key)
            self._dict_queue[key].put(value)

    def pop(self):
        with self._lock:
            while self._dict_queue[self._id_heap[0]].empty():
                del self._dict_queue[self._id_heap[0]]
                heapq.heappop(self._id_heap)
            return self._dict_queue[self._id_heap[0]].get(block=False)

    def empty(self) -> bool:
        """
        checks all queues in dictionary self._dict_queue

        Returns:
            bool: True if true

        """
        with self._lock:
            return all([q.empty() for q in self._dict_queue.values()])


class Singleton(type):
    """
    Singleton impolemented via metaclass
    """

    def __init__(self, *args, **kwargs):
        self._instance = None
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if self._instance is None:
            self._instance = super().__call__(*args, **kwargs)
            return self._instance
        else:
            return self._instance


class SigHandler(metaclass=Singleton):
    """
    Class for handling signals. singleton implementation.
    """

    def __init__(self):
        super().__init__()
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.finish = False

    def exit_gracefully(self, signum, frame):
        logger.debug('signal received')
        self.finish = True

