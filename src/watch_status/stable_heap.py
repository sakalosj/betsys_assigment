import heapq
import queue
import threading
from collections import defaultdict


class StableHeap:
    """
    class implements stable heap behavior using heap for getting lowest index and dictionary with queues(FIFO)

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

