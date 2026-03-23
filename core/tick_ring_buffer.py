import threading
from collections import deque
from typing import Any, List


class TickRingBuffer:
    """
    Ultra-fast ring buffer for tick dispatch.
    Uses deque for O(1) append / popleft operations.

    FIX #22: all mutating operations are now protected by a threading.Lock
    so that concurrent push/drain from different threads cannot lose items
    or produce incorrect drain counts.
    """

    def __init__(self, maxlen: int = 10000):
        self._buf = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def push(self, item: Any) -> None:
        with self._lock:
            self._buf.append(item)

    def pop(self) -> Any:
        with self._lock:
            if self._buf:
                return self._buf.popleft()
            return None

    def drain(self, limit: int = 1000) -> List[Any]:
        with self._lock:
            items = []
            for _ in range(limit):
                if not self._buf:
                    break
                items.append(self._buf.popleft())
            return items

    def peek(self) -> Any:
        with self._lock:
            if self._buf:
                return self._buf[0]
            return None

    def clear(self) -> None:
        with self._lock:
            self._buf.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)

    def __bool__(self) -> bool:
        with self._lock:
            return bool(self._buf)