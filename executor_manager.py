from __future__ import annotations

import concurrent.futures
import logging
import threading
from typing import Any, Callable, Dict, Optional


logger = logging.getLogger(__name__)


class ExecutorManager:
    """
    Gestore centralizzato executor/thread pool.

    Obiettivi:
    - submit compatibile con più call style
    - timeout di default opzionale
    - tracking task per nome
    - shutdown pulito
    - nessun blocco inutile lato caller

    Supporta chiamate:
    - submit("task_name", fn, *args, **kwargs)
    - submit(fn, *args, **kwargs)
    """

    def __init__(
        self,
        max_workers: int = 4,
        default_timeout: Optional[float] = 30,
        thread_name_prefix: str = "PickfairExec",
    ):
        self.max_workers = max(1, int(max_workers or 1))
        self.default_timeout = default_timeout
        self.thread_name_prefix = str(thread_name_prefix or "PickfairExec")

        self._lock = threading.RLock()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=self.thread_name_prefix,
        )
        self._shutdown = False
        self._futures: Dict[str, concurrent.futures.Future] = {}
        self._counter = 0

    # =========================================================
    # INTERNAL
    # =========================================================
    def _next_task_name(self) -> str:
        with self._lock:
            self._counter += 1
            return f"task_{self._counter}"

    def _normalize_submit_call(self, *args, **kwargs):
        if not args:
            raise TypeError("submit richiede almeno una callable")

        if callable(args[0]):
            task_name = self._next_task_name()
            fn = args[0]
            fn_args = args[1:]
            return task_name, fn, fn_args, kwargs

        if len(args) < 2:
            raise TypeError("submit(name, fn, ...) richiede anche la callable")

        task_name = str(args[0] or self._next_task_name())
        fn = args[1]
        if not callable(fn):
            raise TypeError("Il secondo argomento di submit deve essere callable")
        fn_args = args[2:]
        return task_name, fn, fn_args, kwargs

    def _wrap_task(self, task_name: str, fn: Callable, *args, **kwargs):
        logger.debug("Executor task start: %s", task_name)
        try:
            return fn(*args, **kwargs)
        except Exception:
            logger.exception("Executor task failed: %s", task_name)
            raise
        finally:
            logger.debug("Executor task end: %s", task_name)

    # =========================================================
    # PUBLIC API
    # =========================================================
    def submit(self, *args, **kwargs):
        """
        Supporta:
        - submit("name", fn, *args, **kwargs)
        - submit(fn, *args, **kwargs)
        """
        task_name, fn, fn_args, fn_kwargs = self._normalize_submit_call(*args, **kwargs)

        with self._lock:
            if self._shutdown:
                raise RuntimeError("ExecutorManager già chiuso")
            future = self._executor.submit(
                self._wrap_task,
                task_name,
                fn,
                *fn_args,
                **fn_kwargs,
            )
            self._futures[task_name] = future

            def _cleanup(_done_future):
                with self._lock:
                    current = self._futures.get(task_name)
                    if current is _done_future:
                        self._futures.pop(task_name, None)

            future.add_done_callback(_cleanup)
            return future

    def map(self, fn: Callable, iterable, timeout: Optional[float] = None):
        with self._lock:
            if self._shutdown:
                raise RuntimeError("ExecutorManager già chiuso")
        return self._executor.map(fn, iterable, timeout=timeout)

    def get_future(self, task_name: str):
        with self._lock:
            return self._futures.get(str(task_name))

    def cancel(self, task_name: str) -> bool:
        with self._lock:
            future = self._futures.get(str(task_name))
            if not future:
                return False
            cancelled = future.cancel()
            if cancelled:
                self._futures.pop(str(task_name), None)
            return cancelled

    def active_tasks(self) -> Dict[str, str]:
        with self._lock:
            out: Dict[str, str] = {}
            for name, future in self._futures.items():
                if future.cancelled():
                    state = "CANCELLED"
                elif future.done():
                    state = "DONE"
                elif future.running():
                    state = "RUNNING"
                else:
                    state = "PENDING"
                out[name] = state
            return out

    def wait(self, task_name: str, timeout: Optional[float] = None) -> Any:
        future = self.get_future(task_name)
        if future is None:
            raise RuntimeError(f"Task non trovata: {task_name}")
        return future.result(timeout=timeout if timeout is not None else self.default_timeout)

    def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True

        try:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        finally:
            with self._lock:
                self._futures.clear()

    # =========================================================
    # STATUS
    # =========================================================
    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_workers": self.max_workers,
                "default_timeout": self.default_timeout,
                "shutdown": self._shutdown,
                "tracked_tasks": len(self._futures),
                "tasks": self.active_tasks(),
            }