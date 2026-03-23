import logging
import threading
import time
from enum import Enum
from typing import Any, Callable


logger = logging.getLogger("CB")


class State(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class PermanentError(Exception):
    pass


class TransientError(Exception):
    pass


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int | None = None,
        max_failures: int | None = None,
        recovery_time: float | None = None,
        recovery_timeout: float | None = None,
        reset_timeout: float = 30.0,
    ):
        if failure_threshold is not None:
            self.max_failures = int(failure_threshold)
        elif max_failures is not None:
            self.max_failures = int(max_failures)
        else:
            self.max_failures = 3

        if recovery_time is not None:
            self.reset_timeout = float(recovery_time)
        elif recovery_timeout is not None:
            self.reset_timeout = float(recovery_timeout)
        else:
            self.reset_timeout = float(reset_timeout)

        self.state = State.CLOSED
        self.failures = 0
        self.opened_at = None

        self._lock = threading.RLock()
        self._half_open_in_flight = False

    # ---------------- CORE ---------------- #

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        with self._lock:
            if self._is_open_unlocked():
                raise RuntimeError(
                    "Circuit breaker OPEN - Chiamate API bloccate temporaneamente"
                )

            if self.state == State.HALF_OPEN:
                if self._half_open_in_flight:
                    raise RuntimeError(
                        "Circuit breaker HALF_OPEN - Test di recupero già in corso"
                    )
                self._half_open_in_flight = True

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result

        except PermanentError:
            with self._lock:
                if self.state == State.HALF_OPEN:
                    self._half_open_in_flight = False
            raise

        except TransientError as e:
            self._on_failure(e)
            raise

        except Exception as e:
            error_str = str(e).lower()

            if any(
                x in error_str
                for x in [
                    "insufficient_funds",
                    "market_closed",
                    "invalid_session",
                    "insufficient funds",
                    "market closed",
                    "invalid session",
                ]
            ):
                with self._lock:
                    if self.state == State.HALF_OPEN:
                        self._half_open_in_flight = False
                raise PermanentError(f"Errore Permanente: {e}") from e

            self._on_failure(e)
            raise TransientError(f"Errore Temporaneo: {e}") from e

    # ---------------- STATE ---------------- #

    def is_open(self) -> bool:
        with self._lock:
            return self._is_open_unlocked()

    def _is_open_unlocked(self) -> bool:
        if self.state != State.OPEN:
            return False

        if self.opened_at is None:
            return False

        if time.time() - self.opened_at > self.reset_timeout:
            self.state = State.HALF_OPEN
            self._half_open_in_flight = False
            return False

        return True

    def is_half_open(self) -> bool:
        with self._lock:
            self._is_open_unlocked()
            return self.state == State.HALF_OPEN

    # ---------------- FAILURE ---------------- #

    def record_failure(self, error: Exception | None = None):
        self._on_failure(error)

    def _on_failure(self, error: Exception | None = None):
        with self._lock:
            self.failures += 1
            self._half_open_in_flight = False

            if error is None:
                error = RuntimeError("failure")

            logger.warning(
                "[CB] Failure (%s/%s): %s",
                self.failures,
                self.max_failures,
                error,
            )

            if self.failures >= self.max_failures:
                self.state = State.OPEN
                self.opened_at = time.time()
                logger.error(
                    "[CB] OPEN for %.2fs",
                    self.reset_timeout,
                )

    # ---------------- SUCCESS ---------------- #

    def _on_success(self):
        with self._lock:
            # FIX #8: only transition to CLOSED from HALF_OPEN, not from OPEN.
            # A probe call can only succeed while in HALF_OPEN; if we are still
            # OPEN (e.g. the single probe call was bypassed by the in-flight
            # guard) we must not silently reset to CLOSED.
            if self.state == State.OPEN:
                return
            if self.state == State.HALF_OPEN:
                logger.info("[CB] Recovery -> CLOSED")
            self._reset_unlocked()

    # ---------------- RESET ---------------- #

    def reset(self):
        with self._lock:
            self._reset_unlocked()

    def _reset_unlocked(self):
        self.state = State.CLOSED
        self.failures = 0
        self.opened_at = None
        self._half_open_in_flight = False