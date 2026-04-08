"""
Telegram Sender - Gestisce l'invio asincrono di messaggi Telegram.
HEDGE-FUND STABLE:
- Anti-FloodWait Loop
- Adaptive Rate Limiting
- No-blocking Queue
- EventBus integration per MASTER copy-trading
- Database logging outbox
"""

import asyncio
import logging
import threading
from dataclasses import dataclass
from queue import Empty, Full, Queue
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("TG_SENDER")


def format_bet_message(
    runner_name: str,
    action: str,
    price: float,
    market_id: str = "",
    selection_id: str = "",
    event_name: str = "",
    market_name: str = "",
    status: str = "MATCHED",
) -> str:
    safe_runner = "" if runner_name is None else str(runner_name)
    safe_action = "" if action is None else str(action).upper().strip()
    safe_market_id = "" if market_id is None else str(market_id)
    safe_selection_id = "" if selection_id is None else str(selection_id)
    safe_event_name = "" if event_name is None else str(event_name)
    safe_market_name = "" if market_name is None else str(market_name)
    safe_status = "" if status is None else str(status)

    try:
        safe_price = float(price)
    except Exception:
        safe_price = 0.0

    return (
        "🟢 MASTER SIGNAL\n\n"
        f"event_name: {safe_event_name}\n"
        f"market_name: {safe_market_name}\n"
        f"selection: {safe_runner}\n"
        f"action: {safe_action}\n"
        f"master_price: {safe_price:.2f}\n"
        f"market_id: {safe_market_id}\n"
        f"selection_id: {safe_selection_id}\n"
        f"status: {safe_status}"
    )


@dataclass
class SendResult:
    success: bool = False
    message_id: Optional[int] = None
    error: Optional[str] = None
    flood_wait: Optional[int] = None


@dataclass
class QueuedMessage:
    chat_id: str
    text: str
    max_retries: int = 3
    callback: Optional[Callable] = None
    message_type: str = "GENERIC"


class AdaptiveRateLimiter:
    def __init__(self, base_delay: float = 0.5):
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.last_send_time = 0.0
        self._lock = threading.Lock()
        self.consecutive_successes = 0

    async def wait_if_needed_async(self):
        # FIX #21: read current_delay and last_send_time under the lock so
        # that concurrent calls from other threads (record_success /
        # record_failure) cannot cause torn reads.
        with self._lock:
            current_delay = self.current_delay
            last_send = self.last_send_time
        now = asyncio.get_event_loop().time()
        elapsed = now - last_send
        if elapsed < current_delay:
            await asyncio.sleep(current_delay - elapsed)
        with self._lock:
            self.last_send_time = asyncio.get_event_loop().time()

    def record_success(self):
        with self._lock:
            self.consecutive_successes += 1
            if self.consecutive_successes > 10:
                self.current_delay = max(self.base_delay, self.current_delay * 0.9)
                self.consecutive_successes = 0

    def record_failure(self):
        with self._lock:
            self.consecutive_successes = 0
            self.current_delay = min(self.base_delay * 5, self.current_delay * 1.5)

    def record_flood_wait(self, wait_seconds: int):
        with self._lock:
            self.consecutive_successes = 0
            self.current_delay = max(
                self.current_delay,
                min(wait_seconds / 10.0, self.base_delay * 10),
            )

    def get_stats(self):
        return {
            "current_delay": self.current_delay,
            "consecutive_successes": self.consecutive_successes,
        }

    def reset(self):
        self.current_delay = self.base_delay
        self.consecutive_successes = 0


class TelegramSender:
    def __init__(
        self,
        client,
        base_delay: float = 0.5,
        event_bus=None,
        default_chat_id: Optional[str] = None,
        db=None,
        queue_maxsize: int = 1000,
    ):
        self.client = client
        self.bus = event_bus
        self.db = db
        self.default_chat_id = (
            str(default_chat_id) if default_chat_id not in (None, "") else None
        )

        self.rate_limiter = AdaptiveRateLimiter(base_delay)
        safe_maxsize = int(queue_maxsize or 0)
        self._queue_maxsize = max(1, safe_maxsize)
        self._queue = Queue(maxsize=self._queue_maxsize)
        self._running = False
        self._worker_thread = None
        # FIX #21: lock used by queue_message to prevent duplicate worker threads
        self._worker_lock = threading.Lock()

        self._messages_sent = 0
        self._messages_failed = 0
        self._messages_queued = 0
        self._messages_dropped = 0
        self._queue_backpressure = False

        if self.bus is not None:
            self.bus.subscribe("QUICK_BET_SUCCESS", self._on_quick_bet_success)
            self.bus.subscribe("DUTCHING_SUCCESS", self._on_dutching_success)
            self.bus.subscribe("CASHOUT_SUCCESS", self._on_cashout_success)

    def _escape(self, value):
        if value is None:
            return ""
        return str(value)

    def _db_log(
        self,
        chat_id,
        message_type,
        text,
        status,
        message_id=None,
        error=None,
        flood_wait=0,
    ):
        if not self.db:
            return
        try:
            self.db.save_telegram_outbox_log(
                chat_id=chat_id,
                message_type=message_type,
                text=text,
                status=status,
                message_id=message_id,
                error=error,
                flood_wait=flood_wait,
            )
        except Exception as e:
            logger.error("[TG_SENDER] DB log error: %s", e)

    async def send_message(
        self,
        chat_id: str,
        text: str,
        max_retries: int = 3,
        message_type: str = "GENERIC",
    ) -> SendResult:
        result = SendResult(
            success=False,
            message_id=None,
            error=None,
            flood_wait=None,
        )

        for attempt in range(max_retries):
            await self.rate_limiter.wait_if_needed_async()

            try:
                entity = await self.client.get_entity(int(chat_id))
                msg = await self.client.send_message(entity, text)

                result.success = True
                result.message_id = msg.id if hasattr(msg, "id") else None
                result.error = None
                result.flood_wait = None

                self.rate_limiter.record_success()
                self._messages_sent += 1

                self._db_log(
                    chat_id=chat_id,
                    message_type=message_type,
                    text=text,
                    status="SENT",
                    message_id=result.message_id,
                    error=None,
                    flood_wait=0,
                )
                return result

            except Exception as e:
                error_str = str(e).lower()

                if "floodwait" in error_str or "flood" in error_str:
                    try:
                        wait_seconds = int("".join(filter(str.isdigit, str(e)))) or 60
                    except Exception:
                        wait_seconds = 60

                    result.success = False
                    result.message_id = None
                    result.flood_wait = wait_seconds
                    result.error = str(e)

                    self.rate_limiter.record_flood_wait(wait_seconds)

                    if attempt >= max_retries - 1:
                        self._messages_failed += 1
                        self._db_log(
                            chat_id=chat_id,
                            message_type=message_type,
                            text=text,
                            status="FAILED",
                            message_id=None,
                            error=result.error,
                            flood_wait=wait_seconds,
                        )
                        break

                    safe_wait = min(wait_seconds, 15)
                    logger.warning(
                        "[TG_SENDER] FloodWait %ss. Safe sleep: %ss. Attempt %s/%s",
                        wait_seconds,
                        safe_wait,
                        attempt + 1,
                        max_retries,
                    )
                    await asyncio.sleep(safe_wait)
                    continue

                result.success = False
                result.message_id = None
                result.error = str(e)

                self.rate_limiter.record_failure()

                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                else:
                    self._messages_failed += 1
                    self._db_log(
                        chat_id=chat_id,
                        message_type=message_type,
                        text=text,
                        status="FAILED",
                        message_id=None,
                        error=result.error,
                        flood_wait=0,
                    )

        return result

    def send_message_sync(
        self,
        chat_id: str,
        text: str,
        max_retries: int = 3,
        message_type: str = "GENERIC",
    ) -> SendResult:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self.send_message(chat_id, text, max_retries, message_type)
            )
        finally:
            loop.close()

    def send_alert_message(self, chat_id: Any, text: str) -> Any:
        """
        Wrapper stabile per invio alert.
        Riusa il normale canale di invio messaggi.
        """
        send_message_sync = getattr(self, "send_message_sync", None)
        if callable(send_message_sync):
            try:
                return send_message_sync(chat_id, text, message_type="ALERT")
            except TypeError:
                pass

        send_message = getattr(self, "send_message", None)
        if callable(send_message):
            try:
                return send_message(chat_id, text)
            except TypeError:
                return send_message(chat_id=chat_id, text=text)

        enqueue_message = getattr(self, "enqueue_message", None)
        if callable(enqueue_message):
            return enqueue_message(chat_id=chat_id, text=text, message_type="ALERT")

        raise RuntimeError("send_alert_message unavailable")

    def queue_message(
        self,
        chat_id: str,
        text: str,
        max_retries: int = 3,
        callback: Optional[Callable] = None,
        message_type: str = "GENERIC",
    ):
        msg = QueuedMessage(
            chat_id=str(chat_id),
            text=text,
            max_retries=max_retries,
            callback=callback,
            message_type=message_type,
        )
        queued = True
        try:
            self._queue.put_nowait(msg)
            self._messages_queued += 1
            self._queue_backpressure = False
        except Full:
            queued = False
            self._messages_dropped += 1
            self._queue_backpressure = True
            logger.error(
                "[TG_SENDER] Queue overflow, dropping message type=%s chat_id=%s",
                message_type,
                chat_id,
            )

        self._db_log(
            chat_id=chat_id,
            message_type=message_type,
            text=text,
            status="QUEUED" if queued else "DROPPED_QUEUE_FULL",
            message_id=None,
            error=None if queued else "queue_full",
            flood_wait=0,
        )

        # FIX #21: guard start_worker under a dedicated lock so that
        # concurrent queue_message calls cannot each see _running==False
        # and spawn multiple worker threads.
        with self._worker_lock:
            if not self._running:
                self.start_worker()
        return queued

    def queue_default_message(
        self,
        text: str,
        max_retries: int = 3,
        callback: Optional[Callable] = None,
        message_type: str = "GENERIC",
    ):
        if not self.default_chat_id:
            logger.warning("[TG_SENDER] Nessun default_chat_id configurato.")
            return

        self.queue_message(
            chat_id=self.default_chat_id,
            text=text,
            max_retries=max_retries,
            callback=callback,
            message_type=message_type,
        )

    def start_worker(self):
        if self._running:
            return

        self._running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name="TelegramSenderWorker",
        )
        self._worker_thread.start()
        logger.info("[TG_SENDER] Worker started")

    def stop_worker(self):
        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        logger.info("[TG_SENDER] Worker stopped")

    def _worker_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while self._running:
            try:
                msg = self._queue.get(timeout=1)

                result = loop.run_until_complete(
                    self.send_message(
                        msg.chat_id,
                        msg.text,
                        msg.max_retries,
                        msg.message_type,
                    )
                )

                if msg.callback:
                    try:
                        msg.callback(result)
                    except Exception as e:
                        logger.error("[TG_SENDER] Callback error: %s", e)

                self._queue.task_done()

            except Empty:
                continue
            except Exception as e:
                logger.error("[TG_SENDER] Worker error: %s", e)

        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()

    def _format_single_signal(
        self,
        runner_name,
        action,
        price,
        market_id,
        selection_id,
        event_name="",
        market_name="",
        status="MATCHED",
    ) -> str:
        return format_bet_message(
            runner_name=runner_name,
            action=action,
            price=price,
            market_id=market_id,
            selection_id=selection_id,
            event_name=event_name,
            market_name=market_name,
            status=status,
        )

    def _format_dutching_signal(self, data: Dict) -> str:
        event_name = self._escape(data.get("event_name", ""))
        market_name = self._escape(data.get("market_name", ""))
        market_id = self._escape(data.get("market_id", ""))
        status = self._escape(data.get("status", "MATCHED"))
        selections = data.get("selections", []) or []

        lines = [
            "🔵 MASTER SIGNAL DUTCHING",
            "",
            f"event_name: {event_name}",
            f"market_name: {market_name}",
            f"market_id: {market_id}",
            f"status: {status}",
            "",
            "legs:",
        ]

        for idx, sel in enumerate(selections, start=1):
            runner_name = self._escape(
                sel.get("runnerName", sel.get("selectionId", ""))
            )
            action = self._escape(
                str(
                    sel.get("effectiveType")
                    or sel.get("side")
                    or data.get("bet_type", "BACK")
                ).upper()
            )
            price = float(sel.get("price", 0.0) or 0.0)
            selection_id = self._escape(sel.get("selectionId", ""))

            lines.append(
                f"{idx}) {runner_name} | {action} | {price:.2f} | selection_id={selection_id}"
            )

        return "\n".join(lines)

    def _on_quick_bet_success(self, data: Dict):
        if data.get("sim", False):
            return
        if not self.default_chat_id:
            return

        runner = data.get("runner_name", "Ignoto")
        price = float(data.get("price", 0.0) or 0.0)
        action = str(data.get("bet_type", "BACK")).upper()
        market_id = data.get("market_id", "")
        selection_id = data.get("selection_id", "")
        event_name = data.get("event_name", "")
        market_name = data.get("market_name", "")
        status = data.get("status", "MATCHED")

        text = self._format_single_signal(
            runner_name=runner,
            action=action,
            price=price,
            market_id=market_id,
            selection_id=selection_id,
            event_name=event_name,
            market_name=market_name,
            status=status,
        )
        self.queue_default_message(text, message_type="MASTER_SIGNAL_SINGLE")

    def _on_dutching_success(self, data: Dict):
        if data.get("sim", False):
            return
        if not self.default_chat_id:
            return

        text = self._format_dutching_signal(data)
        self.queue_default_message(text, message_type="MASTER_SIGNAL_DUTCHING")

    def _on_cashout_success(self, data: Dict):
        if not self.default_chat_id:
            return

        green = float(data.get("green_up", 0.0) or 0.0)
        status = self._escape(data.get("status", "DONE"))

        text = (
            "🚨 CASHOUT ESEGUITO\n\n"
            f"green_up: {green:.2f}\n"
            f"status: {status}"
        )
        self.queue_default_message(text, message_type="MASTER_CASHOUT")

    def get_queue_size(self) -> int:
        return self._queue.qsize()

    def get_stats(self) -> Dict:
        return {
            "rate_limiter": self.rate_limiter.get_stats(),
            "queue_size": self.get_queue_size(),
            "queue_maxsize": self._queue_maxsize,
            "messages_sent": self._messages_sent,
            "messages_failed": self._messages_failed,
            "messages_queued": self._messages_queued,
            "messages_dropped": self._messages_dropped,
            "queue_backpressure": self._queue_backpressure,
            "worker_running": self._running,
        }

    def reset_stats(self):
        self._messages_sent = 0
        self._messages_failed = 0
        self._messages_queued = 0
        self.rate_limiter.reset()


_global_sender = None


def get_telegram_sender(
    client=None,
    base_delay: float = 0.5,
    event_bus=None,
    default_chat_id: Optional[str] = None,
    db=None,
) -> Optional[TelegramSender]:
    global _global_sender
    if _global_sender is None and client is not None:
        _global_sender = TelegramSender(
            client=client,
            base_delay=base_delay,
            event_bus=event_bus,
            default_chat_id=default_chat_id,
            db=db,
        )
    return _global_sender


def init_telegram_sender(
    client,
    base_delay: float = 0.5,
    event_bus=None,
    default_chat_id: Optional[str] = None,
    db=None,
) -> TelegramSender:
    global _global_sender
    _global_sender = TelegramSender(
        client=client,
        base_delay=base_delay,
        event_bus=event_bus,
        default_chat_id=default_chat_id,
        db=db,
    )
    return _global_sender
