from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any, Callable, Dict, Iterable, List, Optional

from betfairlightweight import StreamListener

logger = logging.getLogger(__name__)


class StreamingConfigError(RuntimeError):
    pass


@dataclass
class StreamingFeedConfig:
    enabled: bool = False
    market_data_mode: str = "poll"
    reconnect_backoff_sec: int = 1
    heartbeat_timeout_sec: int = 2
    snapshot_fallback_enabled: bool = True
    snapshot_fallback_interval_sec: int = 5
    max_markets: int = 25
    market_ids: List[str] = None  # type: ignore[assignment]
    event_type_ids: List[str] = None  # type: ignore[assignment]
    country_codes: List[str] = None  # type: ignore[assignment]
    market_types: List[str] = None  # type: ignore[assignment]
    use_full_ladder: bool = False
    fields: List[str] = None  # type: ignore[assignment]
    ladder_levels: int = 3
    conflate_ms: int = 0
    heartbeat_ms: int = 1000
    segmentation_enabled: bool = True

    def __post_init__(self) -> None:
        self.market_ids = list(self.market_ids or [])
        self.event_type_ids = list(self.event_type_ids or [])
        self.country_codes = list(self.country_codes or [])
        self.market_types = list(self.market_types or [])
        if self.fields is None:
            self.fields = ["EX_BEST_OFFERS", "EX_MARKET_DEF", "EX_LTP"]
        else:
            self.fields = list(self.fields)


class StreamingFeed:
    """
    Betfair market-data stream lifecycle manager.

    - Dedicated to stream market data only (no order/account logic)
    - Uses betfairlightweight StreamListener/create_stream/subscribe_to_markets
    - Preserves initialClk/clk across reconnects
    - Reconnects on heartbeat silence
    - Keeps session alive every 15 minutes
    """

    def __init__(
        self,
        *,
        client_getter: Callable[[], Any],
        config: Dict[str, Any],
        on_market_book: Callable[[Dict[str, Any]], None],
        on_disconnect: Optional[Callable[[Dict[str, Any]], None]] = None,
        listener_factory: Optional[Callable[..., Any]] = None,
    ):
        self.client_getter = client_getter
        self.config = StreamingFeedConfig(**dict(config or {}))
        self.on_market_book = on_market_book
        self.on_disconnect = on_disconnect
        self.listener_factory = listener_factory or StreamListener

        self._stop_event = threading.Event()
        self._run_thread: Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None

        self._stream: Any = None
        self._stream_client: Any = None
        self._listener: Any = None

        self._lock = threading.Lock()
        self._connected = False
        self._degraded_503 = False
        self._last_message_at = 0.0
        self._last_snapshot_at = 0.0

        self._clk = ""
        self._initial_clk = ""

    @property
    def healthy(self) -> bool:
        with self._lock:
            return bool(self._connected and not self._degraded_503)

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "running": bool(self._run_thread and self._run_thread.is_alive()),
                "connected": bool(self._connected),
                "degraded_503": bool(self._degraded_503),
                "clk": self._clk,
                "initial_clk": self._initial_clk,
                "last_message_at": self._last_message_at,
            }

    def start(self) -> Dict[str, Any]:
        self._validate_subscription_bounds()
        if self._run_thread and self._run_thread.is_alive():
            return {"started": True, "reason": "already_running"}

        self._stop_event.clear()
        self._run_thread = threading.Thread(target=self._run_loop, name="streaming-feed", daemon=True)
        self._run_thread.start()
        return {"started": True}

    def stop(self) -> Dict[str, Any]:
        self._stop_event.set()
        self._safe_stream_stop()

        if self._run_thread and self._run_thread.is_alive():
            self._run_thread.join(timeout=2.0)
        if self._keepalive_thread and self._keepalive_thread.is_alive():
            self._keepalive_thread.join(timeout=2.0)

        with self._lock:
            self._connected = False
        return {"stopped": True}

    def _run_loop(self) -> None:
        backoff = max(1, int(self.config.reconnect_backoff_sec or 1))

        while not self._stop_event.is_set():
            try:
                self._connect_and_consume()
                backoff = max(1, int(self.config.reconnect_backoff_sec or 1))
            except Exception as exc:
                reason = str(exc)
                self._notify_disconnect(reason=reason)
                if self._stop_event.wait(timeout=backoff):
                    break
                backoff = min(30, max(1, backoff * 2))

    def _connect_and_consume(self) -> None:
        self._stream_client = self.client_getter()
        if self._stream_client is None:
            raise RuntimeError("STREAM_CLIENT_UNAVAILABLE")

        output_queue: Queue = Queue()
        self._listener = self.listener_factory(output_queue=output_queue)

        streaming = getattr(self._stream_client, "streaming", None)
        if streaming is None or not callable(getattr(streaming, "create_stream", None)):
            raise RuntimeError("STREAMING_INTERFACE_UNAVAILABLE")

        self._stream = streaming.create_stream(listener=self._listener)

        subscribe_kwargs = self._build_subscribe_kwargs()
        self._stream.subscribe_to_markets(**subscribe_kwargs)

        with self._lock:
            self._connected = True
            self._degraded_503 = False
            self._last_message_at = time.monotonic()

        self._start_keepalive_loop()

        while not self._stop_event.is_set():
            timeout_s = max(0.5, float(self.config.heartbeat_timeout_sec) / 2.0)
            try:
                message = output_queue.get(timeout=timeout_s)
            except Empty:
                if self._is_heartbeat_dead():
                    raise RuntimeError("STREAM_HEARTBEAT_TIMEOUT")
                continue

            self._process_message(message)

    def _start_keepalive_loop(self) -> None:
        if self._keepalive_thread and self._keepalive_thread.is_alive():
            return

        self._keepalive_thread = threading.Thread(
            target=self._keepalive_loop,
            name="streaming-feed-keepalive",
            daemon=True,
        )
        self._keepalive_thread.start()

    def _keepalive_loop(self) -> None:
        while not self._stop_event.wait(timeout=900.0):
            client = self._stream_client
            if client is None:
                return
            keep_alive = getattr(client, "keep_alive", None)
            if not callable(keep_alive):
                continue
            try:
                keep_alive()
            except Exception:
                logger.exception("stream keep_alive failed")

    def _process_message(self, message: Any) -> None:
        with self._lock:
            self._last_message_at = time.monotonic()

        if isinstance(message, dict):
            status_code = str(message.get("status") or "")
            if status_code == "503":
                with self._lock:
                    self._degraded_503 = True
                logger.warning("Betfair stream in degraded mode (503): continuing without disconnect")
                return

            clk = str(message.get("clk") or "")
            initial_clk = str(message.get("initialClk") or "")
            if clk:
                self._clk = clk
            if initial_clk:
                self._initial_clk = initial_clk

            for market_book in self._extract_market_books(message):
                self._safe_on_market_book(market_book)
            return

        if isinstance(message, list):
            for item in message:
                self._process_message(item)

    def _extract_market_books(self, message: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        books = []

        raw_books = message.get("market_books")
        if isinstance(raw_books, list):
            for item in raw_books:
                if isinstance(item, dict):
                    books.append(dict(item))
            return books

        # Betfair stream message market changes form
        market_changes = message.get("mc") or []
        for change in market_changes:
            if not isinstance(change, dict):
                continue
            market_id = str(change.get("id") or "").strip()
            if not market_id:
                continue
            runners = []
            for rc in change.get("rc") or []:
                if not isinstance(rc, dict):
                    continue
                runner = {
                    "selectionId": rc.get("id"),
                    "ltp": rc.get("ltp"),
                    "ex": {
                        "availableToBack": self._parse_price_points(rc.get("batb") or []),
                        "availableToLay": self._parse_price_points(rc.get("batl") or []),
                    },
                }
                runners.append(runner)
            books.append({"marketId": market_id, "runners": runners})
        return books

    def _parse_price_points(self, points: List[Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in points:
            if not isinstance(row, (list, tuple)) or len(row) < 3:
                continue
            out.append({"price": row[1], "size": row[2]})
        return out

    def _safe_on_market_book(self, market_book: Dict[str, Any]) -> None:
        try:
            self.on_market_book(dict(market_book or {}))
        except Exception:
            logger.exception("stream market-book callback failed")

    def _safe_stream_stop(self) -> None:
        stream = self._stream
        self._stream = None
        self._listener = None
        self._stream_client = None

        if stream is None:
            return

        stop = getattr(stream, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                logger.exception("stream stop failed")

    def _notify_disconnect(self, *, reason: str) -> None:
        with self._lock:
            self._connected = False

        self._safe_stream_stop()
        if not callable(self.on_disconnect):
            return
        try:
            self.on_disconnect(
                {
                    "reason": str(reason or ""),
                    "last_snapshot_at": self._last_snapshot_at,
                    "clk": self._clk,
                    "initial_clk": self._initial_clk,
                }
            )
        except Exception:
            logger.exception("stream disconnect callback failed")

    def _is_heartbeat_dead(self) -> bool:
        with self._lock:
            if self._last_message_at <= 0:
                return False
            elapsed = time.monotonic() - self._last_message_at
        timeout = max(1.0, float(self.config.heartbeat_timeout_sec))
        return elapsed > timeout

    def _build_subscribe_kwargs(self) -> Dict[str, Any]:
        self._validate_subscription_bounds()

        market_filter: Dict[str, Any] = {}
        if self.config.market_ids:
            market_filter["market_ids"] = list(self.config.market_ids)[: self.config.max_markets]
        if self.config.event_type_ids:
            market_filter["event_type_ids"] = list(self.config.event_type_ids)
        if self.config.country_codes:
            market_filter["country_codes"] = list(self.config.country_codes)
        if self.config.market_types:
            market_filter["market_types"] = list(self.config.market_types)

        fields = list(self.config.fields)
        if self.config.use_full_ladder:
            if "EX_ALL_OFFERS" not in fields:
                fields.append("EX_ALL_OFFERS")
        else:
            fields = [f for f in fields if f != "EX_ALL_OFFERS"]

        kwargs: Dict[str, Any] = {
            "market_filter": market_filter,
            "market_data_filter": {
                "fields": fields,
                "ladder_levels": int(self.config.ladder_levels),
                "conflate_ms": int(self.config.conflate_ms),
                "segmentation_enabled": bool(self.config.segmentation_enabled),
            },
            "conflate_ms": int(self.config.conflate_ms),
            "heartbeat_ms": int(self.config.heartbeat_ms),
            "segmentation_enabled": bool(self.config.segmentation_enabled),
        }

        if self._initial_clk:
            kwargs["initial_clk"] = self._initial_clk
        if self._clk:
            kwargs["clk"] = self._clk

        return kwargs

    def _validate_subscription_bounds(self) -> None:
        cfg = self.config

        mode = str(cfg.market_data_mode or "poll").strip().lower()
        if mode not in {"poll", "stream", "hybrid"}:
            raise StreamingConfigError("INVALID_MARKET_DATA_MODE")

        if mode == "poll" or not cfg.enabled:
            return

        has_market_ids = bool(cfg.market_ids)
        has_bounded_combo = bool(cfg.event_type_ids) and (
            bool(cfg.market_types) or bool(cfg.country_codes)
        )

        if not has_market_ids and not has_bounded_combo:
            raise StreamingConfigError("STREAM_SUBSCRIPTION_UNBOUNDED")

        if has_market_ids and len(cfg.market_ids) > int(cfg.max_markets or 1):
            raise StreamingConfigError("STREAM_TOO_MANY_MARKETS")

        if not cfg.use_full_ladder and any(f == "EX_ALL_OFFERS" for f in cfg.fields):
            raise StreamingConfigError("EX_ALL_OFFERS_REQUIRES_OPT_IN")
