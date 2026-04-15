from __future__ import annotations

import logging
import threading
import time
from copy import deepcopy
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
    max_auth_failures: int = 2

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
        session_gate: Optional[Callable[[], Any]] = None,
    ):
        self.client_getter = client_getter
        self.config = StreamingFeedConfig(**dict(config or {}))
        self.on_market_book = on_market_book
        self.on_disconnect = on_disconnect
        self.listener_factory = listener_factory or StreamListener
        self.session_gate = session_gate

        self._stop_event = threading.Event()
        self._run_thread: Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None
        self._stream_reader_thread: Optional[threading.Thread] = None

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
        self._market_cache: Dict[str, Dict[str, Any]] = {}
        self._auth_failure_count = 0
        self._auth_degraded = False
        self._last_auth_error = ""
        self._last_transport_error = ""
        self._keepalive_failure_count = 0
        self._last_keepalive_error = ""

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
                "auth_failure_count": int(self._auth_failure_count),
                "auth_degraded": bool(self._auth_degraded),
                "last_auth_error": self._last_auth_error,
                "last_transport_error": self._last_transport_error,
                "keepalive_failure_count": int(self._keepalive_failure_count),
                "last_keepalive_error": self._last_keepalive_error,
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
                self._auth_failure_count = 0
                self._last_auth_error = ""
                self._last_transport_error = ""
            except StreamAuthError as exc:
                self._auth_failure_count += 1
                self._last_auth_error = str(exc)
                self._notify_disconnect(reason=str(exc), kind="auth")
                if self._auth_failure_count >= int(self.config.max_auth_failures or 1):
                    self._auth_degraded = True
                    break
                if self._stop_event.wait(timeout=backoff):
                    break
                backoff = min(30, max(1, backoff * 2))
            except Exception as exc:
                reason = str(exc)
                self._last_transport_error = reason
                self._notify_disconnect(reason=reason, kind="transport")
                if self._stop_event.wait(timeout=backoff):
                    break
                backoff = min(30, max(1, backoff * 2))

    def _connect_and_consume(self) -> None:
        self._stream_client = self.client_getter()
        if self._stream_client is None:
            raise RuntimeError("STREAM_CLIENT_UNAVAILABLE")
        self._ensure_session_ready()

        output_queue: Queue = Queue()
        self._listener = self.listener_factory(output_queue=output_queue)

        streaming = getattr(self._stream_client, "streaming", None)
        if streaming is None or not callable(getattr(streaming, "create_stream", None)):
            raise RuntimeError("STREAMING_INTERFACE_UNAVAILABLE")

        self._stream = streaming.create_stream(listener=self._listener)

        subscribe_kwargs = self._build_subscribe_kwargs()
        self._stream.subscribe_to_markets(**subscribe_kwargs)
        self._start_stream_reader()

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

    def _start_stream_reader(self) -> None:
        start = getattr(self._stream, "start", None)
        if not callable(start):
            return
        if self._stream_reader_thread and self._stream_reader_thread.is_alive():
            return

        def _reader() -> None:
            try:
                start()
            except Exception:
                logger.exception("stream reader failed")

        self._stream_reader_thread = threading.Thread(
            target=_reader,
            name="streaming-feed-reader",
            daemon=True,
        )
        self._stream_reader_thread.start()

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
            except Exception as exc:
                self._mark_keepalive_failure(str(exc))
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
            return

        resource_book = self._resource_to_market_book(message)
        if resource_book:
            merged = self._merge_market_snapshot(resource_book)
            if merged:
                self._safe_on_market_book(merged)

    def _extract_market_books(self, message: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        books = []

        raw_books = message.get("market_books")
        if isinstance(raw_books, list):
            for item in raw_books:
                if isinstance(item, dict):
                    merged = self._merge_market_snapshot(item)
                    if merged:
                        books.append(merged)
            return books

        # Betfair stream message market changes form
        market_changes = message.get("mc") or []
        for change in market_changes:
            if not isinstance(change, dict):
                continue
            merged = self._merge_market_change(change)
            if merged:
                books.append(merged)
        return books

    def _merge_market_snapshot(self, market_book: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        market_id = str(
            market_book.get("marketId")
            or market_book.get("market_id")
            or market_book.get("id")
            or ""
        ).strip()
        if not market_id:
            return None

        state = self._get_or_init_market_state(market_id)
        self._merge_market_definition(state, market_book.get("marketDefinition"))
        if "status" in market_book and market_book.get("status") is not None:
            state["status"] = market_book.get("status")
        if "inplay" in market_book:
            state["inplay"] = bool(market_book.get("inplay"))
        if "inPlay" in market_book:
            state["inplay"] = bool(market_book.get("inPlay"))

        for runner in market_book.get("runners") or []:
            if not isinstance(runner, dict):
                continue
            self._merge_runner_snapshot(state, runner)
        return self._state_to_market_book(state)

    def _merge_market_change(self, change: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        market_id = str(change.get("id") or change.get("marketId") or "").strip()
        if not market_id:
            return None

        state = self._get_or_init_market_state(market_id)
        self._merge_market_definition(state, change.get("marketDefinition"))
        for rc in change.get("rc") or []:
            if not isinstance(rc, dict):
                continue
            self._merge_runner_change(state, rc)
        return self._state_to_market_book(state)

    def _get_or_init_market_state(self, market_id: str) -> Dict[str, Any]:
        state = self._market_cache.get(market_id)
        if state is None:
            state = {
                "marketId": market_id,
                "market_id": market_id,
                "status": "",
                "inplay": False,
                "marketDefinition": {},
                "runners_by_id": {},
            }
            self._market_cache[market_id] = state
        return state

    def _merge_market_definition(self, state: Dict[str, Any], market_definition: Any) -> None:
        if not isinstance(market_definition, dict):
            return

        existing = dict(state.get("marketDefinition") or {})
        existing.update({k: deepcopy(v) for k, v in market_definition.items()})
        state["marketDefinition"] = existing

        if market_definition.get("status") is not None:
            state["status"] = market_definition.get("status")
        if market_definition.get("inPlay") is not None:
            state["inplay"] = bool(market_definition.get("inPlay"))

        for runner_def in market_definition.get("runners") or []:
            if not isinstance(runner_def, dict):
                continue
            selection_id = runner_def.get("id")
            if selection_id in (None, ""):
                continue
            runner_state = self._get_or_init_runner_state(state, selection_id)
            if runner_def.get("name") is not None:
                runner_state["runnerName"] = runner_def.get("name")
            if runner_def.get("status") is not None:
                runner_state["status"] = runner_def.get("status")
            if runner_def.get("hc") is not None:
                runner_state["handicap"] = runner_def.get("hc")
            if runner_def.get("sortPriority") is not None:
                runner_state["sortPriority"] = runner_def.get("sortPriority")

    def _get_or_init_runner_state(self, state: Dict[str, Any], selection_id: Any) -> Dict[str, Any]:
        key = str(selection_id)
        runners = state["runners_by_id"]
        runner_state = runners.get(key)
        if runner_state is None:
            runner_state = {
                "selectionId": int(selection_id),
                "runnerName": "",
                "status": "",
                "handicap": 0,
                "sortPriority": 0,
                "ltp": None,
                "ex": {
                    "availableToBack": [],
                    "availableToLay": [],
                    "tradedVolume": [],
                },
            }
            runners[key] = runner_state
        return runner_state

    def _merge_runner_snapshot(self, state: Dict[str, Any], runner: Dict[str, Any]) -> None:
        selection_id = runner.get("selectionId") or runner.get("selection_id")
        if selection_id in (None, ""):
            return
        runner_state = self._get_or_init_runner_state(state, selection_id)

        if runner.get("runnerName") is not None:
            runner_state["runnerName"] = runner.get("runnerName")
        if runner.get("status") is not None:
            runner_state["status"] = runner.get("status")
        if runner.get("handicap") is not None:
            runner_state["handicap"] = runner.get("handicap")
        if runner.get("sortPriority") is not None:
            runner_state["sortPriority"] = runner.get("sortPriority")
        if runner.get("ltp") is not None:
            runner_state["ltp"] = runner.get("ltp")

        ex = runner.get("ex") if isinstance(runner.get("ex"), dict) else {}
        if "availableToBack" in ex and ex.get("availableToBack") is not None:
            runner_state["ex"]["availableToBack"] = list(ex.get("availableToBack") or [])
        if "availableToLay" in ex and ex.get("availableToLay") is not None:
            runner_state["ex"]["availableToLay"] = list(ex.get("availableToLay") or [])
        if "tradedVolume" in ex and ex.get("tradedVolume") is not None:
            runner_state["ex"]["tradedVolume"] = list(ex.get("tradedVolume") or [])

    def _merge_runner_change(self, state: Dict[str, Any], rc: Dict[str, Any]) -> None:
        selection_id = rc.get("id")
        if selection_id in (None, ""):
            return
        runner_state = self._get_or_init_runner_state(state, selection_id)

        if rc.get("status") is not None:
            runner_state["status"] = rc.get("status")
        if rc.get("hc") is not None:
            runner_state["handicap"] = rc.get("hc")
        if rc.get("ltp") is not None:
            runner_state["ltp"] = rc.get("ltp")
        if rc.get("tv") is not None:
            runner_state["ex"]["tradedVolume"] = self._parse_price_points(rc.get("tv") or [])
        if rc.get("trd") is not None:
            runner_state["ex"]["tradedVolume"] = self._parse_price_points(rc.get("trd") or [])
        if rc.get("batb") is not None:
            runner_state["ex"]["availableToBack"] = self._merge_ladder_points(
                runner_state["ex"]["availableToBack"],
                self._parse_price_points(rc.get("batb") or []),
            )
        if rc.get("batl") is not None:
            runner_state["ex"]["availableToLay"] = self._merge_ladder_points(
                runner_state["ex"]["availableToLay"],
                self._parse_price_points(rc.get("batl") or []),
            )

    def _state_to_market_book(self, state: Dict[str, Any]) -> Dict[str, Any]:
        runners = []
        for sid in sorted(state.get("runners_by_id", {}).keys(), key=lambda x: int(x)):
            runner_state = state["runners_by_id"][sid]
            runner = {
                "selectionId": runner_state.get("selectionId"),
                "runnerName": runner_state.get("runnerName"),
                "status": runner_state.get("status"),
                "handicap": runner_state.get("handicap"),
                "sortPriority": runner_state.get("sortPriority"),
                "ltp": runner_state.get("ltp"),
                "ex": {
                    "availableToBack": list(runner_state.get("ex", {}).get("availableToBack") or []),
                    "availableToLay": list(runner_state.get("ex", {}).get("availableToLay") or []),
                    "tradedVolume": list(runner_state.get("ex", {}).get("tradedVolume") or []),
                },
            }
            # Compatibility with existing consumers expecting flattened ladders
            runner["availableToBack"] = list(runner["ex"]["availableToBack"])
            runner["availableToLay"] = list(runner["ex"]["availableToLay"])
            runners.append(runner)

        return {
            "marketId": state.get("marketId"),
            "market_id": state.get("market_id"),
            "status": state.get("status"),
            "inplay": bool(state.get("inplay", False)),
            "marketDefinition": deepcopy(state.get("marketDefinition") or {}),
            "runners": runners,
        }

    def _parse_price_points(self, points: List[Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in points:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            if len(row) >= 3:
                level = row[0]
                price = row[1]
                size = row[2]
            else:
                level = None
                price = row[0]
                size = row[1]
            item: Dict[str, Any] = {"price": price, "size": size}
            if isinstance(level, (int, float)):
                item["level"] = int(level)
            out.append(item)
        return out

    def _merge_ladder_points(self, existing: List[Dict[str, Any]], updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not updates:
            return list(existing or [])

        # No explicit levels -> treat update as full ladder replacement.
        if not any("level" in item for item in updates):
            return [{"price": item.get("price"), "size": item.get("size")} for item in updates]

        by_level: Dict[int, Dict[str, Any]] = {}
        for idx, item in enumerate(existing or []):
            level = item.get("level")
            if not isinstance(level, int):
                level = idx
            by_level[level] = {"price": item.get("price"), "size": item.get("size")}

        for item in updates:
            level = item.get("level")
            if not isinstance(level, int):
                continue
            size = item.get("size")
            if size is not None and float(size) <= 0:
                by_level.pop(level, None)
                continue
            by_level[level] = {"price": item.get("price"), "size": size}

        merged = []
        for level in sorted(by_level.keys()):
            row = by_level[level]
            merged.append({"price": row.get("price"), "size": row.get("size")})
        return merged

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
        if self._stream_reader_thread and self._stream_reader_thread.is_alive():
            self._stream_reader_thread.join(timeout=2.0)
        self._stream_reader_thread = None

    def _resource_to_market_book(self, resource: Any) -> Optional[Dict[str, Any]]:
        market_id = str(
            getattr(resource, "market_id", None)
            or getattr(resource, "marketId", None)
            or ""
        ).strip()
        if not market_id:
            return None

        market_definition = self._resource_market_definition_to_dict(
            getattr(resource, "market_definition", None)
        )
        status = getattr(resource, "status", None)
        inplay = getattr(resource, "inplay", None)
        if inplay is None:
            inplay = getattr(resource, "in_play", None)

        runners = []
        for runner in getattr(resource, "runners", None) or []:
            selection_id = getattr(runner, "selection_id", None)
            if selection_id in (None, ""):
                continue
            ex = getattr(runner, "ex", None)
            available_to_back = self._coerce_price_ladder(getattr(ex, "available_to_back", None))
            available_to_lay = self._coerce_price_ladder(getattr(ex, "available_to_lay", None))
            traded_volume = self._coerce_price_ladder(getattr(ex, "traded_volume", None))
            runners.append(
                {
                    "selectionId": selection_id,
                    "status": getattr(runner, "status", None),
                    "handicap": getattr(runner, "handicap", None),
                    "ltp": getattr(runner, "last_price_traded", None),
                    "ex": {
                        "availableToBack": available_to_back,
                        "availableToLay": available_to_lay,
                        "tradedVolume": traded_volume,
                    },
                }
            )

        return {
            "marketId": market_id,
            "status": status,
            "inplay": bool(inplay) if inplay is not None else False,
            "marketDefinition": market_definition,
            "runners": runners,
        }

    def _resource_market_definition_to_dict(self, market_definition: Any) -> Dict[str, Any]:
        if market_definition is None:
            return {}

        out: Dict[str, Any] = {}
        status = getattr(market_definition, "status", None)
        if status is not None:
            out["status"] = status
        in_play = getattr(market_definition, "in_play", None)
        if in_play is not None:
            out["inPlay"] = bool(in_play)

        runners_out = []
        for runner_def in getattr(market_definition, "runners", None) or []:
            rid = getattr(runner_def, "selection_id", None)
            if rid in (None, ""):
                continue
            runners_out.append(
                {
                    "id": rid,
                    "name": getattr(runner_def, "name", None),
                    "status": getattr(runner_def, "status", None),
                    "hc": getattr(runner_def, "handicap", None),
                    "sortPriority": getattr(runner_def, "sort_priority", None),
                }
            )
        if runners_out:
            out["runners"] = runners_out
        return out

    def _coerce_price_ladder(self, points: Any) -> List[Dict[str, Any]]:
        if points is None:
            return []
        out: List[Dict[str, Any]] = []
        for point in points:
            if isinstance(point, dict):
                price = point.get("price")
                size = point.get("size")
            else:
                price = getattr(point, "price", None)
                size = getattr(point, "size", None)
            if price is None or size is None:
                continue
            out.append({"price": price, "size": size})
        return out

    def _notify_disconnect(self, *, reason: str, kind: str = "transport") -> None:
        with self._lock:
            self._connected = False

        self._safe_stream_stop()
        if not callable(self.on_disconnect):
            return
        try:
            self.on_disconnect(
                {
                    "reason": str(reason or ""),
                    "kind": str(kind or "transport"),
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

    def _ensure_session_ready(self) -> None:
        if callable(self.session_gate):
            result = self.session_gate()
            if isinstance(result, dict):
                ok = bool(result.get("ok", False))
                if ok:
                    return
                reason = str(result.get("reason") or "SESSION_GATE_FAILED")
                raise StreamAuthError(reason)
            if bool(result):
                return
            raise StreamAuthError("SESSION_GATE_FAILED")

        # Fallback safety when no explicit session gate is provided.
        token = str(getattr(self._stream_client, "session_token", "") or "").strip()
        if not token:
            raise StreamAuthError("SESSION_TOKEN_MISSING")

    def _mark_keepalive_failure(self, error: str) -> None:
        self._keepalive_failure_count += 1
        self._last_keepalive_error = str(error or "UNKNOWN_KEEPALIVE_ERROR")
        if "SESSION_EXPIRED" in self._last_keepalive_error.upper() or "INVALID_SESSION" in self._last_keepalive_error.upper():
            self._last_auth_error = self._last_keepalive_error


class StreamAuthError(RuntimeError):
    pass
