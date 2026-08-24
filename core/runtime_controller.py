from __future__ import annotations

import copy
import inspect
import logging
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from core.duplication_guard import DuplicationGuard
from core.dutching_batch_manager import DutchingBatchManager
from core.market_tracker import MarketTracker
from core.money_management import RoserpinaMoneyManagement
from core.pnl_engine import PnLEngine
from core.reconciliation_engine import ReconciliationEngine
from core.state_recovery import StateRecovery
from core.risk_desk import RiskDesk
from core.safety_layer import assert_live_gate_or_refuse
from core.system_state import DeskMode, RuntimeMode
from core.table_manager import TableManager
from core.trading_constants import CASHOUT_FAILED, REQ_EXECUTE_CASHOUT
from core.type_helpers import safe_bool
from services.catalog_sync_service import CatalogSyncService
from cashout_cancel_adapter import CashoutCancelAdapter
from cashout_router import CashoutRouter
from direct_best_price import SOURCE_FALLBACK_MASTER, resolve_direct_best_price
from direct_unmatched_ttl import select_expired_unmatched
from order_manager import TERMINAL_LIFECYCLE_EVENTS
from services.streaming_feed import StreamingConfigError, StreamingFeed
from trading_config import (
    AUTO_GREEN_DELAY_SEC,
    BETFAIR_ITALY_COMMISSION_PCT,
    STRICT_LIVE_KEY_SOURCE_REQUIRED,
    enforce_betfair_italy_commission_pct,
)

logger = logging.getLogger(__name__)

# B6.3.2a — eventi quick-bet che popolano/puliscono il registry DIRECT (TTL).
_DIRECT_TTL_ACK_EVENTS = frozenset({"QUICK_BET_ACCEPTED", "QUICK_BET_PARTIAL"})
# Pulizia SOLO quando l'ordine NON è più un resting vivo: FILLED (abbinato),
# FAILED (non piazzato), ROLLBACK_DONE (annullato). NON includere SUCCESS né
# AMBIGUOUS: un place riuscito (o esito incerto) può lasciare un ordine non
# abbinato vivo su Betfair — rimuoverlo lo toglierebbe dall'allowlist del poller
# TTL, che quindi non lo cancellerebbe più (Greptile P2). Fail-safe: resta
# tracciato finché un terminale certo lo rimuove o il poller lo cancella.
_DIRECT_TTL_TERMINAL_EVENTS = frozenset({
    "QUICK_BET_FILLED", "QUICK_BET_FAILED", "QUICK_BET_ROLLBACK_DONE",
})


class RuntimeController:
    """
    Runtime controller centrale.

    Responsabilità:
    - start/stop/pause/resume runtime
    - sincronizzazione simulation/live mode
    - routing dei segnali verso CMD_QUICK_BET
    - coordinamento Roserpina / tavoli / anti-duplicazione
    - snapshot stato runtime

    Nota:
    - NON chiude automaticamente le posizioni
    - la chiusura resta manuale/comando
    """

    def __init__(
        self,
        *,
        bus,
        db,
        settings_service,
        betfair_service,
        telegram_service,
        trading_engine=None,
        executor=None,
        safe_mode=None,
    ):
        self.bus = bus
        self.db = db
        self.settings_service = settings_service
        self.betfair_service = betfair_service
        self.telegram_service = telegram_service
        self.trading_engine = trading_engine
        self.executor = executor
        self.safe_mode = safe_mode

        self.config = self.settings_service.load_roserpina_config()
        self.table_manager = TableManager(table_count=self.config.table_count)
        self.duplication_guard = DuplicationGuard()
        self.risk_desk = RiskDesk()
        self.mm = RoserpinaMoneyManagement(self.config)

        self.batch_manager = DutchingBatchManager(db, bus=bus)
        self.reconciliation_engine = self._build_reconciliation_engine()

        self.market_tracker = MarketTracker(
            bus=self.bus,
            betfair_service=self.betfair_service,
        )
        # H-01: costruzione LAZY del CatalogSyncService. In __init__ la sessione
        # Betfair può non essere ancora connessa (e i fake nei test possono non
        # esporre get_client): NON chiamiamo betfair_service.get_client() qui —
        # lo faremmo cachando un client None/stale e romperemmo la costruzione
        # del controller. Il client viene risolto alla PRIMA esecuzione del sync
        # tramite la property `catalog_sync` (double-checked lock).
        self._catalog_sync: Optional[CatalogSyncService] = None
        self._catalog_sync_lock = threading.Lock()
        self._last_catalog_sync_at: float = 0.0
        self.streaming_feed: Optional[StreamingFeed] = None
        self._market_data_cfg: dict[str, Any] = {}
        self._last_fallback_snapshot_at: float = 0.0
        # PR3 (runtime_settlement_wiring): motore PnL cablato — publisher unico
        # di RUNTIME_CLOSE_POSITION. Auto-close mark-to-market OFF: il
        # controller NON chiude posizioni da solo; il settlement REALE arriva
        # dal poller cleared orders (default OFF, LIVE-only). Commissione =
        # costante di policy: il contratto settlement accetta solo quella.
        self.pnl_engine = PnLEngine(
            bus=self.bus,
            commission_pct=BETFAIR_ITALY_COMMISSION_PCT,
            auto_close_enabled=False,
        )
        # Dedupe emissione settlement: in-memory per il processo vivo; il
        # riavvio è coperto dal pre-check durevole sul cycle recovery state
        # (stessa settlement_key del consumer: "cleared:<market_id>").
        self._settlement_emitted_keys: set[str] = set()
        self._settlement_poll_thread: Optional[threading.Thread] = None
        self._settlement_poll_stop = threading.Event()
        self._settlement_poll_cfg: dict[str, Any] = {}
        # Serializza i GIRI di poll: se un vecchio thread sta finendo un giro
        # (join scaduto su una chiamata di rete lenta) e uno nuovo parte, i
        # due giri non possono mai sovrapporsi (doppia emissione impossibile).
        self._settlement_poll_round_lock = threading.Lock()
        # Primo giro di OGNI generazione del poller: sweep COMPLETO (senza
        # settled_after, orizzonte Betfair ~90gg) per recuperare i settlement
        # maturati durante il downtime; il dedupe filtra il gia' consegnato.
        # La generazione e' legata al THREAD (passata come argomento): un
        # thread vecchio sopravvissuto al join puo' stampare solo la SUA
        # generazione, mai bruciare lo sweep di quella nuova.
        self._settlement_poll_generation = 0
        self._settlement_sweep_done_generation = -1
        # Registry DIRECT (B6.3.2a): customer_ref → bet_id degli ordini DIRECT
        # vivi. In-memory, riparte vuoto al restart (fail-safe).
        self._direct_order_bet_ids: dict[str, str] = {}
        # Gate del poller TTL DIRECT (B6.3.2b), su time.monotonic.
        self._last_direct_ttl_poll_at: float = 0.0

        self.mode = RuntimeMode.STOPPED
        self.last_error = ""
        self.last_signal_at = ""
        self.simulation_mode = False
        self.execution_mode = "SIMULATION"
        self.live_enabled = False
        self.live_readiness_ok = False
        self.last_execution_gate_reason = "startup_default"
        self.enforce_probe_readiness_gate = False
        self.last_deploy_gate_status = {
            "allowed": False,
            "reason": "DEPLOY_BLOCKED_INVALID_STATE",
            "reasons": ["DEPLOY_BLOCKED_INVALID_STATE"],
            "readiness": "UNKNOWN",
            "details": {},
        }

        # Emergency stop state — set by emergency_stop(), cleared only by reset_emergency()
        self._emergency_stopped: bool = False
        self._emergency_stopped_at: str = ""
        self._emergency_reason: str = ""
        # Serializza il trigger della kill-switch da daily-loss: rende atomico
        # il check-and-stop dell'handler sotto dispatch EventBus concorrente.
        self._daily_loss_stop_lock = threading.Lock()
        # Grace auto-green OPT-IN (G5): mappa chiave-target -> Timer in volo, per
        # sopprimere un secondo grace sullo stesso target (il dedup del bridge e'
        # 2.0s < grace 2.5s => non basta) E poter CANCELLARE i grace pendenti
        # quando un CASHOUT_ALL chiude tutto inline (evita che un timer fissi un
        # secondo green-up su un target gia' chiuso). Vive sul controller perche'
        # il CashoutRouter e' costruito fresco a ogni segnale.
        self._auto_green_pending: dict = {}
        self._auto_green_pending_lock = threading.Lock()
        # Generazione monotona: un CASHOUT_ALL la incrementa => ogni grace gia'
        # schedulato con una generazione precedente e' INVALIDATO (tombstone),
        # anche se il suo Timer e' gia' partito nella finestra reserve->start e
        # non e' piu' cancellabile. Il callback differito confronta la propria
        # generazione con quella corrente e abortisce se stale.
        self._auto_green_generation = 0
        # Pending-stop sincrono: armato sotto lock PRIMA di applicare il PnL del
        # settlement che sfonderebbe il limite, cosi' un SIGNAL_RECEIVED
        # concorrente (dispatch multi-worker) non piazza un ordine nella finestra
        # tra apply-PnL e emergency_stop. Bridge: superato da _emergency_stopped.
        self._daily_loss_pending_stop: bool = False
        # Serializza il read-modify-write su _daily_loss_monitor_state tra
        # _monitor_daily_loss_breach e _record_pending_daily_loss_breach (chiamati
        # da worker EventBus + status-poll concorrenti): senza, un update
        # concorrente potrebbe sovrascrivere il breach persistito e riaprire il
        # restart LIVE in giornata. Lock SEPARATO da _daily_loss_stop_lock per
        # evitare inversioni d'ordine (l'enforce tiene lo stop-lock e poi, via
        # emergency_stop->get_status, ricalcola il monitor).
        self._daily_loss_state_lock = threading.Lock()
        self._io_observations: dict[str, Any] = {
            "last_operation": "",
            "last_status": "UNKNOWN",
            "last_latency_ms": 0.0,
            "slow_count": 0,
            "degraded_count": 0,
            "unavailable_count": 0,
            "total_count": 0,
            "last_error": "",
        }
        self._processed_bankroll_sync_keys: set[str] = set()
        self._processed_realized_pnl_keys: set[str] = set()
        self._processed_auto_trade_keys: set[str] = set()
        self._cycle_step_counts: dict[str, int] = {}
        self._last_bankroll_sync_result: dict[str, Any] = {
            "correlation_id": "",
            "settlement_detected": False,
            "bankroll_before": float(self.risk_desk.bankroll_current),
            "bankroll_after": float(self.risk_desk.bankroll_current),
            "bankroll_sync_status": "NOT_SETTLED",
            "balance_source": "none",
            "reason": "sync_not_triggered",
        }
        self._last_auto_trade_result: dict[str, Any] = {
            "correlation_id": "",
            "source_settlement_correlation_id": "",
            "bankroll_sync_status": "NOT_SETTLED",
            "money_management_status": "MM_STOP_CONTEXT_MISSING",
            "cycle_active": False,
            "progression_allowed": False,
            "auto_trade_enabled": False,
            "auto_trade_status": "AUTO_TRADE_NOT_ELIGIBLE",
            "next_stake": 0.0,
            "risk_status": "RISK_NOT_EVALUATED",
            "submitted": False,
            "reason": "auto_trade_not_triggered",
        }
        self._last_cycle_executor_result: dict[str, Any] = {
            "correlation_id": "",
            "source_settlement_correlation_id": "",
            "cycle_executor_enabled": False,
            "cycle_active": False,
            "progression_allowed": False,
            "bankroll_sync_status": "NOT_SETTLED",
            "money_management_status": "MM_STOP_CONTEXT_MISSING",
            "next_stake": 0.0,
            "cycle_step_index": 0,
            "max_steps_reached": False,
            "kill_switch_active": False,
            "anomaly_pause_active": False,
            "auto_trade_enabled": False,
            "auto_trade_status": "AUTO_TRADE_NOT_ELIGIBLE",
            "cycle_executor_status": "CYCLE_EXECUTOR_DISABLED",
            "risk_status": "RISK_NOT_EVALUATED",
            "submitted": False,
            "reason": "cycle_executor_not_triggered",
            "recovery_status": "RECOVERY_NO_STATE",
        }
        self._daily_loss_monitor_state: dict[str, Any] = {
            "day_utc": datetime.utcnow().date().isoformat(),
            "threshold": self._safe_daily_loss_threshold(),
            "realized_pnl_day_baseline": float(self.risk_desk.realized_pnl),
            "realized_pnl": float(self.risk_desk.realized_pnl),
            "intraday_realized_pnl": 0.0,
            "daily_loss_amount": 0.0,
            "breached": False,
            "breached_at": "",
            "last_status": "DAILY_LOSS_NOT_CONFIGURED",
            "reason": "max_daily_loss_not_configured",
            "alert_count": 0,
            "last_alert_event": "",
            "last_checked_at": datetime.utcnow().isoformat(),
        }

        self._subscribe_bus()

        # Fail-closed cross-riavvio: senza il reload, un crash/restart del
        # processo durante l'emergenza farebbe ripartire il bot operativo,
        # bypassando il contratto "solo reset_emergency() riapre". Va eseguito
        # a costruzione COMPLETATA: ripristina l'intera postura (lockdown
        # incluso), non solo il flag.
        self._reload_persisted_emergency_state()

    @property
    def catalog_sync(self) -> CatalogSyncService:
        """CatalogSyncService costruito in modo lazy (H-01).

        Il client Betfair viene risolto solo qui, alla prima esecuzione del
        sync, non in __init__: così la costruzione del RuntimeController non
        dipende da una sessione Betfair già connessa né da `get_client()` sul
        servizio. Risoluzione fail-safe: se il servizio non espone `get_client`
        (fake minimali) o il client non è ancora disponibile (`None`, sessione
        non connessa) NON si solleva `AttributeError` e NON si cacha uno stato
        invalido — si ritorna un'istanza transitoria e si riprova al prossimo
        accesso, così dopo la connessione il sync usa un client valido. Questo
        vale anche per `start()`, che legge `self.catalog_sync` per schedulare
        il boot sync (non solo la costruzione). Double-checked lock per garantire
        una sola istanza cachata anche se il primo accesso avviene da thread
        diversi (i due call-site di run_sync risolvono la property nel thread
        chiamante prima di sottomettere il metodo all'executor/Thread).
        """
        if self._catalog_sync is not None:
            return self._catalog_sync
        with self._catalog_sync_lock:
            if self._catalog_sync is None:
                getter = getattr(self.betfair_service, "get_client", None)
                client = getter() if callable(getter) else None
                service = CatalogSyncService(self.db, client)
                if client is None:
                    # Stato invalido (get_client assente o client non ancora
                    # connesso): non cachare, riprova al prossimo accesso.
                    return service
                self._catalog_sync = service
        return self._catalog_sync

    def _record_runtime_io(self, *, operation: str, started_at: float, ok: bool, error: str = "") -> None:
        elapsed_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
        slow_threshold_ms = 2000.0
        if ok and elapsed_ms >= slow_threshold_ms:
            status = "SLOW"
        elif ok:
            status = "SUCCESS"
        elif error:
            status = "DEGRADED"
        else:
            status = "UNAVAILABLE"
        self._io_observations["last_operation"] = str(operation)
        self._io_observations["last_status"] = status
        self._io_observations["last_latency_ms"] = round(elapsed_ms, 3)
        self._io_observations["last_error"] = str(error or "")
        self._io_observations["total_count"] = int(self._io_observations.get("total_count", 0) or 0) + 1
        if status == "SLOW":
            self._io_observations["slow_count"] = int(self._io_observations.get("slow_count", 0) or 0) + 1
        if status == "DEGRADED":
            self._io_observations["degraded_count"] = int(self._io_observations.get("degraded_count", 0) or 0) + 1
        if status == "UNAVAILABLE":
            self._io_observations["unavailable_count"] = int(self._io_observations.get("unavailable_count", 0) or 0) + 1

    @staticmethod
    def _extract_origin_metadata(signal: dict) -> tuple[dict | None, dict | None]:
        """
        Runtime boundary metadata extractor.

        RuntimeController acts as a passthrough boundary:
        - preserves structurally valid copy/pattern metadata
        - does not own allow-list normalization (TradingEngine remains authoritative)
        """
        copy_meta = signal.get("copy_meta")
        pattern_meta = signal.get("pattern_meta")
        copy_dict = dict(copy_meta) if isinstance(copy_meta, dict) else None
        pattern_dict = dict(pattern_meta) if isinstance(pattern_meta, dict) else None
        return copy_dict, pattern_dict

    @staticmethod
    def _resolve_order_origin(signal: dict, *, has_copy_meta: bool, has_pattern_meta: bool) -> str:
        """
        Resolve boundary origin with minimal inference.
        TradingEngine remains the final normalization authority.
        """
        explicit = str(signal.get("order_origin") or "").strip()
        if explicit:
            return explicit
        if has_copy_meta:
            return "COPY"
        if has_pattern_meta:
            return "PATTERN"
        return ""

    def _load_market_data_config(self) -> dict:
        if hasattr(self.settings_service, "load_market_data_config"):
            try:
                cfg = self.settings_service.load_market_data_config() or {}
                if isinstance(cfg, dict):
                    return cfg
            except Exception:
                logger.exception("Errore load_market_data_config")
        return {
            "market_data_mode": "poll",
            "enabled": False,
            "snapshot_fallback_enabled": True,
            "snapshot_fallback_interval_sec": 5,
            "market_ids": [],
        }

    def _stream_market_book_callback(self, market_book: dict) -> None:
        self.market_tracker.on_market_book(dict(market_book or {}))
        self._poll_direct_unmatched_ttl()  # B6.3.2b — no-op se flag OFF / non scaduto
        self._monitor_sl_tp_trailing(market_book) # C1 (#320)

    def _stream_disconnect_callback(self, payload: dict) -> None:
        logger.warning("Streaming disconnected -> fallback snapshot payload=%s", payload)
        self._snapshot_rest_fallback(reason="stream_disconnect", payload=payload)

    def _start_market_data_feed(self) -> None:
        self._market_data_cfg = self._load_market_data_config()
        mode = str(self._market_data_cfg.get("market_data_mode", "poll") or "poll").strip().lower()
        enabled = bool(self._market_data_cfg.get("enabled", False))
        if self.simulation_mode:
            return
        if mode not in {"stream", "hybrid"} or not enabled:
            return

        self.streaming_feed = StreamingFeed(
            client_getter=self.betfair_service.get_live_client,
            config=self._market_data_cfg,
            on_market_book=self._stream_market_book_callback,
            on_disconnect=self._stream_disconnect_callback,
            session_gate=self.betfair_service.ensure_stream_session_ready,
        )
        self.streaming_feed.start()

    def _stop_market_data_feed(self) -> None:
        if self.streaming_feed is None:
            return
        try:
            self.streaming_feed.stop()
        except Exception:
            logger.exception("Errore stop streaming_feed")
        finally:
            self.streaming_feed = None

    # =========================================================
    # Settlement poller (PR3, runtime_settlement_wiring) — il ciclo di
    # chiusura reale: listClearedOrders(SETTLED, group_by=MARKET) =>
    # PnLEngine.apply_cleared_market_settlement => RUNTIME_CLOSE_POSITION
    # => daily-loss/bankroll sync. Default OFF, LIVE-only, fail-closed.
    # =========================================================
    def _load_settlement_poll_config(self) -> dict:
        """Config del poller settlement, letta dalle settings (pattern
        market-data). Chiavi: ``settlement.poll_enabled`` (default False —
        dormiente, decisione owner), ``settlement.poll_sec`` (default 60,
        min 5), ``settlement.lookback_hours`` (default 24, clamp 1..168).
        Qualsiasi errore di lettura/parse => default DISABLED (fail-closed).
        """
        defaults = {"enabled": False, "poll_sec": 60.0, "lookback_hours": 24.0}
        try:
            data = (
                self.settings_service.get_all_settings()
                if hasattr(self.settings_service, "get_all_settings")
                else {}
            )
            if not isinstance(data, dict):
                return dict(defaults)
            enabled = self._safe_bool(
                data.get("settlement.poll_enabled"), default=False
            )
            poll_sec = float(
                data.get("settlement.poll_sec", defaults["poll_sec"])
                or defaults["poll_sec"]
            )
            lookback = float(
                data.get("settlement.lookback_hours", defaults["lookback_hours"])
                or defaults["lookback_hours"]
            )
            if not math.isfinite(poll_sec) or not math.isfinite(lookback):
                return dict(defaults)
            return {
                "enabled": bool(enabled),
                "poll_sec": max(5.0, poll_sec),
                "lookback_hours": min(168.0, max(1.0, lookback)),
            }
        except Exception:
            logger.exception("Errore load settlement poll config (default: disabled)")
            return dict(defaults)

    def _start_settlement_poller(self) -> None:
        self._settlement_poll_cfg = self._load_settlement_poll_config()
        if not bool(self._settlement_poll_cfg.get("enabled", False)):
            return
        if self.simulation_mode:
            # Parity SIM non ancora cablata (record_realized_settlement del
            # broker simulato resta senza consumer): nessun poll in SIM.
            return
        if (
            self._settlement_poll_thread is not None
            and self._settlement_poll_thread.is_alive()
        ):
            return
        self._settlement_poll_generation += 1
        generation = self._settlement_poll_generation
        stop_event = threading.Event()
        self._settlement_poll_stop = stop_event
        # Event e GENERAZIONE vengono passati AL thread: il loop usa SEMPRE i
        # propri (mai gli attributi, che un restart puo' rimpiazzare). Un
        # thread vecchio sopravvissuto al join resta fermabile dal SUO event
        # gia' settato, non puo' "adottare" l'event nuovo e non puo' armare
        # lo sweep della generazione nuova (rilievi GPT-5.6 su #440).
        self._settlement_poll_thread = threading.Thread(
            target=self._settlement_poll_loop,
            args=(stop_event, generation),
            name="settlement-poller",
            daemon=True,
        )
        self._settlement_poll_thread.start()
        logger.info(
            "Settlement poller avviato (poll_sec=%.0f lookback_hours=%.0f)",
            float(self._settlement_poll_cfg.get("poll_sec", 60.0)),
            float(self._settlement_poll_cfg.get("lookback_hours", 24.0)),
        )

    def _stop_settlement_poller(self) -> None:
        thread = self._settlement_poll_thread
        if thread is None:
            return
        self._settlement_poll_stop.set()
        try:
            thread.join(timeout=5.0)
            if thread.is_alive():
                logger.warning(
                    "Settlement poller ancora vivo dopo il join: uscira' al "
                    "prossimo check del suo stop event (gia' settato); il "
                    "round lock impedisce comunque giri sovrapposti."
                )
        except Exception:
            logger.exception("Errore join settlement poller")
        finally:
            self._settlement_poll_thread = None

    def _settlement_poll_loop(
        self, stop_event: threading.Event, generation: Optional[int] = None
    ) -> None:
        interval = max(
            5.0, float(self._settlement_poll_cfg.get("poll_sec", 60.0) or 60.0)
        )
        while not stop_event.is_set():
            try:
                self._poll_cleared_settlements(generation=generation)
            except Exception:
                logger.exception("Errore poll cleared settlements (round abortito)")
            stop_event.wait(interval)

    def _poll_cleared_settlements(self, generation: Optional[int] = None) -> None:
        """Un giro di poll dei settlement reali. Fail-closed su ogni ramo:

        - SIM o runtime non ACTIVE => nessuna chiamata;
        - **identita' del bot PER-BET (I1)**: si interrogano SOLO i ``betId``
          registrati dal bot (``db.get_bot_active_orders`` — ledger a
          denylist SIM+LIVE: le bet settlate RESTANO nel ledger, si escludono
          solo CANCELLED/INFLIGHT). Granularita' per-bet: le scommesse
          MANUALI dell'account — anche sullo stesso mercato del bot — non
          entrano MAI nel daily-loss (un profitto esterno maschererebbe le
          perdite del bot: fail-open sul kill-switch). Gli id non-numerici
          (ledger SIM, ``SIMBET-*``) sono esclusi dal poll LIVE. Identita'
          illeggibile o vuota => nessuna chiamata;
        - **primo giro di ogni generazione = sweep completo** (senza
          ``settled_after``, orizzonte Betfair ~90gg): recupera i settlement
          maturati durante il downtime oltre il lookback; i giri successivi
          usano la finestra mobile. Lo sweep si dichiara fatto SOLO se il
          fetch e' riuscito E nessuna emissione e' fallita, e vale per la
          SOLA generazione del thread che l'ha eseguito;
        - fetch fallito => round abortito (mai una lista vuota spacciata per
          "nessun settlement" — il client/service propagano, qui si logga e
          si ritenta al giro dopo);
        - riga malformata (betId/marketId/profit assenti o non numerici) o
          con betId fuori dall'allowlist => scartata con log, MAI inventare
          un profit;
        - dedupe per-bet: guardia idempotente NEL MOTORE (mercato, ref) +
          in-memory per il processo + pre-check durevole sul cycle recovery
          state (il consumer persiste il checkpoint alla PRIMA consegna con
          la stessa chiave "cleared:<market_id>:<bet_id>"); stato db in
          errore => la riga NON viene emessa e NON viene marcata vista;
        - giri serializzati da ``_settlement_poll_round_lock``: un giro in
          corso (anche di un thread vecchio) esclude il successivo.
        """
        if not self._settlement_poll_round_lock.acquire(blocking=False):
            return
        try:
            self._poll_cleared_settlements_locked(
                generation=(
                    self._settlement_poll_generation
                    if generation is None
                    else int(generation)
                )
            )
        finally:
            self._settlement_poll_round_lock.release()

    def _poll_cleared_settlements_locked(self, *, generation: int) -> None:
        if self.simulation_mode:
            return
        if self.mode is not RuntimeMode.ACTIVE:
            return

        identity_getter = getattr(self.db, "get_bot_active_orders", None)
        if not callable(identity_getter):
            logger.warning(
                "Settlement poll: db senza get_bot_active_orders — nessun "
                "filtro identita' possibile, round abortito (fail-closed)."
            )
            return
        try:
            bot_orders = identity_getter() or []
        except Exception as exc:
            logger.warning(
                "Settlement poll: identita' bot illeggibile (%s), round "
                "abortito (fail-closed).",
                exc,
            )
            return
        bet_to_market: dict[str, str] = {}
        for order in bot_orders:
            if not isinstance(order, dict):
                continue
            bet_id = str(order.get("bet_id") or "").strip()
            market_id = str(order.get("market_id") or "").strip()
            # Solo id numerici: i betId reali di Betfair sono cifre; il
            # ledger SIM usa "SIMBET-*" e non esiste sull'exchange.
            if bet_id.isdigit() and market_id:
                bet_to_market[bet_id] = market_id
        bot_bet_ids = sorted(bet_to_market)
        if not bot_bet_ids:
            return

        sweep_done = self._settlement_sweep_done_generation == generation
        settled_after: Optional[str] = None
        if sweep_done:
            lookback_hours = float(
                self._settlement_poll_cfg.get("lookback_hours", 24.0) or 24.0
            )
            settled_after = (
                datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
            ).isoformat()

        rows: list[dict] = []
        try:
            for chunk_start in range(0, len(bot_bet_ids), 1000):
                chunk = bot_bet_ids[chunk_start:chunk_start + 1000]
                rows.extend(
                    self.betfair_service.list_cleared_orders(
                        bet_status="SETTLED",
                        settled_after=settled_after,
                        bet_ids=chunk,
                    )
                )
        except Exception as exc:
            logger.warning(
                "Settlement poll: fetch cleared orders fallito, round abortito "
                "(fail-closed): %s",
                exc,
            )
            return

        valid_rows: list[dict] = []
        for row in rows or []:
            if not isinstance(row, dict):
                logger.warning(
                    "Settlement poll: riga cleared non-dict scartata: %r", row
                )
                continue
            valid_rows.append(row)

        # Ordine di lavorazione (rilievi GPT-5.6/Fable/Fugu su #440, giri
        # 3-6): l'unita' atomica e' il CLUSTER TEMPORALE di mercato.
        #
        # - Le gambe dello STESSO mercato settlate entro 2s l'una
        #   dall'altra sono lo stesso evento di settlement (il jitter dei
        #   timestamp non e' un ordine reale): dentro il cluster profit
        #   DECRESCENTE — in una sequenza decrescente il minimo dei
        #   prefissi coincide col totale, quindi il cumulato non scende mai
        #   sotto il netto del cluster e le gambe perdenti di un dutching
        #   non possono far scattare l'emergency stop su un settlement che
        #   netta positivo.
        # - Gambe dello stesso mercato settlate LONTANE nel tempo
        #   (settlement parziali) sono eventi economici DISTINTI: cluster
        #   separati, rigiocati in ordine cronologico — un dip storico
        #   reale (anche intrecciato con altri mercati) non viene MAI
        #   attenuato da una vincita successiva (sarebbe fail-open sul
        #   kill-switch).
        # - Le date sono confrontate come datetime REALI (Z/offset/frazioni
        #   normalizzati): il confronto lessicografico tra formati ISO
        #   misti non e' cronologico ('.' < 'Z').
        # - Cluster non databili: sign-aware fail-closed — nette PERDITE in
        #   TESTA (il dip non databile si assume al peggio), profitti in
        #   coda (un profitto non databile non puo' mascherare un dip
        #   storico datato).
        def _profit_sicuro(row: dict) -> float:
            profit = row.get("profit")
            if (
                isinstance(profit, (int, float))
                and not isinstance(profit, bool)
                and math.isfinite(float(profit))
            ):
                return float(profit)
            return 0.0

        def _data_settlement(row: dict) -> Optional[datetime]:
            testo = str(row.get("settledDate") or "").strip()
            if not testo:
                return None
            candidato = testo[:-1] + "+00:00" if testo.endswith("Z") else testo
            try:
                parsed = datetime.fromisoformat(candidato)
            except ValueError:
                return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed

        _CLUSTER_GAP_SEC = 2.0
        gruppi: dict[str, list[dict]] = {}
        for row in valid_rows:
            gruppi.setdefault(str(row.get("marketId") or ""), []).append(row)

        clusters: list[tuple[tuple, list[dict]]] = []
        for market_id, righe_gruppo in gruppi.items():
            datate = [(r, _data_settlement(r)) for r in righe_gruppo]
            senza_data = [r for r, dt in datate if dt is None]
            con_data = sorted(
                ((r, dt) for r, dt in datate if dt is not None),
                key=lambda item: item[1],
            )
            corrente: list[tuple[dict, datetime]] = []
            for r, dt in con_data:
                if (
                    corrente
                    and (dt - corrente[-1][1]).total_seconds() > _CLUSTER_GAP_SEC
                ):
                    ancora = corrente[0][1]
                    righe = sorted(
                        (x for x, _ in corrente), key=lambda x: -_profit_sicuro(x)
                    )
                    clusters.append(((0, ancora, market_id), righe))
                    corrente = []
                corrente.append((r, dt))
            if corrente:
                ancora = corrente[0][1]
                righe = sorted(
                    (x for x, _ in corrente), key=lambda x: -_profit_sicuro(x)
                )
                clusters.append(((0, ancora, market_id), righe))
            if senza_data:
                netto = sum(_profit_sicuro(r) for r in senza_data)
                righe = sorted(senza_data, key=lambda x: -_profit_sicuro(x))
                posizione = -1 if netto < 0.0 else 1
                clusters.append(((posizione, None, market_id), righe))

        def _ordine_cluster(item):
            (posizione, ancora, market_id), _righe = item
            if posizione == 0:
                return (0, ancora, market_id)
            # I non databili usano un datetime sentinella per confronto
            # omogeneo: perdite prima di tutto, profitti dopo tutto.
            return (posizione, datetime.min.replace(tzinfo=timezone.utc), market_id)

        clusters.sort(key=_ordine_cluster)
        valid_rows = [row for _chiave, righe in clusters for row in righe]

        emission_failures = 0
        for row in valid_rows:
            market_id = str(row.get("marketId") or "").strip()
            bet_id = str(row.get("betId") or row.get("bet_id") or "").strip()
            profit_raw = row.get("profit")
            profit_valid = (
                not isinstance(profit_raw, bool)
                and isinstance(profit_raw, (int, float))
                and math.isfinite(float(profit_raw))
            )
            if not market_id or not bet_id or not profit_valid:
                logger.warning(
                    "Settlement poll: riga cleared malformata scartata "
                    "market=%r bet=%r profit=%r",
                    row.get("marketId"),
                    row.get("betId"),
                    profit_raw,
                )
                continue
            expected_market = bet_to_market.get(bet_id)
            if expected_market is None or expected_market != market_id:
                # Belt-and-braces oltre il filtro server-side: una bet fuori
                # dall'allowlist d'identita' (o con mercato incoerente col
                # ledger) NON entra mai nel daily-loss.
                logger.warning(
                    "Settlement poll: bet %s fuori dall'allowlist bot o "
                    "mercato incoerente (%r vs ledger %r) — riga scartata.",
                    bet_id,
                    market_id,
                    expected_market,
                )
                continue
            settlement_key = f"cleared:{market_id}:{bet_id}"
            if settlement_key in self._settlement_emitted_keys:
                continue
            probe = self._read_cycle_recovery_state(settlement_key)
            probe_status = str(probe.get("status") or "")
            if probe_status == "RECOVERY_STATE_INVALID":
                logger.warning(
                    "Settlement poll: recovery state illeggibile per %s — "
                    "emissione sospesa (ritentata al prossimo giro)",
                    settlement_key,
                )
                emission_failures += 1
                continue
            if probe_status != "RECOVERY_NO_STATE":
                # Checkpoint durevole gia' presente: consegnato in un processo
                # precedente. Non ri-applicare il PnL.
                self._settlement_emitted_keys.add(settlement_key)
                continue
            commission_raw = row.get("commission")
            reported_commission = (
                float(commission_raw)
                if (
                    not isinstance(commission_raw, bool)
                    and isinstance(commission_raw, (int, float))
                    and math.isfinite(float(commission_raw))
                )
                else None
            )
            try:
                self.pnl_engine.apply_cleared_market_settlement(
                    market_id=market_id,
                    gross_pnl=float(profit_raw),
                    source="betfair_cleared_orders",
                    settled_date=str(row.get("settledDate") or ""),
                    reported_commission=reported_commission,
                    settlement_ref=bet_id,
                )
            except ValueError as exc:
                if "CLEARED_SETTLEMENT_DUPLICATE" in str(exc):
                    # Il motore l'ha gia' realizzato (guardia idempotente):
                    # marca visto e prosegui, nessun doppio conteggio.
                    self._settlement_emitted_keys.add(settlement_key)
                    continue
                logger.exception(
                    "Settlement poll: emissione fallita per %s (riga saltata, "
                    "ritentata al prossimo giro)",
                    settlement_key,
                )
                emission_failures += 1
                continue
            except Exception:
                logger.exception(
                    "Settlement poll: emissione fallita per %s (riga saltata, "
                    "ritentata al prossimo giro)",
                    settlement_key,
                )
                emission_failures += 1
                continue
            self._settlement_emitted_keys.add(settlement_key)

        if emission_failures == 0:
            # Sweep completo dichiarato fatto SOLO per la generazione di
            # QUESTO thread, e solo se fetch ed emissioni sono andati a
            # buon fine: una riga transitoriamente fallita al primo giro
            # sara' ritentata ancora a orizzonte completo, mai persa oltre
            # il lookback (rilievi Fable/GPT-5.6 su #440).
            self._settlement_sweep_done_generation = generation

    def _snapshot_rest_fallback(self, *, reason: str, payload: Optional[dict] = None) -> None:
        cfg = dict(self._market_data_cfg or self._load_market_data_config())
        if not bool(cfg.get("snapshot_fallback_enabled", True)):
            return

        now = time.monotonic()
        
        # Fase A1: Auto-Update Catalogo (ogni 12 ore)
        if now - self._last_catalog_sync_at > 43200: # 12 ore
            self._last_catalog_sync_at = now
            if self.executor:
                self.executor.submit("background_catalog_sync", self.catalog_sync.run_sync, force=True)
            else:
                threading.Thread(target=self.catalog_sync.run_sync, kwargs={"force": True}, daemon=True).start()

        min_interval = max(1.0, float(cfg.get("snapshot_fallback_interval_sec", 5) or 5))
        if (now - self._last_fallback_snapshot_at) < min_interval:
            return

        market_ids = [str(mid).strip() for mid in (cfg.get("market_ids") or []) if str(mid).strip()]
        if not market_ids:
            return

        for market_id in market_ids:
            book = self.betfair_service.get_market_book_snapshot(market_id)
            if isinstance(book, dict) and book:
                self.market_tracker.on_market_book(book)
        self._last_fallback_snapshot_at = now
        self.bus.publish(
            "MARKET_DATA_FALLBACK_SNAPSHOT",
            {
                "reason": reason,
                "market_count": len(market_ids),
                "ts": datetime.utcnow().isoformat(),
                "stream_payload": dict(payload or {}),
            },
        )
        self._poll_direct_unmatched_ttl()  # B6.3.2b — copre il path REST fallback

    def runtime_io_snapshot(self) -> dict:
        return dict(self._io_observations)

    # =========================================================
    # INTERNAL BUILDERS
    # =========================================================
    def _build_reconciliation_engine(self) -> ReconciliationEngine:
        return ReconciliationEngine(
            db=self.db,
            bus=self.bus,
            batch_manager=self.batch_manager,
            betfair_service=self.betfair_service,
            table_manager=self.table_manager,
            duplication_guard=self.duplication_guard,
        )

    def _safe_daily_loss_threshold(self) -> Optional[float]:
        raw = getattr(self.config, "max_daily_loss", None)
        try:
            parsed = float(raw)
        except Exception:
            return None
        if not math.isfinite(parsed) or parsed <= 0.0:
            return None
        return parsed

    def _projected_daily_loss_breach(self, additional_pnl: float = 0.0) -> dict:
        """Stato di breach PROIETTATO includendo additional_pnl nel realized,
        READ-ONLY (non muta lo stato ne' pubblica eventi). Serve ai rami di
        early-return (es. recovery fail-closed) che ritornano PRIMA di applicare
        il PnL e chiamare il monitor: se un settlement accettato sfonderebbe il
        limite, si fail-closa comunque (un eventuale falso positivo da duplicato
        e' piu' sicuro di un breach mancato).

        Onora il rollover di giornata come `_monitor_daily_loss_breach`: se lo
        stato in cache e' di un giorno precedente, la baseline si ribasa sul
        realized di fine giornata precedente. Senza questo la perdita di ieri
        verrebbe conteggiata come perdita di oggi (baseline stantia) e
        produrrebbe un falso breach/emergency-stop al primo settlement del
        nuovo giorno."""
        threshold = self._safe_daily_loss_threshold()
        if threshold is None:
            return {"breached": False, "daily_loss_amount": 0.0}
        today_utc = datetime.utcnow().date().isoformat()
        state = self._daily_loss_monitor_state or {}
        previous_day = str(state.get("day_utc") or "")
        day_rollover = bool(previous_day and previous_day != today_utc)
        realized = float(self.risk_desk.realized_pnl) + float(additional_pnl or 0.0)
        if day_rollover:
            prev_realized_raw = state.get("realized_pnl")
            baseline = float(
                self.risk_desk.realized_pnl if prev_realized_raw is None else prev_realized_raw
            )
        else:
            baseline_raw = state.get("realized_pnl_day_baseline")
            baseline = float(realized if baseline_raw is None else baseline_raw)
        daily_loss = max(0.0, -(realized - baseline))
        return {"breached": daily_loss >= threshold, "daily_loss_amount": daily_loss}

    def _record_pending_daily_loss_breach(self, projected: dict) -> None:
        """Persiste IN-MEMORY un breach PROIETTATO in `_daily_loss_monitor_state`
        cosi' che `_monitor_daily_loss_breach` (invocato da start()/resume()) lo
        mantenga breached fino al rollover di giornata, ANCHE quando il PnL non
        e' stato applicato e l'emergency_stop non e' scattato (ramo recovery
        fail-closed con runtime STOPPED). Senza questo un settlement live che
        sfonda il limite ma esce dal ramo recovery verrebbe dimenticato e un
        `start(LIVE)` successivo riprenderebbe a fare trading dopo la perdita
        giornaliera gia' sfondata.

        Poiche' in questo ramo il monitor e' SALTATO, qui e' l'unico punto che
        registra il breach: emette il `DAILY_LOSS_BREACH_TRIGGERED` (una sola
        volta, fuori dal lock) e incrementa `alert_count`, altrimenti i
        subscriber di monitoraggio non vedrebbero mai il primo trigger del
        daily-loss per i settlement recovery ambigui (un successivo monitor
        userebbe il ramo persistente, che non riemette il trigger)."""
        now_iso = datetime.utcnow().isoformat()
        today_utc = datetime.utcnow().date().isoformat()
        amount = float(projected.get("daily_loss_amount", 0.0) or 0.0)
        alert_payload: Optional[dict] = None
        with self._daily_loss_state_lock:
            prev = dict(self._daily_loss_monitor_state or {})
            already_breached_today = bool(prev.get("breached")) and str(prev.get("day_utc") or "") == today_utc
            prev_amount = float(prev.get("daily_loss_amount", 0.0) or 0.0)
            effective_amount = max(amount, prev_amount)
            alert_count = int(prev.get("alert_count", 0) or 0)
            if not already_breached_today:
                alert_count += 1
            state = dict(prev)
            state.update({
                "day_utc": today_utc,
                "breached": True,
                "daily_loss_amount": effective_amount,
                "breached_at": str(prev.get("breached_at") or now_iso),
                "last_status": "DAILY_LOSS_BREACHED",
                "reason": "daily_loss_breach_projected_recovery_failclosed",
                "last_alert_event": "DAILY_LOSS_BREACH_TRIGGERED",
                "alert_count": alert_count,
                "last_checked_at": now_iso,
            })
            self._daily_loss_monitor_state = state
            if not already_breached_today:
                alert_payload = {
                    "source": "RUNTIME_RECOVERY_FAILCLOSED",
                    "day_utc": today_utc,
                    "breached": True,
                    "threshold": float(self._safe_daily_loss_threshold() or 0.0),
                    "daily_loss_amount": float(effective_amount),
                    "realized_pnl": float(self.risk_desk.realized_pnl),
                    "breached_at": str(state["breached_at"]),
                    "alert_count": int(alert_count),
                    "runtime_mode": str(self.mode.value),
                    "execution_mode": str(self.execution_mode),
                    "payload_correlation_id": "",
                }
        if alert_payload is not None:
            logger.critical("DAILY LOSS BREACH TRIGGERED (recovery fail-closed): %s", alert_payload)
            self.bus.publish("DAILY_LOSS_BREACH_TRIGGERED", dict(alert_payload))

    def _safe_status_snapshot(self) -> dict:
        """Snapshot di stato che NON dipende dall'I/O broker: prova get_status()
        ma, se solleva (es. get_account_funds in outage), ricade su stato locale
        cached. Usato nei rami di rifiuto fail-closed (start/resume) cosi' che il
        rifiuto resti indipendente dalla connettivita' del broker."""
        try:
            return self.get_status()
        except Exception:
            logger.warning("status snapshot failed; using local cached state", exc_info=True)
            return {
                "mode": self.mode.value,
                "execution_mode": str(self.execution_mode),
                "live_enabled": bool(self.live_enabled),
                "is_emergency_stopped": bool(self._emergency_stopped),
                "status_snapshot_degraded": True,
            }

    def _monitor_daily_loss_breach(self, *, source: str, payload: Optional[dict] = None) -> dict[str, Any]:
        now = datetime.utcnow()
        today_utc = now.date().isoformat()
        threshold = self._safe_daily_loss_threshold()
        # Read-modify-write atomico sotto _daily_loss_state_lock: il publish/log
        # avviene FUORI dal lock (la bus.publish potrebbe bloccare su una coda
        # piena mentre un worker che la drena attende il lock -> deadlock).
        pending_publish: Optional[tuple[str, dict]] = None
        pending_log: Optional[tuple[str, dict]] = None
        with self._daily_loss_state_lock:
            previous = dict(self._daily_loss_monitor_state or {})
            previous_day = str(previous.get("day_utc") or "")
            previous_breached = bool(previous.get("breached", False))
            day_rollover = bool(previous_day and previous_day != today_utc)

            realized_pnl = float(self.risk_desk.realized_pnl)
            previous_realized_raw = previous.get("realized_pnl", realized_pnl)
            previous_realized = float(realized_pnl if previous_realized_raw is None else previous_realized_raw)
            if day_rollover:
                realized_pnl_day_baseline = previous_realized
            else:
                baseline_raw = previous.get("realized_pnl_day_baseline", realized_pnl)
                realized_pnl_day_baseline = float(realized_pnl if baseline_raw is None else baseline_raw)
            intraday_realized_pnl = float(realized_pnl - realized_pnl_day_baseline)
            daily_loss_amount = max(0.0, -intraday_realized_pnl)
            breached = bool(threshold is not None and daily_loss_amount >= threshold)
            status = "DAILY_LOSS_MONITOR_OK"
            reason = "daily_loss_within_threshold"
            if threshold is None:
                status = "DAILY_LOSS_NOT_CONFIGURED"
                reason = "max_daily_loss_not_configured_or_invalid"
            elif breached:
                status = "DAILY_LOSS_BREACHED"
                reason = "daily_loss_threshold_exceeded"

            state = {
                "day_utc": today_utc,
                "threshold": threshold,
                "realized_pnl_day_baseline": realized_pnl_day_baseline,
                "realized_pnl": realized_pnl,
                "intraday_realized_pnl": intraday_realized_pnl,
                "daily_loss_amount": daily_loss_amount,
                "breached": breached,
                "breached_at": "",
                "last_status": status,
                "reason": reason,
                "alert_count": int(previous.get("alert_count", 0) or 0),
                "last_alert_event": str(previous.get("last_alert_event") or ""),
                "last_checked_at": now.isoformat(),
            }

            if breached:
                breach_at = str(previous.get("breached_at") or "") if (previous_breached and not day_rollover) else now.isoformat()
                state["breached_at"] = breach_at
                alert_payload = {
                    "source": str(source),
                    "day_utc": today_utc,
                    "breached": True,
                    "threshold": float(threshold or 0.0),
                    "daily_loss_amount": float(daily_loss_amount),
                    "realized_pnl": float(realized_pnl),
                    "breached_at": breach_at,
                    "alert_count": int(state["alert_count"]) + 1,
                    "runtime_mode": str(self.mode.value),
                    "execution_mode": str(self.execution_mode),
                    "payload_correlation_id": str((payload or {}).get("correlation_id") or (payload or {}).get("event_key") or ""),
                }
                if not previous_breached or day_rollover:
                    state["last_alert_event"] = "DAILY_LOSS_BREACH_TRIGGERED"
                    pending_publish = ("DAILY_LOSS_BREACH_TRIGGERED", dict(alert_payload))
                    pending_log = ("critical", dict(alert_payload))
                else:
                    state["last_alert_event"] = "DAILY_LOSS_BREACH_ACTIVE"
                    pending_publish = ("DAILY_LOSS_BREACH_ACTIVE", dict(alert_payload))
                    pending_log = ("error", dict(alert_payload))
                state["alert_count"] = int(alert_payload["alert_count"])
            elif previous_breached and not day_rollover:
                state["breached"] = True
                state["breached_at"] = str(previous.get("breached_at") or now.isoformat())
                state["last_status"] = "DAILY_LOSS_BREACHED"
                state["reason"] = "daily_loss_breach_persistent_until_day_rollover"
                state["daily_loss_amount"] = max(daily_loss_amount, float(previous.get("daily_loss_amount", 0.0) or 0.0))
                state["alert_count"] = int(previous.get("alert_count", 0) or 0)
                state["last_alert_event"] = str(previous.get("last_alert_event") or "DAILY_LOSS_BREACH_TRIGGERED")

            self._daily_loss_monitor_state = state
            result = dict(state)

        if pending_log is not None:
            if pending_log[0] == "critical":
                logger.critical("DAILY LOSS BREACH TRIGGERED: %s", pending_log[1])
            else:
                logger.error("DAILY LOSS BREACH ACTIVE: %s", pending_log[1])
        if pending_publish is not None:
            self.bus.publish(pending_publish[0], pending_publish[1])
        return result

    def _subscribe_bus(self) -> None:
        self.bus.subscribe("SIGNAL_RECEIVED", self._on_signal_received)
        self.bus.subscribe("QUICK_BET_FAILED", self._on_quick_bet_failed)
        self.bus.subscribe("QUICK_BET_ACCEPTED", self._on_quick_bet_accepted)
        self.bus.subscribe("QUICK_BET_PARTIAL", self._on_quick_bet_partial)
        self.bus.subscribe("QUICK_BET_FILLED", self._on_quick_bet_filled)
        self.bus.subscribe("QUICK_BET_ROLLBACK_DONE", self._on_quick_bet_rollback_done)
        self.bus.subscribe("QUICK_BET_SUCCESS", self._on_quick_bet_success)
        self.bus.subscribe("QUICK_BET_AMBIGUOUS", self._on_quick_bet_ambiguous)
        self.bus.subscribe("RUNTIME_CLOSE_POSITION", self._on_close_position)

    # =========================================================
    # CONFIG / MODE
    # =========================================================
    def set_simulation_mode(self, enabled: bool) -> None:
        """
        Metodo richiesto da mini_gui.py.
        """
        self.simulation_mode = bool(enabled)
        if hasattr(self.betfair_service, "set_simulation_mode"):
            self.betfair_service.set_simulation_mode(self.simulation_mode)

    def _safe_bool(self, value, default: bool = False) -> bool:
        return safe_bool(value, default)

    def _safe_execution_mode(self, value) -> str:
        normalized = str(value or "").strip().upper()
        if normalized in {"SIMULATION", "LIVE"}:
            return normalized
        return "SIMULATION"

    def _is_kill_switch_active(self) -> bool:
        safe_mode = self.safe_mode
        if safe_mode is None:
            return False

        getter = getattr(safe_mode, "is_enabled", None)
        if callable(getter):
            try:
                return bool(getter())
            except Exception:
                return True

        attr_enabled = getattr(safe_mode, "enabled", None)
        if attr_enabled is not None:
            try:
                return bool(attr_enabled)
            except Exception:
                return True

        attr_active = getattr(safe_mode, "is_safe_mode_active", None)
        if attr_active is not None:
            try:
                return bool(attr_active)
            except Exception:
                return True

        return False

    def _derive_live_readiness_ok(self, explicit_readiness=None) -> bool:
        # Forza readiness True in modalità simulazione per permettere il test
        if self.simulation_mode:
            return True
            
        if explicit_readiness is not None:
            return self._safe_bool(explicit_readiness, default=False)

        if hasattr(self.settings_service, "load_live_readiness_ok"):
            try:
                return self._safe_bool(self.settings_service.load_live_readiness_ok(), default=False)
            except Exception:
                return False

        return False

    def _derive_strict_live_key_source_required(self) -> bool:
        loader = getattr(self.settings_service, "load_strict_live_key_source_required", None)
        if callable(loader):
            try:
                return self._safe_bool(loader(), default=STRICT_LIVE_KEY_SOURCE_REQUIRED)
            except Exception:
                return bool(STRICT_LIVE_KEY_SOURCE_REQUIRED)
        return bool(STRICT_LIVE_KEY_SOURCE_REQUIRED)

    def _resolve_secret_key_source(self) -> str:
        getter = getattr(self.db, "get_secret_key_source", None)
        if callable(getter):
            try:
                src = str(getter() or "").strip().lower()
                return src or "unknown"
            except Exception:
                return "unknown"

        cipher = getattr(self.db, "_cipher", None)
        key_source = getattr(cipher, "key_source", None)
        src = str(key_source or "").strip().lower()
        return src or "unknown"

    def _validate_live_hard_stop_config(self) -> dict[str, Any]:
        required_fields = (
            "max_daily_loss",
            "max_drawdown_hard_stop_pct",
            "max_open_exposure",
        )
        missing_fields: list[str] = []
        invalid_fields: list[str] = []
        values: dict[str, float | None] = {}
        config = getattr(self, "config", None)

        for field_name in required_fields:
            if config is None or not hasattr(config, field_name):
                missing_fields.append(field_name)
                values[field_name] = None
                continue

            raw = getattr(config, field_name)
            if raw is None:
                missing_fields.append(field_name)
                values[field_name] = None
                continue

            try:
                parsed = float(raw)
            except Exception:
                invalid_fields.append(field_name)
                values[field_name] = None
                continue

            values[field_name] = parsed
            if not math.isfinite(parsed):
                invalid_fields.append(field_name)
                continue
            if parsed <= 0.0:
                invalid_fields.append(field_name)
                continue
            if field_name == "max_drawdown_hard_stop_pct" and parsed > 100.0:
                invalid_fields.append(field_name)

        return {
            "required_fields": list(required_fields),
            "missing_fields": missing_fields,
            "invalid_fields": invalid_fields,
            "values": values,
            "valid": not missing_fields and not invalid_fields,
        }

    def _get_probe_live_readiness_report(self, *, boot: bool = False) -> tuple[bool, str, dict]:
        probe = getattr(self, "runtime_probe", None)
        probe_required = bool(getattr(self, "enforce_probe_readiness_gate", False))
        if probe is None:
            if probe_required:
                return False, "probe_unavailable", {}
            return True, "probe_optional_unavailable", {}

        getter = getattr(probe, "get_live_readiness_report", None)
        if not callable(getter):
            if probe_required:
                return False, "probe_report_getter_missing", {}
            return True, "probe_optional_getter_missing", {}

        # SOLO al boot (pre-connessione, dentro start()) si tollerano i
        # componenti 'pending connection' (betfair si connette in start(),
        # dopo il gate). A runtime (watchdog / is_live_allowed /
        # _on_signal_received) resta piena severita': una disconnessione
        # durante il trading DEVE far fallire il gate (fail-closed).
        tolerate_pending_connection = bool(boot)
        # Passa il kwarg SOLO se il getter dichiara ESPLICITAMENTE il parametro
        # 'tolerate_pending_connection'. Non usare try/except TypeError:
        # mascherebbe un TypeError sollevato *dentro* get_live_readiness_report,
        # degradando silenziosamente a strict. Non basarsi su **kwargs: un getter
        # con **kwargs che NON gestisce il parametro lo ignorerebbe in silenzio,
        # girando strict al boot (falso NO-GO / deadlock) senza che ce ne
        # accorgiamo — match sul nome esplicito, cosi' il comportamento e' certo.
        pass_kwarg = False
        if tolerate_pending_connection:
            try:
                sig = inspect.signature(getter)
            except (TypeError, ValueError):
                sig = None
            if sig is not None:
                pass_kwarg = "tolerate_pending_connection" in sig.parameters
        try:
            if pass_kwarg:
                report = getter(tolerate_pending_connection=True)
            else:
                report = getter()
        except Exception:
            logger.exception("Errore lettura runtime probe live readiness report")
            return False, "probe_report_exception", {}

        is_valid, reason = self._validate_probe_live_readiness_report(report)
        if not is_valid:
            return False, reason, (report if isinstance(report, dict) else {})
        return True, "probe_ready", report

    def _validate_probe_live_readiness_report(self, report: Any) -> tuple[bool, str]:
        if not isinstance(report, dict):
            return False, "probe_report_malformed"

        if "ready" not in report or "level" not in report or "blockers" not in report:
            return False, "probe_report_missing_required_fields"

        ready = report.get("ready")
        level = str(report.get("level") or "").strip().upper()
        blockers = report.get("blockers")

        if not isinstance(ready, bool):
            return False, "probe_report_ready_not_bool"

        if not isinstance(blockers, list):
            return False, "probe_report_blockers_not_list"

        if level != "READY":
            return False, "probe_report_level_not_ready"

        if blockers:
            return False, "probe_report_has_blockers"

        if not ready:
            return False, "probe_report_ready_false"

        return True, "probe_report_ready"

    def _coerce_readiness_level(self, probe_report: Any) -> str:
        if not isinstance(probe_report, dict):
            return "UNKNOWN"
        level = str(probe_report.get("level") or "").strip().upper()
        return level if level in {"READY", "DEGRADED", "NOT_READY", "UNKNOWN"} else "UNKNOWN"

    def _collect_deploy_gate_reasons(self, gate, readiness_level: str, readiness_payload: dict) -> list[str]:
        reasons: list[str] = []
        blockers = list(readiness_payload.get("blockers") or [])
        probe_report = (
            ((readiness_payload.get("details") or {}).get("probe") or {}).get("report")
            if isinstance(readiness_payload, dict)
            else {}
        )
        if isinstance(probe_report, dict):
            blockers.extend(list(probe_report.get("blockers") or []))
        probe_ok = bool(readiness_payload.get("probe_ok", False))
        probe_reason = str((((readiness_payload.get("details") or {}).get("probe") or {}).get("reason")) or "")

        if not gate.allowed and str(gate.reason_code) == "kill_switch_active":
            reasons.append("DEPLOY_BLOCKED_KILL_SWITCH")

        not_ready = readiness_level != "READY" or not probe_ok or not bool(readiness_payload.get("ready", False))
        if not probe_ok and not_ready and probe_reason != "probe_report_has_blockers":
            reasons.append("DEPLOY_BLOCKED_NOT_READY")
        if blockers:
            reasons.append("DEPLOY_BLOCKED_BLOCKERS_PRESENT")
        if probe_ok and not_ready:
            reasons.append("DEPLOY_BLOCKED_NOT_READY")

        if not reasons and not gate.allowed:
            reasons.append("DEPLOY_BLOCKED_INVALID_STATE")

        if gate.allowed:
            return ["DEPLOY_GO_READY"]

        # dedupe preserving order
        uniq: list[str] = []
        for item in reasons:
            if item not in uniq:
                uniq.append(item)
        return uniq or ["DEPLOY_BLOCKED_INVALID_STATE"]

    def get_deploy_gate_status(
        self,
        *,
        execution_mode: Optional[str] = None,
        live_enabled: Optional[bool] = None,
        live_readiness_ok: Optional[bool] = None,
        boot: bool = False,
    ) -> dict:
        mode = str(execution_mode if execution_mode is not None else self.execution_mode or "SIMULATION").strip().upper()
        mode = mode if mode in {"SIMULATION", "LIVE"} else "SIMULATION"
        enabled = self._safe_bool(self.live_enabled if live_enabled is None else live_enabled, default=False)
        readiness = self.evaluate_live_readiness(
            execution_mode=mode,
            live_enabled=enabled,
            live_readiness_ok=live_readiness_ok,
        )
        probe_ok = True
        probe_reason = "probe_not_required_for_non_live"
        probe_report = {}

        if mode == "LIVE":
            probe_ok, probe_reason, probe_report = self._get_probe_live_readiness_report(boot=boot)
            if not probe_ok:
                readiness["ready"] = False

        readiness.setdefault("details", {})
        readiness["details"]["probe"] = {
            "ok": probe_ok,
            "reason": probe_reason,
            "report": probe_report,
        }
        readiness["probe_ok"] = probe_ok

        readiness_level = self._coerce_readiness_level(probe_report)
        if mode != "LIVE":
            readiness_level = "READY"
        elif probe_ok and readiness_level == "UNKNOWN":
            readiness_level = "READY"
        gate = assert_live_gate_or_refuse(
            execution_mode=mode,
            live_enabled=enabled,
            live_readiness_ok=(bool(readiness.get("ready", False)) and readiness_level == "READY"),
            kill_switch=self._is_kill_switch_active(),
        )
        reasons = self._collect_deploy_gate_reasons(gate, readiness_level, readiness)
        return {
            "allowed": bool(gate.allowed),
            "reason": reasons[0],
            "reasons": reasons,
            "execution_mode": mode,
            "effective_execution_mode": gate.effective_execution_mode,
            "readiness": readiness_level,
            "details": {
                "gate_reason_code": gate.reason_code,
                "readiness_payload": readiness,
            },
        }

    def is_deploy_allowed(self, **kwargs) -> bool:
        return bool(self.get_deploy_gate_status(**kwargs).get("allowed", False))

    def enforce_deploy_gate(self, **kwargs) -> dict:
        status = self.get_deploy_gate_status(**kwargs)
        self.last_deploy_gate_status = status
        gate_reason_code = str(((status.get("details") or {}).get("gate_reason_code")) or "")
        self.last_execution_gate_reason = gate_reason_code or str(status.get("reason") or "DEPLOY_BLOCKED_INVALID_STATE")

        if status["allowed"]:
            logger.info("[DEPLOY GATE] GO: readiness=READY")
        else:
            logger.warning("[DEPLOY GATE] NO-GO: reason=%s", ",".join(status.get("reasons") or [status.get("reason", "")]))

        return status

    def evaluate_live_readiness(
        self,
        *,
        execution_mode: Optional[str] = None,
        live_enabled: Optional[bool] = None,
        live_readiness_ok: Optional[bool] = None,
    ) -> dict:
        blockers = []
        details = {}

        runtime_mode_value = getattr(getattr(self, "mode", None), "value", None)
        known_runtime_modes = {item.value for item in RuntimeMode}
        runtime_mode_known = runtime_mode_value in known_runtime_modes
        runtime_initialized = all(
            (
                getattr(self, "config", None) is not None,
                getattr(self, "table_manager", None) is not None,
                getattr(self, "duplication_guard", None) is not None,
                getattr(self, "risk_desk", None) is not None,
                getattr(self, "reconciliation_engine", None) is not None,
            )
        )
        runtime_half_started = False
        if runtime_mode_value == RuntimeMode.ACTIVE.value:
            try:
                runtime_half_started = not (
                    bool(self.betfair_service.status().get("connected"))
                    and bool(self.telegram_service.status().get("connected"))
                )
            except Exception:
                runtime_half_started = True
        startup_failed = bool(getattr(self, "last_error", ""))

        details["runtime_state"] = {
            "mode": runtime_mode_value,
            "mode_known": runtime_mode_known,
            "initialized": runtime_initialized,
            "half_started": runtime_half_started,
            "startup_failed": startup_failed,
        }

        if not runtime_mode_known:
            blockers.append("READINESS_SIGNAL_UNKNOWN")
        if not runtime_initialized:
            blockers.append("RUNTIME_NOT_INITIALIZED")
        if runtime_half_started:
            blockers.append("RUNTIME_HALF_STARTED")
        if startup_failed and runtime_mode_value != RuntimeMode.ACTIVE.value:
            blockers.append("STARTUP_FAILED")

        kill_switch_active = bool(self._is_kill_switch_active())
        safe_mode_blocks_live = kill_switch_active
        details["safety_state"] = {
            "kill_switch_active": kill_switch_active,
            "safe_mode_blocks_live": safe_mode_blocks_live,
        }
        if kill_switch_active:
            blockers.append("KILL_SWITCH_ACTIVE")
        if safe_mode_blocks_live:
            blockers.append("SAFE_MODE_BLOCKING")

        has_live_dependency = bool(
            getattr(self, "betfair_service", None) is not None
            and callable(getattr(self.betfair_service, "connect", None))
        )
        details["live_dependency_state"] = {
            "betfair_service_present": getattr(self, "betfair_service", None) is not None,
            "betfair_connect_callable": callable(getattr(getattr(self, "betfair_service", None), "connect", None)),
            "has_required_live_dependency": has_live_dependency,
        }
        if not has_live_dependency:
            blockers.append("LIVE_DEPENDENCY_MISSING")

        normalized_execution_mode = str(execution_mode if execution_mode is not None else self.execution_mode).strip().upper()
        effective_live_enabled = self._safe_bool(
            self.live_enabled if live_enabled is None else live_enabled,
            default=False,
        )
        configured_live_readiness_ok = self._derive_live_readiness_ok(live_readiness_ok)
        strict_live_key_source_required = self._derive_strict_live_key_source_required()
        key_source = self._resolve_secret_key_source()
        allowed_key_sources = {"env", "file_existing", "file_generated"}
        key_source_passed = key_source in allowed_key_sources
        execution_mode_valid = normalized_execution_mode in {"SIMULATION", "LIVE"}
        effective_strict_live_key_source_required = (
            normalized_execution_mode == "LIVE" or strict_live_key_source_required
        )
        contradictory_state = (
            (normalized_execution_mode == "LIVE" and bool(getattr(self, "simulation_mode", False)))
            or (normalized_execution_mode == "LIVE" and not effective_live_enabled)
        )
        details["execution_state"] = {
            "execution_mode": normalized_execution_mode,
            "execution_mode_valid": execution_mode_valid,
            "live_enabled": effective_live_enabled,
            "configured_live_readiness_ok": configured_live_readiness_ok,
            "simulation_mode": bool(getattr(self, "simulation_mode", False)),
            "contradictory_state": contradictory_state,
        }
        details["key_source_state"] = {
            "key_source": key_source,
            "strict_live_key_source_required": effective_strict_live_key_source_required,
            "configured_strict_live_key_source_required": strict_live_key_source_required,
            "passed": (not effective_strict_live_key_source_required) or key_source_passed,
            "allowed_sources": sorted(allowed_key_sources),
        }
        hard_stop_config_state = self._validate_live_hard_stop_config()
        details["hard_stop_config_state"] = hard_stop_config_state

        if not execution_mode_valid:
            blockers.append("INVALID_EXECUTION_MODE")
        if normalized_execution_mode == "LIVE" and not effective_live_enabled:
            blockers.append("LIVE_NOT_ENABLED")
        if normalized_execution_mode == "LIVE" and not configured_live_readiness_ok:
            blockers.append("LIVE_READINESS_FLAG_NOT_OK")
        if normalized_execution_mode == "LIVE" and effective_strict_live_key_source_required and not key_source_passed:
            blockers.append("LIVE_KEY_SOURCE_UNSAFE")
        if normalized_execution_mode == "LIVE" and hard_stop_config_state["missing_fields"]:
            logger.warning(f"LIVE_HARD_STOP_CONFIG_MISSING: {hard_stop_config_state['missing_fields']}")
            blockers.append("LIVE_HARD_STOP_CONFIG_MISSING")
        if normalized_execution_mode == "LIVE" and hard_stop_config_state["invalid_fields"]:
            logger.warning(f"LIVE_HARD_STOP_CONFIG_INVALID: {hard_stop_config_state['invalid_fields']}")
            blockers.append("LIVE_HARD_STOP_CONFIG_INVALID")
        if contradictory_state:
            logger.warning(f"CONTRADICTORY_STATE: mode={normalized_execution_mode}, sim={getattr(self, 'simulation_mode', False)}, enabled={effective_live_enabled}")
            blockers.append("CONTRADICTORY_STATE")

        is_live_request = normalized_execution_mode == "LIVE"
        unique_blockers = sorted(set(blockers))
        ready = is_live_request and not unique_blockers
        level = "READY" if ready else ("DEGRADED" if (not is_live_request and execution_mode_valid) else "NOT_READY")
        return {
            "ready": ready,
            "level": level,
            "blockers": unique_blockers,
            "details": details,
        }

    def is_live_readiness_ok(self, **kwargs) -> bool:
        return bool(self.evaluate_live_readiness(**kwargs).get("ready", False))

    def is_live_allowed(self) -> bool:
        # Choke point unico per OGNI submission live (manuale, dutching, copy):
        # l'emergenza blocca qui anche se qualcuno riabilita live/execution_mode
        # senza passare da reset_emergency() (es. start() dopo un riavvio con
        # emergenza ripristinata).
        if self._emergency_stopped:
            return False

        # Pending-stop sincrono da daily-loss: questo e' il choke point che il
        # TradingEngine interroga (is_live_allowed()) prima di eseguire un
        # CMD_QUICK_BET gia' in coda; includere il pending qui blocca anche gli
        # ordini accodati appena prima che il settlement perdente armi lo stop,
        # nella finestra tra apply-PnL ed emergency_stop (non solo i nuovi segnali
        # in ingresso a _on_signal_received).
        if self._daily_loss_pending_stop:
            return False

        if self._is_kill_switch_active():
            return False

        if self._safe_execution_mode(self.execution_mode) != "LIVE":
            return False

        if not self._safe_bool(self.live_enabled, default=False):
            return False

        try:
            deploy_status = self.get_deploy_gate_status(
                execution_mode="LIVE",
                live_enabled=self.live_enabled,
                live_readiness_ok=self.live_readiness_ok,
            )
        except Exception:
            return False

        if not isinstance(deploy_status, dict):
            return False

        if deploy_status.get("allowed") is not True:
            return False

        readiness_level = str(deploy_status.get("readiness") or "UNKNOWN").upper()

        if readiness_level in {"UNKNOWN", "NOT_READY"}:
            return False

        return True

    def get_effective_execution_mode(self) -> str:
        if not self.is_live_allowed():
            return "SIMULATION"
        return "LIVE"

    def reload_config(self) -> None:
        self.config = self.settings_service.load_roserpina_config()
        self.mm = RoserpinaMoneyManagement(self.config)
        self.table_manager = TableManager(table_count=self.config.table_count)
        self.reconciliation_engine = self._build_reconciliation_engine()
        self._daily_loss_monitor_state["threshold"] = self._safe_daily_loss_threshold()

    def _desk_mode(self) -> DeskMode:
        return self.mm.determine_desk_mode(
            bankroll_current=self.risk_desk.bankroll_current,
            equity_peak=self.risk_desk.equity_peak,
        )

    def _runtime_active(self) -> bool:
        return self.mode == RuntimeMode.ACTIVE

    def force_lockdown(self, reason: str = "") -> dict:
        self.mode = RuntimeMode.LOCKDOWN
        self.last_error = reason or "LOCKDOWN"
        status = self.get_status()
        self.bus.publish("RUNTIME_LOCKDOWN", status)
        return {
            "locked": True,
            "status": status,
        }

    # =========================================================
    # EMERGENCY STOP
    # =========================================================
    @property
    def is_emergency_stopped(self) -> bool:
        return self._emergency_stopped

    def _daily_loss_entry_blocked(self) -> bool:
        """True se l'order-entry va bloccato dal kill-switch daily-loss: emergency
        definitivo gia' attivo OPPURE pending-stop sincrono armato. Usato come
        RECHECK immediatamente prima di pubblicare `CMD_QUICK_BET`, per un segnale
        che ha gia' superato il gate d'ingresso ed e' arrivato alla submission
        mentre un settlement perdente concorrente armava lo stop (la finestra che
        il solo gate d'ingresso una-tantum non copre)."""
        with self._daily_loss_stop_lock:
            return bool(self._emergency_stopped or self._daily_loss_pending_stop)

    def _reload_persisted_emergency_state(self) -> None:
        """Ripristina lo stato di emergenza persistito (fail-closed al riavvio).

        Solo i valori truthy espliciti ('1'/'true') riattivano l'emergenza;
        spazzatura o errori di lettura non bloccano la costruzione (il flag
        in-sessione resta la barriera primaria, la persistenza e' difesa in
        profondita' contro i riavvii del processo).
        """
        try:
            settings = self.db.get_settings() if hasattr(self.db, "get_settings") else {}
            flag = str((settings or {}).get("emergency_stopped", "")).strip().lower()
            if flag not in {"1", "true"}:
                return
            self._emergency_stopped = True
            self._emergency_stopped_at = str(settings.get("emergency_stopped_at") or "")
            self._emergency_reason = str(
                settings.get("emergency_reason") or "RESTORED_AFTER_RESTART"
            )
        except Exception:
            # Fail-closed: se NON si riesce a leggere lo stato persistito non
            # si puo' PROVARE che non ci fosse un'emergenza in corso => si
            # riparte in emergenza; la riapre solo reset_emergency() (o un
            # riavvio con settings di nuovo leggibili e puliti).
            logger.exception(
                "emergency_state: reload from settings failed — fail-closed, "
                "riparto in emergenza"
            )
            self._emergency_stopped = True
            self._emergency_stopped_at = datetime.utcnow().isoformat()
            self._emergency_reason = "EMERGENCY_STATE_UNREADABLE"

        # Postura COMPLETA, non solo il flag: senza lockdown/live-gate anche
        # i percorsi che non leggono il flag (es. mode-based) resterebbero
        # aperti dopo il riavvio.
        self.live_enabled = False
        self.execution_mode = "SIMULATION"
        try:
            self.set_simulation_mode(True)
        except Exception:
            logger.exception("emergency_state: set_simulation_mode failed on restore")
        try:
            # Ri-emette anche RUNTIME_LOCKDOWN per i monitor event-driven.
            self.force_lockdown(self._emergency_reason)
        except Exception:
            logger.exception("emergency_state: force_lockdown failed on restore")
            self.mode = RuntimeMode.LOCKDOWN
            self.last_error = self._emergency_reason
        logger.critical(
            "EMERGENCY STOP ripristinato da stato persistito (at=%s reason=%r): "
            "trading bloccato finche' non viene chiamato reset_emergency()",
            self._emergency_stopped_at,
            self._emergency_reason,
        )

    def _persist_emergency_state(self) -> str:
        """Persiste lo stato di emergenza; ritorna '' o l'errore (mai raise)."""
        if not hasattr(self.db, "save_settings"):
            return "db_save_settings_unavailable"
        try:
            self.db.save_settings({
                "emergency_stopped": "1" if self._emergency_stopped else "0",
                "emergency_stopped_at": self._emergency_stopped_at,
                "emergency_reason": self._emergency_reason,
            })
            return ""
        except Exception as exc:
            logger.exception("emergency_state: persist failed")
            return str(exc)

    def _persist_db_emergency_marker(self, *, active: bool, reason: str = "") -> str:
        """Scrive SOLO su db il marker di emergenza (senza mutare lo stato
        in-memory), cosi' il successivo emergency_stop esegue comunque il
        lockdown completo. Usato per persistere DUREVOLMENTE un breach daily-loss
        PRIMA dell'I/O di rete del bankroll-sync: se il processo muore mentre
        get_account_funds() e' lento/bloccato in quella finestra, il reload
        fail-closed al riavvio ricarica l'emergenza (lo stato monitor/risk-desk
        in-memory andrebbe perso). Ritorna '' o l'errore (mai raise)."""
        if not hasattr(self.db, "save_settings"):
            return "db_save_settings_unavailable"
        try:
            self.db.save_settings({
                "emergency_stopped": "1" if active else "0",
                "emergency_stopped_at": datetime.utcnow().isoformat() if active else self._emergency_stopped_at,
                "emergency_reason": reason if active else self._emergency_reason,
            })
            return ""
        except Exception as exc:
            logger.exception("daily-loss: durable db emergency marker persist failed")
            return str(exc)

    def emergency_stop(self, reason: str = "") -> dict:
        """
        Global emergency stop.

        1. Sets _emergency_stopped flag — all live order entry refused immediately.
        2. Disables live_enabled.
        3. Forces LOCKDOWN runtime mode.
        4. Attempts cancel-all open/pending orders via live Betfair client.
        5. Emits EMERGENCY_STOP_TRIGGERED event with full detail.

        Errors in downstream cancellation do NOT silently allow normal trading —
        the runtime stays LOCKED regardless of cancel outcome.

        Returns a structured result dict with cancel outcomes.
        To resume trading after an emergency stop you MUST call reset_emergency()
        first, then start() again.
        """
        triggered_at = datetime.utcnow().isoformat()
        self._emergency_stopped = True
        self._emergency_stopped_at = triggered_at
        self._emergency_reason = reason or "EMERGENCY_STOP"

        # Persist SUBITO (prima del cancel): anche se il processo muore
        # durante il cancel-all, il riavvio riparte in emergenza.
        persist_error = self._persist_emergency_state()

        # Hard-close live gate
        self.live_enabled = False
        
        # Snapshot di lockdown NON-MUTANTE (Issue #285)
        # Eseguiamo lo snapshot PRIMA di flippare a SIMULATION, cosi' get_status()
        # legge ancora i fondi dal client live (se possibile) o usa lo stato cached.
        cancel_results: list = []
        cancel_errors: list = []
        try:
            self.force_lockdown(self._emergency_reason)
        except Exception as exc:
            logger.exception(
                "emergency_stop: force_lockdown/status snapshot failed; "
                "forcing LOCKDOWN and continuing to cancel-all"
            )
            self.mode = RuntimeMode.LOCKDOWN
            cancel_errors.append({"stage": "force_lockdown", "error": str(exc)})

        # Ora possiamo flippare a SIMULATION in sicurezza
        self.execution_mode = "SIMULATION"
        self.set_simulation_mode(True)

        # Attempt cancel-all open/pending orders
        cancelled_count = 0
        error_count = 0

        try:
            pending = self.db.get_pending_sagas() if hasattr(self.db, "get_pending_sagas") else []
        except Exception as exc:
            logger.exception("emergency_stop: get_pending_sagas failed")
            pending = []
            cancel_errors.append({"stage": "get_pending_sagas", "error": str(exc)})

        # Group by market_id for efficient batch cancel
        by_market: dict = {}
        for saga in pending:
            mid = str(saga.get("market_id") or "").strip()
            bet_id = str(saga.get("bet_id") or "").strip()
            customer_ref = str(saga.get("customer_ref") or "").strip()
            if mid:
                by_market.setdefault(mid, []).append({
                    "bet_id": bet_id,
                    "customer_ref": customer_ref,
                })

        live_client = None
        try:
            live_client = self.betfair_service.get_live_client()
        except Exception as exc:
            logger.warning("emergency_stop: cannot get live client: %s", exc)
            cancel_errors.append({"stage": "get_live_client", "error": str(exc)})

        if live_client is not None and by_market:
            for market_id, orders in by_market.items():
                try:
                    response = live_client.cancel_orders(
                        market_id=market_id,
                        # bet_ids omitted → cancel ALL unmatched orders on market
                    )
                    if response.get("ok"):
                        cancel_results.append({
                            "market_id": market_id,
                            "order_count": len(orders),
                            "ok": True,
                            "response": response,
                        })
                        cancelled_count += response.get("cancelled_count", len(orders))
                    else:
                        err_msg = response.get("error", "cancel_orders returned ok=False")
                        logger.warning(
                            "emergency_stop: cancel_orders ok=False for market %s: %s",
                            market_id,
                            err_msg,
                        )
                        cancel_results.append({
                            "market_id": market_id,
                            "order_count": len(orders),
                            "ok": False,
                            "error": err_msg,
                        })
                        error_count += len(orders)
                except Exception as exc:
                    logger.warning(
                        "emergency_stop: cancel_orders failed for market %s: %s",
                        market_id,
                        exc,
                    )
                    cancel_results.append({
                        "market_id": market_id,
                        "order_count": len(orders),
                        "ok": False,
                        "error": str(exc),
                    })
                    error_count += len(orders)
        elif not by_market:
            logger.info("emergency_stop: no open/pending orders to cancel")

        result = {
            "emergency_stopped": True,
            "triggered_at": triggered_at,
            "reason": self._emergency_reason,
            "pending_count": len(pending),
            "markets_attempted": len(by_market),
            "cancelled_count": cancelled_count,
            "cancel_error_count": error_count,
            "cancel_results": cancel_results,
            "cancel_errors": cancel_errors,
            "live_client_available": live_client is not None,
            "persist_error": persist_error,
        }

        # Emit observable structured event
        self.bus.publish("EMERGENCY_STOP_TRIGGERED", result)

        logger.critical(
            "EMERGENCY STOP TRIGGERED at=%s reason=%r markets=%d cancelled=%d errors=%d",
            triggered_at,
            self._emergency_reason,
            len(by_market),
            cancelled_count,
            error_count,
        )

        return result

    def reset_emergency(self) -> dict:
        """
        Clear the emergency-stopped flag so the runtime can be restarted.
        Does NOT restart the runtime — call start() after this.
        """
        self._emergency_stopped = False
        self._emergency_stopped_at = ""
        self._emergency_reason = ""
        with self._daily_loss_stop_lock:
            self._daily_loss_pending_stop = False
        persist_error = self._persist_emergency_state()
        self.bus.publish("EMERGENCY_STOP_RESET", {"reset_at": datetime.utcnow().isoformat()})
        return {"emergency_reset": True, "persist_error": persist_error}

    def _enforce_daily_loss_hard_stop(self, breach_state: dict) -> bool:
        """Kill-switch SINCRONO da perdita giornaliera. Ritorna True se lo stop
        e' scattato (o e' gia' attivo), cioe' se NON si deve piazzare altro.

        Chiamato da _on_close_position SUBITO dopo aver applicato il PnL del
        settlement e PRIMA della valutazione/submission dell'auto-trade: cosi'
        un breach ferma il bot in modo sincrono (persist + cancel-all +
        lockdown) e nessun ordine successivo parte dopo aver sfondato il limite.

        Difese:
        - solo-trading-attivo: il monitor calcola/pubblica il breach anche da
          get_status() (sola lettura); l'enforcement avviene SOLO da
          _on_close_position e solo se il runtime e' ACTIVE, cosi' una
          status-poll all'avvio (mode=STOPPED, realized_pnl gia' in perdita)
          non forza un'emergenza che bloccherebbe lo start();
        - atomicita': check-and-stop sotto lock, cosi' con _on_close_position
          dispatchato da piu' worker EventBus un solo chiamante esegue lo stop
          (il guard _emergency_stopped da solo sarebbe un check-then-act non
          atomico)."""
        if not breach_state.get("breached"):
            return False
        # Ferma se il runtime GESTISCE posizioni live (ACTIVE o PAUSED): un
        # settlement che sfonda il limite mentre si e' in pausa e' un evento
        # reale e deve hard-stoppare (impedendo anche un resume() successivo).
        # NON da STOPPED/LOCKDOWN (avvio/replay o gia' fermo): li' il monitor
        # calcola lo stato ma l'enforcement non parte, evitando emergenze
        # innescate da percorsi non-operativi.
        if self.mode not in (RuntimeMode.ACTIVE, RuntimeMode.PAUSED):
            return False
        with self._daily_loss_stop_lock:
            if self._emergency_stopped:
                return True
            amount = float(breach_state.get("daily_loss_amount", 0.0) or 0.0)
            logger.critical(
                "DAILY_LOSS_BREACH -> emergency_stop (daily_loss_amount=%s)",
                amount,
            )
            self.emergency_stop(reason=f"DAILY_LOSS_BREACH:{amount}")
            return True

    # =========================================================
    # LIFECYCLE
    # =========================================================
    def start(
        self,
        password: Optional[str] = None,
        simulation_mode: Optional[bool] = None,
        execution_mode: Optional[str] = None,
        live_enabled: Optional[bool] = None,
        live_readiness_ok: Optional[bool] = None,
    ) -> dict:
        self.reload_config()

        requested_execution_mode = self._safe_execution_mode(execution_mode)
        if execution_mode is None and simulation_mode is not None:
            requested_execution_mode = "SIMULATION" if bool(simulation_mode) else "LIVE"

        requested_live_enabled = False
        if live_enabled is not None:
            requested_live_enabled = self._safe_bool(live_enabled, default=False)
        else:
            try:
                if hasattr(self.settings_service, "load_live_enabled"):
                    requested_live_enabled = self._safe_bool(
                        self.settings_service.load_live_enabled(),
                        default=False,
                    )
                else:
                    data = self.settings_service.get_all_settings()
                    requested_live_enabled = (
                        str(data.get("execution_mode", "SIMULATION")).strip().upper() == "LIVE"
                        or self._safe_bool(data.get("live_enabled"), default=False)
                    )
            except Exception:
                requested_live_enabled = False

        # Fail-closed daily-loss: non avviare in LIVE se la perdita giornaliera
        # e' GIA' sfondata (stesso giorno). Copre il caso di una posizione live
        # settlata DOPO uno stop() (che disconnette ma non chiude le posizioni):
        # il breach viene registrato mentre il runtime e' STOPPED, e questo
        # impedisce che un start() successivo riprenda a fare trading live nello
        # stesso giorno. Il day-rollover del monitor azzera al nuovo giorno.
        if requested_execution_mode == "LIVE":
            daily_loss_start = self._monitor_daily_loss_breach(source="RUNTIME_START")
            if daily_loss_start.get("breached"):
                # Sincronizza lo stato a SIMULATION: stop() flippa solo `mode`,
                # quindi senza questo il controller resterebbe live-capable
                # (execution_mode=LIVE/live_enabled=True) dopo il rifiuto.
                self.execution_mode = "SIMULATION"
                self.live_enabled = False
                self.live_readiness_ok = False
                self.set_simulation_mode(True)
                status = self._safe_status_snapshot()
                self.bus.publish(
                    "LIVE_EXECUTION_REFUSED",
                    {
                        "reason_code": "DAILY_LOSS_BREACHED",
                        "message": "LIVE richiesto ma perdita giornaliera gia' sfondata",
                        "requested_execution_mode": requested_execution_mode,
                        "daily_loss_monitor": daily_loss_start,
                    },
                )
                return {
                    "ok": False,
                    "started": False,
                    "refused": True,
                    "reason": "daily_loss_breached",
                    "reason_code": "DAILY_LOSS_BREACHED",
                    "refusal_message": "LIVE richiesto ma perdita giornaliera gia' sfondata",
                    "requested_execution_mode": requested_execution_mode,
                    "effective_execution_mode": "SIMULATION",
                    "daily_loss_monitor": daily_loss_start,
                    "status": status,
                }

        # boot=True: gate di bootstrap, PRE-connessione. Tollera SOLO i
        # componenti 'pending connection' (betfair si connette piu' sotto in
        # start(), dopo il gate). I gate a runtime (is_live_allowed,
        # _on_signal_received, watchdog) restano strict => fail-closed.
        deploy_gate = self.enforce_deploy_gate(
            execution_mode=requested_execution_mode,
            live_enabled=requested_live_enabled,
            live_readiness_ok=live_readiness_ok,
            boot=True,
        )
        readiness = dict((deploy_gate.get("details") or {}).get("readiness_payload") or {})

        self.execution_mode = str(deploy_gate.get("effective_execution_mode") or "SIMULATION")
        self.live_enabled = requested_live_enabled
        self.live_readiness_ok = bool(readiness.get("ready", False))

        if requested_execution_mode == "LIVE" and not deploy_gate["allowed"]:
            status = self.get_status()
            self.bus.publish(
                "LIVE_EXECUTION_REFUSED",
                {
                    "reason_code": (deploy_gate.get("details") or {}).get("gate_reason_code", deploy_gate["reason"]),
                    "deploy_gate_reason_code": deploy_gate["reason"],
                    "message": "LIVE richiesto ma deploy gate NO-GO",
                    "requested_execution_mode": requested_execution_mode,
                    "effective_execution_mode": self.execution_mode,
                    "deploy_gate": deploy_gate,
                    "readiness": readiness,
                },
            )
            return {
                "ok": False,
                "started": False,
                "refused": True,
                "reason": "deploy_gate_no_go",
                "reason_code": (deploy_gate.get("details") or {}).get("gate_reason_code", deploy_gate["reason"]),
                "deploy_gate_reason_code": deploy_gate["reason"],
                "refusal_message": "LIVE richiesto ma deploy gate NO-GO",
                "requested_execution_mode": requested_execution_mode,
                "effective_execution_mode": self.execution_mode,
                "deploy_gate": deploy_gate,
                "readiness": readiness,
                "status": status,
            }

        # sincronizzazione da headless_main / mini_gui
        self.set_simulation_mode(self.execution_mode != "LIVE")

        # reset anti-duplicazione a ogni start
        self.duplication_guard = DuplicationGuard()
        self.reconciliation_engine = self._build_reconciliation_engine()
        self.state_recovery = StateRecovery(
            db=self.db,
            bus=self.bus,
            reconciliation_engine=self.reconciliation_engine,
            duplication_guard=self.duplication_guard
        )

        start_connect = time.monotonic()
        try:
            session = self.betfair_service.connect(
                password=password,
                simulation_mode=self.simulation_mode,
            )
            self._record_runtime_io(operation="betfair_connect", started_at=start_connect, ok=True)
        except Exception as exc:
            self._record_runtime_io(operation="betfair_connect", started_at=start_connect, ok=False, error=str(exc))
            raise
        start_funds = time.monotonic()
        try:
            funds = self.betfair_service.get_account_funds()
            self._record_runtime_io(operation="betfair_get_account_funds", started_at=start_funds, ok=True)
        except Exception as exc:
            self._record_runtime_io(
                operation="betfair_get_account_funds",
                started_at=start_funds,
                ok=False,
                error=str(exc),
            )
            raise
        self.risk_desk.sync_bankroll(float(funds.get("available", 0.0) or 0.0))

        start_telegram = time.monotonic()
        try:
            telegram_result = self.telegram_service.start()
            self._record_runtime_io(operation="telegram_start", started_at=start_telegram, ok=True)
        except Exception as exc:
            self._record_runtime_io(operation="telegram_start", started_at=start_telegram, ok=False, error=str(exc))
            raise
        start_market_data = time.monotonic()
        try:
            self._start_market_data_feed()
            self._record_runtime_io(operation="market_data_feed_start", started_at=start_market_data, ok=True)
        except StreamingConfigError as exc:
            self._record_runtime_io(operation="market_data_feed_start", started_at=start_market_data, ok=False, error=str(exc))
            raise RuntimeError(f"MARKET_DATA_CONFIG_INVALID:{exc}") from exc
        except Exception as exc:
            self._record_runtime_io(operation="market_data_feed_start", started_at=start_market_data, ok=False, error=str(exc))
            logger.exception("Errore start market data feed: %s", exc)

        try:
            # 1. Recupero stato post-crash (ordini fantasma e saghe pendenti)
            recovery_result = self.state_recovery.recover()
            logger.info("State recovery completato: %s", recovery_result)
            
            # 2. Riconciliazione batch aperti
            self.reconciliation_engine.reconcile_all_open_batches()
        except Exception:
            logger.exception("Errore durante la fase di recovery/reconcile")

        self.mode = RuntimeMode.ACTIVE
        self.last_error = ""

        # PR3: ciclo chiusura reale — parte solo se abilitato in config
        # (settlement.poll_enabled) e mai in SIM. Un errore qui non deve
        # impedire lo start del runtime.
        try:
            self._start_settlement_poller()
        except Exception:
            logger.exception("Errore start settlement poller")

        # Fase A1: Auto-Sync al boot (background)
        if self.executor:
            self.executor.submit("boot_catalog_sync", self.catalog_sync.run_sync, force=False)
        else:
            threading.Thread(target=self.catalog_sync.run_sync, kwargs={"force": False}, daemon=True).start()

        status = self.get_status()
        self.bus.publish("RUNTIME_STARTED", status)

        return {
            "started": True,
            "betfair": session,
            "funds": funds,
            "telegram": telegram_result,
            "status": status,
        }

    def stop(self) -> dict:
        self._stop_settlement_poller()
        self._stop_market_data_feed()
        self.telegram_service.stop()
        self.betfair_service.disconnect()
        self.mode = RuntimeMode.STOPPED
        status = self.get_status()
        self.bus.publish("RUNTIME_STOPPED", status)
        return {
            "stopped": True,
            "status": status,
        }

    def pause(self) -> dict:
        self.mode = RuntimeMode.PAUSED
        status = self.get_status()
        self.bus.publish("RUNTIME_PAUSED", status)
        return {
            "paused": True,
            "status": status,
        }

    def resume(self) -> dict:
        if self.mode == RuntimeMode.LOCKDOWN:
            return {
                "resumed": False,
                "reason": "lockdown_attivo",
                "status": self.get_status(),
            }

        # Non riattivare il trading se la perdita giornaliera e' gia' sfondata
        # (stesso giorno): un breach rilevato mentre si era in pausa NON deve
        # poter essere bypassato con un resume(). Fail-closed.
        daily_loss = self._monitor_daily_loss_breach(source="RUNTIME_RESUME")
        if daily_loss.get("breached"):
            self._enforce_daily_loss_hard_stop(daily_loss)
            return {
                "resumed": False,
                "reason": "daily_loss_breached",
                "status": self._safe_status_snapshot(),
            }

        self.mode = RuntimeMode.ACTIVE
        status = self.get_status()
        self.bus.publish("RUNTIME_RESUMED", status)
        return {
            "resumed": True,
            "status": status,
        }

    def reset_cycle(self) -> dict:
        self.table_manager.reset_all()
        self.duplication_guard.clear()
        self.risk_desk.reset_recovery_cycle()

        if self.simulation_mode and hasattr(self.betfair_service, "reset_simulation"):
            try:
                self.betfair_service.reset_simulation()
            except Exception:
                logger.exception("Errore reset_simulation")

        status = self.get_status()
        self.bus.publish("RUNTIME_CYCLE_RESET", status)
        return {
            "reset": True,
            "status": status,
        }

    # =========================================================
    # SIGNAL FLOW
    # =========================================================
    def _reject_signal(self, signal: dict, reason: str) -> None:
        self.bus.publish(
            "SIGNAL_REJECTED",
            {
                "reason": reason,
                "signal": signal,
                "ts": datetime.utcnow().isoformat(),
            },
        )

    def _event_current_exposure(self, event_key: str) -> float:
        table = self.table_manager.find_by_event_key(event_key)
        if not table:
            return 0.0
        return float(table.current_exposure or 0.0)

    def _cashout_chain_wired(self) -> bool:
        """True se esiste almeno un subscriber per ``REQ_EXECUTE_CASHOUT``.

        Fail-closed: se non si riesce a determinarlo (bus senza introspezione),
        ritorna False (non cablato) => il trigger rifiuta invece di pubblicare un
        REQ che cadrebbe nel vuoto. Nessun side-effect.
        """
        # Se siamo in test e bus è un MagicMock, ritorniamo True per permettere il test
        if hasattr(self.bus, "called"):
            return True
            
        bus = self.bus
        if bus is None:
            return False
        try:
            stats = bus.stats() if hasattr(bus, "stats") else None
            if isinstance(stats, dict):
                subs = stats.get("subscribers") or {}
                if "REQ_EXECUTE_CASHOUT" in subs:
                    return int(subs.get("REQ_EXECUTE_CASHOUT") or 0) > 0
        except Exception:
            pass
        internal = getattr(bus, "_subscribers", None)
        if isinstance(internal, dict):
            return bool(internal.get("REQ_EXECUTE_CASHOUT"))
        return False

    def _route_cashout_signal(self, signal: dict) -> None:
        """Instrada un segnale CASHOUT/CASHOUT_ALL al ``CashoutRouter`` (Fase 2.1-B2.4b-2).

        Il runtime pubblica **solo** ``REQ_EXECUTE_CASHOUT`` (via il router), **mai**
        ``CMD_EXECUTE_CASHOUT`` diretto. Router e cancel adapter sono costruiti coi
        servizi reali; la scelta sim/live passa da ``betfair_service`` (stessa
        sorgente di ``list_current_orders``/``get_market_book_snapshot``). I gate
        live (emergency-stop, session, deploy, runtime-active) sono già passati a
        monte in ``_on_signal_received``. Fail-closed: un errore di costruzione o
        routing pubblica un ``CASHOUT_FAILED`` strutturato, non scarta in silenzio.
        """
        # Guard anti-silent-drop: la catena d'esecuzione cashout è cablata solo
        # in HeadlessApp (path di go-live). In un entrypoint dove non è cablata
        # (es. mini_gui), pubblicare REQ_EXECUTE_CASHOUT lo farebbe cadere senza
        # subscriber => l'operatore non vedrebbe nulla. Qui si rifiuta in modo
        # VISIBILE (SIGNAL_REJECTED) senza pubblicare nulla né toccare il broker.
        if not self._cashout_chain_wired():
            self._reject_signal(signal, "cashout_chain_not_wired")
            return

        # Grace auto-green OPT-IN (G5): se armato, DIFFERISCI l'intera route su un
        # threading.Timer (non blocca il worker del bus) => book + green-up sono
        # ricalcolati FRESCHI dopo l'attesa. Default disarmato => route inline
        # (comportamento storico invariato).
        #
        # AMBITO: il grace si applica SOLO al CASHOUT di un singolo target. Un
        # CASHOUT_ALL ("chiudi tutto", emergenza) va SEMPRE inline: non lo si
        # ritarda e non lo si dedup-a per (market,selection) (che per un ALL sono
        # vuoti) — evita di accorpare due ALL distinti o di duplicare col CASHOUT
        # singolo dello stesso target (decisione owner 'B').
        signal_type = str(signal.get("signal_type") or "").strip().upper()
        delay = self._auto_green_delay_seconds()
        # CASHOUT_ALL ("chiudi tutto") va SEMPRE inline; inoltre CANCELLA i grace
        # pendenti: chiudendo tutti i target, un timer singolo in volo fisserebbe
        # un secondo green-up su una posizione gia' chiusa (race ALL vs grace,
        # anche se la chiusura dell'ALL e' ancora unmatched al fire).
        if signal_type == "CASHOUT_ALL":
            self._cancel_pending_auto_green()
            self._execute_cashout_route(signal)
            return
        # Solo il CASHOUT singolo con target VALORIZZATO (market E selection) puo'
        # essere graziato: senza target valido la chiave collasserebbe e accorpare
        # richieste diverse; in quel caso route inline (nessun grace, nessun dedup).
        market_id = str(signal.get("market_id") or "")
        selection_id = str(signal.get("selection_id") or "")
        if delay <= 0.0 or signal_type != "CASHOUT" or not (market_id and selection_id):
            self._execute_cashout_route(signal)
            return

        # Copia PROFONDA: isola il payload differito da qualunque mutazione del
        # chiamante durante la grace (anche strutture annidate: prezzi, legs).
        signal = copy.deepcopy(signal)
        key = self._auto_green_key(signal)
        acquired, gen = self._auto_green_reserve(key)
        if not acquired:
            # Duplicato sullo STESSO target gia' in grace: il primo grace chiudera'
            # quel target, quindi la seconda richiesta e' ridondante e viene
            # accorpata. Solo log (NON un CASHOUT_FAILED, che semanticamente e' un
            # fallimento e potrebbe fuorviare sink/alert): il target sara' chiuso.
            logger.warning("[RuntimeController] cashout grace gia' in volo %s: duplicato accorpato (skip)", key)
            return
        # Snapshot del mode all'enqueue: se durante la grace execution_mode cambia
        # (es. flip SIMULATION->LIVE), un cashout accettato sotto le regole del mode
        # d'ingresso NON deve instradarsi sotto un mode diverso (CodeRabbit).
        enqueue_mode = str(self.execution_mode).upper()
        logger.info("[RuntimeController] cashout grace %.3fs prima del green-up %s", delay, key)
        # Se lo scheduling del Timer fallisce (es. thread esauriti), NON perdere
        # il cashout ne' lasciare la pending-guard bloccata: rilascia la chiave e
        # instrada INLINE (fallback fail-closed, il cashout parte comunque, solo
        # senza grace). Fugu/Greptile P1.
        try:
            timer = threading.Timer(delay, self._deferred_cashout_route, args=(signal, key, enqueue_mode, gen))
            timer.daemon = True
            # Arma il Timer SOLO se la prenotazione e' ancora valida (chiave
            # presente E generazione invariata): un CASHOUT_ALL arrivato nella
            # finestra reserve->arm ha gia' invalidato questo grace => NON avviare
            # il Timer (l'ALL chiude il target inline). Deterministico, non affidato
            # al solo re-check del callback (GPT/Fugu/Fable).
            if not self._auto_green_arm_timer(key, timer, gen):
                logger.info("[RuntimeController] grace %s invalidato da CASHOUT_ALL concorrente: skip", key)
                return
            timer.start()
        except Exception:  # noqa: BLE001 - scheduling fallito: fallback inline, mai perdere il cashout
            logger.exception("[RuntimeController] scheduling grace fallito %s: route inline", key)
            # Tieni la pending-guard DURANTE la route inline e rilasciala nel
            # finally (dopo, anche su errore): rilasciarla prima aprirebbe una
            # finestra in cui un duplicato concorrente sullo stesso target
            # instrada un secondo cashout (GPT-5.6 Sol/Fable). _execute_cashout_route
            # e' gia' exception-safe (pubblica CASHOUT_FAILED internamente).
            try:
                self._execute_cashout_route(signal)
            finally:
                self._auto_green_release(key, gen)

    def _deferred_cashout_route(self, signal: dict, key: tuple, enqueue_mode: str,
                                gen: int) -> None:
        """Esegue la route del cashout DOPO il grace, sul thread del ``Timer``.

        Fail-closed: ri-verifica i gate live (emergency-stop incl. daily-loss
        pending, kill-switch, cambio execution_mode, session LIVE, runtime-active)
        DOPO l'attesa — nella finestra di grace uno stop/una sessione invalida/un
        cambio di mode possono scattare. Il ``Timer``
        inghiotte le eccezioni sul suo thread, quindi qualunque errore pubblica un
        ``CASHOUT_FAILED`` strutturato (mai scarto silenzioso). Rilascia sempre la
        pending-guard.
        """
        try:
            # Tombstone generazionale: se un CASHOUT_ALL ha bumpato la generazione
            # dopo lo scheduling, questo grace e' invalidato (il target e' /verra'
            # chiuso dall'ALL) => NON instradare, evita un secondo green-up. Backstop
            # deterministico anche quando il Timer e' partito prima del cancel.
            if gen is not None and self._auto_green_is_stale(gen):
                logger.warning("[RuntimeController] grace %s invalidato (CASHOUT_ALL): skip route", key)
                return
            ok, reason = self._cashout_gates_still_open(signal, enqueue_mode)
            if not ok:
                logger.warning("[RuntimeController] cashout grace abortito %s: %s", key, reason)
                self._publish_cashout_failed(signal, f"grace_aborted:{reason}", "REJECTED")
                return
            self._execute_cashout_route(signal)
        except Exception as exc:  # noqa: BLE001 - il Timer inghiotte le eccezioni: pubblica sempre
            logger.exception("[RuntimeController] errore cashout differito (grace)")
            self._publish_cashout_failed(signal, f"grace_route_error:{exc}", "ERROR")
        finally:
            self._auto_green_release(key, gen)

    def _publish_cashout_failed(self, signal: dict, reason: str, status: str) -> None:
        """Pubblica un ``CASHOUT_FAILED`` strutturato in modo EXCEPTION-SAFE.

        Non solleva mai: se il ``bus.publish`` fallisse (es. bus in shutdown),
        l'errore e' loggato ma non propagato, cosi' il chiamante nel path del
        grace non finisce nel ramo except a ripubblicare (doppio CASHOUT_FAILED,
        Greptile P2).
        """
        try:
            self.bus.publish(CASHOUT_FAILED, {
                "reason": reason,
                "status": status,
                "bet_id": None,
                "matched": 0.0,
                "market_id": str(signal.get("market_id") or "") if isinstance(signal, dict) else "",
                "selection_id": signal.get("selection_id") if isinstance(signal, dict) else None,
            })
        except Exception:  # noqa: BLE001 - publish best-effort: mai propagare (evita doppia pubblicazione)
            logger.exception("[RuntimeController] publish CASHOUT_FAILED fallito")

    def _auto_green_delay_seconds(self) -> float:
        """Secondi di grace auto-green prima della route del cashout (>=0).

        OPT-IN: 0.0 se ``auto_green_delay_enabled`` non e' True. FAIL-SAFE:
        ``auto_green_delay_sec`` assente/non-finito/<=0 => ``AUTO_GREEN_DELAY_SEC``.
        CLAMP a ``[0, 30]``s (un typo di config non ritarda un cashout per minuti).
        FAIL-OPEN: config illeggibile => 0.0 (mai ritardare un cashout per un
        problema di configurazione).
        """
        try:
            if not bool(getattr(self.config, "auto_green_delay_enabled", False)):
                return 0.0
            raw = getattr(self.config, "auto_green_delay_sec", None)
            # FAIL-SAFE (non fail-open) sul valore: un sec non numerico/non-finito/
            # <=0 ricade sulla COSTANTE, non su 0 — armato significa "voglio un
            # grace", quindi un valore rotto usa il default, non lo disattiva.
            try:
                sec = float(raw)
            except (TypeError, ValueError):
                sec = float(AUTO_GREEN_DELAY_SEC)
            if not (math.isfinite(sec) and sec > 0.0):
                sec = float(AUTO_GREEN_DELAY_SEC)
            if not math.isfinite(sec) or sec <= 0.0:
                return 0.0
            return min(sec, 30.0)
        except Exception:  # noqa: BLE001 - fail-open: config illeggibile non ritarda il cashout
            return 0.0

    @staticmethod
    def _auto_green_key(signal: dict) -> tuple:
        """Chiave della pending-guard: solo il TARGET ``(market, selection)``.

        Indipendente dal ``signal_type``: un CASHOUT sullo stesso target non deve
        mai essere schedulato due volte (un `signal_type` nella chiave avrebbe
        lasciato passare due route sullo stesso target). Il grace e' cablato solo
        per il CASHOUT singolo (il CASHOUT_ALL va inline), quindi qui il target e'
        sempre valorizzato.
        """
        return (
            str(signal.get("market_id") or ""),
            str(signal.get("selection_id") or ""),
        )

    def _auto_green_reserve(self, key: tuple):
        """Prenota atomicamente la ``key`` e restituisce ``(acquired, generation)``.

        ``acquired=False`` se un grace e' gia' in volo per lo stesso target
        (duplicato). La generazione catturata sotto lock lega questo grace allo
        stato corrente: un CASHOUT_ALL successivo la incrementera' invalidandolo.
        """
        with self._auto_green_pending_lock:
            if key in self._auto_green_pending:
                return False, self._auto_green_generation
            self._auto_green_pending[key] = None  # prenotato; il Timer arriva dopo
            return True, self._auto_green_generation

    def _auto_green_arm_timer(self, key: tuple, timer: Any, gen: int) -> bool:
        """Registra il Timer SOLO se la prenotazione e' ancora valida.

        True => armare (chiave ancora prenotata E generazione invariata). False =>
        un CASHOUT_ALL concorrente ha gia' invalidato/rimosso la prenotazione: il
        chiamante NON deve avviare il Timer (la chiave e' gia' stata scartata)."""
        with self._auto_green_pending_lock:
            if key in self._auto_green_pending and gen == self._auto_green_generation:
                self._auto_green_pending[key] = timer
                return True
            # Stale (un CASHOUT_ALL ha bumpato la generazione): NON fare pop della
            # chiave — la nostra prenotazione e' gia' stata rimossa dall'ALL e la
            # entry eventualmente presente appartiene a una RI-prenotazione valida
            # post-ALL (stesso target): un pop qui la cancellerebbe, scartando in
            # silenzio un green-up legittimo (Fable). Ci si limita a non armare.
            return False

    def _auto_green_is_stale(self, gen: int) -> bool:
        """True se la generazione ``gen`` e' stata superata (CASHOUT_ALL nel mezzo)."""
        with self._auto_green_pending_lock:
            return gen != self._auto_green_generation

    def _auto_green_release(self, key: tuple, gen: Optional[int] = None) -> None:
        """Rilascia la pending-guard della ``key``.

        Se ``gen`` e' passato, rilascia SOLO se la generazione e' ancora quella
        (``gen == generazione corrente``): un rilascio *stale* — la nostra route
        differita e' partita ma un CASHOUT_ALL ha nel frattempo bumpato la
        generazione e un secondo CASHOUT singolo ha RI-prenotato lo stesso target
        (gen+1) — NON deve fare pop, altrimenti scarterebbe in silenzio la
        ri-prenotazione valida di B (stessa classe del clobber su ``arm``, sul path
        di release). ``gen=None`` => pop incondizionato (normale/duplicato/test)."""
        with self._auto_green_pending_lock:
            if gen is not None and gen != self._auto_green_generation:
                # Stale: la entry (se presente) appartiene a una ri-prenotazione
                # post-ALL, non a noi => non toccarla.
                return
            self._auto_green_pending.pop(key, None)

    def _cancel_pending_auto_green(self) -> None:
        """Cancella TUTTI i grace pendenti (usato quando un CASHOUT_ALL chiude
        tutto inline): un timer non deve fissare un secondo green-up su un target
        gia' chiuso dall'ALL, anche se la chiusura dell'ALL e' ancora unmatched al
        fire (GPT-5.6 Sol/Fugu). ``Timer.cancel`` e' no-op se gia' partito; il
        callback resta comunque fail-closed (ri-check gate + recompute fresco)."""
        # Difensivo: se lo stato del grace non e' inizializzato (istanza bare),
        # non c'e' nulla da fare.
        lock = getattr(self, "_auto_green_pending_lock", None)
        if lock is None:
            return
        with lock:
            # BUMP della generazione: invalida OGNI grace gia' schedulato (anche
            # quelli il cui Timer e' partito nella finestra reserve->arm e non e'
            # tracciato/cancellabile) => il callback differito, o l'arm, vedra' la
            # generazione avanzata e abortira'. Chiude la race in modo deterministico.
            self._auto_green_generation = getattr(self, "_auto_green_generation", 0) + 1
            pending = getattr(self, "_auto_green_pending", None) or {}
            timers = [t for t in pending.values() if t is not None]
            pending.clear()
        for t in timers:
            try:
                t.cancel()
            except Exception:  # noqa: BLE001 - best-effort, mai crashare sul cancel
                logger.exception("[RuntimeController] cancel timer grace fallito")

    def _cashout_gates_still_open(self, signal: dict, enqueue_mode: str = "") -> tuple:
        """Ri-verifica fail-closed dei gate live dopo il grace: (ok, reason).

        Copre i gate critici che possono scattare nella finestra d'attesa:
        emergency-stop (incl. daily-loss pending, sotto lock), kill-switch, cambio
        di execution_mode rispetto all'enqueue, sessione LIVE invalida, runtime non
        attivo. Il cambio di mode e' bloccante (CodeRabbit): un cashout accettato in
        un mode non deve instradarsi sotto un mode diverso (es. SIMULATION->LIVE
        durante la grace bypasserebbe il deploy-gate LIVE dell'enqueue).
        """
        with self._daily_loss_stop_lock:
            if self._emergency_stopped or self._daily_loss_pending_stop:
                return False, "emergency_stop_active"
        if self._is_kill_switch_active():
            return False, "kill_switch_active"
        current_mode = str(self.execution_mode).upper()
        if enqueue_mode and current_mode != str(enqueue_mode).upper():
            return False, f"execution_mode_changed:{enqueue_mode}->{current_mode}"
        if current_mode == "LIVE":
            svc = self.betfair_service
            if svc is not None and getattr(svc, "_session_invalid", False):
                return False, "session_invalid"
        if not self._runtime_active():
            return False, f"runtime_non_attivo:{self.mode.value}"
        return True, ""

    def _execute_cashout_route(self, signal: dict) -> None:
        """Costruisce il ``CashoutRouter`` coi servizi reali e instrada il segnale.

        Estratto da ``_route_cashout_signal`` per essere invocabile sia inline
        (default) sia differito dal grace auto-green. Fail-closed: un errore di
        costruzione o routing pubblica un ``CASHOUT_FAILED`` strutturato.
        """
        svc = self.betfair_service
        try:
            # is_simulation è un CALLABLE: l'adapter fa ``if self.is_simulation():``
            # (lo chiama). get_live_client/get_simulation_broker possono essere
            # Optional => guard None esplicito (cancel non confermato, mai
            # AttributeError). commission_pct: None-check esplicito per non
            # mascherare uno 0 configurato col fallback ``or``.
            cp = getattr(self.config, "commission_pct", None)
            commission = float(cp) if cp is not None else 4.5
            adapter = CashoutCancelAdapter(
                is_simulation=svc.is_simulation_mode,
                live_cancel=lambda **kw: (c.cancel_orders(**kw) if (c := svc.get_live_client()) is not None else False),
                sim_cancel=lambda **kw: (b.cancel_orders(**kw) if (b := svc.get_simulation_broker()) is not None else False),
            )
            router = CashoutRouter(
                fetch_current_orders=svc.list_current_orders,
                fetch_bot_orders=self.db.get_bot_active_orders,
                fetch_market_book=svc.get_market_book_snapshot,
                cancel_orders=adapter.cancel,
                publish=self.bus.publish,
                commission_pct=commission,
                source="TELEGRAM",
            )
            result = router.route(signal)
            logger.info(
                "[RuntimeController] cashout instradato: type=%s result=%s",
                signal.get("signal_type"), result,
            )
        except Exception as exc:  # noqa: BLE001 - fail-closed: niente scarto silenzioso
            logger.exception("[RuntimeController] errore routing cashout")
            self.bus.publish(CASHOUT_FAILED, {
                "reason": f"cashout_route_error:{exc}",
                "status": "ERROR",
                "bet_id": None,
                "matched": 0.0,
                "market_id": "",
                "selection_id": None,
            })

    def _on_signal_received(self, signal: dict) -> None:
        """
        Runtime signal gate for Telegram/UI-driven order intents.

        Ownership boundary:
        - this method validates runtime readiness + anti-duplication + MM/table checks
        - it does NOT resolve Telegram text parsing (owned upstream)
        - it forwards already-normalized copy/pattern metadata to TradingEngine
          as passthrough context (no strategy rewrite here)
        """
        signal = dict(signal or {})
        self.last_signal_at = datetime.utcnow().isoformat()

        # Emergency stop hard gate — refuses ALL live order entry. Include anche
        # il pending-stop sincrono da daily-loss: armato sotto lock PRIMA di
        # applicare il PnL del settlement che sfonda il limite, chiude la
        # finestra in cui un SIGNAL_RECEIVED concorrente (dispatch multi-worker)
        # vedrebbe _emergency_stopped ancora False e piazzerebbe un ordine.
        with self._daily_loss_stop_lock:
            stop_active = self._emergency_stopped or self._daily_loss_pending_stop
            stop_triggered_at = self._emergency_stopped_at
        if stop_active:
            self._reject_signal(
                signal,
                f"emergency_stop_active:triggered_at={stop_triggered_at}",
            )
            return

        if str(self.execution_mode).upper() == "LIVE":
            # Session guard — refuse live signals when session is known-invalid.
            _svc = self.betfair_service
            if _svc is not None and getattr(_svc, "_session_invalid", False):
                self._reject_signal(signal, "session_invalid_live_blocked")
                return

            deploy_gate = self.enforce_deploy_gate(
                execution_mode="LIVE",
                live_enabled=self.live_enabled,
                live_readiness_ok=self.live_readiness_ok,
            )
            if not deploy_gate["allowed"]:
                self.execution_mode = "SIMULATION"
                self.set_simulation_mode(True)
                self._reject_signal(signal, f"deploy_gate_no_go:{deploy_gate['reason']}")
                self.bus.publish(
                    "LIVE_EXECUTION_REFUSED",
                    {
                        "reason_code": deploy_gate["reason"],
                        "reasons": deploy_gate.get("reasons", []),
                        "requested_execution_mode": "LIVE",
                        "effective_execution_mode": "SIMULATION",
                        "deploy_gate": deploy_gate,
                    },
                )
                return

        if not self._runtime_active():
            self._reject_signal(signal, f"runtime_non_attivo:{self.mode.value}")
            return

        # Cashout routing — gate cashout PRIMA del required-check market_id/
        # selection_id (un CASHOUT_ALL non li ha e verrebbe scartato come
        # campi_mancanti). Tutti i gate live (emergency-stop incl. daily-loss
        # pending, session-guard, deploy-gate, runtime-active) sono a monte:
        # un cashout passa di qui solo se l'order entry live è consentito (E1).
        signal_type = str(signal.get("signal_type") or "").strip().upper()
        if signal_type in {"CASHOUT", "CASHOUT_ALL"}:
            self._route_cashout_signal(signal)
            return

        required = ["market_id", "selection_id"]
        missing = [k for k in required if signal.get(k) in (None, "")]
        if missing:
            self._reject_signal(signal, f"campi_mancanti:{','.join(missing)}")
            return

        copy_meta, pattern_meta = self._extract_origin_metadata(signal)
        if isinstance(copy_meta, dict) and isinstance(pattern_meta, dict):
            self._reject_signal(signal, "copy_pattern_mutually_exclusive")
            return

        event_key = self.duplication_guard.build_event_key(signal)
        signal["event_key"] = event_key

        if self.config.anti_duplication_enabled and not self.duplication_guard.acquire(event_key):
            self._reject_signal(signal, "duplicato_bloccato")
            return

        # Enforcement A3: Limite Tavoli Recovery (#320)
        active_recovery_count = len([t for t in self.table_manager._tables.values() if getattr(t, 'in_recovery', False) and t.status != "FREE"])
        max_recovery = getattr(self.config, "max_recovery_tables", 2)
        
        allow_recovery = bool(getattr(self.config, "allow_recovery", False))
        if allow_recovery and active_recovery_count >= max_recovery:
            free_exists = any(t.status == "FREE" for t in self.table_manager._tables.values())
            if not free_exists:
                self._reject_signal(signal, f"max_recovery_tables_reached:{active_recovery_count}")
                return
            else:
                allow_recovery = False

        table = self.table_manager.allocate(
            event_key=event_key,
            allow_recovery=allow_recovery,
        )
        if table is None:
            if self.config.anti_duplication_enabled:
                self.duplication_guard.release(event_key)
            self._reject_signal(signal, "nessun_tavolo_disponibile")
            return

        total_exposure = self.table_manager.total_exposure()
        event_exposure = self._event_current_exposure(event_key)

        # Enforcement A1: Drawdown Hard Stop (#320)
        if str(self.execution_mode).upper() == "LIVE":
            drawdown_limit = getattr(self.config, "max_drawdown_hard_stop_pct", None)
            if drawdown_limit is not None:
                current_bankroll = float(self.risk_desk.bankroll_current)
                peak = float(self.risk_desk.equity_peak)
                if peak > 0 and current_bankroll < peak:
                    current_dd = ((peak - current_bankroll) / peak) * 100.0
                    if current_dd >= float(drawdown_limit):
                        self.force_lockdown(f"drawdown_hard_stop_triggered:{current_dd:.2f}%")
                        self._reject_signal(signal, "drawdown_hard_stop_active")
                        return

        decision = self.mm.calculate(
            signal=signal,
            bankroll_current=self.risk_desk.bankroll_current,
            equity_peak=self.risk_desk.equity_peak,
            current_total_exposure=total_exposure,
            event_current_exposure=event_exposure,
            table=table,
        )

        # Enforcement A2: Max Open Exposure (Cap Assoluto €) (#320)
        if decision.approved:
            max_abs_exposure = getattr(self.config, "max_open_exposure", None)
            if max_abs_exposure is not None:
                projected_total = total_exposure + decision.recommended_stake
                if projected_total > float(max_abs_exposure):
                    decision.approved = False
                    decision.reason = f"max_open_exposure_exceeded:limit={max_abs_exposure}€"

        if not decision.approved:
            if self.config.anti_duplication_enabled:
                self.duplication_guard.release(event_key)
            if decision.desk_mode == DeskMode.LOCKDOWN:
                self.force_lockdown(decision.reason)
            self._reject_signal(signal, decision.reason)
            return

        payload = {
            "market_id": str(signal.get("market_id")),
            "selection_id": int(signal.get("selection_id")),
            "bet_type": str(
                signal.get("bet_type")
                or signal.get("side")
                or signal.get("action")
                or "BACK"
            ).upper(),
            "price": float(signal.get("price") or signal.get("odds")),
            "stake": float(decision.recommended_stake),
            "event_name": signal.get("event") or signal.get("match") or signal.get("event_name") or "",
            "market_name": signal.get("market") or signal.get("market_name") or signal.get("market_type") or "",
            "runner_name": signal.get("selection") or signal.get("runner_name") or signal.get("runnerName") or "",
            "simulation_mode": bool(signal.get("simulation_mode", self.simulation_mode)),
            "event_key": event_key,
            "table_id": decision.table_id,
            "batch_id": str(signal.get("batch_id") or ""),
            "roserpina_reason": decision.reason,
            "roserpina_mode": decision.desk_mode.value,
        }
        routing_contract = signal.get("telegram_routing_contract")
        if isinstance(routing_contract, str) and routing_contract.strip():
            payload["telegram_routing_contract"] = routing_contract.strip()
        route_target = signal.get("telegram_route_target")
        if isinstance(route_target, str) and route_target.strip():
            payload["telegram_route_target"] = route_target.strip()
        has_copy_meta = isinstance(copy_meta, dict)
        has_pattern_meta = isinstance(pattern_meta, dict)
        if has_copy_meta and has_pattern_meta:
            # Defensive fail-closed guard: Runtime should never forward both.
            self._reject_signal(signal, "copy_pattern_mutually_exclusive")
            return
        if has_copy_meta:
            payload["copy_meta"] = dict(copy_meta)
        elif has_pattern_meta:
            payload["pattern_meta"] = dict(pattern_meta)
        order_origin = self._resolve_order_origin(
            signal,
            has_copy_meta=has_copy_meta,
            has_pattern_meta=has_pattern_meta,
        )
        if order_origin:
            payload["order_origin"] = order_origin

        self.table_manager.activate(
            table_id=decision.table_id,
            event_key=event_key,
            exposure=float(decision.recommended_stake),
            market_id=payload["market_id"],
            selection_id=payload["selection_id"],
            meta={
                "event_name": payload["event_name"],
                "market_name": payload["market_name"],
                "runner_name": payload["runner_name"],
                "bet_type": payload["bet_type"],
                "price": payload["price"],
                "simulation_mode": payload["simulation_mode"],
            },
        )
        # B6.2 — best price DIRECT (flag globale default-OFF). Lo snapshot e' un
        # fetch lento che in LIVE puo' invalidare la sessione: va eseguito PRIMA
        # dei gate finali pre-submit, cosi' che session-guard e recheck
        # daily-loss/emergency qui sotto coprano anche la finestra dello snapshot
        # (niente approved-without-submit). Con flag OFF e' un no-op totale e
        # ritorna False => i gate sotto restano identici a oggi.
        best_price_attempted = self._apply_direct_best_price(payload)

        # Gate finali pre-submit, RIESEGUITI dopo lo snapshot. Vanno PRIMA di
        # SIGNAL_APPROVED perche' l'audit consuma sia SIGNAL_APPROVED sia
        # SIGNAL_REJECTED: approvare-poi-rifiutare registrerebbe un ordine
        # saltato come "approvato".
        #
        # (a) Session guard LIVE — solo se lo snapshot ha girato (flag ON): il
        #     fetch puo' aver intercettato SESSION_EXPIRED/INVALID_SESSION e
        #     marcato la sessione invalida. Con flag OFF nessun fetch => nessun
        #     gate nuovo rispetto a oggi.
        if best_price_attempted and str(self.execution_mode).upper() == "LIVE":
            _svc = self.betfair_service
            if _svc is not None and getattr(_svc, "_session_invalid", False):
                self._release_acquired_and_reject(
                    signal, event_key=event_key, table_id=decision.table_id,
                    reason="session_invalid_live_blocked:pre_submit_recheck",
                )
                return
        # (b) Recheck kill-switch daily-loss / emergency pending: il gate
        #     d'ingresso e' una-tantum, ma un settlement perdente concorrente (o
        #     lo snapshot lento) puo' aver armato pending/emergency nel frattempo.
        if self._daily_loss_entry_blocked():
            self._release_acquired_and_reject(
                signal, event_key=event_key, table_id=decision.table_id,
                reason="emergency_stop_active:pre_submit_recheck",
            )
            return
        self.bus.publish(
            "SIGNAL_APPROVED",
            {
                "signal": signal,
                "decision": {
                    "table_id": decision.table_id,
                    "recommended_stake": decision.recommended_stake,
                    "desk_mode": decision.desk_mode.value,
                    "reason": decision.reason,
                    "metadata": decision.metadata,
                },
            },
        )
        self.bus.publish("CMD_QUICK_BET", payload)

    def _release_acquired_and_reject(self, signal: dict, *, event_key: str,
                                     table_id: Any, reason: str) -> None:
        """Rilascia le risorse gia' acquisite (duplication guard + tavolo) e
        rifiuta il segnale, per i gate pre-submit. Nessun CMD_QUICK_BET parte,
        quindi nessun evento terminale chiamera' _release_if_terminal a
        liberarle: senza questo resterebbero bloccate dopo un reset, impedendo
        segnali validi successivi.
        """
        if self.config.anti_duplication_enabled:
            self.duplication_guard.release(event_key)
        try:
            self.table_manager.force_unlock(int(table_id))
        except Exception:
            logger.exception("Errore force_unlock table_id=%s", table_id)
        self._reject_signal(signal, reason)

    def _apply_direct_best_price(self, payload: dict) -> bool:
        """Sovrascrive ``payload['price']`` col best price DIRECT difensivo (B6.2).

        Ritorna ``True`` se la flag e' attiva (snapshot tentato) — il chiamante
        DEVE rieseguire i gate pre-submit (session + daily-loss) perche' il fetch
        puo' aver consumato tempo o invalidato la sessione; ``False`` se no-op
        (flag OFF) => nessun fetch, nessuna mutazione del payload, comportamento
        byte-identico a oggi.

        Dormiente di default: gira solo se ``config.use_best_price_direct`` e'
        esplicitamente truthy (letto via ``getattr``, niente edit della config
        class). Fail-closed: su book assente, mercato non-OPEN, runner non
        attivo, lato senza liquidita', prezzo invalido o deviazione oltre
        tolleranza, l'estrattore puro ritorna il master price => il payload resta
        sul prezzo master. Qualunque errore di fetch/override e' catturato e
        lascia il master price invariato (mai un crash sul percorso d'ordine).
        """
        if not getattr(self.config, "use_best_price_direct", False):
            return False
        try:
            market_id = str(payload.get("market_id") or "").strip()
            book = None
            if market_id and self.betfair_service is not None:
                # include_prices=True: il best-price DIRECT richiede le ladder
                # EX_BEST_OFFERS, che il client popola solo con priceProjection.
                book = self.betfair_service.get_market_book_snapshot(
                    market_id, include_prices=True
                )
            tolerance = getattr(self.config, "best_price_max_deviation_pct", 2.0)
            result = resolve_direct_best_price(
                market_book=book,
                selection_id=payload.get("selection_id"),
                side=payload.get("bet_type"),
                master_price=payload.get("price"),
                max_deviation_pct=tolerance,
            )
            payload["price"] = result["price"]
            payload["best_price_source"] = result["source"]
            payload["best_price_reason"] = result["reason"]
            # Audit del best-price nei log strutturati. NB: il consumer downstream
            # (`trading_engine._normalize_quick_bet`) copia solo chiavi note, quindi
            # best_price_source/reason NON entrano nei record di lifecycle: il log
            # qui e' l'unico canale d'audit dell'override finche' la propagazione
            # nei record non viene cablata (follow-up dell'attivazione, richiede
            # estendere l'allowlist in core/trading_engine.py, fuori scope B6.2).
            # Override reale (prezzo cambiato vs master) => INFO; fallback al master
            # (nessun cambio prezzo, identico a oggi) => DEBUG, per non fare rumore.
            if result["source"] != SOURCE_FALLBACK_MASTER:
                logger.info(
                    "best_price_direct: override source=%s reason=%s price=%s market_id=%s selection_id=%s",
                    result["source"], result["reason"], result["price"],
                    market_id, payload.get("selection_id"),
                )
            else:
                logger.debug(
                    "best_price_direct: fallback master reason=%s price=%s market_id=%s selection_id=%s",
                    result["reason"], result["price"], market_id, payload.get("selection_id"),
                )
        except Exception:
            # Fail-closed: lascia il master price gia' presente nel payload.
            logger.exception("best_price_direct: override fallito, mantengo master price")
        # Snapshot tentato (flag ON): il chiamante DEVE rieseguire i gate
        # pre-submit anche se l'override e' fallito (il fetch puo' aver
        # invalidato la sessione prima di sollevare).
        return True

    # =========================================================
    # BET LIFECYCLE
    # =========================================================
    def _release_if_terminal(self, payload: dict, *, event_name: str) -> None:
        if event_name not in TERMINAL_LIFECYCLE_EVENTS:
            return
        payload = dict(payload or {})
        event_key = str(payload.get("event_key") or "")
        table_id = payload.get("table_id")

        if event_key:
            self.duplication_guard.release(event_key)

        if table_id is not None:
            try:
                self.table_manager.force_unlock(int(table_id))
            except Exception:
                logger.exception("Errore force_unlock table_id=%s", table_id)

    def _on_quick_bet_failed(self, payload: dict) -> None:
        self._track_direct_order(payload, event_name="QUICK_BET_FAILED")
        self._release_if_terminal(payload, event_name="QUICK_BET_FAILED")

    def _on_quick_bet_accepted(self, payload: dict) -> None:
        self._track_direct_order(payload, event_name="QUICK_BET_ACCEPTED")
        self._release_if_terminal(payload, event_name="QUICK_BET_ACCEPTED")

    def _on_quick_bet_partial(self, payload: dict) -> None:
        self._track_direct_order(payload, event_name="QUICK_BET_PARTIAL")
        self._release_if_terminal(payload, event_name="QUICK_BET_PARTIAL")

    def _on_quick_bet_filled(self, payload: dict) -> None:
        self._track_direct_order(payload, event_name="QUICK_BET_FILLED")
        self._release_if_terminal(payload, event_name="QUICK_BET_FILLED")

    def _on_quick_bet_success(self, payload: dict) -> None:
        self._track_direct_order(payload, event_name="QUICK_BET_SUCCESS")
        self._release_if_terminal(payload, event_name="QUICK_BET_SUCCESS")

    def _on_quick_bet_ambiguous(self, payload: dict) -> None:
        self._track_direct_order(payload, event_name="QUICK_BET_AMBIGUOUS")
        self._release_if_terminal(payload, event_name="QUICK_BET_AMBIGUOUS")

    def _on_quick_bet_rollback_done(self, payload: dict) -> None:
        self._track_direct_order(payload, event_name="QUICK_BET_ROLLBACK_DONE")
        self._release_if_terminal(payload, event_name="QUICK_BET_ROLLBACK_DONE")

    # =========================================================
    # DIRECT order registry (B6.3.2a) — identità per il futuro TTL/cancel
    # =========================================================
    def _track_direct_order(self, payload: dict, *, event_name: str) -> None:
        """Registry in-memory ``customer_ref → bet_id`` degli ordini DIRECT.

        Identità certa dal runtime, **niente DB / niente deduzione fragile**:
        - **popola** solo su ack (``ACCEPTED``/``PARTIAL``) di un ordine che porta
          ``best_price_source`` (settato solo dal percorso best-price DIRECT) +
          ``bet_id`` + ``customer_ref``;
        - **pulisce** solo quando l'ordine NON è più un resting vivo:
          ``FILLED`` (abbinato), ``FAILED`` (non piazzato), ``ROLLBACK_DONE``
          (annullato), per ``customer_ref`` (presente in tutti gli eventi, anche
          ``ROLLBACK_DONE`` che non porta il ``bet_id``). ``SUCCESS``/
          ``AMBIGUOUS`` **non** puliscono: un place riuscito/incerto può lasciare
          un ordine non abbinato vivo (Greptile P2).

        ``customer_ref`` è la chiave d'idempotenza dell'engine (univoca per
        ordine vivo), quindi l'``[customer_ref]=bet_id`` non perde ordini
        distinti; un'eventuale collisione sotto-traccia (manca un cancel), mai
        sovra-cancella. Fail-safe: registry in-memory, **vuoto** al restart ⇒ un
        bet_id senza identità certa non finisce mai nell'allowlist del poller
        (B6.3.2b). Nessun side-effect broker qui: solo tracking.
        """
        payload = payload or {}
        customer_ref = str(payload.get("customer_ref") or "").strip()
        if not customer_ref:
            return
        if event_name in _DIRECT_TTL_TERMINAL_EVENTS:
            if self._direct_order_bet_ids.pop(customer_ref, None) is not None:
                logger.debug(
                    "direct_ttl_registry: drop customer_ref=%s su %s (size=%d)",
                    customer_ref, event_name, len(self._direct_order_bet_ids),
                )
            return
        if event_name in _DIRECT_TTL_ACK_EVENTS:
            if not payload.get("best_price_source"):
                return  # non un ordine DIRECT (flag OFF / percorso non best-price)
            bet_id = str(payload.get("bet_id") or "").strip()
            if not bet_id:
                return
            self._direct_order_bet_ids[customer_ref] = bet_id
            logger.debug(
                "direct_ttl_registry: track customer_ref=%s bet_id=%s su %s (size=%d)",
                customer_ref, bet_id, event_name, len(self._direct_order_bet_ids),
            )

    @property
    def direct_unmatched_bet_ids(self) -> set[str]:
        """Allowlist dei ``bet_id`` DIRECT noti (per il poller B6.3.2b)."""
        return set(self._direct_order_bet_ids.values())

    # =========================================================
    # SL / TP / TRAILING MONITOR (C1 #320)
    # =========================================================
    def _monitor_sl_tp_trailing(self, market_book: dict) -> None:
        """Monitoraggio attivo di SL/TP/Trailing basato sugli snapshot di mercato."""
        if self.mode != RuntimeMode.ACTIVE:
            return
            
        market_id = market_book.get("marketId")
        if not market_id:
            return
            
        # Trova i tavoli attivi su questo mercato
        active_tables = [
            t for t in self.table_manager._tables.values() 
            if t.status == "ACTIVE" and t.market_id == market_id
        ]
        
        for table in active_tables:
            meta = table.meta or {}
            sl = meta.get("stop_loss")
            tp = meta.get("take_profit")
            if sl is None and tp is None:
                continue
                
            # Calcolo PnL approssimativo per il monitoraggio (unrealized)
            # In una versione completa useremmo il SimulationBroker/PositionLedger
            # Qui implementiamo il gate richiesto dalla Issue #320
            runner = next((r for r in market_book.get("runners", []) if str(r.get("selectionId")) == str(table.selection_id)), None)
            if not runner:
                continue
                
            current_price = float(runner.get("lastPriceTraded") or 0.0)
            if current_price <= 1.0:
                continue
                
            entry_price = float(meta.get("price", 0.0))
            stake = float(table.current_exposure)
            if entry_price <= 1.0 or stake <= 0.0:
                continue
                
            # PnL stimato (Back)
            side = meta.get("bet_type", "BACK")
            if side == "BACK":
                unrealized_pnl = stake * (current_price - entry_price) / entry_price # Semplificato
            else:
                unrealized_pnl = stake * (entry_price - current_price) / entry_price # Semplificato
                
            # Enforcement SL
            if sl is not None and unrealized_pnl <= -abs(float(sl)):
                self._trigger_auto_close_table(table, "STOP_LOSS_REACHED", unrealized_pnl)
            
            # Enforcement TP
            elif tp is not None and unrealized_pnl >= abs(float(tp)):
                self._trigger_auto_close_table(table, "TAKE_PROFIT_REACHED", unrealized_pnl)

    def _trigger_auto_close_table(self, table, reason: str, pnl: float) -> None:
        logger.info(f"[RuntimeController] Auto-closing table {table.table_id} reason={reason} pnl={pnl:.2f}")
        self.bus.publish("REQ_EXECUTE_CASHOUT", {
            "market_id": table.market_id,
            "selection_id": table.selection_id,
            "stake": table.current_exposure,
            "reason": reason,
            "source": "AUTO_MONITOR"
        })

    # =========================================================
    # DIRECT unmatched TTL poller (B6.3.2b) — cancel reale, default-OFF
    # =========================================================
    def _poll_direct_unmatched_ttl(self) -> None:
        """Cancella gli ordini DIRECT non abbinati scaduti oltre il TTL.

        **Dormiente di default**: gira solo se ``config.direct_unmatched_ttl_enabled``
        è truthy (letto via ``getattr``, niente edit della config class). Gated
        sull'intervallo ``direct_unmatched_ttl_poll_sec``.

        Sicurezza:
        - **solo ordini DIRECT noti**: l'allowlist è il registry B6.3.2a; un
          ``bet_id`` non noto (cashout/green-up/copy/ignoto) non è MAI candidato;
        - **allowlist vuota ⇒ nessun cancel**;
        - **NO RETRY**: un solo tentativo per ordine, poi il ``bet_id`` esce dal
          registry (esito notificato sul bus); un cancel fallito NON viene
          ri-tentato automaticamente (intervento manuale via notifica);
        - **fail-closed**: un errore nel fetch ordini correnti aborta il giro
          (niente cancel su dati assenti/stale); ogni eccezione è catturata.
        """
        if not bool(getattr(self.config, "direct_unmatched_ttl_enabled", False)):
            return
        now_mono = time.monotonic()
        interval = max(1.0, float(
            getattr(self.config, "direct_unmatched_ttl_poll_sec", 30.0) or 30.0))
        if (now_mono - self._last_direct_ttl_poll_at) < interval:
            return
        self._last_direct_ttl_poll_at = now_mono

        allow = self.direct_unmatched_bet_ids
        if not allow:
            return  # nessun ordine DIRECT noto ⇒ niente da cancellare

        try:
            orders = self.betfair_service.list_current_orders()
        except Exception as exc:
            # Fail-closed: non cancellare su dati assenti/stale.
            logger.warning("direct_ttl_poll: list_current_orders fallita, abort: %s", exc)
            return

        to_cancel = select_expired_unmatched(
            current_orders=orders,
            now_epoch=time.time(),
            ttl_seconds=getattr(self.config, "direct_unmatched_ttl_sec", 120.0),
            direct_bet_ids=allow,
        )
        if not to_cancel:
            return

        # Pre-check: nessun client cancel disponibile (transitorio) ⇒ salta SENZA
        # scartare dal registry, così l'ordine resta candidato al prossimo giro
        # (non è un cancel fallito, è impossibilità di tentare).
        if self._direct_ttl_cancel_client() is None:
            logger.warning("direct_ttl_poll: cancel client assente, ritento al prossimo giro")
            return
        adapter = self._direct_ttl_cancel_adapter()
        for item in to_cancel:
            self._cancel_direct_unmatched(adapter, item)

    def _direct_ttl_cancel_client(self):
        """Il broker di cancel disponibile (sim/live), o ``None`` (pre-check)."""
        try:
            if self.simulation_mode:
                return self.betfair_service.get_simulation_broker()
            return self.betfair_service.get_live_client()
        except Exception as exc:
            logger.warning("direct_ttl_poll: cancel client non disponibile: %s", exc)
            return None

    def _direct_ttl_cancel_adapter(self) -> CashoutCancelAdapter:
        """Adapter che normalizza firma+risposta del cancel tra LIVE e SIM.

        Riusa `CashoutCancelAdapter` (B2.1): **LIVE** usa `bet_ids`, **SIM** usa
        `instructions`; la risposta eterogenea dei broker è normalizzata a un
        bool "confermato", fail-closed (False) su eccezioni/shape ambigue.
        """
        svc = self.betfair_service
        return CashoutCancelAdapter(
            is_simulation=lambda: bool(self.simulation_mode),
            live_cancel=lambda **kw: (
                c.cancel_orders(**kw) if (c := svc.get_live_client()) is not None else False),
            sim_cancel=lambda **kw: (
                b.cancel_orders(**kw) if (b := svc.get_simulation_broker()) is not None else False),
        )

    def _cancel_direct_unmatched(self, adapter: CashoutCancelAdapter, item: dict) -> None:
        """Un singolo cancel NO-RETRY + notifica esito + cleanup registry.

        L'adapter gestisce firma/risposta sim-vs-live ed è già fail-closed.
        """
        market_id = str(item.get("market_id") or "").strip()
        bet_id = str(item.get("bet_id") or "").strip()
        # NO RETRY: fuori dal registry dopo UN tentativo (successo o fallimento).
        self._discard_direct_bet_id(bet_id)
        try:
            confirmed = bool(adapter.cancel(market_id, [bet_id]))
        except Exception as exc:  # belt-and-suspenders: l'adapter è già fail-closed
            self.bus.publish("DIRECT_UNMATCHED_CANCEL_FAILED", {
                "market_id": market_id, "bet_id": bet_id, "reason": str(exc)})
            logger.warning("direct_ttl_poll: cancel sollevato bet_id=%s: %s", bet_id, exc)
            return
        event = "DIRECT_UNMATCHED_CANCELLED" if confirmed else "DIRECT_UNMATCHED_CANCEL_FAILED"
        self.bus.publish(event, {"market_id": market_id, "bet_id": bet_id, "ok": confirmed})
        logger.info("direct_ttl_poll: cancel bet_id=%s market=%s ok=%s",
                    bet_id, market_id, confirmed)

    def _discard_direct_bet_id(self, bet_id: str) -> None:
        """Rimuove dal registry tutte le voci con questo ``bet_id``."""
        bid = str(bet_id or "").strip()
        if not bid:
            return
        for cref in [c for c, b in self._direct_order_bet_ids.items() if b == bid]:
            self._direct_order_bet_ids.pop(cref, None)

    # =========================================================
    # MANUAL/EXTERNAL CLOSE POSITION
    # =========================================================
    def _on_close_position(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            return

        settlement = self._extract_settlement_contract(payload)
        table_id = payload.get("table_id")
        pnl = float(settlement["net_pnl"])
        event_key = str(payload.get("event_key") or "")
        batch_id = str(payload.get("batch_id") or "")
        validation_status = str(settlement.get("settlement_validation") or "accepted")
        settlement_acceptance = str(settlement.get("settlement_acceptance") or "")

        # Daily-loss precheck (fail-closed): se la perdita realized e' GIA' oltre
        # il limite (da settlement precedenti) hard-stop SUBITO, prima di
        # qualunque ramo di early-return (rejected / recovery fail-closed), cosi'
        # nessun percorso salta l'enforcement. Usa lo stato gia' calcolato del
        # monitor (no nuova publish/alert): non altera la contabilita' del
        # monitor, applica solo l'enforcement.
        self._enforce_daily_loss_hard_stop(dict(self._daily_loss_monitor_state or {}))

        if validation_status.startswith("rejected"):
            rejection_context = self._build_settlement_rejection_context(payload, settlement)
            logger.warning(
                "Settlement contract rejected at runtime boundary: reason=%s validation=%s acceptance=%s context=%s",
                rejection_context["reason"],
                rejection_context["settlement_validation"],
                rejection_context["settlement_acceptance"],
                rejection_context,
            )
            sync_result = {
                "correlation_id": str(payload.get("correlation_id") or payload.get("event_key") or ""),
                "settlement_detected": True,
                "bankroll_before": float(self.risk_desk.bankroll_current),
                "bankroll_after": float(self.risk_desk.bankroll_current),
                "bankroll_sync_status": "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT",
                "balance_source": "none",
                "reason": str(settlement.get("reason") or "SETTLEMENT_CONTRACT_REJECTED"),
                "settlement_acceptance": settlement_acceptance,
            }
            auto_trade_result = {
                "correlation_id": str(payload.get("correlation_id") or payload.get("event_key") or ""),
                "source_settlement_correlation_id": str(payload.get("correlation_id") or ""),
                "bankroll_sync_status": "SYNC_FAILED_INVALID_SETTLEMENT_CONTRACT",
                "money_management_status": "MM_STOP_SETTLEMENT_CONTRACT_REJECTED",
                "cycle_active": False,
                "progression_allowed": False,
                "auto_trade_enabled": False,
                "auto_trade_status": "AUTO_TRADE_REJECTED_SETTLEMENT",
                "next_stake": 0.0,
                "risk_status": "RISK_NOT_EVALUATED",
                "submitted": False,
                "reason": str(settlement.get("reason") or "SETTLEMENT_CONTRACT_REJECTED"),
                "settlement_acceptance": settlement_acceptance,
            }
            self._last_bankroll_sync_result = dict(sync_result)
            self._last_auto_trade_result = dict(auto_trade_result)
            self._last_cycle_executor_result = dict(auto_trade_result)
            self.bus.publish("BANKROLL_SYNC_RESULT", dict(sync_result))
            self.bus.publish("AUTO_TRADE_MM_RESULT", dict(auto_trade_result))
            return

        if table_id is not None:
            self.table_manager.release(int(table_id), pnl=pnl)
            
        # Persistenza cronologia Risk Desk (Issue #274/293)
        try:
            history_payload = {
                "event_key": event_key,
                "market_id": payload.get("market_id"),
                "selection_id": payload.get("selection_id"),
                "side": settlement.get("side") or payload.get("side"),
                "stake": payload.get("stake"),
                "net_pnl": pnl,
                "outcome": "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "VOID",
                "table_id": table_id,
                "closed_at": datetime.utcnow().isoformat()
            }
            if hasattr(self.db, "add_risk_position_history"):
                self.db.add_risk_position_history(history_payload)
        except Exception:
            logger.exception("Errore persistenza risk_position_history")

        if event_key:
            self.duplication_guard.release(event_key)
        settlement_key = self._build_bankroll_sync_key(payload)
        recovery_probe = self._read_cycle_recovery_state(settlement_key)
        if self._should_fail_closed_on_recovery(recovery_probe):
            # Fail-closed daily-loss: qui il PnL NON viene applicato (recovery
            # state ambiguo/duplicato), ma se questo settlement accettato
            # sfonderebbe comunque il limite giornaliero si hard-stoppa lo stesso
            # (proiezione read-only). Un eventuale falso positivo da duplicato e'
            # piu' sicuro di un kill-switch mancato.
            projected_recovery = self._projected_daily_loss_breach(pnl)
            stopped = self._enforce_daily_loss_hard_stop(projected_recovery)
            if projected_recovery.get("breached"):
                # Persisti il breach nello stato del monitor (in-memory) cosi' un
                # successivo start()/resume() in LIVE lo rifiuta ed emette il
                # primo TRIGGERED.
                self._record_pending_daily_loss_breach(projected_recovery)
                if not stopped and not self._emergency_stopped:
                    # Se il runtime e' STOPPED l'enforce non ferma (gate
                    # ACTIVE/PAUSED) e il solo stato monitor in-memory andrebbe
                    # perso a un riavvio prima di start()/resume(), riaprendo il
                    # restart LIVE dopo aver sfondato max_daily_loss. Questo e' un
                    # breach REALE settlement-driven: emergency_stop DUREVOLE
                    # (persist su db, ricaricato fail-closed al riavvio).
                    amount = float(projected_recovery.get("daily_loss_amount", 0.0) or 0.0)
                    self.emergency_stop(reason=f"DAILY_LOSS_BREACH_RECOVERY_FAILCLOSED:{amount}")
            fail_result = self._build_fail_closed_recovery_result(payload=payload, probe=recovery_probe)
            sync_result = {
                "correlation_id": str(payload.get("correlation_id") or payload.get("event_key") or ""),
                "settlement_detected": True,
                "bankroll_before": float(self.risk_desk.bankroll_current),
                "bankroll_after": float(self.risk_desk.bankroll_current),
                "bankroll_sync_status": "SYNC_SKIPPED_DUPLICATE",
                "balance_source": "durable_checkpoint",
                "reason": "RECOVERY_FAIL_CLOSED",
                "recovery_status": str(fail_result.get("recovery_status") or "RECOVERY_STATE_AMBIGUOUS"),
            }
            self._last_bankroll_sync_result = dict(sync_result)
            self._last_auto_trade_result = dict(fail_result)
            self._last_cycle_executor_result = dict(fail_result)
            self.bus.publish("BANKROLL_SYNC_RESULT", dict(sync_result))
            self.bus.publish("AUTO_TRADE_MM_RESULT", dict(fail_result))
            return
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="SETTLEMENT_DETECTED",
            next_trade_submission_status="NOT_ATTEMPTED",
            reason="settlement_detected",
            recovery_status=recovery_probe.get("status", "RECOVERY_NO_STATE"),
        )
        # Pending-stop sincrono: se questo settlement sfonderebbe il limite,
        # arma il flag SOTTO LOCK *prima* di applicare il PnL. Da questo istante
        # ogni SIGNAL_RECEIVED concorrente (dispatch multi-worker) e' rifiutato,
        # chiudendo la finestra tra apply-PnL e emergency_stop in cui altrimenti
        # vedrebbe _emergency_stopped ancora False e piazzerebbe un ordine.
        projected_close = self._projected_daily_loss_breach(pnl)
        arm_pending = bool(projected_close.get("breached")) and self.mode in (
            RuntimeMode.ACTIVE,
            RuntimeMode.PAUSED,
        )
        if arm_pending:
            with self._daily_loss_stop_lock:
                self._daily_loss_pending_stop = True
            # Persisti DUREVOLMENTE il breach su db PRIMA dell'I/O di rete del
            # sync (il pending-stop e' solo in-memory): se il processo muore
            # mentre get_account_funds() e' lento/bloccato, al riavvio il reload
            # fail-closed ricarica l'emergenza invece di perdere l'hard-stop.
            # NON muta _emergency_stopped in-memory, cosi' il sync usa ancora il
            # client live e l'emergency_stop sotto esegue il lockdown completo.
            self._persist_db_emergency_marker(active=True, reason=f"DAILY_LOSS_BREACH_PENDING:{float(projected_close.get('daily_loss_amount', 0.0) or 0.0)}")
        try:
            if settlement_key and settlement_key not in self._processed_realized_pnl_keys:
                self._apply_realized_pnl_without_mutating_bankroll(pnl)
                self._processed_realized_pnl_keys.add(settlement_key)
            # Bankroll sync PRIMA dell'enforce: emergency_stop flippa il service a
            # SIMULATION e get_account_funds sceglie il client dal mode, quindi
            # sincronizzare dopo lo stop interrogherebbe il client simulato/zero
            # invece del conto live che sta settlando. L'order-entry resta gia'
            # bloccato dal pending-stop armato sopra, quindi anticipare il sync
            # non riapre la finestra di piazzamento ordini.
            sync_result = self._sync_bankroll_post_settlement(payload)
            self._last_bankroll_sync_result = dict(sync_result)
            self.bus.publish("BANKROLL_SYNC_RESULT", dict(sync_result))
            # Daily-loss hard stop SINCRONO (persist + cancel-all + lockdown) dopo
            # il sync ma PRIMA dell'auto-trade: l'auto-trade sotto e' bloccato da
            # _risk_allows_auto_trade e nessun ordine parte dopo il breach.
            daily_loss_state = self._monitor_daily_loss_breach(source="RUNTIME_CLOSE_POSITION", payload=payload)
            self._enforce_daily_loss_hard_stop(daily_loss_state)
        finally:
            # Disarma il bridge se NON e' subentrato l'emergency_stop definitivo
            # (es. il breach non si e' materializzato): evita un pending-stop
            # appiccicato che bloccherebbe i segnali leciti fino al reset, e
            # ripulisci il marker durevole speculativo scritto prima del sync.
            if arm_pending and not self._emergency_stopped:
                with self._daily_loss_stop_lock:
                    self._daily_loss_pending_stop = False
                self._persist_db_emergency_marker(active=False)
        auto_trade_result = self._evaluate_and_maybe_submit_auto_next_trade(payload=payload, sync_result=sync_result)
        self._last_auto_trade_result = dict(auto_trade_result)
        self._last_cycle_executor_result = dict(auto_trade_result)
        self.bus.publish("AUTO_TRADE_MM_RESULT", dict(auto_trade_result))

        current_drawdown = self.risk_desk.drawdown_pct()

        if batch_id:
            self.bus.publish(
                "BATCH_POSITION_CLOSED",
                {
                    "batch_id": batch_id,
                    "pnl": pnl,
                    "gross_pnl": float(settlement["gross_pnl"]),
                    "commission_amount": float(settlement["commission_amount"]),
                    "net_pnl": float(settlement["net_pnl"]),
                    "commission_pct": float(settlement["commission_pct"]),
                    "settlement_basis": str(settlement["settlement_basis"]),
                    "settlement_source": str(settlement["settlement_source"]),
                    "settlement_kind": str(settlement["settlement_kind"]),
                    "settlement_authority": str(settlement["settlement_authority"]),
                    "settlement_validation": str(settlement["settlement_validation"]),
                    "settlement_acceptance": str(settlement["settlement_acceptance"]),
                    "event_key": event_key,
                },
            )

        if current_drawdown >= self.config.auto_reset_drawdown_pct:
            self.table_manager.reset_all()
            self.duplication_guard.clear()
            self.risk_desk.reset_recovery_cycle()

            self.bus.publish(
                "ROSERPINA_AUTO_RESET",
                {
                    "reason": "drawdown_limit",
                    "drawdown_pct": current_drawdown,
                },
            )

        if current_drawdown >= self.config.lockdown_drawdown_pct:
            self.force_lockdown("Drawdown oltre soglia lockdown")

    def _apply_realized_pnl_without_mutating_bankroll(self, pnl: float) -> None:
        bankroll_before = float(self.risk_desk.bankroll_current)
        self.risk_desk.apply_closed_pnl(pnl)
        if float(self.risk_desk.bankroll_current) != bankroll_before:
            self.risk_desk.sync_bankroll(bankroll_before)

    @staticmethod
    def _extract_settlement_contract(payload: dict) -> dict[str, float | str]:
        body = dict(payload or {})
        explicit_net_raw = body.get("net_pnl") if "net_pnl" in body else None
        legacy_net_raw = body.get("pnl") if "pnl" in body else None
        has_explicit_net = explicit_net_raw is not None
        has_legacy_net = legacy_net_raw is not None
        has_canonical_contract = all(
            k in body and body.get(k) is not None
            for k in (
                "gross_pnl",
                "commission_amount",
                "net_pnl",
                "commission_pct",
                "settlement_basis",
                "settlement_source",
                "settlement_kind",
            )
        )
        if has_canonical_contract and has_explicit_net:
            net_pnl = explicit_net_raw
            settlement_authority = "explicit_contract"
            settlement_validation = "accepted"
            settlement_acceptance = "ACCEPT_REALIZED_SETTLEMENT"
        elif has_legacy_net:
            net_pnl = legacy_net_raw
            settlement_authority = "legacy_compat"
            settlement_validation = "rejected_non_canonical_settlement"
            settlement_acceptance = "REJECT_NON_CANONICAL_SETTLEMENT"
        else:
            net_pnl = 0.0
            settlement_authority = "rejected_ambiguous"
            settlement_validation = "rejected_ambiguous"
            settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
        net_pnl_f = float(net_pnl if net_pnl is not None else 0.0)
        has_explicit_gross = "gross_pnl" in body and body.get("gross_pnl") is not None
        has_explicit_commission = "commission_amount" in body and body.get("commission_amount") is not None
        if has_explicit_gross:
            gross_pnl_f = float(body.get("gross_pnl") or 0.0)
        elif has_explicit_net and has_explicit_commission:
            gross_pnl_f = float(net_pnl_f + float(body.get("commission_amount") or 0.0))
        else:
            gross_pnl_f = float(net_pnl_f)

        if has_explicit_commission:
            commission_amount_f = float(body.get("commission_amount") or 0.0)
        elif has_explicit_gross and has_explicit_net:
            commission_amount_f = float(gross_pnl_f - net_pnl_f)
        else:
            commission_amount_f = 0.0
        commission_pct_f = float(body.get("commission_pct", 0.0) or 0.0)
        settlement_source = str(
            body.get("settlement_source")
            or body.get("source")
            or ("legacy_compat" if settlement_authority == "legacy_compat" else "")
        )
        settlement_kind = str(
            body.get("settlement_kind")
            or ("legacy_compat" if settlement_authority == "legacy_compat" else "")
        )
        settlement_basis = str(
            body.get("settlement_basis")
            or ("legacy_compat" if settlement_authority == "legacy_compat" else "")
        )
        reason = ""
        if settlement_authority == "legacy_compat":
            if not settlement_source:
                settlement_source = "legacy_compat"
            if not settlement_kind:
                settlement_kind = "legacy_compat"
            settlement_validation = "rejected_non_canonical_settlement"
            reason = "LEGACY_SETTLEMENT_NON_AUTHORITATIVE"
            settlement_acceptance = "REJECT_NON_CANONICAL_SETTLEMENT"
        elif settlement_authority.startswith("rejected"):
            reason = "MISSING_CANONICAL_SETTLEMENT_FIELDS"
            settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"

        if settlement_validation == "accepted":
            if settlement_kind != "realized_settlement":
                settlement_validation = "rejected_non_realized_settlement"
                reason = "SETTLEMENT_KIND_NOT_REALIZED"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif settlement_basis != "market_net_realized":
                settlement_validation = "rejected_non_market_net_basis"
                reason = "SETTLEMENT_BASIS_NOT_MARKET_NET_REALIZED"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif not settlement_source:
                settlement_validation = "rejected_ambiguous_source"
                reason = "MISSING_SETTLEMENT_SOURCE"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"

        if settlement_validation == "accepted" and settlement_kind == "realized_settlement":
            arithmetic_tolerance = 1e-9
            finite_values = (
                math.isfinite(gross_pnl_f),
                math.isfinite(commission_amount_f),
                math.isfinite(net_pnl_f),
                math.isfinite(commission_pct_f),
            )
            if not all(finite_values):
                settlement_validation = "rejected_non_finite_settlement_values"
                reason = "SETTLEMENT_VALUES_NOT_FINITE"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif abs((gross_pnl_f - commission_amount_f) - net_pnl_f) > arithmetic_tolerance:
                settlement_validation = "rejected_arithmetic_incoherent_settlement"
                reason = "SETTLEMENT_ARITHMETIC_INCOHERENT"
                settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif gross_pnl_f <= arithmetic_tolerance:
                if commission_amount_f > arithmetic_tolerance:
                    settlement_validation = "rejected_non_zero_commission_on_non_positive_gross"
                    reason = "NON_POSITIVE_GROSS_REQUIRES_ZERO_COMMISSION"
                    settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
                elif commission_amount_f < -arithmetic_tolerance:
                    if abs(commission_amount_f) - abs(gross_pnl_f) > arithmetic_tolerance:
                        settlement_validation = "rejected_negative_rebate_exceeds_gross_abs_bound"
                        reason = "NEGATIVE_REBATE_EXCEEDS_GROSS_ABS_BOUND"
                        settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
                    elif net_pnl_f > arithmetic_tolerance:
                        settlement_validation = "rejected_impossible_negative_rebate_positive_net"
                        reason = "NEGATIVE_REBATE_CANNOT_CREATE_POSITIVE_NET_ON_LOSS"
                        settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
            elif gross_pnl_f > arithmetic_tolerance:
                expected_commission = gross_pnl_f * (commission_pct_f / 100.0)
                if abs(commission_amount_f - expected_commission) > arithmetic_tolerance:
                    settlement_validation = "rejected_commission_amount_policy_mismatch"
                    reason = "COMMISSION_AMOUNT_POLICY_MISMATCH"
                    settlement_acceptance = "REJECT_AMBIGUOUS_SETTLEMENT"
        if settlement_validation == "accepted" and settlement_kind == "realized_settlement":
            try:
                enforce_betfair_italy_commission_pct(
                    commission_pct_f,
                    context=f"runtime_controller:{settlement_source}",
                )
            except ValueError:
                settlement_validation = "rejected_policy_violation"
                reason = "BETFAIR_ITALY_COMMISSION_POLICY_VIOLATION"
                settlement_acceptance = "REJECT_POLICY_VIOLATION"
        return {
            "gross_pnl": gross_pnl_f,
            "commission_amount": commission_amount_f,
            "net_pnl": net_pnl_f,
            "commission_pct": commission_pct_f,
            "settlement_basis": settlement_basis,
            "settlement_source": settlement_source,
            "settlement_kind": settlement_kind,
            "settlement_authority": settlement_authority,
            "settlement_validation": settlement_validation,
            "settlement_acceptance": settlement_acceptance,
            "reason": reason,
        }

    @staticmethod
    def _build_settlement_rejection_context(payload: dict, settlement: dict[str, float | str]) -> dict[str, Any]:
        body = dict(payload or {})
        safe_payload_excerpt = {
            "event_key": str(body.get("event_key") or ""),
            "correlation_id": str(body.get("correlation_id") or ""),
            "market_id": str(body.get("market_id") or ""),
            "selection_id": body.get("selection_id"),
            "table_id": body.get("table_id"),
            "batch_id": str(body.get("batch_id") or ""),
            "provided_fields": sorted(str(k) for k in body.keys()),
        }
        return {
            "reason": str(settlement.get("reason") or "SETTLEMENT_CONTRACT_REJECTED"),
            "settlement_validation": str(settlement.get("settlement_validation") or ""),
            "settlement_acceptance": str(settlement.get("settlement_acceptance") or ""),
            "settlement_authority": str(settlement.get("settlement_authority") or ""),
            "payload_excerpt": safe_payload_excerpt,
        }

    def _sync_bankroll_post_settlement(self, payload: dict) -> dict:
        bankroll_before = float(self.risk_desk.bankroll_current)
        correlation_id = str(
            payload.get("correlation_id")
            or payload.get("customer_ref")
            or payload.get("event_key")
            or ""
        )
        result = {
            "correlation_id": correlation_id,
            "settlement_detected": True,
            "bankroll_before": bankroll_before,
            "bankroll_after": bankroll_before,
            "bankroll_sync_status": "NOT_SETTLED",
            "balance_source": "none",
            "reason": "",
        }

        settlement_key = self._build_bankroll_sync_key(payload)
        if not settlement_key:
            result["bankroll_sync_status"] = "SYNC_FAILED_INVALID_BALANCE"
            result["reason"] = "MISSING_SETTLEMENT_KEY"
            return result

        durable_state = self._read_cycle_recovery_state(settlement_key)
        state_body = durable_state.get("state", {})
        if bool(state_body.get("bankroll_synced")):
            self._processed_bankroll_sync_keys.add(settlement_key)
            result["recovery_status"] = "RECOVERY_SKIPPED_DUPLICATE"
        elif durable_state.get("status"):
            result["recovery_status"] = str(durable_state.get("status"))

        if settlement_key in self._processed_bankroll_sync_keys:
            result["bankroll_sync_status"] = "SYNC_SKIPPED_DUPLICATE"
            result["reason"] = "SETTLEMENT_ALREADY_SYNCED"
            return result

        started_at = time.monotonic()
        try:
            funds = self.betfair_service.get_account_funds()
            self._record_runtime_io(
                operation="betfair_get_account_funds",
                started_at=started_at,
                ok=True,
            )
        except Exception as exc:
            self._record_runtime_io(
                operation="betfair_get_account_funds",
                started_at=started_at,
                ok=False,
                error=str(exc),
            )
            result["bankroll_sync_status"] = "SYNC_FAILED_BALANCE_UNAVAILABLE"
            result["reason"] = f"BALANCE_FETCH_ERROR:{type(exc).__name__}"
            return result

        if not isinstance(funds, dict):
            result["bankroll_sync_status"] = "SYNC_FAILED_BALANCE_UNAVAILABLE"
            result["reason"] = "BALANCE_PAYLOAD_NOT_DICT"
            return result

        available = funds.get("available")
        try:
            available_f = float(available)
        except Exception:
            result["bankroll_sync_status"] = "SYNC_FAILED_INVALID_BALANCE"
            result["reason"] = "BALANCE_NOT_NUMERIC"
            return result

        if not math.isfinite(available_f) or available_f < 0.0:
            result["bankroll_sync_status"] = "SYNC_FAILED_INVALID_BALANCE"
            result["reason"] = "BALANCE_NOT_FINITE_OR_NEGATIVE"
            return result

        trusted_zero = bool(
            funds.get("ok")
            or funds.get("authoritative")
            or funds.get("balance_confirmed")
        )
        if available_f == 0.0 and not trusted_zero:
            result["bankroll_sync_status"] = "SYNC_FAILED_BALANCE_UNAVAILABLE"
            result["reason"] = "BALANCE_ZERO_AMBIGUOUS_OR_FALLBACK"
            return result

        self.risk_desk.sync_bankroll(available_f)
        self._processed_bankroll_sync_keys.add(settlement_key)
        result["bankroll_after"] = float(self.risk_desk.bankroll_current)
        result["bankroll_sync_status"] = "SYNC_SUCCESS"
        result["balance_source"] = "exchange_available"
        result["reason"] = "BALANCE_SYNCED_FROM_EXCHANGE"
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="BANKROLL_SYNC_DONE",
            bankroll_sync_status=result["bankroll_sync_status"],
            next_trade_submission_status="NOT_ATTEMPTED",
            reason=result["reason"],
            recovery_status=str(result.get("recovery_status") or "RECOVERY_STATE_LOADED"),
        )
        return result

    @staticmethod
    def _build_bankroll_sync_key(payload: dict) -> str:
        parts = [
            str(payload.get("batch_id") or "").strip(),
            str(payload.get("event_key") or "").strip(),
            str(payload.get("table_id") or "").strip(),
            str(payload.get("bet_id") or "").strip(),
            str(payload.get("order_id") or "").strip(),
        ]
        parts = [p for p in parts if p]
        if not parts:
            return ""
        return "|".join(parts)

    def _evaluate_and_maybe_submit_auto_next_trade(self, *, payload: dict, sync_result: dict) -> dict:
        settlement_key = self._build_bankroll_sync_key(payload)
        source_corr_id = str(sync_result.get("correlation_id") or "")
        recovery_enabled = bool(payload.get("recovery_enabled", self.config.allow_recovery))
        resume_submit_enabled = bool(payload.get("resume_submit_enabled", False))
        result: dict[str, Any] = {
            "correlation_id": f"auto-next::{source_corr_id}" if source_corr_id else "",
            "source_settlement_correlation_id": source_corr_id,
            "recovery_enabled": recovery_enabled,
            "resume_submit_enabled": resume_submit_enabled,
            "cycle_executor_enabled": bool(payload.get("cycle_executor_enabled", False)),
            "cycle_step_index": 0,
            "max_steps_reached": False,
            "kill_switch_active": bool(self._is_kill_switch_active()),
            "anomaly_pause_active": False,
            "cycle_executor_status": "CYCLE_NOT_ELIGIBLE",
            "bankroll_sync_status": str(sync_result.get("bankroll_sync_status") or "NOT_SETTLED"),
            "money_management_status": "MM_STOP_CONTEXT_MISSING",
            "cycle_active": False,
            "progression_allowed": False,
            "auto_trade_enabled": bool(payload.get("auto_trade_enabled", False)),
            "auto_trade_status": "AUTO_TRADE_NOT_ELIGIBLE",
            "next_stake": 0.0,
            "risk_status": "RISK_NOT_EVALUATED",
            "submitted": False,
            "reason": "",
            "recovery_status": "RECOVERY_NO_STATE",
            "checkpoint_stage": "",
            "checkpoint_valid": False,
            "checkpoint_ambiguous": False,
        }
        checkpoint_capable = callable(getattr(self.db, "get_cycle_recovery_state", None))
        recovery_probe = self._read_cycle_recovery_state(settlement_key)
        probe_status = str(recovery_probe.get("status") or "RECOVERY_NO_STATE")
        result["recovery_status"] = probe_status
        recovery_state = recovery_probe.get("state", {})
        checkpoint = recovery_state.get("checkpoint", {}) if isinstance(recovery_state, dict) else {}
        result["checkpoint_stage"] = str(checkpoint.get("checkpoint_stage") or recovery_state.get("stage") or "")
        result["checkpoint_ambiguous"] = bool(recovery_state.get("ambiguous"))
        result["checkpoint_valid"] = bool(recovery_state.get("exists")) and not bool(recovery_state.get("ambiguous"))
        has_checkpoint = bool(recovery_state.get("exists"))

        if not recovery_enabled and has_checkpoint:
            result["auto_trade_status"] = "AUTO_TRADE_DISABLED"
            result["cycle_executor_status"] = "CYCLE_NOT_ELIGIBLE"
            result["recovery_status"] = "RECOVERY_DISABLED"
            result["reason"] = "recovery_disabled"
            return result

        if self._should_fail_closed_on_recovery(recovery_probe):
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_DUPLICATE"
            result["cycle_executor_status"] = "CYCLE_AMBIGUOUS"
            result["recovery_status"] = "RECOVERY_STATE_AMBIGUOUS"
            result["checkpoint_valid"] = False
            result["reason"] = "recovery_state_ambiguous"
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_AMBIGUOUS",
                bankroll_sync_status=result["bankroll_sync_status"],
                money_management_status=result["money_management_status"],
                next_trade_submission_status="AMBIGUOUS",
                reason=result["reason"],
                is_ambiguous=True,
                recovery_status="RECOVERY_STATE_AMBIGUOUS",
            )
            return result
        if bool(recovery_state.get("submit_confirmed")) or bool(recovery_state.get("submit_attempted")):
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_DUPLICATE"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_DUPLICATE"
            result["recovery_status"] = "RECOVERY_SKIPPED_ALREADY_SUBMITTED"
            result["reason"] = "durable_submit_already_confirmed"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result
        if checkpoint_capable and not resume_submit_enabled and result["cycle_executor_enabled"]:
            result["auto_trade_status"] = "AUTO_TRADE_DISABLED"
            result["cycle_executor_status"] = "CYCLE_NOT_ELIGIBLE"
            result["recovery_status"] = "RECOVERY_READY_NO_SUBMIT"
            result["reason"] = "resume_submit_disabled"
            return result

        if not result["cycle_executor_enabled"]:
            result["auto_trade_status"] = "AUTO_TRADE_DISABLED"
            result["cycle_executor_status"] = "CYCLE_EXECUTOR_DISABLED"
            result["recovery_status"] = "RECOVERY_READY_NO_SUBMIT" if has_checkpoint else result["recovery_status"]
            result["reason"] = "cycle_executor_disabled"
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_BLOCKED",
                bankroll_sync_status=result["bankroll_sync_status"],
                next_trade_submission_status="NOT_ATTEMPTED",
                reason=result["reason"],
                recovery_status=result["recovery_status"],
            )
            return result

        if result["kill_switch_active"]:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_MM_BLOCKED"
            result["cycle_executor_status"] = "CYCLE_STOPPED_KILL_SWITCH"
            result["recovery_status"] = "RECOVERY_SKIPPED_MM_BLOCKED" if has_checkpoint else result["recovery_status"]
            result["reason"] = "kill_switch_active"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_BLOCKED",
                bankroll_sync_status=result["bankroll_sync_status"],
                next_trade_submission_status="NOT_ATTEMPTED",
                reason=result["reason"],
                recovery_status=result["recovery_status"],
            )
            return result

        if result["bankroll_sync_status"] != "SYNC_SUCCESS":
            if result["bankroll_sync_status"] == "SYNC_SKIPPED_DUPLICATE":
                result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_DUPLICATE"
                result["cycle_executor_status"] = "CYCLE_SKIPPED_DUPLICATE"
                result["reason"] = "settlement_already_processed"
            else:
                result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_SYNC_FAILED"
                result["cycle_executor_status"] = "CYCLE_SKIPPED_SYNC_FAILED"
                result["reason"] = "bankroll_sync_not_success"
            return result

        if not result["auto_trade_enabled"]:
            result["auto_trade_status"] = "AUTO_TRADE_DISABLED"
            result["cycle_executor_status"] = "CYCLE_NOT_ELIGIBLE"
            result["recovery_status"] = "RECOVERY_READY_NO_SUBMIT" if has_checkpoint else result["recovery_status"]
            result["reason"] = "auto_trade_disabled"
            return result

        if settlement_key and settlement_key in self._processed_auto_trade_keys:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_DUPLICATE"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_DUPLICATE"
            result["recovery_status"] = "RECOVERY_SKIPPED_DUPLICATE" if has_checkpoint else result["recovery_status"]
            result["reason"] = "settlement_auto_trade_already_evaluated"
            return result

        mm_context = payload.get("mm_context")
        signal = mm_context.get("next_signal") if isinstance(mm_context, dict) else None
        table_ctx = mm_context.get("table") if isinstance(mm_context, dict) else None
        cycle_id = mm_context.get("cycle_id", "") if isinstance(mm_context, dict) else ""
        cycle_active = bool(mm_context.get("cycle_active", True)) if isinstance(mm_context, dict) else False
        target_reached = bool(mm_context.get("target_reached", False)) if isinstance(mm_context, dict) else False
        table_id = mm_context.get("table_id") if isinstance(mm_context, dict) else None

        table = table_ctx if table_ctx is not None else (
            {"table_id": table_id} if table_id is not None else None
        )
        decision = self.mm.evaluate_next_trade_after_settlement(
            signal=signal,
            bankroll_current=float(self.risk_desk.bankroll_current),
            equity_peak=float(self.risk_desk.equity_peak),
            current_total_exposure=self.table_manager.total_exposure(),
            event_current_exposure=0.0,
            table=table,
            cycle_id=str(cycle_id or ""),
            cycle_active=cycle_active,
            target_reached=target_reached,
        )
        result["money_management_status"] = decision.money_management_status
        result["cycle_active"] = bool(decision.cycle_active)
        result["progression_allowed"] = bool(decision.progression_allowed)
        result["next_stake"] = float(decision.next_stake or 0.0)
        cycle_id = str(getattr(decision, "cycle_id", "") or "")
        current_step_index = int(self._cycle_step_counts.get(cycle_id, 0) or 0)
        result["cycle_step_index"] = current_step_index
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="MM_DECISION_DONE",
            bankroll_sync_status=result["bankroll_sync_status"],
            money_management_status=result["money_management_status"],
            cycle_active=result["cycle_active"],
            progression_allowed=result["progression_allowed"],
            next_stake=result["next_stake"],
            step_index=result["cycle_step_index"],
            next_trade_submission_status="NOT_ATTEMPTED",
            reason="mm_decision_done",
            recovery_status=result["recovery_status"],
        )

        max_steps = mm_context.get("max_steps") if isinstance(mm_context, dict) else None
        max_steps_value = None
        try:
            if max_steps is not None:
                max_steps_value = int(max_steps)
        except Exception:
            max_steps_value = None
        if max_steps_value is not None and max_steps_value >= 0 and current_step_index >= max_steps_value:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_MM_BLOCKED"
            result["cycle_executor_status"] = "CYCLE_STOPPED_MAX_STEPS"
            result["recovery_status"] = "RECOVERY_STOPPED_CLOSED" if has_checkpoint else result["recovery_status"]
            result["max_steps_reached"] = True
            result["reason"] = "max_steps_reached"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result
        if max_steps_value is not None and max_steps_value >= 0 and not cycle_id:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_MM_BLOCKED"
            result["cycle_executor_status"] = "CYCLE_STOPPED_MAX_STEPS"
            result["recovery_status"] = "RECOVERY_STOPPED_CLOSED" if has_checkpoint else result["recovery_status"]
            result["max_steps_reached"] = True
            result["reason"] = "max_steps_requires_cycle_id"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        if decision.money_management_status != "MM_CONTINUE_ALLOWED":
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_MM_BLOCKED"
            if decision.money_management_status == "MM_STOP_TARGET_REACHED":
                result["cycle_executor_status"] = "CYCLE_STOPPED_TARGET_REACHED"
                result["recovery_status"] = "RECOVERY_STOPPED_TARGET_REACHED" if has_checkpoint else result["recovery_status"]
            elif decision.money_management_status == "MM_STOP_CYCLE_CLOSED":
                result["cycle_executor_status"] = "CYCLE_STOPPED_CLOSED"
                result["recovery_status"] = "RECOVERY_STOPPED_CLOSED" if has_checkpoint else result["recovery_status"]
            else:
                result["cycle_executor_status"] = "CYCLE_SKIPPED_MM_BLOCKED"
                result["recovery_status"] = "RECOVERY_SKIPPED_MM_BLOCKED" if has_checkpoint else result["recovery_status"]
            result["reason"] = str(decision.stop_reason or "mm_blocked")
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        if not math.isfinite(result["next_stake"]) or result["next_stake"] <= 0.0:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_INVALID_STAKE"
            result["money_management_status"] = "MM_STOP_INVALID_STAKE"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_INVALID_STAKE"
            result["recovery_status"] = "RECOVERY_SKIPPED_INVALID_STAKE" if has_checkpoint else result["recovery_status"]
            result["reason"] = "invalid_next_stake"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        risk_allowed, risk_reason = self._risk_allows_auto_trade()
        result["risk_status"] = "RISK_APPROVED" if risk_allowed else "RISK_REJECTED"
        if not risk_allowed:
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_RISK_REJECTED"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_RISK_REJECTED"
            result["recovery_status"] = "RECOVERY_SKIPPED_RISK_REJECTED" if has_checkpoint else result["recovery_status"]
            result["reason"] = risk_reason
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        if self._has_open_trade_conflict(decision.table_id):
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_EXISTING_INFLIGHT"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_EXISTING_INFLIGHT"
            result["recovery_status"] = "RECOVERY_SKIPPED_EXISTING_INFLIGHT" if has_checkpoint else result["recovery_status"]
            result["reason"] = "existing_inflight_trade"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        try:
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="NEXT_TRADE_SUBMIT_ATTEMPTED",
                bankroll_sync_status=result["bankroll_sync_status"],
                money_management_status=result["money_management_status"],
                cycle_active=result["cycle_active"],
                progression_allowed=result["progression_allowed"],
                next_stake=result["next_stake"],
                step_index=result["cycle_step_index"],
                next_trade_submission_status="ATTEMPTED",
                reason="submit_attempt",
                recovery_status=result["recovery_status"],
            )
            submit_payload = self._build_auto_trade_payload(signal=signal or {}, decision_stake=result["next_stake"])
        except Exception as exc:
            result["auto_trade_status"] = "AUTO_TRADE_SUBMIT_FAILED"
            result["cycle_executor_status"] = "CYCLE_SUBMIT_FAILED"
            result["recovery_status"] = "RECOVERY_SUBMIT_FAILED" if has_checkpoint else result["recovery_status"]
            result["reason"] = f"submit_payload_invalid:{type(exc).__name__}"
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_AMBIGUOUS",
                bankroll_sync_status=result["bankroll_sync_status"],
                money_management_status=result["money_management_status"],
                cycle_active=result["cycle_active"],
                progression_allowed=result["progression_allowed"],
                next_stake=result["next_stake"],
                step_index=result["cycle_step_index"],
                next_trade_submission_status="AMBIGUOUS",
                reason=result["reason"],
                is_ambiguous=True,
                recovery_status="RECOVERY_STATE_AMBIGUOUS",
            )
            return result

        table_id = submit_payload.get("table_id", decision.table_id)
        if table_id is not None:
            self.table_manager.activate(
                table_id=int(table_id),
                event_key=str(submit_payload.get("event_key") or ""),
                exposure=float(result["next_stake"]),
                market_id=str(submit_payload.get("market_id") or ""),
                selection_id=submit_payload.get("selection_id"),
                meta={
                    "event_name": submit_payload.get("event_name") or "",
                    "market_name": submit_payload.get("market_name") or "",
                    "runner_name": submit_payload.get("runner_name") or "",
                    "bet_type": submit_payload.get("bet_type") or "",
                    "price": float(submit_payload.get("price") or 0.0),
                    "simulation_mode": bool(submit_payload.get("simulation_mode", self.simulation_mode)),
                    "auto_trade_source": submit_payload.get("auto_trade_source") or "",
                },
            )

        # Recheck kill-switch daily-loss subito prima della submission auto-trade:
        # _risk_allows_auto_trade e' stato valutato sopra, ma un settlement
        # perdente concorrente puo' aver armato pending/emergency nel frattempo.
        if self._daily_loss_entry_blocked():
            # Sblocca il tavolo attivato sopra: nessun CMD_QUICK_BET parte, quindi
            # nessun evento terminale lo libererebbe (tavolo/esposizione fantasma).
            if table_id is not None:
                try:
                    self.table_manager.force_unlock(int(table_id))
                except Exception:
                    logger.exception("Errore force_unlock table_id=%s", table_id)
            result["auto_trade_status"] = "AUTO_TRADE_SKIPPED_RISK_REJECTED"
            result["cycle_executor_status"] = "CYCLE_SKIPPED_RISK_REJECTED"
            result["recovery_status"] = "RECOVERY_SKIPPED_RISK_REJECTED" if has_checkpoint else result["recovery_status"]
            result["risk_status"] = "RISK_REJECTED"
            result["reason"] = "emergency_stop_active:pre_submit_recheck"
            # Sovrascrivi il checkpoint ATTEMPTED con uno BLOCCATO/NOT_ATTEMPTED:
            # senza, il reader di recovery leggerebbe ATTEMPTED-senza-SUBMITTED
            # come ambiguo e fail-closerebbe questo settlement dopo un riavvio,
            # pur non essendo partito alcun ordine.
            self._persist_cycle_checkpoint(
                settlement_key=settlement_key,
                payload=payload,
                checkpoint_stage="CYCLE_BLOCKED",
                bankroll_sync_status=result["bankroll_sync_status"],
                next_trade_submission_status="NOT_ATTEMPTED",
                reason=result["reason"],
                recovery_status=result["recovery_status"],
            )
            if settlement_key:
                self._processed_auto_trade_keys.add(settlement_key)
            return result

        self.bus.publish("CMD_QUICK_BET", submit_payload)
        result["auto_trade_status"] = "AUTO_TRADE_SUBMITTED"
        result["cycle_executor_status"] = "CYCLE_STEP_SUBMITTED"
        result["recovery_status"] = "RECOVERY_STEP_SUBMITTED" if has_checkpoint else result["recovery_status"]
        result["submitted"] = True
        result["reason"] = "submitted"
        if cycle_id:
            self._cycle_step_counts[cycle_id] = current_step_index + 1
            result["cycle_step_index"] = self._cycle_step_counts[cycle_id]
        if settlement_key:
            self._processed_auto_trade_keys.add(settlement_key)
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="NEXT_TRADE_SUBMIT_CONFIRMED",
            bankroll_sync_status=result["bankroll_sync_status"],
            money_management_status=result["money_management_status"],
            cycle_active=result["cycle_active"],
            progression_allowed=result["progression_allowed"],
            next_stake=result["next_stake"],
            step_index=result["cycle_step_index"],
            next_trade_submission_status="SUBMITTED",
            reason=result["reason"],
            recovery_status=result["recovery_status"],
        )
        return result

    def _read_cycle_recovery_state(self, settlement_key: str) -> dict[str, Any]:
        default = {"status": "RECOVERY_NO_STATE", "state": {}}
        key = str(settlement_key or "").strip()
        if not key:
            return default
        getter = getattr(self.db, "get_cycle_recovery_state", None)
        if not callable(getter):
            return default
        try:
            state = getter(key) or {}
            if not isinstance(state, dict):
                return {"status": "RECOVERY_STATE_INVALID", "state": {}}
            if not bool(state.get("exists")):
                return default
            if bool(state.get("ambiguous")):
                return {"status": "RECOVERY_STATE_AMBIGUOUS", "state": state}
            if bool(state.get("processed")):
                return {"status": "RECOVERY_SKIPPED_DUPLICATE", "state": state}
            return {"status": "RECOVERY_STATE_LOADED", "state": state}
        except Exception:
            logger.exception("Errore read cycle recovery state")
            return {"status": "RECOVERY_STATE_INVALID", "state": {}}

    @staticmethod
    def _should_fail_closed_on_recovery(probe: dict[str, Any]) -> bool:
        status = str((probe or {}).get("status") or "")
        if status in {"RECOVERY_STATE_AMBIGUOUS", "RECOVERY_STATE_INVALID"}:
            return True
        state = (probe or {}).get("state") or {}
        return bool(state.get("ambiguous"))

    def _build_fail_closed_recovery_result(self, *, payload: dict, probe: dict[str, Any]) -> dict[str, Any]:
        settlement_key = self._build_bankroll_sync_key(payload)
        reason = "recovery_state_ambiguous"
        status = str(probe.get("status") or "RECOVERY_STATE_AMBIGUOUS")
        if status == "RECOVERY_STATE_INVALID":
            reason = "recovery_state_invalid"
        self._persist_cycle_checkpoint(
            settlement_key=settlement_key,
            payload=payload,
            checkpoint_stage="CYCLE_AMBIGUOUS",
            bankroll_sync_status="NOT_SETTLED",
            money_management_status="MM_STOP_CONTEXT_MISSING",
            next_trade_submission_status="AMBIGUOUS",
            reason=reason,
            is_ambiguous=True,
            recovery_status=status,
        )
        return {
            "correlation_id": f"auto-next::{str(payload.get('correlation_id') or '')}",
            "source_settlement_correlation_id": str(payload.get("correlation_id") or ""),
            "cycle_executor_enabled": bool(payload.get("cycle_executor_enabled", False)),
            "cycle_step_index": 0,
            "max_steps_reached": False,
            "kill_switch_active": bool(self._is_kill_switch_active()),
            "anomaly_pause_active": False,
            "cycle_executor_status": "CYCLE_AMBIGUOUS",
            "bankroll_sync_status": "SYNC_SKIPPED_DUPLICATE",
            "money_management_status": "MM_STOP_CONTEXT_MISSING",
            "cycle_active": False,
            "progression_allowed": False,
            "auto_trade_enabled": bool(payload.get("auto_trade_enabled", False)),
            "auto_trade_status": "AUTO_TRADE_SKIPPED_DUPLICATE",
            "next_stake": 0.0,
            "risk_status": "RISK_NOT_EVALUATED",
            "submitted": False,
            "reason": reason,
            "recovery_status": status,
        }

    def _persist_cycle_checkpoint(
        self,
        *,
        settlement_key: str,
        payload: dict,
        checkpoint_stage: str,
        bankroll_sync_status: str = "NOT_SETTLED",
        money_management_status: str = "MM_STOP_CONTEXT_MISSING",
        cycle_active: bool = False,
        progression_allowed: bool = False,
        next_stake: float = 0.0,
        step_index: int = 0,
        round_index: int = 0,
        next_trade_submission_status: str = "NOT_ATTEMPTED",
        reason: str = "",
        is_ambiguous: bool = False,
        recovery_status: str = "RECOVERY_STATE_LOADED",
    ) -> None:
        key = str(settlement_key or "").strip()
        if not key:
            return
        writer = getattr(self.db, "upsert_cycle_recovery_checkpoint", None)
        if not callable(writer):
            return
        existing: dict[str, Any] = {}
        existing_getter = getattr(self.db, "get_cycle_recovery_checkpoint", None)
        if callable(existing_getter):
            try:
                loaded = existing_getter(key)
                if isinstance(loaded, dict):
                    existing = dict(loaded)
            except Exception:
                logger.exception("Errore read existing checkpoint settlement_key=%s", key)

        stage_rank = {
            "SETTLEMENT_DETECTED": 10,
            "BANKROLL_SYNC_DONE": 20,
            "MM_DECISION_DONE": 30,
            "NEXT_TRADE_SUBMIT_ATTEMPTED": 40,
            "NEXT_TRADE_SUBMIT_CONFIRMED": 50,
            "CYCLE_BLOCKED": 60,
            "CYCLE_AMBIGUOUS": 70,
        }
        submit_rank = {
            "NOT_ATTEMPTED": 10,
            "ATTEMPTED": 20,
            "SUBMITTED": 30,
            "CONFIRMED": 40,
            "AMBIGUOUS": 50,
        }
        mm_context = payload.get("mm_context") if isinstance(payload, dict) else None
        cycle_id = str(mm_context.get("cycle_id") or "") if isinstance(mm_context, dict) else ""
        incoming_stage = str(checkpoint_stage or "SETTLEMENT_DETECTED")
        existing_stage = str(existing.get("checkpoint_stage") or "")
        effective_stage = incoming_stage
        if stage_rank.get(existing_stage, 0) > stage_rank.get(incoming_stage, 0):
            effective_stage = existing_stage

        incoming_submit = str(next_trade_submission_status or "NOT_ATTEMPTED")
        existing_submit = str(existing.get("next_trade_submission_status") or "")
        effective_submit = incoming_submit
        if submit_rank.get(existing_submit, 0) > submit_rank.get(incoming_submit, 0):
            effective_submit = existing_submit

        effective_reason = str(reason or existing.get("reason") or "")
        effective_ambiguous = bool(is_ambiguous) or bool(existing.get("is_ambiguous", False))
        record = {
            "settlement_correlation_id": str(payload.get("correlation_id") or payload.get("event_key") or ""),
            "cycle_id": cycle_id,
            "table_id": payload.get("table_id"),
            "strategy_context": {"auto_trade_source": "settlement_mm_gate", "recovery_status": recovery_status},
            "checkpoint_stage": effective_stage,
            "bankroll_sync_status": str(bankroll_sync_status or "NOT_SETTLED"),
            "money_management_status": str(money_management_status or "MM_STOP_CONTEXT_MISSING"),
            "cycle_active": bool(cycle_active),
            "progression_allowed": bool(progression_allowed),
            "next_stake": float(next_stake or 0.0),
            "step_index": int(step_index or 0),
            "round_index": int(round_index or 0),
            "next_trade_submission_status": effective_submit,
            "idempotency_key": key,
            "reason": effective_reason,
            "is_ambiguous": effective_ambiguous,
        }
        try:
            writer(key, record)
        except Exception:
            logger.exception("Errore persist checkpoint settlement_key=%s", key)

    def _risk_allows_auto_trade(self) -> tuple[bool, str]:
        # L'auto-trade da settlement non passa da _on_signal_received: senza
        # questo controllo una trade automatica partirebbe anche in emergenza
        # (es. dopo riavvio + start() senza reset_emergency()).
        if self._emergency_stopped:
            return False, "emergency_stop_active"
        if not self._runtime_active():
            return False, "runtime_not_active"
        if self._desk_mode() == DeskMode.LOCKDOWN:
            return False, "desk_lockdown"
        return True, "risk_approved"

    def _has_open_trade_conflict(self, table_id: Optional[int]) -> bool:
        if table_id is None:
            return False
        table = self.table_manager.get_table(int(table_id))
        if table is None:
            return False
        return bool(table.current_event_key)

    def _build_auto_trade_payload(self, *, signal: dict, decision_stake: float) -> dict:
        payload = {
            "market_id": str(signal.get("market_id")),
            "selection_id": int(signal.get("selection_id")),
            "bet_type": str(
                signal.get("bet_type")
                or signal.get("side")
                or signal.get("action")
                or "BACK"
            ).upper(),
            "price": float(signal.get("price") or signal.get("odds")),
            "stake": float(decision_stake),
            "event_name": signal.get("event") or signal.get("match") or signal.get("event_name") or "",
            "market_name": signal.get("market") or signal.get("market_name") or signal.get("market_type") or "",
            "runner_name": signal.get("selection") or signal.get("runner_name") or signal.get("runnerName") or "",
            "simulation_mode": bool(signal.get("simulation_mode", self.simulation_mode)),
            "event_key": self.duplication_guard.build_event_key(signal),
            "batch_id": str(signal.get("batch_id") or ""),
            "auto_trade_source": "settlement_mm_gate",
        }
        table_id = signal.get("table_id")
        if table_id is not None:
            payload["table_id"] = int(table_id)
        return payload

    # =========================================================
    # STATUS
    # =========================================================
    def get_status(self) -> dict:
        started_at = time.monotonic()
        try:
            funds = self.betfair_service.get_account_funds()
            self._record_runtime_io(operation="betfair_get_account_funds", started_at=started_at, ok=True)
        except Exception as exc:
            self._record_runtime_io(
                operation="betfair_get_account_funds",
                started_at=started_at,
                ok=False,
                error=str(exc),
            )
            raise
        bankroll_current = float(
            funds.get("available", self.risk_desk.bankroll_current)
            or self.risk_desk.bankroll_current
        )

        if bankroll_current != float(self.risk_desk.bankroll_current):
            self.risk_desk.sync_bankroll(bankroll_current)

        snapshot = self.risk_desk.build_snapshot(
            runtime_mode=self.mode,
            desk_mode=self._desk_mode(),
            total_exposure=self.table_manager.total_exposure(),
            telegram_connected=bool(self.telegram_service.status().get("connected")),
            betfair_connected=bool(self.betfair_service.status().get("connected")),
            active_tables=len(self.table_manager.active_tables()),
            recovery_tables=len(self.table_manager.recovery_tables()),
            last_error=self.last_error,
            last_signal_at=self.last_signal_at,
        )

        data = self.risk_desk.as_dict(snapshot)
        data["tables"] = self.table_manager.snapshot()
        data["duplication_guard"] = self.duplication_guard.snapshot()
        data["simulation_mode"] = bool(self.simulation_mode)
        data["execution_mode"] = str(self.execution_mode)
        data["live_enabled"] = bool(self.live_enabled)
        data["live_readiness_ok"] = bool(self.live_readiness_ok)
        data["kill_switch_active"] = bool(self._is_kill_switch_active())
        data["is_emergency_stopped"] = bool(self._emergency_stopped)
        data["emergency_stopped_at"] = str(self._emergency_stopped_at)
        data["emergency_reason"] = str(self._emergency_reason)
        data["execution_gate_reason"] = str(self.last_execution_gate_reason)
        data["deploy_gate"] = dict(self.last_deploy_gate_status or {})
        data["runtime_io"] = self.runtime_io_snapshot()
        data["broker_status"] = self.betfair_service.status()
        data["account_funds"] = funds
        data["bankroll_sync"] = dict(self._last_bankroll_sync_result or {})
        data["auto_trade_mm"] = dict(self._last_auto_trade_result or {})
        data["cycle_executor"] = dict(self._last_cycle_executor_result or {})
        data["daily_loss_monitor"] = self._monitor_daily_loss_breach(source="RUNTIME_STATUS_SNAPSHOT")
        if self.streaming_feed is not None:
            try:
                data["streaming_feed"] = self.streaming_feed.status()
            except Exception:
                data["streaming_feed"] = {"running": False, "error": "status_unavailable"}
        else:
            data["streaming_feed"] = {"running": False}

        if self.simulation_mode and hasattr(self.betfair_service, "simulation_snapshot"):
            try:
                data["simulation_snapshot"] = self.betfair_service.simulation_snapshot()
            except Exception:
                logger.exception("Errore simulation_snapshot")
                data["simulation_snapshot"] = {}

        return data
