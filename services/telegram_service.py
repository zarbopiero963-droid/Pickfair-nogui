from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from observability.telegram_health_probe import TelegramHealthProbe
from observability.telegram_invariant_guard import TelegramInvariantSnapshot
from recovery.telegram_autoheal import (
    TelegramAutohealAction,
    TelegramAutohealDecision,
    TelegramAutohealHistory,
    TelegramAutohealPolicy,
    TelegramAutohealSnapshot,
)
from telegram_listener import TelegramListener

logger = logging.getLogger(__name__)


class TelegramService:
    """
    Service Telegram runtime-safe.

    Responsabilità:
    - avvio / stop listener
    - inoltro segnali al bus
    - preserva simulation_mode nei payload
    - non contiene logica di trading
    """

    def __init__(self, settings_service, db, bus):
        self.settings_service = settings_service
        self.db = db
        self.bus = bus
        self.listener: Optional[TelegramListener] = None
        self.connected = False
        self.last_error = ""
        self.state = "CREATED"
        self.intentional_stop = False
        self.reconnect_attempts = 0
        self.reconnect_in_progress = False
        self.last_successful_message_ts: str | None = None
        self.listener_started = False
        self.handlers_registered = 0
        self.active_network_resources = 0
        self._health_probe = TelegramHealthProbe()
        self._autoheal_policy = TelegramAutohealPolicy()
        self._restart_attempts_total = 0
        self._restart_timestamps: list[float] = []
        self._lockout_active = False
        self._lockout_since_ts: float | None = None
        self._lockout_reason = ""
        self._last_restart_ts: float | None = None
        self._last_autoheal_action = TelegramAutohealAction.NO_ACTION.value
        self._last_autoheal_decision_reason = "not_evaluated"
        self._restart_in_progress = False

    def _set_state(self, new_state: str) -> None:
        allowed_states = {"CREATED", "CONNECTING", "CONNECTED", "RECONNECTING", "STOPPED", "FAILED"}
        if new_state not in allowed_states:
            raise ValueError(f"Invalid Telegram service state: {new_state}")
        self.state = new_state

    # =========================================================
    # INTERNAL CALLBACKS
    # =========================================================
    def _handle_signal(self, signal: dict) -> None:
        signal = dict(signal or {})
        signal["received_at"] = datetime.utcnow().isoformat()
        self.last_successful_message_ts = signal["received_at"]

        # conserva eventuale flag simulation_mode già presente
        signal["simulation_mode"] = bool(signal.get("simulation_mode", False))

        if hasattr(self.db, "save_received_signal"):
            try:
                self.db.save_received_signal(signal)
            except Exception as exc:
                logger.warning("save_received_signal fallita: %s", exc)

        self.bus.publish("SIGNAL_RECEIVED", signal)

    def _handle_status(self, *args) -> None:
        """
        Compatibile con callback:
        - on_status(message)
        - on_status(status, message)
        """
        if len(args) >= 2:
            status = str(args[0] or "")
            message = str(args[1] or "")
        elif len(args) == 1:
            status = "INFO"
            message = str(args[0] or "")
        else:
            status = "INFO"
            message = ""

        self.bus.publish(
            "TELEGRAM_STATUS",
            {
                "status": status,
                "message": message,
            },
        )

    def _refresh_runtime_truth_from_listener(self) -> None:
        status_getter = getattr(self.listener, "status", None) if self.listener else None
        if callable(status_getter):
            snap = status_getter() or {}
            self.state = str(snap.get("state") or self.state)
            listener_reconnect_attempts = int(snap.get("reconnect_attempts", 0) or 0)
            self.reconnect_attempts = max(self.reconnect_attempts, listener_reconnect_attempts)
            self.reconnect_in_progress = bool(snap.get("reconnect_in_progress", self.reconnect_in_progress))
            self.listener_started = bool(snap.get("listener_started", self.listener_started))
            self.handlers_registered = int(snap.get("handlers_registered", self.handlers_registered) or 0)
            self.active_network_resources = int(snap.get("active_network_resources", self.active_network_resources) or 0)
            if not self.last_error:
                self.last_error = str(snap.get("last_error") or "")
            if self.last_successful_message_ts is None:
                self.last_successful_message_ts = snap.get("last_successful_message_ts")
        self.connected = self.state == "CONNECTED"

    # =========================================================
    # LIFECYCLE
    # =========================================================
    def start(self) -> dict:
        cfg = self.settings_service.load_telegram_config()

        if not cfg.enabled:
            self.intentional_stop = True
            self._set_state("STOPPED")
            self.connected = False
            return {
                "started": False,
                "reason": "telegram_disabled",
                "state": self.state,
            }

        if not cfg.api_id or not cfg.api_hash:
            self.last_error = "Configurazione Telegram incompleta"
            self.intentional_stop = False
            self._set_state("FAILED")
            raise RuntimeError(self.last_error)

        if self.state in {"CONNECTING", "CONNECTED", "RECONNECTING"}:
            return {
                "started": True,
                "reason": "already_running",
                "chat_count": len(cfg.monitored_chat_ids),
                "state": self.state,
            }

        if self.listener and str(getattr(self.listener, "state", "")) in {"CONNECTING", "CONNECTED", "RECONNECTING"}:
            return {
                "started": True,
                "reason": "already_running",
                "chat_count": len(cfg.monitored_chat_ids),
                "state": self.state,
            }

        try:
            self.intentional_stop = False
            self.reconnect_in_progress = False
            self._set_state("CONNECTING")
            self.listener = TelegramListener(
                api_id=int(cfg.api_id),
                api_hash=cfg.api_hash,
                session_string=cfg.session_string or None,
            )

            self.listener.set_database(self.db)
            self.listener.set_monitored_chats(cfg.monitored_chat_ids)
            self.listener.set_callbacks(
                on_signal=self._handle_signal,
                on_status=self._handle_status,
            )
            self.handlers_registered = sum(
                1 for cb in (self._handle_signal, self._handle_status) if callable(cb)
            )

            start_result = self.listener.start()
            self.listener_started = bool(start_result.get("started", False))
            self.last_error = str(start_result.get("error") or "")
            self._refresh_runtime_truth_from_listener()
            if self.state == "CREATED":
                self._set_state("STOPPED")
            if self.last_error:
                self._set_state("FAILED")
                self.connected = False

            return {
                "started": bool(self.listener_started),
                "chat_count": len(cfg.monitored_chat_ids),
                "state": self.state,
                "connected": self.connected,
            }

        except Exception as exc:
            self.connected = False
            self.listener = None
            self.last_error = str(exc)
            self.intentional_stop = False
            self.reconnect_in_progress = False
            self._set_state("FAILED")
            logger.exception("Errore start Telegram listener: %s", exc)
            raise

    def stop(self) -> None:
        if self.state == "STOPPED" and not self.listener:
            self.connected = False
            return

        self.intentional_stop = True
        self.reconnect_in_progress = False
        if self.listener:
            try:
                self.listener.stop()
            except Exception as exc:
                logger.warning("Errore stop Telegram listener: %s", exc)

        self.listener = None
        self.connected = False
        self._set_state("STOPPED")
        self.active_network_resources = 0

    def restart(self) -> dict:
        if self.intentional_stop:
            return {"started": False, "reason": "intentional_stop", "state": self.state}
        if self._restart_in_progress:
            return {"started": False, "reason": "restart_in_progress", "state": self.state}
        if self.state in {"CONNECTING", "RECONNECTING"}:
            return {"started": False, "reason": "connection_in_progress", "state": self.state}

        self._restart_in_progress = True
        self.intentional_stop = False
        self.reconnect_in_progress = True
        self.reconnect_attempts += 1
        self._restart_attempts_total += 1
        now_ts = self._autoheal_policy.now()
        self._last_restart_ts = now_ts
        self._restart_timestamps.append(now_ts)
        self._restart_timestamps = [
            ts for ts in self._restart_timestamps if (now_ts - ts) <= self._autoheal_policy.restart_window_sec
        ]
        self._set_state("RECONNECTING")
        try:
            self.stop()
            self.intentional_stop = False
            self.reconnect_in_progress = False
            result = self.start()
            if not bool(result.get("started", False)):
                result["recovered"] = False
            return result
        finally:
            self._restart_in_progress = False

    # =========================================================
    # STATUS
    # =========================================================
    def status(self) -> dict:
        listener_state = str(getattr(self.listener, "state", "")) if self.listener else ""
        if listener_state:
            self.state = listener_state
        running = bool(self.listener and getattr(self.listener, "running", False))
        self.connected = self.state == "CONNECTED"
        if self.listener:
            self._refresh_runtime_truth_from_listener()
        return {
            "connected": bool(self.connected),
            "running": running,
            "state": self.state,
            "intentional_stop": bool(self.intentional_stop),
            "reconnect_attempts": int(self.reconnect_attempts),
            "reconnect_in_progress": bool(self.reconnect_in_progress),
            "last_error": self.last_error,
            "last_successful_message_ts": self.last_successful_message_ts,
            "listener_started": bool(self.listener_started),
            "handlers_registered": int(self.handlers_registered),
            "active_network_resources": int(self.active_network_resources),
            "restart_attempts_total": int(self._restart_attempts_total),
            "restart_attempts_in_window": len(self._restart_timestamps),
            "last_restart_ts": self._last_restart_ts,
            "lockout_active": bool(self._lockout_active),
            "lockout_reason": self._lockout_reason,
            "last_autoheal_action": self._last_autoheal_action,
            "last_autoheal_decision_reason": self._last_autoheal_decision_reason,
            "recovery_allowed": bool(not self._lockout_active and not self.intentional_stop),
        }

    def runtime_snapshot(self) -> dict:
        listener_snapshot = {}
        if self.listener and callable(getattr(self.listener, "runtime_snapshot", None)):
            listener_snapshot = self.listener.runtime_snapshot() or {}
        status = self.status()
        return {
            "state": str(status["state"]),
            "running": bool(status["running"]),
            "listener_started": bool(status["listener_started"]),
            "client_alive": bool(listener_snapshot.get("client_alive", False)),
            "handlers_registered": int(status["handlers_registered"]),
            "reconnect_in_progress": bool(status["reconnect_in_progress"]),
            "reconnect_attempts": int(status["reconnect_attempts"]),
            "active_network_resources": int(status["active_network_resources"]),
            "intentional_stop": bool(status["intentional_stop"]),
            "retry_loop_active": bool(status["reconnect_in_progress"]),
            "last_error": str(status["last_error"] or ""),
            "last_successful_message_ts": status["last_successful_message_ts"],
        }

    def health_status(self, *, checked_at: str | None = None) -> dict:
        snap = self.runtime_snapshot()
        invariant_snapshot = TelegramInvariantSnapshot(
            state=str(snap["state"]),
            listener_started=bool(snap["listener_started"]),
            client_alive=bool(snap["client_alive"]),
            handlers_registered=int(snap["handlers_registered"]),
            reconnect_in_progress=bool(snap["reconnect_in_progress"]),
            reconnect_attempts=int(snap["reconnect_attempts"]),
            active_network_resources=int(snap["active_network_resources"]),
            intentional_stop=bool(snap["intentional_stop"]),
            retry_loop_active=bool(snap["retry_loop_active"]),
            running=bool(snap["running"]),
            last_error=str(snap["last_error"] or ""),
            last_successful_message_ts=snap["last_successful_message_ts"],
            now_ts=checked_at,
        )
        health = self._health_probe.evaluate(invariant_snapshot, checked_at=checked_at)
        return {
            "state": health.state,
            "healthy": health.healthy,
            "degraded": health.degraded,
            "failed": health.failed,
            "last_error": health.last_error,
            "reconnect_attempts": health.reconnect_attempts,
            "reconnect_in_progress": health.reconnect_in_progress,
            "last_successful_message_ts": health.last_successful_message_ts,
            "handlers_registered": health.handlers_registered,
            "client_alive": health.client_alive,
            "intentional_stop": health.intentional_stop,
            "invariant_ok": health.invariant_ok,
            "active_alert_codes": list(health.active_alert_codes),
            "checked_at": health.checked_at,
        }

    def get_sender(self):
        sender = getattr(self, "sender", None)
        if sender is not None:
            return sender
        if callable(getattr(self, "send_alert_message", None)):
            return self
        return None

    def evaluate_autoheal(
        self,
        *,
        checked_at_ts: float | None,
        startup_grace_active: bool,
        reconnect_grace_active: bool,
        failure_escalated: bool,
    ) -> TelegramAutohealDecision:
        health = self.health_status()
        now_ts = float(checked_at_ts if checked_at_ts is not None else self._autoheal_policy.now())
        snapshot = TelegramAutohealSnapshot(
            state=str(health.get("state") or self.state),
            invariant_ok=bool(health.get("invariant_ok", True)),
            active_alert_codes=tuple(str(c) for c in (health.get("active_alert_codes") or [])),
            reconnect_attempts=int(health.get("reconnect_attempts", 0) or 0),
            restart_attempts_total=int(self._restart_attempts_total),
            restart_in_progress=bool(self._restart_in_progress),
            intentional_stop=bool(health.get("intentional_stop", self.intentional_stop)),
            startup_grace_active=bool(startup_grace_active),
            reconnect_grace_active=bool(reconnect_grace_active),
            lockout_active=bool(self._lockout_active),
            last_error_category=str(health.get("last_error") or ""),
            failure_escalated=bool(failure_escalated),
            listener_stale="STALE_RUNTIME" in set(health.get("active_alert_codes") or []),
            now_ts=now_ts,
        )
        history = TelegramAutohealHistory(
            restart_timestamps=tuple(self._restart_timestamps),
            lockout_since_ts=self._lockout_since_ts,
        )
        decision = self._autoheal_policy.evaluate(snapshot, history)
        self._last_autoheal_action = decision.action.value
        self._last_autoheal_decision_reason = decision.reason
        if decision.action == TelegramAutohealAction.ENTER_FAILED_LOCKOUT:
            self._lockout_active = True
            self._lockout_since_ts = now_ts
            self._lockout_reason = decision.reason
        elif self._lockout_active and self._lockout_since_ts is not None:
            if (now_ts - self._lockout_since_ts) >= self._autoheal_policy.lockout_sec:
                self._lockout_active = False
                self._lockout_since_ts = None
                self._lockout_reason = ""
        return decision

    def run_autoheal_once(
        self,
        *,
        checked_at_ts: float | None,
        startup_grace_active: bool,
        reconnect_grace_active: bool,
        failure_escalated: bool,
    ) -> dict:
        decision = self.evaluate_autoheal(
            checked_at_ts=checked_at_ts,
            startup_grace_active=startup_grace_active,
            reconnect_grace_active=reconnect_grace_active,
            failure_escalated=failure_escalated,
        )
        if decision.action == TelegramAutohealAction.SCHEDULE_RESTART:
            restarted = self.restart()
            return {
                "action": decision.action.value,
                "reason": decision.reason,
                "failure_class": decision.failure_class.value,
                "restart_result": dict(restarted),
            }
        return {
            "action": decision.action.value,
            "reason": decision.reason,
            "failure_class": decision.failure_class.value,
        }
