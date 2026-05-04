from __future__ import annotations

import logging
import tkinter as tk
from tkinter import messagebox, simpledialog

from theme import COLORS
from services.telegram_signal_processor import TelegramSignalProcessor
from services.telegram_bet_resolver import TelegramBetResolver
from telegram_sanitizer import sanitize_telegram_payload as _sanitize_telegram_payload
from observability.sanitizers import sanitize_dict

logger = logging.getLogger(__name__)


class TelegramModule:
    """
    Mixin Telegram per MiniPickfairGUI.

    Funzioni:
    - start/stop listener
    - gestione chat monitorate
    - CRUD pattern avanzati
    - gestione segnali Telegram
    - se il segnale ha già market_id/selection_id -> usa TelegramSignalProcessor
    - se NON li ha -> prova a risolvere automaticamente via TelegramBetResolver
    """

    TELEGRAM_BOUNDARY_STAGE = "telegram_ingestion_normalized_v1"
    TELEGRAM_ROUTING_CONTRACT = "telegram_authoritative_routing_v1"
    TELEGRAM_PRIMARY_ROUTE = "SIGNAL_RECEIVED"
    TELEGRAM_COMPAT_FALLBACK_ROUTE = "REQ_QUICK_BET"

    # =========================================================
    # INTERNAL HELPERS
    # =========================================================
    def _get_signal_processor(self):
        processor = getattr(self, "_telegram_signal_processor", None)
        if processor is None:
            processor = TelegramSignalProcessor()
            self._telegram_signal_processor = processor
        return processor

    def _get_bet_resolver(self):
        resolver = getattr(self, "_telegram_bet_resolver", None)
        if resolver is None:
            client_getter = None

            # preferisci il service broker
            if hasattr(self, "betfair_service") and self.betfair_service is not None:
                client_getter = self.betfair_service.get_client
            elif hasattr(self, "runtime") and getattr(self.runtime, "betfair_service", None) is not None:
                client_getter = self.runtime.betfair_service.get_client
            elif hasattr(self, "betfair_client"):
                client_getter = lambda: getattr(self, "betfair_client", None)

            resolver = TelegramBetResolver(client_getter=client_getter)
            self._telegram_bet_resolver = resolver

        return resolver

    def _safe_refresh_telegram_signals_tree(self):
        try:
            if hasattr(self, "_refresh_telegram_signals_tree"):
                self._refresh_telegram_signals_tree()
        except Exception as e:
            logger.exception("[TelegramModule] Errore refresh signals tree: %s", e)

    def _safe_refresh_telegram_chats_tree(self):
        try:
            if hasattr(self, "_refresh_telegram_chats_tree"):
                self._refresh_telegram_chats_tree()
        except Exception as e:
            logger.exception("[TelegramModule] Errore refresh chats tree: %s", e)

    def _safe_refresh_rules_tree(self):
        try:
            if hasattr(self, "_refresh_rules_tree"):
                self._refresh_rules_tree()
        except Exception as e:
            logger.exception("[TelegramModule] Errore refresh rules tree: %s", e)

    def _safe_parse_stake(self) -> float:
        try:
            raw = self.tg_auto_stake_var.get()
            if isinstance(raw, str):
                raw = raw.replace(",", ".").strip()
            stake = float(raw)
            return stake if stake > 0 else 1.0
        except Exception:
            return 1.0

    def _safe_parse_int_optional(self, value):
        if value in (None, ""):
            return None
        try:
            return int(str(value).strip())
        except Exception:
            return None

    def _safe_bool_from_text(self, value: str, default: bool = False) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y", "si", "sì", "on"}

    def _submit_non_blocking(self, task_name: str, worker_fn, on_success, on_error) -> None:
        submit_fn = getattr(getattr(self, "executor", None), "submit", None)
        if not callable(submit_fn):
            try:
                on_success(worker_fn())
            except Exception as exc:
                on_error(exc)
            return

        try:
            future = submit_fn(task_name, worker_fn)
        except Exception as exc:
            on_error(exc)
            return

        if hasattr(future, "add_done_callback"):
            def _done(fut):
                try:
                    result = fut.result()
                    if hasattr(self, "uiq") and self.uiq:
                        self.uiq.post(on_success, result)
                    else:
                        on_success(result)
                except Exception as exc:
                    if hasattr(self, "uiq") and self.uiq:
                        self.uiq.post(on_error, exc)
                    else:
                        on_error(exc)

            future.add_done_callback(_done)
            return

        try:
            on_success(future)
        except Exception as exc:
            on_error(exc)

    def _safe_db_save_received_signal(
        self,
        selection,
        action,
        price,
        stake,
        status,
        signal=None,
    ):
        if not hasattr(self, "db") or self.db is None:
            return
        if not hasattr(self.db, "save_received_signal"):
            return

        try:
            payload = dict(signal or {})
            payload.update(
                {
                    "selection": selection,
                    "action": action,
                    "price": float(price or 0.0),
                    "stake": float(stake or 0.0),
                    "status": str(status or ""),
                }
            )
            self.db.save_received_signal(payload)
        except Exception as e:
            logger.exception("[TelegramModule] Errore save_received_signal status=%s: %s", status, e)

    def _bus_has_subscriber(self, event_name: str) -> bool:
        event_name = str(event_name or "")
        if not event_name:
            return False

        try:
            subscribers = getattr(self.bus, "subscribers", None)
            if isinstance(subscribers, dict):
                return bool(list(subscribers.get(event_name) or []))
        except Exception:
            logger.exception("[TelegramModule] Errore accesso bus.subscribers")

        try:
            private_subscribers = getattr(self.bus, "_subscribers", None)
            if isinstance(private_subscribers, dict):
                lock = getattr(self.bus, "_lock", None)
                if lock is not None and hasattr(lock, "__enter__"):
                    with lock:
                        return bool(list(private_subscribers.get(event_name) or []))
                return bool(list(private_subscribers.get(event_name) or []))
        except Exception:
            logger.exception("[TelegramModule] Errore accesso bus._subscribers")

        return False

    def _publish_order_signal(self, payload: dict) -> str:
        """
        Authoritative Telegram routing boundary.

        Contract:
        1) Telegram payload MUST already be normalized at ingestion boundary.
        2) Primary route is SIGNAL_RECEIVED (RuntimeController gate owner).
        3) REQ_QUICK_BET is explicit compatibility fallback only when runtime
           signal gate is not subscribed.
        """
        payload = dict(payload or {})
        stage = str(payload.get("telegram_boundary_stage") or "").strip()
        if stage != self.TELEGRAM_BOUNDARY_STAGE:
            raise ValueError("TELEGRAM_ROUTING_BOUNDARY_INVALID_STAGE")

        payload["telegram_routing_contract"] = self.TELEGRAM_ROUTING_CONTRACT

        if self._bus_has_subscriber(self.TELEGRAM_PRIMARY_ROUTE):
            payload["telegram_route_target"] = self.TELEGRAM_PRIMARY_ROUTE
            self.bus.publish(self.TELEGRAM_PRIMARY_ROUTE, payload)
            return self.TELEGRAM_PRIMARY_ROUTE

        if self._bus_has_subscriber(self.TELEGRAM_COMPAT_FALLBACK_ROUTE):
            payload["telegram_route_target"] = self.TELEGRAM_COMPAT_FALLBACK_ROUTE
            self.bus.publish(self.TELEGRAM_COMPAT_FALLBACK_ROUTE, payload)
            return self.TELEGRAM_COMPAT_FALLBACK_ROUTE

        raise RuntimeError("TELEGRAM_ROUTING_NO_SUBSCRIBERS")

    def _needs_resolution(self, signal_data: dict) -> bool:
        market_id = signal_data.get("market_id") or signal_data.get("marketId")
        selection_id = signal_data.get("selection_id") or signal_data.get("selectionId")
        return not market_id or not selection_id

    def _resolve_signal_to_payload(self, signal_data: dict, stake: float):
        """
        Se il segnale ha già market_id/selection_id usa il normalizzatore standard.
        Se non li ha, usa il resolver automatico over successivo / linea esplicita.
        """
        processor = self._get_signal_processor()
        normalized_result = processor.normalize_ingestion_signal(signal_data)
        if not normalized_result.get("ok"):
            return None, f"INVALID_SIGNAL:{normalized_result.get('error_code')}"

        normalized_signal = dict(normalized_result.get("normalized_signal") or {})

        if not self._needs_resolution(normalized_signal):
            payload = processor.build_runtime_signal(
                signal=normalized_signal,
                stake=stake,
                simulation_mode=bool(getattr(self, "simulation_mode", False)),
            )
            if payload:
                payload["telegram_boundary_stage"] = normalized_signal.get("boundary_stage")
            return payload, "DIRECT"

        resolver = self._get_bet_resolver()
        resolved = resolver.resolve(normalized_signal, aggressive_best_price=True)
        if not resolved:
            return None, "UNRESOLVED"

        payload = resolved.to_order_payload(
            stake=stake,
            simulation_mode=bool(getattr(self, "simulation_mode", False)),
        )

        payload["raw_signal"] = dict(normalized_signal.get("raw_signal") or {})
        payload["telegram_boundary_stage"] = normalized_signal.get("boundary_stage")
        payload["resolution_mode"] = "AUTO_RESOLVED"
        return payload, "AUTO_RESOLVED"

    # =========================================================
    # START / STOP LISTENER
    # =========================================================
    def _start_telegram_listener(self):
        try:
            settings = self.db.get_telegram_settings() if hasattr(self, "db") else {}
            settings = settings or {}
        except Exception as e:
            logger.exception("[TelegramModule] Errore lettura telegram settings: %s", e)
            settings = {}

        if not settings.get("api_id") or not settings.get("api_hash"):
            messagebox.showwarning(
                "Attenzione",
                "Configura e salva le credenziali Telegram prima di avviare il listener.",
            )
            return

        existing_listener = getattr(self, "telegram_listener", None)
        if existing_listener and getattr(existing_listener, "running", False):
            messagebox.showinfo("Info", "Listener già in esecuzione")
            return

        try:
            from telegram_listener import TelegramListener

            api_id = int(settings["api_id"])
            api_hash = str(settings["api_hash"]).strip()
            session_string = settings.get("session_string")

            listener = TelegramListener(
                api_id=api_id,
                api_hash=api_hash,
                session_string=session_string,
                db=self.db,
            )

            monitored_chats = []
            try:
                chats = self.db.get_telegram_chats() if hasattr(self, "db") else []
                chats = chats or []
            except Exception as e:
                logger.exception("[TelegramModule] Errore lettura telegram chats: %s", e)
                chats = []

            for chat in chats:
                if not chat.get("is_active", True):
                    continue
                try:
                    monitored_chats.append(int(chat["chat_id"]))
                except Exception:
                    logger.warning("[TelegramModule] chat_id non valido ignorato: %s", chat)

            if not monitored_chats:
                messagebox.showwarning(
                    "Attenzione",
                    "Nessuna chat monitorata attiva. Aggiungine almeno una.",
                )
                return

            if hasattr(listener, "set_monitored_chats"):
                listener.set_monitored_chats(monitored_chats)
            if hasattr(listener, "set_database"):
                listener.set_database(self.db)

            if hasattr(listener, "set_callbacks"):
                listener.set_callbacks(
                    on_signal=self._handle_telegram_signal,
                    on_message=None,
                    on_status=lambda st, msg: self.bus.publish(
                        "TELEGRAM_STATUS",
                        {"status": st, "message": msg},
                    ),
                )

            self.telegram_listener = listener

            if hasattr(self.telegram_listener, "start"):
                try:
                    self.telegram_listener.start(monitored_chats=monitored_chats)
                except TypeError:
                    self.telegram_listener.start()

            self.telegram_status = "LISTENING"

            if hasattr(self, "tg_status_label") and self.tg_status_label.winfo_exists():
                self.tg_status_label.configure(
                    text=f"Stato: {self.telegram_status}",
                    text_color=COLORS["success"],
                )

            messagebox.showinfo("Successo", "Telegram Listener avviato")

        except Exception as e:
            logger.exception("[TelegramModule] Impossibile avviare listener: %s", e)
            messagebox.showerror("Errore", f"Impossibile avviare listener: {str(e)}")

    def _stop_telegram_listener(self):
        listener = getattr(self, "telegram_listener", None)
        if listener and getattr(listener, "running", False):
            try:
                listener.stop()
            except Exception as e:
                logger.exception("[TelegramModule] Errore stop listener: %s", e)

            self.telegram_status = "STOPPED"

            if hasattr(self, "tg_status_label") and self.tg_status_label.winfo_exists():
                self.tg_status_label.configure(
                    text=f"Stato: {self.telegram_status}",
                    text_color=COLORS["error"],
                )

            messagebox.showinfo("Info", "Telegram Listener fermato")

    # =========================================================
    # SIGNAL FLOW
    # =========================================================
    def _handle_telegram_signal(self, signal):
        """
        Listener -> handler -> direct payload or resolver -> publish order signal
        """

        def safe_process_signal():
            processor = self._get_signal_processor()
            normalized_result = processor.normalize_ingestion_signal(signal)
            signal_data = dict(normalized_result.get("normalized_signal") or {})

            if not normalized_result.get("ok"):
                error_code = str(normalized_result.get("error_code") or "INVALID_SIGNAL")
                error_reason = str(normalized_result.get("error_reason") or "invalid telegram signal")
                logger.warning("[TelegramModule] Scarto segnale telegram non valido: %s (%s)", error_code, error_reason)
                self._safe_db_save_received_signal(
                    selection="Unknown",
                    action="BACK",
                    price=0.0,
                    stake=self._safe_parse_stake(),
                    status="ERROR",
                    signal={
                        "error_code": error_code,
                        "error_reason": error_reason,
                        "raw_signal": _sanitize_telegram_payload(signal),
                    },
                )
                self._safe_refresh_telegram_signals_tree()
                return

            action = processor.normalize_action(signal_data)
            selection_id = processor.parse_selection_id(signal_data)
            market_id = processor.parse_market_id(signal_data)
            original_price = processor.parse_price(signal_data)
            selection_name = processor.parse_selection_name(signal_data, selection_id)
            stake = self._safe_parse_stake()

            if original_price is None:
                original_price = 0.0

            self._safe_db_save_received_signal(
                selection=selection_name,
                action=action,
                price=original_price,
                stake=stake,
                status="RECEIVED",
                signal=_sanitize_telegram_payload(signal_data),
            )
            self._safe_refresh_telegram_signals_tree()

            auto_bet_enabled = bool(
                hasattr(self, "tg_auto_bet_var")
                and self.tg_auto_bet_var is not None
                and self.tg_auto_bet_var.get()
            )
            confirm_enabled = bool(
                hasattr(self, "tg_confirm_var")
                and self.tg_confirm_var is not None
                and self.tg_confirm_var.get()
            )

            if not auto_bet_enabled:
                if confirm_enabled:
                    msg = (
                        f"Segnale ricevuto:\n"
                        f"{selection_name or signal_data.get('event_name', 'Segnale Telegram')}\n"
                        f"Tipo: {action}\n"
                        f"Quota Master: {float(original_price or 0.0):.2f}\n\n"
                        f"Inviare il segnale al runtime?"
                    )
                    if not messagebox.askyesno("Nuovo Segnale Telegram", msg):
                        self._safe_db_save_received_signal(
                            selection=selection_name,
                            action=action,
                            price=original_price,
                            stake=stake,
                            status="IGNORED",
                            signal=_sanitize_telegram_payload(signal_data),
                        )
                        self._safe_refresh_telegram_signals_tree()
                        return
                else:
                    self._safe_db_save_received_signal(
                        selection=selection_name,
                        action=action,
                        price=original_price,
                        stake=stake,
                        status="IGNORED",
                        signal=_sanitize_telegram_payload(signal_data),
                    )
                    self._safe_refresh_telegram_signals_tree()
                    return

            def _worker():
                payload, resolution_mode = self._resolve_signal_to_payload(signal_data, stake=stake)
                if not payload:
                    return {
                        "ok": False,
                        "status": "ERROR",
                        "selection_name": selection_name,
                        "action": action,
                        "price": original_price,
                        "stake": stake,
                    }

                payload["raw_signal"] = _sanitize_telegram_payload(dict(signal_data or {}))
                payload["resolution_mode"] = resolution_mode

                logger.info(
                    "[TelegramModule] Inoltro segnale betting (%s): %s",
                    resolution_mode,
                    sanitize_dict(dict(payload or {})),
                )
                route_target = self._publish_order_signal(payload)
                payload["telegram_route_target"] = route_target
                return {
                    "ok": True,
                    "payload": payload,
                    "selection_name": selection_name,
                    "action": action,
                    "price": original_price,
                    "stake": stake,
                }

            def _on_success(result):
                result = dict(result or {})
                if not bool(result.get("ok")):
                    logger.error(
                        "[TelegramModule] Segnale non risolvibile: %s",
                        sanitize_dict(dict(signal_data or {})),
                    )
                    self._safe_db_save_received_signal(
                        selection=result.get("selection_name", selection_name),
                        action=result.get("action", action),
                        price=result.get("price", original_price),
                        stake=result.get("stake", stake),
                        status="ERROR",
                        signal=_sanitize_telegram_payload(signal_data),
                    )
                    self._safe_refresh_telegram_signals_tree()
                    return

                payload = dict(result.get("payload") or {})
                self._safe_db_save_received_signal(
                    selection=payload.get("runner_name", selection_name),
                    action=payload.get("bet_type", action),
                    price=payload.get("price", original_price),
                    stake=payload.get("stake", stake),
                    status="SUBMITTED",
                    signal=_sanitize_telegram_payload({**signal_data, "resolved_payload": payload}),
                )
                self._safe_refresh_telegram_signals_tree()

            def _on_error(exc):
                e = exc
                logger.exception("[TelegramModule] Errore publish ordine: %s", e)
                self._safe_db_save_received_signal(
                    selection=selection_name,
                    action=action,
                    price=original_price,
                    stake=stake,
                    status="ERROR",
                    signal=_sanitize_telegram_payload(signal_data),
                )
                self._safe_refresh_telegram_signals_tree()

            self._submit_non_blocking(
                "telegram_signal_resolution",
                _worker,
                _on_success,
                _on_error,
            )

        if hasattr(self, "uiq") and self.uiq:
            self.uiq.post(safe_process_signal)
        else:
            safe_process_signal()

    # =========================================================
    # STATUS
    # =========================================================
    def _update_telegram_status(self, status, message):
        self.telegram_status = status
        color = COLORS["success"] if status == "LISTENING" else COLORS["error"]

        if hasattr(self, "tg_status_label") and self.tg_status_label.winfo_exists():
            self.tg_status_label.configure(
                text=f"Stato: {status} - {message}",
                text_color=color,
            )

    # =========================================================
    # CHATS TREE
    # =========================================================
    def _refresh_telegram_chats_tree(self):
        if not hasattr(self, "tg_chats_tree") or not self.tg_chats_tree.winfo_exists():
            return

        self.tg_chats_tree.delete(*self.tg_chats_tree.get_children())
        chats = self.db.get_telegram_chats()

        for chat in chats:
            state = "Sì" if chat.get("is_active") else "No"
            title = chat.get("title") or chat.get("username") or str(chat.get("chat_id"))
            self.tg_chats_tree.insert(
                "",
                tk.END,
                iid=str(chat["chat_id"]),
                values=(title, state),
            )

    def _remove_telegram_chat(self):
        selected = getattr(self, "tg_chats_tree", None) and self.tg_chats_tree.selection()
        if not selected:
            return

        chats = self.db.get_telegram_chats()
        updated_chats = [c for c in chats if str(c["chat_id"]) not in selected]
        self.db.replace_telegram_chats(updated_chats)
        self._safe_refresh_telegram_chats_tree()

    def _add_selected_available_chats(self):
        selected = getattr(self, "tg_available_tree", None) and self.tg_available_tree.selection()
        if not selected:
            return

        for item_id in selected:
            item = self.tg_available_tree.item(item_id)
            values = item.get("values", [])
            name = values[2] if len(values) > 2 else str(item_id)
            self.db.save_telegram_chat(
                chat_id=item_id,
                title=name,
                is_active=True,
            )

        self._safe_refresh_telegram_chats_tree()
        messagebox.showinfo("Successo", "Chat aggiunte al monitoraggio.")

    # =========================================================
    # ADVANCED SIGNAL PATTERNS
    # =========================================================
    def _prompt_signal_pattern_payload(self, current=None):
        current = current or {}

        label = simpledialog.askstring(
            "Regola Pattern",
            "Nome regola:",
            initialvalue=current.get("label", ""),
        )
        if label is None or not str(label).strip():
            return None

        pattern = simpledialog.askstring(
            "Regola Pattern",
            "Regex pattern:",
            initialvalue=current.get("pattern", ""),
        )
        if pattern is None or not str(pattern).strip():
            return None

        market_type = simpledialog.askstring(
            "Regola Pattern",
            "Market type (es. MATCH_ODDS, OVER_UNDER, NEXT_GOAL):",
            initialvalue=current.get("market_type", "MATCH_ODDS"),
        )
        if market_type is None:
            return None

        bet_side = simpledialog.askstring(
            "Regola Pattern",
            "Bet side (BACK / LAY / vuoto):",
            initialvalue=current.get("bet_side", ""),
        )
        if bet_side is None:
            return None

        selection_template = simpledialog.askstring(
            "Regola Pattern",
            "Selection template (es. Over {over_line} / vuoto):",
            initialvalue=current.get("selection_template", ""),
        )
        if selection_template is None:
            return None

        min_minute = simpledialog.askstring(
            "Regola Pattern",
            "Min minuto (vuoto = nessun filtro):",
            initialvalue="" if current.get("min_minute") is None else str(current.get("min_minute")),
        )
        if min_minute is None:
            return None

        max_minute = simpledialog.askstring(
            "Regola Pattern",
            "Max minuto (vuoto = nessun filtro):",
            initialvalue="" if current.get("max_minute") is None else str(current.get("max_minute")),
        )
        if max_minute is None:
            return None

        min_score = simpledialog.askstring(
            "Regola Pattern",
            "Min score totale (vuoto = nessun filtro):",
            initialvalue="" if current.get("min_score") is None else str(current.get("min_score")),
        )
        if min_score is None:
            return None

        max_score = simpledialog.askstring(
            "Regola Pattern",
            "Max score totale (vuoto = nessun filtro):",
            initialvalue="" if current.get("max_score") is None else str(current.get("max_score")),
        )
        if max_score is None:
            return None

        live_only_txt = simpledialog.askstring(
            "Regola Pattern",
            "Solo live? (yes/no):",
            initialvalue="yes" if current.get("live_only", False) else "no",
        )
        if live_only_txt is None:
            return None

        priority_txt = simpledialog.askstring(
            "Regola Pattern",
            "Priority (numero, default 100):",
            initialvalue=str(current.get("priority", 100)),
        )
        if priority_txt is None:
            return None

        return {
            "label": str(label).strip(),
            "pattern": str(pattern).strip(),
            "market_type": str(market_type or "MATCH_ODDS").strip() or "MATCH_ODDS",
            "bet_side": str(bet_side or "").strip().upper(),
            "selection_template": str(selection_template or "").strip(),
            "min_minute": self._safe_parse_int_optional(min_minute),
            "max_minute": self._safe_parse_int_optional(max_minute),
            "min_score": self._safe_parse_int_optional(min_score),
            "max_score": self._safe_parse_int_optional(max_score),
            "live_only": self._safe_bool_from_text(live_only_txt, False),
            "priority": self._safe_parse_int_optional(priority_txt) or 100,
            "enabled": bool(current.get("enabled", True)),
        }

    def _minute_range_text(self, rule):
        a = rule.get("min_minute")
        b = rule.get("max_minute")
        if a is None and b is None:
            return "-"
        return f"{'' if a is None else a}-{'' if b is None else b}"

    def _score_range_text(self, rule):
        a = rule.get("min_score")
        b = rule.get("max_score")
        if a is None and b is None:
            return "-"
        return f"{'' if a is None else a}-{'' if b is None else b}"

    def _refresh_rules_tree(self):
        if not hasattr(self, "rules_tree") or not self.rules_tree.winfo_exists():
            return

        self.rules_tree.delete(*self.rules_tree.get_children())
        rules = self.db.get_signal_patterns()

        for rule in rules:
            state = "Sì" if rule.get("enabled") else "No"
            self.rules_tree.insert(
                "",
                tk.END,
                iid=str(rule["id"]),
                values=(
                    state,
                    rule.get("label", ""),
                    rule.get("market_type", "MATCH_ODDS"),
                    rule.get("bet_side", ""),
                    rule.get("selection_template", ""),
                    self._minute_range_text(rule),
                    self._score_range_text(rule),
                    "Sì" if rule.get("live_only") else "No",
                    rule.get("priority", 100),
                    rule.get("pattern", ""),
                ),
            )

    def _add_signal_pattern(self):
        payload = self._prompt_signal_pattern_payload()
        if not payload:
            return

        try:
            self.db.save_signal_pattern(
                pattern=payload["pattern"],
                label=payload["label"],
                enabled=True,
                bet_side=payload["bet_side"],
                market_type=payload["market_type"],
                selection_template=payload["selection_template"],
                min_minute=payload["min_minute"],
                max_minute=payload["max_minute"],
                min_score=payload["min_score"],
                max_score=payload["max_score"],
                live_only=payload["live_only"],
                priority=payload["priority"],
            )
            self._safe_refresh_rules_tree()
            messagebox.showinfo("Successo", "Regola aggiunta correttamente.")
        except Exception as e:
            logger.exception("[TelegramModule] Errore add_signal_pattern: %s", e)
            messagebox.showerror("Errore", f"Impossibile aggiungere la regola: {e}")

    def _edit_signal_pattern(self):
        if not hasattr(self, "rules_tree") or not self.rules_tree.winfo_exists():
            return

        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola da modificare.")
            return

        pattern_id = selected[0]
        rules = self.db.get_signal_patterns()
        current = next((r for r in rules if str(r["id"]) == str(pattern_id)), None)

        if not current:
            messagebox.showerror("Errore", "Regola non trovata nel database.")
            return

        payload = self._prompt_signal_pattern_payload(current=current)
        if not payload:
            return

        try:
            self.db.update_signal_pattern(
                pattern_id=int(pattern_id),
                pattern=payload["pattern"],
                label=payload["label"],
                bet_side=payload["bet_side"],
                market_type=payload["market_type"],
                selection_template=payload["selection_template"],
                min_minute=payload["min_minute"],
                max_minute=payload["max_minute"],
                min_score=payload["min_score"],
                max_score=payload["max_score"],
                live_only=payload["live_only"],
                priority=payload["priority"],
            )
            self._safe_refresh_rules_tree()
            messagebox.showinfo("Successo", "Regola aggiornata correttamente.")
        except Exception as e:
            logger.exception("[TelegramModule] Errore edit_signal_pattern: %s", e)
            messagebox.showerror("Errore", f"Impossibile aggiornare la regola: {e}")

    def _delete_signal_pattern(self):
        if not hasattr(self, "rules_tree") or not self.rules_tree.winfo_exists():
            return

        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola da eliminare.")
            return

        pattern_id = selected[0]

        if not messagebox.askyesno("Conferma eliminazione", "Vuoi davvero eliminare la regola selezionata?"):
            return

        try:
            self.db.delete_signal_pattern(int(pattern_id))
            self._safe_refresh_rules_tree()
            messagebox.showinfo("Successo", "Regola eliminata.")
        except Exception as e:
            logger.exception("[TelegramModule] Errore delete_signal_pattern: %s", e)
            messagebox.showerror("Errore", f"Impossibile eliminare la regola: {e}")

    def _toggle_signal_pattern(self):
        if not hasattr(self, "rules_tree") or not self.rules_tree.winfo_exists():
            return

        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola da attivare/disattivare.")
            return

        pattern_id = selected[0]

        try:
            new_state = self.db.toggle_signal_pattern(int(pattern_id))
            self._safe_refresh_rules_tree()
            stato_txt = "attivata" if new_state else "disattivata"
            messagebox.showinfo("Successo", f"Regola {stato_txt}.")
        except Exception as e:
            logger.exception("[TelegramModule] Errore toggle_signal_pattern: %s", e)
            messagebox.showerror("Errore", f"Impossibile cambiare stato della regola: {e}")

    # =========================================================
    # SIGNALS TREE
    # =========================================================
    def _refresh_telegram_signals_tree(self):
        if not hasattr(self, "tg_signals_tree") or not self.tg_signals_tree.winfo_exists():
            return

        self.tg_signals_tree.delete(*self.tg_signals_tree.get_children())

        if hasattr(self.db, "get_received_signals"):
            signals = self.db.get_received_signals(limit=50)
            for sig in signals:
                date_str = str(sig.get("received_at", ""))[:16]
                sel = sig.get("selection", "")
                action = sig.get("action", "")
                price = f"{float(sig.get('price', 0) or 0):.2f}"
                stake = f"{float(sig.get('stake', 0) or 0):.2f}"
                status = sig.get("status", "")

                tag = ""
                if status in ("RECEIVED", "SUBMITTED"):
                    tag = "success"
                elif status in ("ERROR", "IGNORED"):
                    tag = "failed"

                self.tg_signals_tree.insert(
                    "",
                    tk.END,
                    values=(date_str, sel, action, price, stake, status),
                    tags=(tag,) if tag else (),
                )

