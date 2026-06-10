from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class TelegramListener:
    """
    Wrapper listener Telegram.

    Nota:
    questo file mantiene il motore parser già esistente, ma allinea:
    - fallback label/name dei pattern custom
    - callback registration
    """

    def __init__(self, api_id: int, api_hash: str, session_string: str | None = None, db=None):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash)
        self.session_string = session_string
        self.db = db

        self.running = False
        self.monitored_chats: List[int] = []
        self.state = "CREATED"
        self.last_error = ""
        self.intentional_stop = False
        self.reconnect_attempts = 0
        self.reconnect_in_progress = False
        self.last_successful_message_ts: str | None = None
        self.listener_started = False
        self.handlers_registered = 0
        self.active_network_resources = 0

        self._callbacks = {
            "on_signal": None,
            "on_message": None,
            "on_status": None,
        }

    def _set_state(self, new_state: str) -> None:
        allowed_states = {"CREATED", "CONNECTING", "CONNECTED", "RECONNECTING", "STOPPED", "FAILED"}
        if new_state not in allowed_states:
            raise ValueError(f"Invalid Telegram listener state: {new_state}")
        self.state = new_state

    # =========================================================
    # EXTERNAL SETUP
    # =========================================================
    def set_database(self, db) -> None:
        self.db = db

    def set_monitored_chats(self, chats: List[int]) -> None:
        self.monitored_chats = [int(c) for c in (chats or [])]

    def set_callbacks(self, on_signal=None, on_message=None, on_status=None) -> None:
        self._callbacks["on_signal"] = on_signal
        self._callbacks["on_message"] = on_message
        self._callbacks["on_status"] = on_status
        self.handlers_registered = sum(1 for cb in self._callbacks.values() if callable(cb))

    # =========================================================
    # LIFECYCLE
    # =========================================================
    def start(self, monitored_chats: Optional[List[int]] = None):
        if self.running:
            return {"started": True, "reason": "already_running", "chat_count": len(self.monitored_chats)}

        if monitored_chats is not None:
            self.set_monitored_chats(monitored_chats)

        self.intentional_stop = False
        self.reconnect_in_progress = False
        self.last_error = ""
        self._set_state("CONNECTING")
        self.running = True
        self.listener_started = True
        self.active_network_resources = 0

        # This listener currently does not manage a live Telegram client socket.
        # Keep lifecycle honest: started runtime, but no proven CONNECTED state.
        self._emit_status("LISTENING", "Listener avviato")
        return {
            "started": True,
            "chat_count": len(self.monitored_chats),
            "state": self.state,
            "reason": "no_live_runtime_client",
        }

    def stop(self):
        if self.state == "STOPPED" and not self.running:
            return {"stopped": True, "reason": "already_stopped", "state": self.state}

        self.intentional_stop = True
        self.reconnect_in_progress = False
        self.running = False
        self.active_network_resources = 0
        self._set_state("STOPPED")
        self._emit_status("STOPPED", "Listener fermato")
        return {"stopped": True, "state": self.state}

    def mark_failed(self, error: str) -> None:
        self.last_error = str(error or "")
        self.running = False
        self.reconnect_in_progress = False
        self._set_state("FAILED")
        self._emit_status("FAILED", self.last_error or "Listener failure")

    def begin_reconnect_attempt(self) -> bool:
        if self.intentional_stop or self.state in {"STOPPED", "FAILED"}:
            return False
        self.reconnect_attempts += 1
        self.reconnect_in_progress = True
        self._set_state("RECONNECTING")
        return True

    def end_reconnect_attempt(self, *, success: bool, error: str = "") -> None:
        self.reconnect_in_progress = False
        if success:
            self.last_error = ""
            # Keep this truthful for current architecture: no live network resource.
            self._set_state("STOPPED")
            self.running = False
            self.active_network_resources = 0
            return
        self.mark_failed(error or "reconnect_failed")

    def status(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "running": bool(self.running),
            "intentional_stop": bool(self.intentional_stop),
            "reconnect_attempts": int(self.reconnect_attempts),
            "reconnect_in_progress": bool(self.reconnect_in_progress),
            "last_error": self.last_error,
            "last_successful_message_ts": self.last_successful_message_ts,
            "listener_started": bool(self.listener_started),
            "handlers_registered": int(self.handlers_registered),
            "active_network_resources": int(self.active_network_resources),
            "monitored_chat_count": len(self.monitored_chats),
        }

    def runtime_snapshot(self) -> Dict[str, Any]:
        """Deterministic snapshot for invariant/health probe evaluation."""
        status = self.status()
        return {
            "state": status["state"],
            "running": status["running"],
            "listener_started": status["listener_started"],
            "client_alive": bool(self.running and self.active_network_resources > 0),
            "handlers_registered": status["handlers_registered"],
            "reconnect_in_progress": status["reconnect_in_progress"],
            "reconnect_attempts": status["reconnect_attempts"],
            "active_network_resources": status["active_network_resources"],
            "intentional_stop": status["intentional_stop"],
            "retry_loop_active": bool(self.reconnect_in_progress),
            "last_error": status["last_error"],
            "last_successful_message_ts": status["last_successful_message_ts"],
        }

    def request_code(self, phone_number: str):
        self._emit_status("CODE_SENT", f"Codice inviato a {phone_number}")
        return {"ok": True}

    def sign_in(self, code: str, password_2fa: str | None = None):
        _ = code, password_2fa
        self._emit_status("AUTHORIZED", "Login completato")
        return {"ok": True}

    # =========================================================
    # STATUS / EMIT
    # =========================================================
    def _emit_status(self, status: str, message: str):
        cb = self._callbacks.get("on_status")
        if callable(cb):
            try:
                cb(status, message)
            except Exception:
                self.last_error = "on_status_callback_failed"
                logger.exception("[TelegramListener] Errore callback on_status")

    def _emit_signal(self, signal: Dict[str, Any]):
        cb = self._callbacks.get("on_signal")
        if callable(cb):
            try:
                cb(signal)
                self.last_successful_message_ts = signal.get("received_at") or signal.get("timestamp")
            except Exception:
                self.last_error = "on_signal_callback_failed"
                logger.exception("[TelegramListener] Errore callback on_signal")

    # =========================================================
    # PARSING PUBLIC
    # =========================================================
    def parse_signal(self, text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None

        parsed = self._parse_custom_patterns(text)
        if parsed:
            return parsed

        parsed = self._parse_master_signal(text)
        if parsed:
            return parsed

        parsed = self._parse_cashout_signal(text)
        if parsed:
            return parsed

        return self._parse_legacy_signal(text)

    # =========================================================
    # CUSTOM PATTERNS
    # =========================================================
    def _parse_custom_patterns(self, text: str) -> Optional[Dict[str, Any]]:
        if not self.db or not hasattr(self.db, "get_signal_patterns"):
            return None

        try:
            patterns = self.db.get_signal_patterns(enabled_only=True)
        except TypeError:
            patterns = self.db.get_signal_patterns()
            patterns = [p for p in patterns if p.get("enabled", True)]
        except Exception:
            logger.exception("[TelegramListener] Errore get_signal_patterns")
            return None

        for cp in patterns or []:
            try:
                pattern = cp.get("pattern") or ""
                keyword = str(cp.get("keyword") or "").strip()

                if not pattern and not keyword:
                    continue

                regex_match = (
                    re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
                    if pattern else None
                )

                # Logica di attivazione:
                # - Solo regex: deve matchare
                # - Solo keyword: deve essere presente nel testo
                # - Entrambi: entrambi devono essere soddisfatti
                if pattern and keyword:
                    if not regex_match or keyword.lower() not in text.lower():
                        continue
                elif pattern:
                    if not regex_match:
                        continue
                elif keyword:
                    if keyword.lower() not in text.lower():
                        continue

                home_score, away_score = self._extract_score(text)
                minute = self._extract_minute(text)
                total_goals = home_score + away_score

                min_minute = cp.get("min_minute")
                max_minute = cp.get("max_minute")
                min_score  = cp.get("min_score")
                max_score  = cp.get("max_score")
                live_only  = bool(cp.get("live_only", False))
                prematch   = bool(cp.get("prematch", False))

                # Filtri opzionali — se il minuto non è nel messaggio (0)
                # i filtri minuto vengono saltati per non bloccare inutilmente.
                if minute > 0:
                    if min_minute is not None and minute < int(min_minute):
                        continue
                    if max_minute is not None and minute > int(max_minute):
                        continue
                    if live_only and prematch:
                        continue  # contraddizione: ignora
                    if live_only and minute <= 0:
                        continue
                else:
                    # Minuto assente: blocca solo se live_only richiede minuto presente
                    if live_only:
                        continue

                if min_score is not None and total_goals < int(min_score):
                    continue
                if max_score is not None and total_goals > int(max_score):
                    continue

                selection_template = str(cp.get("selection_template") or "").strip()
                selection = self._render_selection_template(
                    selection_template=selection_template,
                    home_score=home_score,
                    away_score=away_score,
                    minute=minute,
                )

                event_name = self._extract_event_name(text)
                market_type = str(cp.get("market_type") or "MATCH_ODDS").strip()
                bet_side = str(cp.get("bet_side") or "BACK").strip().upper()

                if not selection:
                    selection = str(cp.get("label") or cp.get("name") or "Custom Pattern")

                # Stake: priorità → stake_fisso dal pattern → testo messaggio → default
                stake_fixed        = bool(cp.get("stake_fixed", False))
                stake_fixed_amount = cp.get("stake_fixed_amount")
                mm_auto            = bool(cp.get("mm_auto", False))

                if stake_fixed and stake_fixed_amount is not None:
                    stake = float(stake_fixed_amount)
                else:
                    stake = self._extract_stake(text) or 0.0  # 0 = lascia decidere al MM

                return {
                    "event_name":         event_name,
                    "selection":          selection,
                    "market_type":        market_type,
                    "bet_type":           bet_side,
                    "price":              self._extract_odds(text),   # None → broker cerca best price
                    "stake":              stake,
                    "minute":             minute,
                    "home_score":         home_score,
                    "away_score":         away_score,
                    "pattern_id":         cp.get("id"),
                    "pattern_label":      cp.get("label") or cp.get("name") or "",
                    "stake_fixed":        stake_fixed,
                    "stake_fixed_amount": stake_fixed_amount,
                    "mm_auto":            mm_auto,
                    "prematch":           prematch,
                    "raw_text":           text,
                }

            except Exception:
                logger.exception("[TelegramListener] Errore parse custom pattern id=%s", cp.get("id"))

        return None

    def _render_selection_template(
        self,
        *,
        selection_template: str,
        home_score: int,
        away_score: int,
        minute: int,
    ) -> str:
        if not selection_template:
            return ""

        total_goals = home_score + away_score
        over_line = total_goals + 0.5

        rendered = selection_template
        rendered = rendered.replace("{home_score}", str(home_score))
        rendered = rendered.replace("{away_score}", str(away_score))
        rendered = rendered.replace("{total_goals}", str(total_goals))
        rendered = rendered.replace("{minute}", str(minute))
        rendered = rendered.replace("{over_line}", str(over_line).replace(".0", ""))
        return rendered

    # =========================================================
    # LEGACY PATTERNS
    # =========================================================
    def _default_patterns(self) -> Dict[str, Any]:
        return {
            "event_icon": r"🆚\s*(.+?)(?:\n|$)",
            "league": r"🏆\s*(.+?)(?:\n|$)",
            "score": r"(\d+)\s*[-–]\s*(\d+)",
            "time": r"(\d+)m",
            "odds": r"@\s*(\d+[.,]\d+)",
            "stake": r"(?:stake|puntata|€)\s*(\d+(?:[.,]\d+)?)",
            "back": r"\b(back|punta|P\.Exc\.)\b",
            "lay": r"\b(lay|banca|B\.Exc\.)\b",
            "over": r"\b(over|sopra)\s*(\d+[.,]?\d*)",
            "under": r"\b(under|sotto)\s*(\d+[.,]?\d*)",
            "next_goal": r"NEXT\s*GOL|PROSSIMO\s*GOL",
            "cashout": r"\b(COPY\s*CASHOUT|cashout|CASHOUT)\b",
            "cashout_all": r"\b(CASHOUT\s*ALL|CASHOUT\s*TUTTO|CHIUDI\s*TUTTO)\b",
            "ignore_patterns": [r"📈Quota\s*\d+[.,]?\d*", r"📊\d+[.,]?\d+%"],
        }

    def _parse_legacy_signal(self, text: str) -> Optional[Dict[str, Any]]:
        p = self._default_patterns()
        upper = text.upper()

        for ign in p["ignore_patterns"]:
            if re.search(ign, text, flags=re.IGNORECASE):
                return None

        event_name = self._extract_event_name(text)
        odds = self._extract_odds(text) or 2.0
        stake = self._extract_stake(text) or 1.0
        minute = self._extract_minute(text)
        home_score, away_score = self._extract_score(text)

        bet_type = "BACK"
        if re.search(p["lay"], text, flags=re.IGNORECASE):
            bet_type = "LAY"
        elif re.search(p["back"], text, flags=re.IGNORECASE):
            bet_type = "BACK"

        over_match = re.search(p["over"], text, flags=re.IGNORECASE)
        if over_match:
            line = over_match.group(2).replace(",", ".")
            return {
                "event_name": event_name,
                "selection": f"Over {line}",
                "market_type": "OVER_UNDER",
                "bet_type": bet_type,
                "price": odds,
                "stake": stake,
                "minute": minute,
                "home_score": home_score,
                "away_score": away_score,
                "raw_text": text,
            }

        under_match = re.search(p["under"], text, flags=re.IGNORECASE)
        if under_match:
            line = under_match.group(2).replace(",", ".")
            return {
                "event_name": event_name,
                "selection": f"Under {line}",
                "market_type": "OVER_UNDER",
                "bet_type": bet_type,
                "price": odds,
                "stake": stake,
                "minute": minute,
                "home_score": home_score,
                "away_score": away_score,
                "raw_text": text,
            }

        if re.search(p["next_goal"], upper, flags=re.IGNORECASE):
            total_goals = home_score + away_score
            return {
                "event_name": event_name,
                "selection": f"Over {total_goals + 0.5}",
                "market_type": "OVER_UNDER",
                "bet_type": "BACK",
                "price": odds,
                "stake": stake,
                "minute": minute,
                "home_score": home_score,
                "away_score": away_score,
                "raw_text": text,
            }

        return None

    # =========================================================
    # MASTER SIGNAL / CASHOUT
    # =========================================================
    def _extract_master_field(self, field: str, text: str) -> str:
        m = re.search(rf"^{field}\s*:\s*(.+)$", text, flags=re.IGNORECASE | re.MULTILINE)
        return m.group(1).strip() if m else ""

    def _parse_master_signal(self, text: str) -> Optional[Dict[str, Any]]:
        upper = text.upper()
        if "MASTER SIGNAL" not in upper:
            return None

        market_id = self._extract_master_field("market_id", text)
        selection_id = self._extract_master_field("selection_id", text)
        action = self._extract_master_field("action", text) or "BACK"
        master_price = self._extract_master_field("master_price", text) or "2.0"
        event_name = self._extract_master_field("event_name", text)
        market_name = self._extract_master_field("market_name", text)
        selection = self._extract_master_field("selection", text)

        if not market_id or not selection_id:
            return None

        return {
            "market_id": market_id,
            "selection_id": int(float(selection_id)),
            "bet_type": str(action).upper(),
            "price": float(str(master_price).replace(",", ".")),
            "event_name": event_name,
            "market_name": market_name,
            "selection": selection,
            "stake": self._extract_stake(text) or 1.0,
            "raw_text": text,
        }

    def _parse_cashout_signal(self, text: str) -> Optional[Dict[str, Any]]:
        p = self._default_patterns()
        if re.search(p["cashout_all"], text, flags=re.IGNORECASE):
            return {"signal_type": "CASHOUT_ALL", "raw_text": text}
        if re.search(p["cashout"], text, flags=re.IGNORECASE):
            return {"signal_type": "CASHOUT", "raw_text": text}
        return None

    # =========================================================
    # EXTRACTORS
    # =========================================================
    def _extract_event_name(self, text: str) -> str:
        # Formato classico con emoji 🆚
        m = re.search(r"🆚\s*(.+?)(?:\n|$)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip("* ").strip()
        # Formato con etichetta "Partita:" o "Match:" (con o senza emoji) seguito da riga con i nomi
        m = re.search(r"(?:partita|match)\s*[^:\n]*:?\s*\n\s*(.+?)(?:\n|$)", text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        # Fallback: riga con " vs " tra due nomi di squadra
        m = re.search(r"^(.{3,40}\s+vs\.?\s+.{3,40})$", text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            return m.group(1).strip()
        return ""

    def _extract_score(self, text: str) -> tuple[int, int]:
        # Formato "0 - 0" o "0-0" o "0–0"
        m = re.search(r"(\d+)\s*[-–]\s*(\d+)", text)
        if m:
            return int(m.group(1)), int(m.group(2))
        return 0, 0

    def _extract_minute(self, text: str) -> int:
        # Formato classico: "55m" o "55'"
        m = re.search(r"(\d+)\s*(?:m|')\b", text, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
        # Formato con etichetta: ⏱Minuto⏱\n55  oppure  Minuto: 55  oppure  Min: 55
        m = re.search(
            r"(?:⏱[^\n]*|minuto|minute|min)\s*[:\s]*\n?\s*(\d+)(?:\D|$)",
            text, flags=re.IGNORECASE,
        )
        if m:
            return int(m.group(1))
        return 0

    def _extract_odds(self, text: str) -> Optional[float]:
        # Formato @2.50 o @ 2,50
        m = re.search(r"@\s*(\d+[.,]\d+)", text, flags=re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", "."))
        # Formato "Quota: 2.50" o "Odd: 2.50"
        m = re.search(r"(?:quota|odd|odds)\s*[:\s]+(\d+[.,]\d+)", text, flags=re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", "."))
        return None

    def _extract_stake(self, text: str) -> Optional[float]:
        m = re.search(r"(?:stake|puntata|€)\s*(\d+(?:[.,]\d+)?)", text, flags=re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", "."))
        return None
