"""
Telegram Listener for betting signals.
Monitors specified channels/groups/chats and parses betting signals.
"""

__all__ = ["TelegramListener", "SignalQueue", "parse_signal_message"]

import asyncio
import os
import re
import threading
from datetime import datetime
from typing import Callable, Dict, List, Optional

from telethon import TelegramClient, events
from telethon.sessions import StringSession


class TelegramListener:
    """Listens to Telegram messages and triggers bet placement."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_string: str = None,
        session_path: str = None,
    ):
        """
        Initialize Telegram listener.

        Args:
            api_id: Telegram API ID (from my.telegram.org)
            api_hash: Telegram API Hash
            session_string: Optional saved session string for persistent login
            session_path: Optional path to session file (preferred over session_string)
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self.session_path = session_path
        self.client: Optional[TelegramClient] = None
        self.running = False
        self.loop = None
        self.thread = None

        self.monitored_chats: List[int] = []
        self.signal_callback: Optional[Callable] = None
        self.message_callback: Optional[Callable] = None
        self.status_callback: Optional[Callable] = None

        self.db = None
        self.custom_patterns = []
        self.signal_patterns = self._default_patterns()

    def _default_patterns(self) -> Dict:
        """Default regex patterns for parsing betting signals."""
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

    def set_signal_patterns(self, patterns: Dict):
        """Update signal parsing patterns."""
        self.signal_patterns.update(patterns or {})

    def set_database(self, db):
        """Set database reference for loading custom patterns."""
        self.db = db
        self.reload_custom_patterns()

    def reload_custom_patterns(self):
        """Reload custom patterns from database."""
        if hasattr(self, "db") and self.db:
            try:
                try:
                    patterns = self.db.get_signal_patterns(enabled_only=True)
                except TypeError:
                    patterns = self.db.get_signal_patterns()
                    patterns = [
                        p for p in (patterns or []) if bool(p.get("enabled", False))
                    ]
                self.custom_patterns = list(patterns or [])
            except Exception:
                self.custom_patterns = []
        else:
            self.custom_patterns = []

    def set_monitored_chats(self, chat_ids: List[int]):
        """Set list of chat IDs to monitor."""
        self.monitored_chats = list(chat_ids or [])

    def set_callbacks(
        self,
        on_signal: Callable = None,
        on_message: Callable = None,
        on_status: Callable = None,
    ):
        """Set callback functions for events."""
        self.signal_callback = on_signal
        self.message_callback = on_message
        self.status_callback = on_status

    def _base_signal(self, text: str, source: str = "LEGACY") -> Dict:
        return {
            "raw_text": text,
            "timestamp": datetime.now().isoformat(),
            "event": None,
            "match": None,
            "league": None,
            "side": None,
            "action": None,
            "selection": None,
            "market_type": None,
            "market": None,
            "odds": None,
            "price": None,
            "stake": None,
            "score_home": None,
            "score_away": None,
            "over_line": None,
            "minute": None,
            "cashout_type": None,
            "market_id": None,
            "selection_id": None,
            "market_name": None,
            "source": source,
        }

    def _normalize_action(self, value: Optional[str], default: str = "BACK") -> str:
        action = str(value or default).upper().strip()
        if action not in ("BACK", "LAY"):
            return default
        return action

    def _safe_float(self, value):
        try:
            if value is None:
                return None
            return float(str(value).replace(",", ".").strip())
        except Exception:
            return None

    def _safe_int(self, value):
        try:
            if value is None or value == "":
                return None
            return int(str(value).strip())
        except Exception:
            return None

    def _extract_master_field(self, text: str, field: str) -> Optional[str]:
        match = re.search(
            rf"^{field}\s*:\s*(.+)$",
            text,
            re.IGNORECASE | re.MULTILINE,
        )
        return match.group(1).strip() if match else None

    def _parse_master_signal(self, text: str) -> Optional[Dict]:
        event = self._extract_master_field(text, "event_name")
        market = self._extract_master_field(text, "market_name")
        selection = self._extract_master_field(text, "selection")
        action_raw = self._extract_master_field(text, "action")
        price_raw = self._extract_master_field(text, "master_price")
        market_id = self._extract_master_field(text, "market_id")
        selection_id_raw = self._extract_master_field(text, "selection_id")

        if not market_id or selection_id_raw is None:
            return None

        selection_id = self._safe_int(selection_id_raw)
        if selection_id is None:
            return None

        action = self._normalize_action(action_raw, default="BACK")
        price = self._safe_float(price_raw)

        signal = self._base_signal(text, source="MASTER_SIGNAL")
        signal.update(
            {
                "event": event,
                "match": event,
                "side": action,
                "action": action,
                "selection": selection,
                "market_type": "MATCH_ODDS",
                "market": market,
                "market_name": market,
                "odds": price,
                "price": price,
                "market_id": str(market_id).strip(),
                "selection_id": selection_id,
            }
        )
        return signal

    def _parse_custom_patterns(self, text: str) -> Optional[Dict]:
        custom_patterns = getattr(self, "custom_patterns", []) or []

        for cp in custom_patterns:
            try:
                pattern = cp.get("pattern", "")
                if not pattern:
                    continue
                if not re.search(pattern, text, re.IGNORECASE):
                    continue

                signal = self._base_signal(text, source="CUSTOM_PATTERN")

                odds_match = re.search(self.signal_patterns["odds"], text, re.IGNORECASE)
                time_match = re.search(self.signal_patterns["time"], text, re.IGNORECASE)
                score_match = re.search(
                    self.signal_patterns["score"],
                    text,
                    re.IGNORECASE,
                )

                if odds_match:
                    odds_value = self._safe_float(odds_match.group(1))
                    signal["odds"] = odds_value
                    signal["price"] = odds_value

                if time_match:
                    signal["minute"] = self._safe_int(time_match.group(1))

                if score_match:
                    signal["score_home"] = self._safe_int(score_match.group(1))
                    signal["score_away"] = self._safe_int(score_match.group(2))

                min_minute = cp.get("min_minute")
                max_minute = cp.get("max_minute")
                minute = signal["minute"]

                if (
                    min_minute is not None
                    and minute is not None
                    and minute < min_minute
                ):
                    continue
                if (
                    max_minute is not None
                    and minute is not None
                    and minute > max_minute
                ):
                    continue
                if cp.get("live_only") and minute is None:
                    continue

                total_goals = (signal["score_home"] or 0) + (signal["score_away"] or 0)
                min_score = cp.get("min_score")
                max_score = cp.get("max_score")

                if min_score is not None and total_goals < min_score:
                    continue
                if max_score is not None and total_goals > max_score:
                    continue

                action = self._normalize_action(cp.get("bet_side"), default="BACK")
                signal["side"] = action
                signal["action"] = action
                signal["market_type"] = cp.get("market_type", "CUSTOM")

                selection_template = cp.get("selection_template", "")
                if selection_template:
                    selection = selection_template
                    selection = selection.replace(
                        "{home_score}",
                        str(signal["score_home"] or 0),
                    )
                    selection = selection.replace(
                        "{away_score}",
                        str(signal["score_away"] or 0),
                    )
                    selection = selection.replace(
                        "{minute}",
                        str(signal["minute"] or 0),
                    )
                    selection = selection.replace("{total_goals}", str(total_goals))
                    selection = selection.replace("{over_line}", str(total_goals + 0.5))
                    signal["selection"] = selection
                else:
                    signal["selection"] = cp.get("name", "Custom Pattern")

                return signal
            except re.error:
                continue
            except Exception:
                continue

        return None

    def _parse_cashout_signal(self, text: str) -> Optional[Dict]:
        if re.search(self.signal_patterns["cashout_all"], text, re.IGNORECASE):
            signal = self._base_signal(text, source="LEGACY")
            signal["market_type"] = "CASHOUT"
            signal["cashout_type"] = "ALL"
            return signal

        if re.search(self.signal_patterns["cashout"], text, re.IGNORECASE):
            signal = self._base_signal(text, source="LEGACY")
            signal["market_type"] = "CASHOUT"
            signal["cashout_type"] = "SINGLE"
            return signal

        return None

    def _extract_event_from_lines(self, text: str) -> Optional[str]:
        icon_match = re.search(self.signal_patterns["event_icon"], text, re.IGNORECASE)
        if icon_match:
            return icon_match.group(1).strip()

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return None

        first = lines[0]
        if any(ch in first for ch in (" - ", " vs ", " VS ", "–")):
            return first

        return None

    def _parse_legacy_signal(self, text: str) -> Optional[Dict]:
        signal = self._base_signal(text, source="LEGACY")

        event_value = self._extract_event_from_lines(text)
        if event_value:
            signal["event"] = event_value
            signal["match"] = event_value

        league_match = re.search(self.signal_patterns["league"], text, re.IGNORECASE)
        if league_match:
            signal["league"] = league_match.group(1).strip()

        score_match = re.search(self.signal_patterns["score"], text, re.IGNORECASE)
        if score_match:
            signal["score_home"] = self._safe_int(score_match.group(1))
            signal["score_away"] = self._safe_int(score_match.group(2))
            if signal["score_home"] is not None and signal["score_away"] is not None:
                total_goals = signal["score_home"] + signal["score_away"]
                signal["over_line"] = total_goals + 0.5

        time_match = re.search(self.signal_patterns["time"], text, re.IGNORECASE)
        if time_match:
            signal["minute"] = self._safe_int(time_match.group(1))

        if re.search(self.signal_patterns["lay"], text, re.IGNORECASE):
            signal["side"] = "LAY"
            signal["action"] = "LAY"
        elif re.search(self.signal_patterns["back"], text, re.IGNORECASE):
            signal["side"] = "BACK"
            signal["action"] = "BACK"

        odds_match = re.search(self.signal_patterns["odds"], text, re.IGNORECASE)
        if odds_match:
            odds_value = self._safe_float(odds_match.group(1))
            signal["odds"] = odds_value
            signal["price"] = odds_value

        stake_match = re.search(
            self.signal_patterns["stake"],
            text.lower(),
            re.IGNORECASE,
        )
        if stake_match:
            signal["stake"] = self._safe_float(stake_match.group(1))

        if re.search(self.signal_patterns["next_goal"], text, re.IGNORECASE):
            signal["market_type"] = "NEXT_GOAL"
            if signal["score_home"] is not None and signal["score_away"] is not None:
                signal["selection"] = f"Over {signal['over_line']}"
                signal["side"] = "BACK"
                signal["action"] = "BACK"

        over_match = re.search(self.signal_patterns["over"], text, re.IGNORECASE)
        if over_match:
            signal["selection"] = f"Over {over_match.group(2)}"
            signal["market_type"] = "OVER_UNDER"
            if not signal["action"]:
                signal["side"] = "BACK"
                signal["action"] = "BACK"

        under_match = re.search(self.signal_patterns["under"], text, re.IGNORECASE)
        if under_match:
            signal["selection"] = f"Under {under_match.group(2)}"
            signal["market_type"] = "OVER_UNDER"
            if not signal["action"]:
                signal["side"] = "BACK"
                signal["action"] = "BACK"

        # FIX #11: only use the score-derived "Over" default when no explicit
        # over/under signal was already parsed from the message text.
        # Previously this block ran unconditionally, discarding an explicit
        # "under" (or "over") keyword that had already been set above.
        if signal["event"] and signal["score_home"] is not None and not signal["selection"]:
            signal["selection"] = f"Over {signal['over_line']}"
            signal["side"] = "BACK"
            signal["action"] = "BACK"
            signal["market_type"] = "OVER_UNDER"
            return signal

        if signal["action"] and signal["selection"]:
            return signal

        return None

    def parse_signal(self, text: str) -> Optional[Dict]:
        """
        Parse message text for betting signals.

        Supporta:
        - formato MASTER SIGNAL (copy trading)
        - formato custom da DB
        - cashout
        - formato legacy
        """
        if not text or not str(text).strip():
            return None

        text = str(text).strip()

        if "MASTER SIGNAL" in text.upper():
            return self._parse_master_signal(text)

        custom = self._parse_custom_patterns(text)
        if custom is not None:
            return custom

        cashout = self._parse_cashout_signal(text)
        if cashout is not None:
            return cashout

        return self._parse_legacy_signal(text)

    async def _connect(self):
        """Connect to Telegram."""
        try:
            if self.session_path:
                self.client = TelegramClient(
                    self.session_path,
                    self.api_id,
                    self.api_hash,
                )
            elif self.session_string:
                self.client = TelegramClient(
                    StringSession(self.session_string),
                    self.api_id,
                    self.api_hash,
                )
            else:
                session_dir = os.path.join(
                    os.environ.get("APPDATA", "."),
                    "Pickfair",
                )
                os.makedirs(session_dir, exist_ok=True)
                session_path = os.path.join(session_dir, "telegram_session")
                self.client = TelegramClient(session_path, self.api_id, self.api_hash)

            await self.client.connect()

            if not await self.client.is_user_authorized():
                if self.status_callback:
                    self.status_callback("AUTH_REQUIRED", "Autenticazione richiesta")
                return False

            if self.status_callback:
                self.status_callback("CONNECTED", "Connesso a Telegram")

            return True

        except Exception as e:
            if self.status_callback:
                self.status_callback("ERROR", str(e))
            return False

    async def _start_listening(self):
        """Start listening for messages."""
        if not self.client:
            return

        @self.client.on(
            events.NewMessage(
                chats=self.monitored_chats if self.monitored_chats else None
            )
        )
        async def handler(event):
            message = event.message
            text = message.text or ""

            chat_id = event.chat_id
            sender_id = event.sender_id

            if self.message_callback:
                self.message_callback(
                    {
                        "chat_id": chat_id,
                        "sender_id": sender_id,
                        "text": text,
                        "timestamp": datetime.now().isoformat(),
                    }
                )

            signal = self.parse_signal(text)
            if signal and self.signal_callback:
                signal["chat_id"] = chat_id
                signal["sender_id"] = sender_id
                self.signal_callback(signal)

        self.running = True
        if self.status_callback:
            self.status_callback(
                "LISTENING",
                f"In ascolto su {len(self.monitored_chats)} chat",
            )

        await self.client.run_until_disconnected()

    def _run_loop(self):
        """Run the asyncio event loop in a thread."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        try:
            connected = self.loop.run_until_complete(self._connect())
            if connected:
                self.loop.run_until_complete(self._start_listening())
        except Exception as e:
            if self.status_callback:
                self.status_callback("ERROR", str(e))
        finally:
            self.running = False
            if self.loop:
                self.loop.close()

    def start(self):
        """Start the listener in a background thread."""
        if self.running:
            return

        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop the listener."""
        self.running = False

        if self.client and self.loop:
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self.client.disconnect(),
                    self.loop,
                )
                future.result(timeout=5)
            except Exception:
                pass

        if self.status_callback:
            self.status_callback("STOPPED", "Listener fermato")

    def get_session_string(self) -> Optional[str]:
        """Get current session string for saving."""
        if self.client:
            return self.client.session.save()
        return None

    async def request_code(self, phone: str):
        """Request authentication code."""
        if not self.client:
            self.client = TelegramClient(StringSession(), self.api_id, self.api_hash)
            await self.client.connect()

        await self.client.send_code_request(phone)

    async def sign_in(self, phone: str, code: str, password: str = None):
        """Complete sign in with code."""
        try:
            await self.client.sign_in(phone, code, password=password)
            return True, self.client.session.save()
        except Exception as e:
            return False, str(e)


class SignalQueue:
    """Thread-safe queue for betting signals."""

    def __init__(self, max_size: int = 100):
        self.queue: List[Dict] = []
        self.max_size = max_size
        self.lock = threading.Lock()

    def add(self, signal: Dict):
        """Add signal to queue."""
        with self.lock:
            self.queue.append(signal)
            if len(self.queue) > self.max_size:
                self.queue.pop(0)

    def get_pending(self) -> List[Dict]:
        """Get all pending signals."""
        with self.lock:
            return list(self.queue)

    def remove(self, signal: Dict):
        """Remove a signal from queue."""
        with self.lock:
            if signal in self.queue:
                self.queue.remove(signal)

    def clear(self):
        """Clear all signals."""
        with self.lock:
            self.queue.clear()


def parse_signal_message(message: str):
    """
    Stable public parser entrypoint for tests and external callers.
    Uses TelegramListener.parse_signal internally.
    """
    if message is None:
        return None

    text = str(message).strip()
    if not text:
        return None

    try:
        listener = TelegramListener(api_id=0, api_hash="")
        return listener.parse_signal(text)
    except Exception:
        return None