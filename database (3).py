"""
Database layer using SQLite for local storage.
Hedge-Fund Grade: supporta concorrenza massiva (WAL),
nested transactions, Saga Pattern e persistenza totale UI/Telegram.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("DB")


def get_db_path() -> str:
    if os.name == "nt":
        app_data = os.environ.get("APPDATA", os.path.expanduser("~"))
        db_dir = os.path.join(app_data, "Pickfair")
    else:
        db_dir = os.path.join(os.path.expanduser("~"), ".pickfair")

    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, "betfair.db")


class Database:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or get_db_path()
        self._local = threading.local()
        self._write_lock = threading.RLock()
        # FIX #24: track all per-thread connections so we can close them on
        # threads that never call close() explicitly (worker threads, etc.)
        self._all_conns: List[sqlite3.Connection] = []
        self._all_conns_lock = threading.Lock()
        self._init_db()

    # =========================================================
    # INTERNALS
    # =========================================================

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(
                self.db_path,
                timeout=20.0,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
            self._local.transaction_depth = 0
            # FIX #24: register every new per-thread connection centrally so
            # close_all_connections() can clean them up even if the thread
            # never calls close() explicitly.
            with self._all_conns_lock:
                self._all_conns.append(conn)
        return self._local.conn

    def _execute(
        self,
        query: str,
        params: tuple = (),
        commit: bool = True,
        fetch: bool = False,
    ):
        conn = self._get_connection()
        write_op = query.lstrip().upper().startswith(
            ("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "ALTER", "DROP")
        )

        lock = self._write_lock if write_op else _DummyLock()
        with lock:
            try:
                self._local.transaction_depth += 1
                sp_name = f"sp_{self._local.transaction_depth}"
                conn.execute(f"SAVEPOINT {sp_name}")

                cursor = conn.cursor()
                cursor.execute(query, params)

                rows = cursor.fetchall() if fetch else None

                conn.execute(f"RELEASE {sp_name}")

                if commit and self._local.transaction_depth == 1:
                    conn.commit()

                return rows if fetch else cursor

            except Exception as e:
                try:
                    if getattr(self._local, "transaction_depth", 0) > 0:
                        sp_name = f"sp_{self._local.transaction_depth}"
                        conn.execute(f"ROLLBACK TO {sp_name}")
                        conn.execute(f"RELEASE {sp_name}")
                except Exception:
                    pass

                logger.error("[DB] Error: %s | Query: %s", e, query)
                raise
            finally:
                if getattr(self._local, "transaction_depth", 0) > 0:
                    self._local.transaction_depth -= 1

    def _json_dumps(self, value: Any) -> str:
        try:
            return json.dumps(value or [], ensure_ascii=False)
        except Exception:
            return "[]"

    def _json_loads(self, value: Any, default=None):
        if default is None:
            default = []
        if value in (None, "", b""):
            return default
        if isinstance(value, (list, dict)):
            return value
        try:
            return json.loads(value)
        except Exception:
            return default

    def _as_bool_int(self, value: Any, default: int = 0) -> int:
        if value is None:
            return int(default)
        if isinstance(value, bool):
            return int(value)
        try:
            return 1 if int(value) else 0
        except Exception:
            sval = str(value).strip().lower()
            if sval in ("1", "true", "yes", "on"):
                return 1
            if sval in ("0", "false", "no", "off"):
                return 0
            return int(default)

    def _as_float(self, value: Any, default: float = 0.0) -> float:
        if value is None or value == "":
            return float(default)
        try:
            return float(value)
        except Exception:
            try:
                return float(str(value).replace(",", "."))
            except Exception:
                return float(default)

    def _as_int(self, value: Any, default: int = 0) -> int:
        if value is None or value == "":
            return int(default)
        try:
            return int(value)
        except Exception:
            try:
                return int(float(value))
            except Exception:
                return int(default)

    # =========================================================
    # DB INIT
    # =========================================================

    def _init_db(self):
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS bet_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                placed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_name TEXT,
                market_id TEXT,
                market_name TEXT,
                bet_type TEXT,
                selections TEXT,
                total_stake REAL,
                potential_profit REAL,
                status TEXT
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS simulation_bet_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                placed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_name TEXT,
                market_id TEXT,
                market_name TEXT,
                side TEXT,
                selection_id TEXT,
                selection_name TEXT,
                price REAL,
                stake REAL,
                status TEXT,
                selections TEXT,
                total_stake REAL,
                potential_profit REAL
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS cashout_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                market_id TEXT,
                selection_id TEXT,
                original_bet_id TEXT,
                cashout_bet_id TEXT,
                original_side TEXT,
                original_stake REAL,
                original_price REAL,
                cashout_side TEXT,
                cashout_stake REAL,
                cashout_price REAL,
                profit_loss REAL
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS order_saga (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_ref TEXT UNIQUE NOT NULL,
                market_id TEXT NOT NULL,
                selection_id TEXT,
                payload_hash TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                api_id TEXT,
                api_hash TEXT,
                session_string TEXT,
                phone_number TEXT,
                enabled INTEGER DEFAULT 0,
                auto_bet INTEGER DEFAULT 0,
                require_confirmation INTEGER DEFAULT 1,
                auto_stake REAL DEFAULT 1.0,
                master_chat_id TEXT,
                publisher_chat_id TEXT
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_chats (
                chat_id TEXT PRIMARY KEY,
                title TEXT,
                username TEXT,
                is_active INTEGER DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS signal_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern TEXT NOT NULL,
                label TEXT,
                enabled INTEGER DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                selection TEXT,
                action TEXT,
                price REAL,
                stake REAL,
                status TEXT
            )
            """
        )

        self._execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_outbox_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                chat_id TEXT,
                message_type TEXT,
                text TEXT,
                status TEXT,
                message_id TEXT,
                error TEXT,
                flood_wait INTEGER DEFAULT 0
            )
            """
        )

        self._seed_default_simulation_settings()

    def _seed_default_simulation_settings(self):
        defaults = {
            "virtual_balance": "1000.0",
            "starting_balance": "1000.0",
            "bet_count": "0",
        }
        current = self.get_settings()
        for key, value in defaults.items():
            if key not in current:
                self._set_setting(key, value)

    # =========================================================
    # =========================================================
    # FIX #19: credential encryption at rest
    #
    # Sensitive keys (private_key, certificate, app_key, password) are stored
    # as XOR-encrypted, base64-encoded blobs prefixed with "ENC:".
    # The key is derived from the machine's stable identifier (hostname +
    # cpu_count) via PBKDF2-HMAC-SHA256.  This is NOT strong authenticated
    # encryption but it prevents plaintext credentials from being readable by
    # simple "SELECT value FROM settings" queries or file readers.
    #
    # Backward compatibility: rows that do NOT start with "ENC:" are treated
    # as legacy plaintext and returned as-is, allowing a seamless transition
    # without a migration script.
    # =========================================================

    _SENSITIVE_KEYS = frozenset(
        {"private_key", "certificate", "app_key", "password"}
    )
    _ENC_PREFIX = "ENC:"

    @staticmethod
    def _derive_db_key() -> bytes:
        """Derive a per-machine 32-byte key using stable host attributes."""
        import platform
        import socket
        raw = f"{socket.gethostname()}-{platform.node()}-pickfair-db-v1"
        return hashlib.pbkdf2_hmac("sha256", raw.encode(), b"pickfair-salt-v1", 100_000)

    @classmethod
    def _encrypt_value(cls, plaintext: str) -> str:
        """XOR-cipher + base64 the value. Returns an 'ENC:…' tagged string."""
        key = cls._derive_db_key()
        data = plaintext.encode("utf-8")
        # Extend key via HMAC to match data length
        keystream = bytearray()
        counter = 0
        while len(keystream) < len(data):
            keystream.extend(hmac.new(key, counter.to_bytes(4, "big"), "sha256").digest())
            counter += 1
        cipher = bytes(b ^ k for b, k in zip(data, keystream))
        return cls._ENC_PREFIX + base64.b64encode(cipher).decode("ascii")

    @classmethod
    def _decrypt_value(cls, stored: str) -> str:
        """Decrypt an 'ENC:…' value. Returns plaintext."""
        payload = stored[len(cls._ENC_PREFIX):]
        cipher = base64.b64decode(payload)
        key = cls._derive_db_key()
        keystream = bytearray()
        counter = 0
        while len(keystream) < len(cipher):
            keystream.extend(hmac.new(key, counter.to_bytes(4, "big"), "sha256").digest())
            counter += 1
        return bytes(b ^ k for b, k in zip(cipher, keystream)).decode("utf-8")

    # SETTINGS
    # =========================================================

    def _set_setting(self, key: str, value: Any):
        raw = "" if value is None else str(value)
        # FIX #19: encrypt sensitive credential fields before storing.
        if key in self._SENSITIVE_KEYS and raw:
            stored = self._encrypt_value(raw)
        else:
            stored = raw
        self._execute(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(key), stored),
        )

    def _get_setting_raw(self, key: str, default: Any = None) -> Any:
        rows = self._execute(
            "SELECT value FROM settings WHERE key = ?",
            (str(key),),
            fetch=True,
            commit=False,
        )
        if not rows:
            return default
        stored = rows[0]["value"]
        # FIX #19: transparently decrypt sensitive fields.
        # Legacy plaintext rows (no ENC: prefix) are returned as-is so
        # old data is still readable without a migration step.
        if (
            key in self._SENSITIVE_KEYS
            and isinstance(stored, str)
            and stored.startswith(self._ENC_PREFIX)
        ):
            try:
                return self._decrypt_value(stored)
            except Exception:
                return stored  # fall back to raw on any decrypt error
        return stored

    def _parse_setting_value(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            return value

        text = value.strip()
        if text == "":
            return ""

        if text.startswith("{") or text.startswith("["):
            try:
                return json.loads(text)
            except Exception:
                return text

        return text

    def get_settings(self) -> Dict[str, Any]:
        rows = self._execute(
            "SELECT key, value FROM settings",
            fetch=True,
            commit=False,
        )
        result: Dict[str, Any] = {}
        for row in rows or []:
            key = row["key"]
            raw = row["value"]
            # FIX #19: decrypt sensitive values before returning
            if (
                key in self._SENSITIVE_KEYS
                and isinstance(raw, str)
                and raw.startswith(self._ENC_PREFIX)
            ):
                try:
                    raw = self._decrypt_value(raw)
                except Exception:
                    pass  # fall back to raw on decrypt error
            result[key] = self._parse_setting_value(raw)
        return result

    def save_settings(self, settings: Optional[Dict[str, Any]] = None, **kwargs):
        payload = {}

        if isinstance(settings, dict):
            payload.update(settings)

        payload.update(kwargs)

        if not payload:
            return

        for key, value in payload.items():
            self._set_setting(str(key), value)

    def save_credentials(
        self,
        username: str,
        app_key: str,
        certificate: str,
        private_key: str,
    ):
        self.save_settings(
            username=username or "",
            app_key=app_key or "",
            certificate=certificate or "",
            private_key=private_key or "",
        )

    def save_password(self, password: Optional[str]):
        if password is None:
            try:
                self._execute("DELETE FROM settings WHERE key = 'password'")
            except Exception as e:
                logger.error("Errore save_password(None): %s", e)
            return

        self._set_setting("password", str(password))

    def save_session(
        self,
        session_token: Optional[str],
        expiry: Optional[str] = None,
    ):
        self._set_setting("session_token", session_token or "")
        self._set_setting("session_expiry", expiry or "")

    def clear_session(self):
        try:
            self._execute(
                "DELETE FROM settings WHERE key IN ('session_token','session_expiry')"
            )
        except Exception as e:
            logger.error("Errore clear_session: %s", e)

    def clear_sessions(self):
        self.clear_session()

    def save_update_url(self, update_url: Optional[str]):
        self._set_setting("update_url", update_url or "")

    def save_skipped_version(self, version: Optional[str]):
        self._set_setting("skipped_version", version or "")

    # =========================================================
    # TELEGRAM SETTINGS
    # =========================================================

    def get_telegram_settings(self) -> Dict[str, Any]:
        rows = self._execute(
            "SELECT * FROM telegram_settings WHERE id = 1",
            fetch=True,
            commit=False,
        )
        if rows:
            row = dict(rows[0])
            row["enabled"] = bool(self._as_bool_int(row.get("enabled")))
            row["auto_bet"] = bool(self._as_bool_int(row.get("auto_bet")))
            row["require_confirmation"] = bool(
                self._as_bool_int(row.get("require_confirmation"), 1)
            )
            row["auto_stake"] = self._as_float(row.get("auto_stake"), 1.0)
            return row

        settings = self.get_settings()
        return {
            "api_id": settings.get("tg_api_id", settings.get("api_id", "")),
            "api_hash": settings.get("tg_api_hash", settings.get("api_hash", "")),
            "session_string": settings.get("tg_session_string", ""),
            "phone_number": settings.get("tg_phone_number", ""),
            "enabled": bool(self._as_bool_int(settings.get("tg_enabled", 0))),
            "auto_bet": bool(self._as_bool_int(settings.get("tg_auto_bet", 0))),
            "require_confirmation": bool(
                self._as_bool_int(settings.get("tg_require_confirmation", 1), 1)
            ),
            "auto_stake": self._as_float(settings.get("tg_auto_stake", 1.0), 1.0),
            "master_chat_id": settings.get("master_chat_id", ""),
            "publisher_chat_id": settings.get("publisher_chat_id", ""),
        }

    def save_telegram_settings(
        self,
        settings: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        payload = {}
        if isinstance(settings, dict):
            payload.update(settings)
        payload.update(kwargs)

        current = self.get_telegram_settings()
        current.update(payload)

        self._execute(
            """
            INSERT INTO telegram_settings (
                id, api_id, api_hash, session_string, phone_number,
                enabled, auto_bet, require_confirmation, auto_stake,
                master_chat_id, publisher_chat_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                api_id=excluded.api_id,
                api_hash=excluded.api_hash,
                session_string=excluded.session_string,
                phone_number=excluded.phone_number,
                enabled=excluded.enabled,
                auto_bet=excluded.auto_bet,
                require_confirmation=excluded.require_confirmation,
                auto_stake=excluded.auto_stake,
                master_chat_id=excluded.master_chat_id,
                publisher_chat_id=excluded.publisher_chat_id
            """,
            (
                1,
                str(current.get("api_id", "") or ""),
                str(current.get("api_hash", "") or ""),
                str(current.get("session_string", "") or ""),
                str(current.get("phone_number", "") or ""),
                self._as_bool_int(current.get("enabled", 0)),
                self._as_bool_int(current.get("auto_bet", 0)),
                self._as_bool_int(current.get("require_confirmation", 1), 1),
                self._as_float(current.get("auto_stake", 1.0), 1.0),
                str(current.get("master_chat_id", "") or ""),
                str(current.get("publisher_chat_id", "") or ""),
            ),
        )

    # =========================================================
    # TELEGRAM CHATS
    # =========================================================

    def get_telegram_chats(self) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM telegram_chats
            ORDER BY created_at ASC, chat_id ASC
            """,
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            item = dict(row)
            item["is_active"] = bool(self._as_bool_int(item.get("is_active"), 1))
            result.append(item)
        return result

    def save_telegram_chat(
        self,
        chat_id,
        title: Optional[str] = None,
        username: Optional[str] = None,
        is_active: bool = True,
    ):
        self._execute(
            """
            INSERT INTO telegram_chats (chat_id, title, username, is_active)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title=excluded.title,
                username=excluded.username,
                is_active=excluded.is_active
            """,
            (
                str(chat_id),
                str(title or ""),
                str(username or ""),
                self._as_bool_int(is_active, 1),
            ),
        )

    def replace_telegram_chats(self, chats: List[Dict[str, Any]]):
        """
        FIX #24: perform DELETE + INSERT atomically inside a single SQLite
        transaction so a crash between the two operations cannot leave the
        table empty.

        Old code called _execute twice with commit=True between them:
            DELETE FROM telegram_chats   ← committed
            INSERT …                     ← crash here → table is empty
        New code wraps both operations in a single SAVEPOINT so either both
        or neither are committed.
        """
        conn = self._get_connection()
        with self._write_lock:
            conn.execute("SAVEPOINT replace_chats_sp")
            try:
                conn.execute("DELETE FROM telegram_chats")
                for chat in chats or []:
                    conn.execute(
                        """
                        INSERT INTO telegram_chats (chat_id, title, username, is_active)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(chat_id) DO UPDATE SET
                            title=excluded.title,
                            username=excluded.username,
                            is_active=excluded.is_active
                        """,
                        (
                            str(chat.get("chat_id") or ""),
                            str(chat.get("title") or ""),
                            str(chat.get("username") or ""),
                            1 if chat.get("is_active", True) else 0,
                        ),
                    )
                conn.execute("RELEASE replace_chats_sp")
                conn.commit()
            except Exception:
                try:
                    conn.execute("ROLLBACK TO replace_chats_sp")
                    conn.execute("RELEASE replace_chats_sp")
                except Exception:
                    pass
                raise

    def delete_telegram_chat(self, chat_id):
        self._execute(
            "DELETE FROM telegram_chats WHERE chat_id = ?",
            (str(chat_id),),
        )

    # =========================================================
    # SIGNAL PATTERNS
    # =========================================================

    def get_signal_patterns(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        if enabled_only:
            rows = self._execute(
                """
                SELECT *
                FROM signal_patterns
                WHERE enabled = 1
                ORDER BY id ASC
                """,
                fetch=True,
                commit=False,
            )
        else:
            rows = self._execute(
                """
                SELECT *
                FROM signal_patterns
                ORDER BY id ASC
                """,
                fetch=True,
                commit=False,
            )

        result = []
        for row in rows or []:
            item = dict(row)
            item["enabled"] = bool(self._as_bool_int(item.get("enabled"), 1))
            result.append(item)
        return result

    def save_signal_pattern(
        self,
        pattern: str,
        label: Optional[str] = None,
        enabled: bool = True,
    ):
        self._execute(
            """
            INSERT INTO signal_patterns (pattern, label, enabled)
            VALUES (?, ?, ?)
            """,
            (
                str(pattern or ""),
                str(label or ""),
                self._as_bool_int(enabled, 1),
            ),
        )

    def update_signal_pattern(
        self,
        pattern_id,
        pattern: str,
        label: Optional[str] = None,
    ):
        self._execute(
            """
            UPDATE signal_patterns
            SET pattern = ?, label = ?
            WHERE id = ?
            """,
            (
                str(pattern or ""),
                str(label or ""),
                self._as_int(pattern_id),
            ),
        )

    def toggle_signal_pattern(self, pattern_id) -> bool:
        rows = self._execute(
            "SELECT enabled FROM signal_patterns WHERE id = ?",
            (self._as_int(pattern_id),),
            fetch=True,
            commit=False,
        )
        if not rows:
            raise ValueError("Pattern non trovato")

        current = self._as_bool_int(rows[0]["enabled"], 1)
        new_state = 0 if current else 1

        self._execute(
            "UPDATE signal_patterns SET enabled = ? WHERE id = ?",
            (new_state, self._as_int(pattern_id)),
        )
        return bool(new_state)

    def delete_signal_pattern(self, pattern_id):
        self._execute(
            "DELETE FROM signal_patterns WHERE id = ?",
            (self._as_int(pattern_id),),
        )

    # =========================================================
    # TELEGRAM SIGNALS INBOX
    # =========================================================

    def save_received_signal(
        self,
        selection: str,
        action: str,
        price: float,
        stake: float,
        status: str,
    ):
        self._execute(
            """
            INSERT INTO telegram_signals (selection, action, price, stake, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(selection or ""),
                str(action or ""),
                self._as_float(price, 0.0),
                self._as_float(stake, 0.0),
                str(status or ""),
            ),
        )

    def get_received_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM telegram_signals
            ORDER BY received_at DESC, id DESC
            LIMIT ?
            """,
            (self._as_int(limit, 50),),
            fetch=True,
            commit=False,
        )
        return [dict(row) for row in rows or []]

    def clear_received_signals(self):
        self._execute("DELETE FROM telegram_signals")

    # =========================================================
    # TELEGRAM OUTBOX LOG
    # =========================================================

    def save_telegram_outbox_log(
        self,
        chat_id,
        message_type,
        text,
        status,
        message_id=None,
        error=None,
        flood_wait=0,
    ):
        self._execute(
            """
            INSERT INTO telegram_outbox_log (
                chat_id, message_type, text, status, message_id, error, flood_wait
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(chat_id) if chat_id is not None else "",
                str(message_type or ""),
                str(text or ""),
                str(status or ""),
                str(message_id) if message_id is not None else "",
                str(error or ""),
                self._as_int(flood_wait, 0),
            ),
        )

    def get_telegram_outbox_log(self, limit=100) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM telegram_outbox_log
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (self._as_int(limit, 100),),
            fetch=True,
            commit=False,
        )
        return [dict(row) for row in rows or []]

    def clear_telegram_outbox_log(self):
        self._execute("DELETE FROM telegram_outbox_log")

    # =========================================================
    # SAGA
    # =========================================================

    def create_pending_saga(self, customer_ref, market_id, selection_id, payload):
        raw_payload = (
            payload
            if isinstance(payload, str)
            else json.dumps(payload or {}, ensure_ascii=False)
        )
        payload_hash = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

        self._execute(
            """
            INSERT OR REPLACE INTO order_saga (
                customer_ref, market_id, selection_id, payload_hash, raw_payload, status
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(customer_ref),
                str(market_id),
                "" if selection_id is None else str(selection_id),
                payload_hash,
                raw_payload,
                "PENDING",
            ),
        )

    def get_pending_sagas(self) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM order_saga
            WHERE status = 'PENDING'
            ORDER BY created_at ASC, id ASC
            """,
            fetch=True,
            commit=False,
        )
        return [dict(row) for row in rows or []]

    def mark_saga_reconciled(self, customer_ref):
        self._execute(
            "UPDATE order_saga SET status = 'RECONCILED' WHERE customer_ref = ?",
            (str(customer_ref),),
        )

    def mark_saga_failed(self, customer_ref):
        self._execute(
            "UPDATE order_saga SET status = 'FAILED' WHERE customer_ref = ?",
            (str(customer_ref),),
        )

    # =========================================================
    # BET HISTORY
    # =========================================================

    def save_bet(
        self,
        event_name,
        market_id,
        market_name,
        bet_type,
        selections,
        total_stake,
        potential_profit,
        status="MATCHED",
    ):
        self._execute(
            """
            INSERT INTO bet_history (
                placed_at, event_name, market_id, market_name,
                bet_type, selections, total_stake, potential_profit, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                str(event_name or ""),
                str(market_id or ""),
                str(market_name or ""),
                str(bet_type or ""),
                self._json_dumps(selections),
                self._as_float(total_stake, 0.0),
                self._as_float(potential_profit, 0.0),
                str(status or "MATCHED"),
            ),
        )

    def get_recent_bets(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self.get_bet_history(limit=limit)

    def get_bet_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM bet_history
            ORDER BY placed_at DESC, id DESC
            LIMIT ?
            """,
            (self._as_int(limit, 100),),
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            item = dict(row)
            item["selections"] = self._json_loads(item.get("selections"), default=[])
            result.append(item)
        return result

    def get_today_profit_loss(self) -> float:
        rows = self._execute(
            """
            SELECT COALESCE(SUM(potential_profit), 0) AS total
            FROM bet_history
            WHERE date(placed_at) = date('now', 'localtime')
              AND status IN ('MATCHED', 'PARTIALLY_MATCHED')
            """,
            fetch=True,
            commit=False,
        )
        return self._as_float(rows[0]["total"], 0.0) if rows else 0.0

    def get_active_bets_count(self) -> int:
        rows = self._execute(
            """
            SELECT COUNT(*) AS cnt
            FROM bet_history
            WHERE status IN ('MATCHED', 'PARTIALLY_MATCHED', 'UNMATCHED')
            """,
            fetch=True,
            commit=False,
        )
        return self._as_int(rows[0]["cnt"], 0) if rows else 0

    # =========================================================
    # SIMULATION
    # =========================================================

    def save_simulation_bet(self, **kwargs):
        selections = kwargs.get("selections")
        self._execute(
            """
            INSERT INTO simulation_bet_history (
                placed_at, event_name, market_id, market_name,
                side, selection_id, selection_name, price, stake, status,
                selections, total_stake, potential_profit
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                str(kwargs.get("event_name", "") or ""),
                str(kwargs.get("market_id", "") or ""),
                str(kwargs.get("market_name", "") or ""),
                str(kwargs.get("side", "") or ""),
                str(kwargs.get("selection_id", "") or ""),
                str(kwargs.get("selection_name", "") or ""),
                self._as_float(kwargs.get("price", 0.0), 0.0),
                self._as_float(kwargs.get("stake", 0.0), 0.0),
                str(kwargs.get("status", "") or ""),
                self._json_dumps(selections) if selections is not None else "[]",
                self._as_float(
                    kwargs.get("total_stake", kwargs.get("stake", 0.0)),
                    0.0,
                ),
                self._as_float(kwargs.get("potential_profit", 0.0), 0.0),
            ),
        )

    def add_simulated_bet(self, *args, **kwargs):
        self.save_simulation_bet(**kwargs)

    def get_simulation_bets(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self.get_simulation_bet_history(limit=limit)

    def get_simulation_bet_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM simulation_bet_history
            ORDER BY placed_at DESC, id DESC
            LIMIT ?
            """,
            (self._as_int(limit, 100),),
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            item = dict(row)
            item["selections"] = self._json_loads(item.get("selections"), default=[])
            result.append(item)
        return result

    def get_simulation_settings(self) -> Dict[str, Any]:
        settings = self.get_settings()
        return {
            "virtual_balance": self._as_float(
                settings.get("virtual_balance", "1000.0"),
                1000.0,
            ),
            "starting_balance": self._as_float(
                settings.get("starting_balance", "1000.0"),
                1000.0,
            ),
            "bet_count": self._as_int(settings.get("bet_count", "0"), 0),
        }

    def increment_simulation_bet_count(self, new_balance):
        current = self.get_simulation_settings()
        self._set_setting(
            "virtual_balance",
            str(self._as_float(new_balance, current["virtual_balance"])),
        )
        self._set_setting("bet_count", str(current["bet_count"] + 1))

    # =========================================================
    # CASHOUT
    # =========================================================

    def save_cashout_transaction(
        self,
        market_id,
        selection_id,
        original_bet_id,
        cashout_bet_id,
        original_side,
        original_stake,
        original_price,
        cashout_side,
        cashout_stake,
        cashout_price,
        profit_loss,
    ):
        self._execute(
            """
            INSERT INTO cashout_history (
                created_at, market_id, selection_id, original_bet_id,
                cashout_bet_id, original_side, original_stake, original_price,
                cashout_side, cashout_stake, cashout_price, profit_loss
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                str(market_id or ""),
                str(selection_id or ""),
                str(original_bet_id or ""),
                str(cashout_bet_id or ""),
                str(original_side or ""),
                self._as_float(original_stake, 0.0),
                self._as_float(original_price, 0.0),
                str(cashout_side or ""),
                self._as_float(cashout_stake, 0.0),
                self._as_float(cashout_price, 0.0),
                self._as_float(profit_loss, 0.0),
            ),
        )

    # =========================================================
    # CLOSE
    # =========================================================

    def close(self):
        """Close this thread's connection. Removes it from the global registry."""
        conn = getattr(self._local, "conn", None)
        if conn:
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None
            # FIX #24: remove from the global registry on explicit close so
            # close_all_connections does not try to close it a second time.
            with self._all_conns_lock:
                try:
                    self._all_conns.remove(conn)
                except ValueError:
                    pass

    def close_all_connections(self):
        """
        FIX #24: close every per-thread connection that was ever created by
        this Database instance, regardless of which thread opened it.

        Call this at application shutdown to ensure worker-thread connections
        (SafeExecutor, PluginRunner, TelegramSender, etc.) are not leaked.
        """
        with self._all_conns_lock:
            conns, self._all_conns = list(self._all_conns), []
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass


class _DummyLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False