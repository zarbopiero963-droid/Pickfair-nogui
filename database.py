from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = "pickfair.db"):
        self.db_path = str(db_path)
        self._local = threading.local()
        self._write_lock = threading.RLock()
        self._ensure_parent_dir()
        self._init_db()

    # =========================================================
    # CORE SQLITE
    # =========================================================
    def _ensure_parent_dir(self) -> None:
        path = Path(self.db_path)
        if path.parent and str(path.parent) not in {"", "."}:
            path.parent.mkdir(parents=True, exist_ok=True)

    def _get_connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    @contextmanager
    def transaction(self):
        conn = self._get_connection()
        with self._write_lock:
            try:
                conn.execute("BEGIN")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _execute(
        self,
        sql: str,
        params: Iterable[Any] | tuple = (),
        *,
        fetch: bool = False,
        fetchone: bool = False,
        commit: bool = True,
    ):
        conn = self._get_connection()
        if commit:
            with self._write_lock:
                cur = conn.execute(sql, tuple(params))
                if fetchone:
                    row = cur.fetchone()
                    conn.commit()
                    return row
                if fetch:
                    rows = cur.fetchall()
                    conn.commit()
                    return rows
                conn.commit()
                return cur
        else:
            cur = conn.execute(sql, tuple(params))
            if fetchone:
                return cur.fetchone()
            if fetch:
                return cur.fetchall()
            return cur

    def close_all_connections(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    # =========================================================
    # INIT SCHEMA
    # =========================================================
    def _init_db(self) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_chats (
                    chat_id TEXT PRIMARY KEY,
                    title TEXT DEFAULT '',
                    is_active INTEGER NOT NULL DEFAULT 1
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS received_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_outbox_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    message_text TEXT,
                    status TEXT DEFAULT '',
                    created_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_saga (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_ref TEXT NOT NULL UNIQUE,
                    batch_id TEXT DEFAULT '',
                    event_key TEXT DEFAULT '',
                    table_id INTEGER,
                    market_id TEXT NOT NULL,
                    selection_id TEXT NOT NULL,
                    bet_type TEXT NOT NULL,
                    price REAL NOT NULL DEFAULT 0.0,
                    stake REAL NOT NULL DEFAULT 0.0,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    bet_id TEXT DEFAULT '',
                    error_text TEXT DEFAULT '',
                    payload_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dutching_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL UNIQUE,
                    event_key TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    event_name TEXT DEFAULT '',
                    market_name TEXT DEFAULT '',
                    table_id INTEGER,
                    strategy TEXT DEFAULT 'DUTCHING',
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    total_legs INTEGER NOT NULL DEFAULT 0,
                    placed_legs INTEGER NOT NULL DEFAULT 0,
                    matched_legs INTEGER NOT NULL DEFAULT 0,
                    failed_legs INTEGER NOT NULL DEFAULT 0,
                    cancelled_legs INTEGER NOT NULL DEFAULT 0,
                    batch_exposure REAL NOT NULL DEFAULT 0.0,
                    avg_profit REAL NOT NULL DEFAULT 0.0,
                    book_pct REAL NOT NULL DEFAULT 0.0,
                    payload_json TEXT DEFAULT '{}',
                    notes TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    closed_at TEXT
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dutching_batch_legs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    leg_index INTEGER NOT NULL,
                    customer_ref TEXT DEFAULT '',
                    market_id TEXT NOT NULL,
                    selection_id TEXT NOT NULL,
                    side TEXT NOT NULL DEFAULT 'BACK',
                    price REAL NOT NULL DEFAULT 0.0,
                    stake REAL NOT NULL DEFAULT 0.0,
                    liability REAL NOT NULL DEFAULT 0.0,
                    bet_id TEXT DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'CREATED',
                    error_text TEXT DEFAULT '',
                    raw_response_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(batch_id, leg_index)
                )
                """
            )

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_order_saga_status ON order_saga(status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_order_saga_batch_id ON order_saga(batch_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_order_saga_event_key ON order_saga(event_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dutching_batches_status ON dutching_batches(status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dutching_legs_batch_id ON dutching_batch_legs(batch_id)"
            )

    # =========================================================
    # SETTINGS / CREDENTIALS
    # =========================================================
    def _set_setting(self, key: str, value: Any) -> None:
        self._execute(
            """
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(key), str(value if value is not None else "")),
        )

    def _get_setting(self, key: str, default: str = "") -> str:
        row = self._execute(
            "SELECT value FROM settings WHERE key = ?",
            (str(key),),
            fetchone=True,
            commit=False,
        )
        if not row:
            return default
        return str(row["value"])

    def save_settings(self, data: Dict[str, Any]) -> None:
        with self.transaction():
            for key, value in (data or {}).items():
                self._set_setting(str(key), value)

    def get_settings(self) -> Dict[str, Any]:
        rows = self._execute(
            "SELECT key, value FROM settings",
            fetch=True,
            commit=False,
        )
        result: Dict[str, Any] = {}
        for row in rows or []:
            result[str(row["key"])] = row["value"]
        return result

    def save_credentials(
        self,
        *,
        username: str,
        app_key: str,
        certificate: str,
        private_key: str,
    ) -> None:
        self.save_settings(
            {
                "username": username,
                "app_key": app_key,
                "certificate": certificate,
                "private_key": private_key,
            }
        )

    def save_password(self, password: str) -> None:
        self._set_setting("password", password)

    def save_session(self, session_token: str, expiry: str = "") -> None:
        self.save_settings(
            {
                "session_token": session_token,
                "session_expiry": expiry,
            }
        )

    def clear_session(self) -> None:
        self.save_settings(
            {
                "session_token": "",
                "session_expiry": "",
            }
        )

    # =========================================================
    # TELEGRAM
    # =========================================================
    def get_telegram_settings(self) -> Dict[str, Any]:
        settings = self.get_settings()
        return {
            "api_id": settings.get("telegram.api_id", settings.get("api_id", "")),
            "api_hash": settings.get("telegram.api_hash", settings.get("api_hash", "")),
            "session_string": settings.get("telegram.session_string", settings.get("session_string", "")),
            "phone_number": settings.get("telegram.phone_number", settings.get("phone_number", "")),
            "enabled": str(settings.get("telegram.enabled", "0")) in {"1", "true", "True"},
            "auto_bet": str(settings.get("telegram.auto_bet", "0")) in {"1", "true", "True"},
            "require_confirmation": str(settings.get("telegram.require_confirmation", "1")) in {"1", "true", "True"},
            "auto_stake": float(settings.get("telegram.auto_stake", 1.0) or 1.0),
        }

    def save_telegram_settings(self, payload: Dict[str, Any]) -> None:
        self.save_settings(
            {
                "telegram.api_id": payload.get("api_id", ""),
                "telegram.api_hash": payload.get("api_hash", ""),
                "telegram.session_string": payload.get("session_string", ""),
                "telegram.phone_number": payload.get("phone_number", ""),
                "telegram.enabled": int(bool(payload.get("enabled", False))),
                "telegram.auto_bet": int(bool(payload.get("auto_bet", False))),
                "telegram.require_confirmation": int(bool(payload.get("require_confirmation", True))),
                "telegram.auto_stake": payload.get("auto_stake", 1.0),
            }
        )

    def get_telegram_chats(self) -> List[Dict[str, Any]]:
        rows = self._execute(
            "SELECT chat_id, title, is_active FROM telegram_chats ORDER BY title, chat_id",
            fetch=True,
            commit=False,
        )
        return [
            {
                "chat_id": row["chat_id"],
                "title": row["title"],
                "is_active": bool(row["is_active"]),
            }
            for row in (rows or [])
        ]

    def save_telegram_chat(self, chat_id: str, title: str, is_active: bool = True) -> None:
        self._execute(
            """
            INSERT INTO telegram_chats(chat_id, title, is_active)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                is_active = excluded.is_active
            """,
            (str(chat_id), str(title or ""), int(bool(is_active))),
        )

    def replace_telegram_chats(self, chats: List[Dict[str, Any]]) -> None:
        with self.transaction() as conn:
            conn.execute("DELETE FROM telegram_chats")
            for item in chats or []:
                conn.execute(
                    """
                    INSERT INTO telegram_chats(chat_id, title, is_active)
                    VALUES (?, ?, ?)
                    """,
                    (
                        str(item.get("chat_id", "")),
                        str(item.get("title", "")),
                        int(bool(item.get("is_active", True))),
                    ),
                )

    def save_received_signal(self, signal: Dict[str, Any]) -> None:
        self._execute(
            """
            INSERT INTO received_signals(signal_json, created_at)
            VALUES (?, ?)
            """,
            (json.dumps(signal or {}, ensure_ascii=False), datetime.utcnow().isoformat()),
        )

    # =========================================================
    # ORDER SAGA
    # =========================================================
    def create_order_saga(
        self,
        *,
        customer_ref: str,
        batch_id: str,
        event_key: str,
        table_id: Optional[int],
        market_id: str,
        selection_id: Any,
        bet_type: str,
        price: float,
        stake: float,
        payload: Dict[str, Any],
        status: str = "PENDING",
    ) -> None:
        now = datetime.utcnow().isoformat()
        self._execute(
            """
            INSERT OR REPLACE INTO order_saga(
                customer_ref, batch_id, event_key, table_id,
                market_id, selection_id, bet_type, price, stake,
                status, payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(customer_ref),
                str(batch_id or ""),
                str(event_key or ""),
                table_id,
                str(market_id),
                str(selection_id),
                str(bet_type),
                float(price or 0.0),
                float(stake or 0.0),
                str(status),
                json.dumps(payload or {}, ensure_ascii=False),
                now,
                now,
            ),
        )

    def update_order_saga(
        self,
        *,
        customer_ref: str,
        status: str,
        bet_id: str = "",
        error_text: str = "",
    ) -> None:
        self._execute(
            """
            UPDATE order_saga
            SET status = ?, bet_id = ?, error_text = ?, updated_at = ?
            WHERE customer_ref = ?
            """,
            (
                str(status),
                str(bet_id or ""),
                str(error_text or ""),
                datetime.utcnow().isoformat(),
                str(customer_ref),
            ),
        )

    def get_order_saga(self, customer_ref: str) -> Optional[Dict[str, Any]]:
        row = self._execute(
            "SELECT * FROM order_saga WHERE customer_ref = ? LIMIT 1",
            (str(customer_ref),),
            fetchone=True,
            commit=False,
        )
        return dict(row) if row else None

    def get_pending_sagas(self) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM order_saga
            WHERE status IN ('PENDING', 'SUBMITTED', 'PLACED', 'PARTIAL', 'ROLLBACK_PENDING')
            ORDER BY created_at ASC, id ASC
            """,
            fetch=True,
            commit=False,
        )
        return [dict(row) for row in (rows or [])]

    def get_batch_sagas(self, batch_id: str) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM order_saga
            WHERE batch_id = ?
            ORDER BY id ASC
            """,
            (str(batch_id),),
            fetch=True,
            commit=False,
        )
        return [dict(row) for row in (rows or [])]
