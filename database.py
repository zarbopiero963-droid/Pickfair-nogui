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

    def _safe_json_loads(self, raw: Any, default: Any) -> Any:
        if raw in (None, ""):
            return default
        try:
            return json.loads(raw)
        except Exception:
            return default

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
                CREATE TABLE IF NOT EXISTS signal_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern TEXT NOT NULL,
                    label TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_received_signals_created_at ON received_signals(created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_patterns_enabled ON signal_patterns(enabled)"
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
            "session_string": settings.get(
                "telegram.session_string",
                settings.get("session_string", ""),
            ),
            "phone_number": settings.get(
                "telegram.phone_number",
                settings.get("phone_number", ""),
            ),
            "enabled": str(settings.get("telegram.enabled", "0")) in {"1", "true", "True"},
            "auto_bet": str(settings.get("telegram.auto_bet", "0")) in {"1", "true", "True"},
            "require_confirmation": str(
                settings.get("telegram.require_confirmation", "1")
            ) in {"1", "true", "True"},
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
                "telegram.require_confirmation": int(
                    bool(payload.get("require_confirmation", True))
                ),
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

    def save_telegram_chat(
        self,
        chat_id: str,
        title: str,
        is_active: bool = True,
    ) -> None:
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

    def save_received_signal(self, signal: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """
        Compatibile con entrambe le forme:
        - save_received_signal({...})
        - save_received_signal(selection=..., action=..., ...)
        """
        payload = dict(signal or {})
        if kwargs:
            payload.update(kwargs)

        if "received_at" not in payload:
            payload["received_at"] = datetime.utcnow().isoformat()

        self._execute(
            """
            INSERT INTO received_signals(signal_json, created_at)
            VALUES (?, ?)
            """,
            (
                json.dumps(payload, ensure_ascii=False),
                datetime.utcnow().isoformat(),
            ),
        )

    def get_received_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT id, signal_json, created_at
            FROM received_signals
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
            fetch=True,
            commit=False,
        )
        result: List[Dict[str, Any]] = []
        for row in rows or []:
            payload = self._safe_json_loads(row["signal_json"], {})
            if not isinstance(payload, dict):
                payload = {}
            payload["id"] = row["id"]
            payload.setdefault("created_at", row["created_at"])
            payload.setdefault("received_at", row["created_at"])
            result.append(payload)
        return result

    # =========================================================
    # SIGNAL PATTERNS
    # =========================================================
    def get_signal_patterns(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        if enabled_only:
            rows = self._execute(
                """
                SELECT id, pattern, label, enabled, created_at, updated_at
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
                SELECT id, pattern, label, enabled, created_at, updated_at
                FROM signal_patterns
                ORDER BY id ASC
                """,
                fetch=True,
                commit=False,
            )

        return [
            {
                "id": row["id"],
                "pattern": row["pattern"],
                "label": row["label"],
                "enabled": bool(row["enabled"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in (rows or [])
        ]

    def save_signal_pattern(
        self,
        *,
        pattern: str,
        label: str,
        enabled: bool = True,
    ) -> int:
        now = datetime.utcnow().isoformat()
        cur = self._execute(
            """
            INSERT INTO signal_patterns(pattern, label, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(pattern or ""),
                str(label or ""),
                int(bool(enabled)),
                now,
                now,
            ),
        )
        return int(cur.lastrowid)

    def update_signal_pattern(
        self,
        *,
        pattern_id: int,
        pattern: str,
        label: str,
    ) -> None:
        self._execute(
            """
            UPDATE signal_patterns
            SET pattern = ?, label = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                str(pattern or ""),
                str(label or ""),
                datetime.utcnow().isoformat(),
                int(pattern_id),
            ),
        )

    def delete_signal_pattern(self, pattern_id: int) -> None:
        self._execute(
            "DELETE FROM signal_patterns WHERE id = ?",
            (int(pattern_id),),
        )

    def toggle_signal_pattern(self, pattern_id: int) -> bool:
        row = self._execute(
            "SELECT enabled FROM signal_patterns WHERE id = ?",
            (int(pattern_id),),
            fetchone=True,
            commit=False,
        )
        if not row:
            raise ValueError(f"Pattern non trovato: {pattern_id}")

        new_state = not bool(row["enabled"])
        self._execute(
            """
            UPDATE signal_patterns
            SET enabled = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                int(new_state),
                datetime.utcnow().isoformat(),
                int(pattern_id),
            ),
        )
        return new_state

    # =========================================================
    # TELEGRAM OUTBOX LOG
    # =========================================================
    def save_telegram_outbox_log(
        self,
        *,
        chat_id: str = "",
        message_text: str = "",
        status: str = "",
    ) -> int:
        cur = self._execute(
            """
            INSERT INTO telegram_outbox_log(chat_id, message_text, status, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                str(chat_id or ""),
                str(message_text or ""),
                str(status or ""),
                datetime.utcnow().isoformat(),
            ),
        )
        return int(cur.lastrowid)

    def get_telegram_outbox_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT id, chat_id, message_text, status, created_at
            FROM telegram_outbox_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
            fetch=True,
            commit=False,
        )
        return [dict(row) for row in (rows or [])]

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

    # =========================================================
    # DUTCHING BATCHES
    # =========================================================
    def create_dutching_batch(
        self,
        *,
        batch_id: str,
        event_key: str,
        market_id: str,
        event_name: str = "",
        market_name: str = "",
        table_id: Optional[int] = None,
        strategy: str = "DUTCHING",
        total_legs: int = 0,
        batch_exposure: float = 0.0,
        avg_profit: float = 0.0,
        book_pct: float = 0.0,
        payload: Optional[Dict[str, Any]] = None,
        notes: str = "",
        status: str = "PENDING",
    ) -> None:
        now = datetime.utcnow().isoformat()
        self._execute(
            """
            INSERT OR REPLACE INTO dutching_batches(
                batch_id, event_key, market_id, event_name, market_name, table_id,
                strategy, status, total_legs, batch_exposure, avg_profit, book_pct,
                payload_json, notes, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(batch_id),
                str(event_key),
                str(market_id),
                str(event_name or ""),
                str(market_name or ""),
                table_id,
                str(strategy or "DUTCHING"),
                str(status or "PENDING"),
                int(total_legs or 0),
                float(batch_exposure or 0.0),
                float(avg_profit or 0.0),
                float(book_pct or 0.0),
                json.dumps(payload or {}, ensure_ascii=False),
                str(notes or ""),
                now,
                now,
            ),
        )

    def update_dutching_batch(
        self,
        *,
        batch_id: str,
        status: Optional[str] = None,
        placed_legs: Optional[int] = None,
        matched_legs: Optional[int] = None,
        failed_legs: Optional[int] = None,
        cancelled_legs: Optional[int] = None,
        notes: Optional[str] = None,
        closed: bool = False,
    ) -> None:
        updates = []
        params: List[Any] = []

        if status is not None:
            updates.append("status = ?")
            params.append(str(status))
        if placed_legs is not None:
            updates.append("placed_legs = ?")
            params.append(int(placed_legs))
        if matched_legs is not None:
            updates.append("matched_legs = ?")
            params.append(int(matched_legs))
        if failed_legs is not None:
            updates.append("failed_legs = ?")
            params.append(int(failed_legs))
        if cancelled_legs is not None:
            updates.append("cancelled_legs = ?")
            params.append(int(cancelled_legs))
        if notes is not None:
            updates.append("notes = ?")
            params.append(str(notes))
        if closed:
            updates.append("closed_at = ?")
            params.append(datetime.utcnow().isoformat())

        updates.append("updated_at = ?")
        params.append(datetime.utcnow().isoformat())
        params.append(str(batch_id))

        self._execute(
            f"""
            UPDATE dutching_batches
            SET {", ".join(updates)}
            WHERE batch_id = ?
            """,
            tuple(params),
        )

    def get_dutching_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        row = self._execute(
            "SELECT * FROM dutching_batches WHERE batch_id = ? LIMIT 1",
            (str(batch_id),),
            fetchone=True,
            commit=False,
        )
        if not row:
            return None
        data = dict(row)
        data["payload"] = self._safe_json_loads(data.pop("payload_json", "{}"), {})
        return data

    def get_open_dutching_batches(self) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM dutching_batches
            WHERE status IN ('PENDING', 'SUBMITTED', 'PARTIAL', 'OPEN')
            ORDER BY created_at ASC, id ASC
            """,
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            data = dict(row)
            data["payload"] = self._safe_json_loads(data.pop("payload_json", "{}"), {})
            result.append(data)
        return result

    # =========================================================
    # DUTCHING LEGS
    # =========================================================
    def create_dutching_leg(
        self,
        *,
        batch_id: str,
        leg_index: int,
        customer_ref: str = "",
        market_id: str,
        selection_id: Any,
        side: str = "BACK",
        price: float = 0.0,
        stake: float = 0.0,
        liability: float = 0.0,
        status: str = "CREATED",
        raw_response: Optional[Dict[str, Any]] = None,
        error_text: str = "",
    ) -> None:
        now = datetime.utcnow().isoformat()
        self._execute(
            """
            INSERT OR REPLACE INTO dutching_batch_legs(
                batch_id, leg_index, customer_ref, market_id, selection_id, side,
                price, stake, liability, status, raw_response_json, error_text,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(batch_id),
                int(leg_index),
                str(customer_ref or ""),
                str(market_id),
                str(selection_id),
                str(side or "BACK"),
                float(price or 0.0),
                float(stake or 0.0),
                float(liability or 0.0),
                str(status or "CREATED"),
                json.dumps(raw_response or {}, ensure_ascii=False),
                str(error_text or ""),
                now,
                now,
            ),
        )

    def update_dutching_leg(
        self,
        *,
        batch_id: str,
        leg_index: int,
        status: str,
        bet_id: str = "",
        error_text: str = "",
        raw_response: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._execute(
            """
            UPDATE dutching_batch_legs
            SET status = ?, bet_id = ?, error_text = ?, raw_response_json = ?, updated_at = ?
            WHERE batch_id = ? AND leg_index = ?
            """,
            (
                str(status),
                str(bet_id or ""),
                str(error_text or ""),
                json.dumps(raw_response or {}, ensure_ascii=False),
                datetime.utcnow().isoformat(),
                str(batch_id),
                int(leg_index),
            ),
        )

    def get_dutching_legs(self, batch_id: str) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT *
            FROM dutching_batch_legs
            WHERE batch_id = ?
            ORDER BY leg_index ASC
            """,
            (str(batch_id),),
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            data = dict(row)
            data["raw_response"] = self._safe_json_loads(
                data.pop("raw_response_json", "{}"),
                {},
            )
            result.append(data)
        return result