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
                CREATE TABLE IF NOT EXISTS signal_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern TEXT NOT NULL,
                    label TEXT DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS received_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    selection TEXT DEFAULT '',
                    action TEXT DEFAULT '',
                    price REAL NOT NULL DEFAULT 0.0,
                    stake REAL NOT NULL DEFAULT 0.0,
                    status TEXT DEFAULT '',
                    signal_json TEXT NOT NULL,
                    received_at TEXT NOT NULL
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
                """
                CREATE TABLE IF NOT EXISTS simulation_state (
                    state_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS simulation_bets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bet_id TEXT NOT NULL UNIQUE,
                    market_id TEXT NOT NULL,
                    selection_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL DEFAULT 0.0,
                    size REAL NOT NULL DEFAULT 0.0,
                    matched_size REAL NOT NULL DEFAULT 0.0,
                    avg_price_matched REAL NOT NULL DEFAULT 0.0,
                    status TEXT NOT NULL DEFAULT 'EXECUTABLE',
                    event_key TEXT DEFAULT '',
                    table_id INTEGER,
                    batch_id TEXT DEFAULT '',
                    event_name TEXT DEFAULT '',
                    market_name TEXT DEFAULT '',
                    runner_name TEXT DEFAULT '',
                    payload_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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
                "CREATE INDEX IF NOT EXISTS idx_received_signals_received_at ON received_signals(received_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_simulation_bets_market_id ON simulation_bets(market_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_simulation_bets_status ON simulation_bets(status)"
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
            "enabled": str(settings.get("telegram.enabled", "0")).lower() in {"1", "true"},
            "auto_bet": str(settings.get("telegram.auto_bet", "0")).lower() in {"1", "true"},
            "require_confirmation": str(settings.get("telegram.require_confirmation", "1")).lower() in {"1", "true"},
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

    # =========================================================
    # SIGNAL PATTERNS
    # =========================================================
    def get_signal_patterns(self) -> List[Dict[str, Any]]:
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

    def save_signal_pattern(self, pattern: str, label: str = "", enabled: bool = True) -> int:
        now = datetime.utcnow().isoformat()
        cur = self._execute(
            """
            INSERT INTO signal_patterns(pattern, label, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(pattern), str(label or ""), int(bool(enabled)), now, now),
        )
        return int(cur.lastrowid)

    def update_signal_pattern(self, pattern_id: int, pattern: str, label: str = "") -> None:
        self._execute(
            """
            UPDATE signal_patterns
            SET pattern = ?, label = ?, updated_at = ?
            WHERE id = ?
            """,
            (str(pattern), str(label or ""), datetime.utcnow().isoformat(), int(pattern_id)),
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
            raise RuntimeError("Pattern non trovato")

        new_state = not bool(row["enabled"])
        self._execute(
            """
            UPDATE signal_patterns
            SET enabled = ?, updated_at = ?
            WHERE id = ?
            """,
            (int(new_state), datetime.utcnow().isoformat(), int(pattern_id)),
        )
        return new_state

    # =========================================================
    # RECEIVED SIGNALS
    # =========================================================
    def save_received_signal(self, signal: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        signal = dict(signal or {})
        if kwargs:
            signal.update(kwargs)

        received_at = str(
            signal.get("received_at")
            or signal.get("created_at")
            or datetime.utcnow().isoformat()
        )

        selection = str(signal.get("selection") or signal.get("runner_name") or signal.get("runnerName") or "")
        action = str(signal.get("action") or signal.get("bet_type") or signal.get("side") or "")
        price = float(signal.get("price", 0.0) or 0.0)
        stake = float(signal.get("stake", 0.0) or 0.0)
        status = str(signal.get("status") or "")

        self._execute(
            """
            INSERT INTO received_signals(selection, action, price, stake, status, signal_json, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                selection,
                action,
                price,
                stake,
                status,
                json.dumps(signal, ensure_ascii=False),
                received_at,
            ),
        )

    def get_received_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT id, selection, action, price, stake, status, signal_json, received_at
            FROM received_signals
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
            fetch=True,
            commit=False,
        )
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            item = {
                "id": row["id"],
                "selection": row["selection"],
                "action": row["action"],
                "price": row["price"],
                "stake": row["stake"],
                "status": row["status"],
                "received_at": row["received_at"],
            }
            try:
                item["signal"] = json.loads(row["signal_json"] or "{}")
            except Exception:
                item["signal"] = {}
            out.append(item)
        return out

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
    def create_dutching_batch(self, batch: Dict[str, Any]) -> None:
        now = datetime.utcnow().isoformat()
        self._execute(
            """
            INSERT OR REPLACE INTO dutching_batches(
                batch_id, event_key, market_id, event_name, market_name, table_id,
                strategy, status, total_legs, placed_legs, matched_legs, failed_legs,
                cancelled_legs, batch_exposure, avg_profit, book_pct, payload_json,
                notes, created_at, updated_at, closed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(batch.get("batch_id", "")),
                str(batch.get("event_key", "")),
                str(batch.get("market_id", "")),
                str(batch.get("event_name", "")),
                str(batch.get("market_name", "")),
                batch.get("table_id"),
                str(batch.get("strategy", "DUTCHING")),
                str(batch.get("status", "PENDING")),
                int(batch.get("total_legs", 0) or 0),
                int(batch.get("placed_legs", 0) or 0),
                int(batch.get("matched_legs", 0) or 0),
                int(batch.get("failed_legs", 0) or 0),
                int(batch.get("cancelled_legs", 0) or 0),
                float(batch.get("batch_exposure", 0.0) or 0.0),
                float(batch.get("avg_profit", 0.0) or 0.0),
                float(batch.get("book_pct", 0.0) or 0.0),
                json.dumps(batch.get("payload", {}), ensure_ascii=False),
                str(batch.get("notes", "")),
                str(batch.get("created_at") or now),
                now,
                batch.get("closed_at"),
            ),
        )

    def update_dutching_batch_status(self, batch_id: str, status: str, notes: str = "") -> None:
        closed_at = datetime.utcnow().isoformat() if status in {"EXECUTED", "FAILED", "CANCELLED"} else None
        self._execute(
            """
            UPDATE dutching_batches
            SET status = ?, notes = ?, updated_at = ?, closed_at = COALESCE(?, closed_at)
            WHERE batch_id = ?
            """,
            (str(status), str(notes or ""), datetime.utcnow().isoformat(), closed_at, str(batch_id)),
        )

    # =========================================================
    # SIMULATION STATE
    # =========================================================
    def save_simulation_state(self, state_key: str, state: Dict[str, Any]) -> None:
        self._execute(
            """
            INSERT INTO simulation_state(state_key, state_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (
                str(state_key),
                json.dumps(state or {}, ensure_ascii=False),
                datetime.utcnow().isoformat(),
            ),
        )

    def get_simulation_state(self, state_key: str = "default") -> Dict[str, Any]:
        row = self._execute(
            "SELECT state_json FROM simulation_state WHERE state_key = ?",
            (str(state_key),),
            fetchone=True,
            commit=False,
        )
        if not row:
            return {}
        try:
            return json.loads(row["state_json"] or "{}")
        except Exception:
            return {}

    def clear_simulation_state(self, state_key: str = "default") -> None:
        self._execute(
            "DELETE FROM simulation_state WHERE state_key = ?",
            (str(state_key),),
        )

    # =========================================================
    # SIMULATION BETS
    # =========================================================
    def save_simulation_bet(self, payload: Dict[str, Any]) -> None:
        now = datetime.utcnow().isoformat()
        self._execute(
            """
            INSERT INTO simulation_bets(
                bet_id, market_id, selection_id, side, price, size, matched_size,
                avg_price_matched, status, event_key, table_id, batch_id,
                event_name, market_name, runner_name, payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bet_id) DO UPDATE SET
                matched_size = excluded.matched_size,
                avg_price_matched = excluded.avg_price_matched,
                status = excluded.status,
                event_key = excluded.event_key,
                table_id = excluded.table_id,
                batch_id = excluded.batch_id,
                event_name = excluded.event_name,
                market_name = excluded.market_name,
                runner_name = excluded.runner_name,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (
                str(payload.get("bet_id", "")),
                str(payload.get("market_id", "")),
                str(payload.get("selection_id", "")),
                str(payload.get("side", "")),
                float(payload.get("price", 0.0) or 0.0),
                float(payload.get("size", 0.0) or 0.0),
                float(payload.get("matched_size", 0.0) or 0.0),
                float(payload.get("avg_price_matched", 0.0) or 0.0),
                str(payload.get("status", "EXECUTABLE")),
                str(payload.get("event_key", "")),
                payload.get("table_id"),
                str(payload.get("batch_id", "")),
                str(payload.get("event_name", "")),
                str(payload.get("market_name", "")),
                str(payload.get("runner_name", "")),
                json.dumps(payload or {}, ensure_ascii=False),
                str(payload.get("created_at") or now),
                now,
            ),
        )

    def get_simulation_bets(self, market_id: str | None = None) -> List[Dict[str, Any]]:
        if market_id:
            rows = self._execute(
                """
                SELECT *
                FROM simulation_bets
                WHERE market_id = ?
                ORDER BY created_at ASC, id ASC
                """,
                (str(market_id),),
                fetch=True,
                commit=False,
            )
        else:
            rows = self._execute(
                """
                SELECT *
                FROM simulation_bets
                ORDER BY created_at ASC, id ASC
                """,
                fetch=True,
                commit=False,
            )
        return [dict(row) for row in (rows or [])]

    def update_simulation_bet_status(self, bet_id: str, status: str) -> None:
        self._execute(
            """
            UPDATE simulation_bets
            SET status = ?, updated_at = ?
            WHERE bet_id = ?
            """,
            (str(status), datetime.utcnow().isoformat(), str(bet_id)),
        )

    # =========================================================
    # OPTIONAL LEGACY HELPERS
    # =========================================================
    def save_bet(self, payload: Dict[str, Any]) -> None:
        logger.info("save_bet placeholder called: %s", payload)

    def save_cashout_transaction(self, payload: Dict[str, Any]) -> None:
        logger.info("save_cashout_transaction placeholder called: %s", payload)

    def save_simulation_bet_runtime(self, payload: Dict[str, Any]) -> None:
        self.save_simulation_bet(payload)