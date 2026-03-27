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
    # SAFE HELPERS
    # =========================================================
    def _utc_now(self) -> str:
        return datetime.utcnow().isoformat()

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            if value in (None, ""):
                return int(default)
            return int(value)
        except Exception:
            return int(default)

    def _safe_bool_int(self, value: Any, default: bool = False) -> int:
        if value is None:
            return int(bool(default))
        if isinstance(value, bool):
            return int(value)
        return int(str(value).strip().lower() in {"1", "true", "yes", "on"})

    def _safe_json_dumps(self, value: Any) -> str:
        try:
            return json.dumps(value if value is not None else {}, ensure_ascii=False)
        except Exception:
            return "{}"

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
                    selection TEXT DEFAULT '',
                    action TEXT DEFAULT '',
                    price REAL NOT NULL DEFAULT 0.0,
                    stake REAL NOT NULL DEFAULT 0.0,
                    status TEXT DEFAULT '',
                    signal_json TEXT NOT NULL DEFAULT '{}',
                    received_at TEXT NOT NULL,
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
                CREATE TABLE IF NOT EXISTS signal_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    label TEXT NOT NULL DEFAULT '',
                    pattern TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    bet_side TEXT DEFAULT '',
                    market_type TEXT DEFAULT 'MATCH_ODDS',
                    selection_template TEXT DEFAULT '',
                    min_minute INTEGER,
                    max_minute INTEGER,
                    min_score INTEGER,
                    max_score INTEGER,
                    live_only INTEGER NOT NULL DEFAULT 0,
                    priority INTEGER NOT NULL DEFAULT 100,
                    extra_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS simulation_state (
                    state_key TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
                )
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS simulation_bets (
                    bet_id TEXT PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    selection_id TEXT NOT NULL,
                    side TEXT NOT NULL DEFAULT 'BACK',
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
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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
                "CREATE INDEX IF NOT EXISTS idx_received_signals_created_at ON received_signals(created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_patterns_enabled_priority ON signal_patterns(enabled, priority, id)"
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
    # TELEGRAM SETTINGS / CHATS
    # =========================================================
    def get_telegram_settings(self) -> Dict[str, Any]:
        settings = self.get_settings()
        return {
            "api_id": settings.get("telegram.api_id", settings.get("api_id", "")),
            "api_hash": settings.get("telegram.api_hash", settings.get("api_hash", "")),
            "session_string": settings.get("telegram.session_string", settings.get("session_string", "")),
            "phone_number": settings.get("telegram.phone_number", settings.get("phone_number", "")),
            "enabled": str(settings.get("telegram.enabled", "0")).lower() in {"1", "true", "yes", "on"},
            "auto_bet": str(settings.get("telegram.auto_bet", "0")).lower() in {"1", "true", "yes", "on"},
            "require_confirmation": str(settings.get("telegram.require_confirmation", "1")).lower() in {"1", "true", "yes", "on"},
            "auto_stake": self._safe_float(settings.get("telegram.auto_stake", 1.0), 1.0),
        }

    def save_telegram_settings(self, payload: Dict[str, Any]) -> None:
        self.save_settings(
            {
                "telegram.api_id": payload.get("api_id", ""),
                "telegram.api_hash": payload.get("api_hash", ""),
                "telegram.session_string": payload.get("session_string", ""),
                "telegram.phone_number": payload.get("phone_number", ""),
                "telegram.enabled": self._safe_bool_int(payload.get("enabled", False)),
                "telegram.auto_bet": self._safe_bool_int(payload.get("auto_bet", False)),
                "telegram.require_confirmation": self._safe_bool_int(payload.get("require_confirmation", True)),
                "telegram.auto_stake": self._safe_float(payload.get("auto_stake", 1.0), 1.0),
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
    # RECEIVED SIGNALS
    # =========================================================
    def save_received_signal(self, signal: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        payload = dict(signal or {})
        payload.update(kwargs)

        selection = str(
            payload.get("selection")
            or payload.get("selection_name")
            or payload.get("runner_name")
            or payload.get("runnerName")
            or ""
        )
        action = str(
            payload.get("action")
            or payload.get("bet_type")
            or payload.get("side")
            or ""
        ).upper()
        price = self._safe_float(payload.get("price", payload.get("odds")), 0.0)
        stake = self._safe_float(payload.get("stake"), 0.0)
        status = str(payload.get("status") or "")
        received_at = str(payload.get("received_at") or self._utc_now())
        created_at = self._utc_now()

        self._execute(
            """
            INSERT INTO received_signals(
                selection, action, price, stake, status,
                signal_json, received_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                selection,
                action,
                price,
                stake,
                status,
                self._safe_json_dumps(payload),
                received_at,
                created_at,
            ),
        )

    def get_received_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self._execute(
            """
            SELECT id, selection, action, price, stake, status,
                   signal_json, received_at, created_at
            FROM received_signals
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(limit or 50)),),
            fetch=True,
            commit=False,
        )

        out: List[Dict[str, Any]] = []
        for row in rows or []:
            try:
                signal_json = json.loads(row["signal_json"] or "{}")
            except Exception:
                signal_json = {}

            out.append(
                {
                    "id": row["id"],
                    "selection": row["selection"],
                    "action": row["action"],
                    "price": self._safe_float(row["price"], 0.0),
                    "stake": self._safe_float(row["stake"], 0.0),
                    "status": row["status"],
                    "received_at": row["received_at"],
                    "created_at": row["created_at"],
                    "signal": signal_json,
                }
            )
        return out

    # =========================================================
    # SIGNAL PATTERNS (FULL ADVANCED SUPPORT)
    # =========================================================
    def get_signal_patterns(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM signal_patterns
        """
        params: List[Any] = []
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY priority ASC, id ASC"

        rows = self._execute(
            sql,
            tuple(params),
            fetch=True,
            commit=False,
        )

        out: List[Dict[str, Any]] = []
        for row in rows or []:
            try:
                extra = json.loads(row["extra_json"] or "{}")
            except Exception:
                extra = {}

            item = {
                "id": row["id"],
                "label": row["label"],
                "pattern": row["pattern"],
                "enabled": bool(row["enabled"]),
                "bet_side": row["bet_side"] or "",
                "market_type": row["market_type"] or "MATCH_ODDS",
                "selection_template": row["selection_template"] or "",
                "min_minute": row["min_minute"],
                "max_minute": row["max_minute"],
                "min_score": row["min_score"],
                "max_score": row["max_score"],
                "live_only": bool(row["live_only"]),
                "priority": self._safe_int(row["priority"], 100),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            item.update(extra if isinstance(extra, dict) else {})
            out.append(item)

        return out

    def save_signal_pattern(
        self,
        *,
        pattern: str,
        label: str,
        enabled: bool = True,
        bet_side: str = "",
        market_type: str = "MATCH_ODDS",
        selection_template: str = "",
        min_minute: Optional[int] = None,
        max_minute: Optional[int] = None,
        min_score: Optional[int] = None,
        max_score: Optional[int] = None,
        live_only: bool = False,
        priority: int = 100,
        extra: Optional[Dict[str, Any]] = None,
    ) -> int:
        now = self._utc_now()

        cur = self._execute(
            """
            INSERT INTO signal_patterns(
                label, pattern, enabled, bet_side, market_type,
                selection_template, min_minute, max_minute,
                min_score, max_score, live_only, priority,
                extra_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(label or ""),
                str(pattern or ""),
                int(bool(enabled)),
                str(bet_side or ""),
                str(market_type or "MATCH_ODDS"),
                str(selection_template or ""),
                None if min_minute in (None, "") else int(min_minute),
                None if max_minute in (None, "") else int(max_minute),
                None if min_score in (None, "") else int(min_score),
                None if max_score in (None, "") else int(max_score),
                int(bool(live_only)),
                self._safe_int(priority, 100),
                self._safe_json_dumps(extra or {}),
                now,
                now,
            ),
        )
        return int(cur.lastrowid)

    def update_signal_pattern(
        self,
        pattern_id: int,
        *,
        pattern: Optional[str] = None,
        label: Optional[str] = None,
        enabled: Optional[bool] = None,
        bet_side: Optional[str] = None,
        market_type: Optional[str] = None,
        selection_template: Optional[str] = None,
        min_minute: Any = None,
        max_minute: Any = None,
        min_score: Any = None,
        max_score: Any = None,
        live_only: Optional[bool] = None,
        priority: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        current = self._execute(
            "SELECT * FROM signal_patterns WHERE id = ? LIMIT 1",
            (int(pattern_id),),
            fetchone=True,
            commit=False,
        )
        if not current:
            raise RuntimeError("Signal pattern non trovato")

        try:
            current_extra = json.loads(current["extra_json"] or "{}")
        except Exception:
            current_extra = {}

        merged_extra = current_extra
        if extra is not None:
            merged_extra = dict(current_extra)
            merged_extra.update(extra)

        self._execute(
            """
            UPDATE signal_patterns
            SET label = ?,
                pattern = ?,
                enabled = ?,
                bet_side = ?,
                market_type = ?,
                selection_template = ?,
                min_minute = ?,
                max_minute = ?,
                min_score = ?,
                max_score = ?,
                live_only = ?,
                priority = ?,
                extra_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                str(current["label"] if label is None else label),
                str(current["pattern"] if pattern is None else pattern),
                int(current["enabled"] if enabled is None else bool(enabled)),
                str(current["bet_side"] if bet_side is None else bet_side),
                str(current["market_type"] if market_type is None else market_type),
                str(current["selection_template"] if selection_template is None else selection_template),
                current["min_minute"] if min_minute is None else (None if min_minute == "" else int(min_minute)),
                current["max_minute"] if max_minute is None else (None if max_minute == "" else int(max_minute)),
                current["min_score"] if min_score is None else (None if min_score == "" else int(min_score)),
                current["max_score"] if max_score is None else (None if max_score == "" else int(max_score)),
                int(current["live_only"] if live_only is None else bool(live_only)),
                self._safe_int(current["priority"] if priority is None else priority, 100),
                self._safe_json_dumps(merged_extra),
                self._utc_now(),
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
            "SELECT enabled FROM signal_patterns WHERE id = ? LIMIT 1",
            (int(pattern_id),),
            fetchone=True,
            commit=False,
        )
        if not row:
            raise RuntimeError("Signal pattern non trovato")

        new_state = not bool(row["enabled"])
        self._execute(
            """
            UPDATE signal_patterns
            SET enabled = ?, updated_at = ?
            WHERE id = ?
            """,
            (int(new_state), self._utc_now(), int(pattern_id)),
        )
        return new_state

    # =========================================================
    # SIMULATION STATE / BETS
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
                str(state_key or "default"),
                self._safe_json_dumps(state or {}),
                self._utc_now(),
            ),
        )

    def get_simulation_state(self, state_key: str = "default") -> Dict[str, Any]:
        row = self._execute(
            "SELECT state_json FROM simulation_state WHERE state_key = ? LIMIT 1",
            (str(state_key or "default"),),
            fetchone=True,
            commit=False,
        )
        if not row:
            return {}
        try:
            return json.loads(row["state_json"] or "{}")
        except Exception:
            return {}

    def load_simulation_state(self, state_key: str = "default") -> Dict[str, Any]:
        return self.get_simulation_state(state_key=state_key)

    def save_simulation_bet(self, payload: Dict[str, Any]) -> None:
        now = self._utc_now()
        bet_id = str(payload.get("bet_id") or "")
        if not bet_id:
            raise RuntimeError("bet_id mancante")

        self._execute(
            """
            INSERT INTO simulation_bets(
                bet_id, market_id, selection_id, side, price, size,
                matched_size, avg_price_matched, status, event_key,
                table_id, batch_id, event_name, market_name, runner_name,
                payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bet_id) DO UPDATE SET
                market_id = excluded.market_id,
                selection_id = excluded.selection_id,
                side = excluded.side,
                price = excluded.price,
                size = excluded.size,
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
                bet_id,
                str(payload.get("market_id") or ""),
                str(payload.get("selection_id") or ""),
                str(payload.get("side") or payload.get("bet_type") or "BACK"),
                self._safe_float(payload.get("price"), 0.0),
                self._safe_float(payload.get("size", payload.get("stake")), 0.0),
                self._safe_float(payload.get("matched_size"), 0.0),
                self._safe_float(payload.get("avg_price_matched"), 0.0),
                str(payload.get("status") or "EXECUTABLE"),
                str(payload.get("event_key") or ""),
                None if payload.get("table_id") in (None, "") else int(payload.get("table_id")),
                str(payload.get("batch_id") or ""),
                str(payload.get("event_name") or ""),
                str(payload.get("market_name") or ""),
                str(payload.get("runner_name") or ""),
                self._safe_json_dumps(payload),
                str(payload.get("created_at") or now),
                now,
            ),
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
        now = self._utc_now()
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
                self._safe_json_dumps(payload or {}),
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
                self._utc_now(),
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
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            item = dict(row)
            try:
                item["payload"] = json.loads(item.get("payload_json") or "{}")
            except Exception:
                item["payload"] = {}
            out.append(item)
        return out

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