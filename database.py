from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from core.secret_cipher import SecretCipher
from core.type_helpers import safe_float, safe_int, safe_bool_int, safe_json_dumps, safe_json_loads
from database_schema import SCHEMA_DDL


logger = logging.getLogger(__name__)

# Fields whose values must be encrypted at rest.
_SECRET_FIELDS: frozenset = frozenset({
    "app_key",
    "password",
    "private_key",
    "certificate",
    "session_token",
    "telegram.api_id",
    "telegram.api_hash",
    "telegram.session_string",
})

_DB_DURABILITY_PROFILES: Dict[str, Dict[str, str]] = {
    # LIVE-safe default: WAL + FULL minimizes committed-data loss on crash/power loss.
    "live_safe": {
        "journal_mode": "WAL",
        "synchronous": "FULL",
    },
    # Faster profile with larger crash-loss window (kept for explicit opt-in only).
    "balanced": {
        "journal_mode": "WAL",
        "synchronous": "NORMAL",
    },
}

_DB_DURABILITY_PROFILE_ENV = "PICKFAIR_DB_DURABILITY_PROFILE"


class Database:
    def __init__(self, db_path: str = "pickfair.db"):
        self.db_path = str(db_path)
        self._local = threading.local()
        self._write_lock = threading.RLock()
        self._cipher = SecretCipher.from_env_or_file()
        self._durability_profile = self._resolve_durability_profile()
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
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0,
            )
            conn.row_factory = sqlite3.Row
            self._apply_durability_pragmas(conn)
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
            self._local.tx_depth = 0
        return conn

    def _resolve_durability_profile(self) -> str:
        raw = os.getenv(_DB_DURABILITY_PROFILE_ENV, "live_safe")
        selected = str(raw or "").strip().lower()
        if selected not in _DB_DURABILITY_PROFILES:
            allowed = ", ".join(sorted(_DB_DURABILITY_PROFILES))
            raise ValueError(
                f"Unsupported DB durability profile '{raw}'. Allowed profiles: {allowed}"
            )
        return selected

    def _apply_durability_pragmas(self, conn: sqlite3.Connection) -> None:
        profile = _DB_DURABILITY_PROFILES[self._durability_profile]
        journal_mode = str(profile["journal_mode"]).strip().upper()
        synchronous = str(profile["synchronous"]).strip().upper()

        if journal_mode == "WAL":
            conn.execute("PRAGMA journal_mode=WAL")
        else:
            raise ValueError(f"Unsupported journal_mode pragma value: {journal_mode}")

        if synchronous == "FULL":
            conn.execute("PRAGMA synchronous=FULL")
        elif synchronous == "NORMAL":
            conn.execute("PRAGMA synchronous=NORMAL")
        else:
            raise ValueError(f"Unsupported synchronous pragma value: {synchronous}")

    def get_durability_profile(self) -> str:
        return self._durability_profile

    def is_wal_mode(self) -> bool:
        conn = self._get_connection()
        row = conn.execute("PRAGMA journal_mode").fetchone()
        mode = str(row[0] if row else "").strip().lower()
        return mode == "wal"

    def _get_tx_depth(self) -> int:
        return int(getattr(self._local, "tx_depth", 0) or 0)

    def _set_tx_depth(self, value: int) -> None:
        self._local.tx_depth = int(value)

    def _in_transaction(self) -> bool:
        return self._get_tx_depth() > 0

    @contextmanager
    def transaction(self):
        conn = self._get_connection()

        with self._write_lock:
            depth = self._get_tx_depth()
            savepoint_name = None

            try:
                if depth == 0:
                    conn.execute("BEGIN")
                else:
                    if depth < 1:
                        raise RuntimeError("invalid nested transaction depth")
                    savepoint_name = "sp_" + str(depth)
                    conn.execute("SAVEPOINT " + savepoint_name)

                self._set_tx_depth(depth + 1)
                yield conn

                new_depth = self._get_tx_depth() - 1
                self._set_tx_depth(new_depth)

                if savepoint_name:
                    conn.execute("RELEASE SAVEPOINT " + savepoint_name)
                elif new_depth == 0:
                    conn.commit()

            except Exception:
                current_depth = max(0, self._get_tx_depth() - 1)
                self._set_tx_depth(current_depth)

                try:
                    if savepoint_name:
                        conn.execute("ROLLBACK TO SAVEPOINT " + savepoint_name)
                        conn.execute("RELEASE SAVEPOINT " + savepoint_name)
                    else:
                        conn.rollback()
                except Exception:
                    logger.exception("Errore rollback transaction")
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
        cur = conn.execute(sql, tuple(params))

        if fetchone:
            row = cur.fetchone()
            if commit and not self._in_transaction():
                conn.commit()
            return row

        if fetch:
            rows = cur.fetchall()
            if commit and not self._in_transaction():
                conn.commit()
            return rows

        if commit and not self._in_transaction():
            conn.commit()

        return cur

    def close_all_connections(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.exception("Errore chiusura connessione SQLite")
            self._local.conn = None
            self._local.tx_depth = 0

    def reopen(self) -> None:
        self.close_all_connections()
        self._get_connection()

    # =========================================================
    # SAFE HELPERS
    # =========================================================
    def _utc_now(self) -> str:
        return datetime.utcnow().isoformat()

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        return safe_float(value, default)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        return safe_int(value, default)

    def _safe_bool_int(self, value: Any, default: bool = False) -> int:
        return safe_bool_int(value, default)

    def _safe_json_dumps(self, value: Any) -> str:
        return safe_json_dumps(value)

    def _safe_json_loads(self, value: Any, default):
        return safe_json_loads(value, default)

    # =========================================================
    # INIT SCHEMA
    # =========================================================
    def _init_db(self) -> None:
        with self.transaction() as conn:
            for stmt in SCHEMA_DDL:
                conn.execute(stmt)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cycle_recovery_checkpoints (
                    settlement_key TEXT PRIMARY KEY,
                    settlement_correlation_id TEXT NOT NULL DEFAULT '',
                    cycle_id TEXT NOT NULL DEFAULT '',
                    table_id INTEGER,
                    strategy_context_json TEXT NOT NULL DEFAULT '{}',
                    checkpoint_stage TEXT NOT NULL DEFAULT 'SETTLEMENT_DETECTED',
                    bankroll_sync_status TEXT NOT NULL DEFAULT 'NOT_SETTLED',
                    money_management_status TEXT NOT NULL DEFAULT 'MM_STOP_CONTEXT_MISSING',
                    cycle_active INTEGER NOT NULL DEFAULT 0,
                    progression_allowed INTEGER NOT NULL DEFAULT 0,
                    next_stake REAL NOT NULL DEFAULT 0.0,
                    step_index INTEGER NOT NULL DEFAULT 0,
                    round_index INTEGER NOT NULL DEFAULT 0,
                    next_trade_submission_status TEXT NOT NULL DEFAULT 'NOT_ATTEMPTED',
                    idempotency_key TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL DEFAULT '',
                    is_ambiguous INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cycle_checkpoint_corr_id "
                "ON cycle_recovery_checkpoints(settlement_correlation_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cycle_checkpoint_stage "
                "ON cycle_recovery_checkpoints(checkpoint_stage)"
            )

        # =========================================================
    # SETTINGS / CREDENTIALS
    # =========================================================
    def _set_setting(self, key: str, value: Any) -> None:
        str_val = str(value if value is not None else "")
        if key in _SECRET_FIELDS and str_val:
            str_val = self._cipher.encrypt(str_val)
        self._execute(
            """
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(key), str_val),
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
        val = str(row["value"])
        if key in _SECRET_FIELDS:
            val = self._cipher.decrypt(val)
        return val

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
            key = str(row["key"])
            val = row["value"]
            if key in _SECRET_FIELDS and val:
                val = self._cipher.decrypt(str(val))
            result[key] = val
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
            "alerts_enabled": str(settings.get("telegram.alerts_enabled", "0")).lower() in {"1", "true", "yes", "on"},
            "alerts_chat_id": settings.get("telegram.alerts_chat_id", ""),
            "alerts_chat_name": settings.get("telegram.alerts_chat_name", ""),
            "min_alert_severity": str(settings.get("telegram.min_alert_severity", "WARNING") or "WARNING").upper(),
            "alert_cooldown_sec": int(settings.get("telegram.alert_cooldown_sec", 0) or 0),
            "alert_dedup_enabled": str(settings.get("telegram.alert_dedup_enabled", "0")).lower()
            in {"1", "true", "yes", "on"},
            "alert_format_rich": str(settings.get("telegram.alert_format_rich", "0")).lower()
            in {"1", "true", "yes", "on"},
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
                "telegram.alerts_enabled": self._safe_bool_int(payload.get("alerts_enabled", False)),
                "telegram.alerts_chat_id": str(payload.get("alerts_chat_id", "") or ""),
                "telegram.alerts_chat_name": str(payload.get("alerts_chat_name", "") or ""),
                "telegram.min_alert_severity": str(payload.get("min_alert_severity", "WARNING") or "WARNING").upper(),
                "telegram.alert_cooldown_sec": int(payload.get("alert_cooldown_sec", 0) or 0),
                "telegram.alert_dedup_enabled": self._safe_bool_int(payload.get("alert_dedup_enabled", False)),
                "telegram.alert_format_rich": self._safe_bool_int(payload.get("alert_format_rich", False)),
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
            signal_json = self._safe_json_loads(row["signal_json"], {})

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
    # SIGNAL PATTERNS
    # =========================================================
    def get_signal_patterns(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM signal_patterns"
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY priority ASC, id ASC"

        rows = self._execute(sql, fetch=True, commit=False)

        out: List[Dict[str, Any]] = []
        for row in rows or []:
            extra = self._safe_json_loads(row["extra_json"], {})
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
            if isinstance(extra, dict):
                item.update(extra)
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

        current_extra = self._safe_json_loads(current["extra_json"], {})
        merged_extra = dict(current_extra) if isinstance(current_extra, dict) else {}
        if extra is not None:
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
        return self._safe_json_loads(row["state_json"], {})

    def load_simulation_state(self, state_key: str = "default") -> Dict[str, Any]:
        return self.get_simulation_state(state_key=state_key)

    # =========================================================
    # CYCLE RECOVERY CHECKPOINTS
    # =========================================================
    def upsert_cycle_recovery_checkpoint(self, settlement_key: str, payload: Dict[str, Any]) -> None:
        key = str(settlement_key or "").strip()
        if not key:
            raise RuntimeError("settlement_key mancante")

        body = dict(payload or {})
        now = self._utc_now()
        existing = self.get_cycle_recovery_checkpoint(key)
        created_at = str((existing or {}).get("created_at") or now)
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
        bankroll_sync_rank = {
            "NOT_SETTLED": 10,
            "SYNC_SKIPPED_DUPLICATE": 20,
            "SYNC_FAILED_BALANCE_UNAVAILABLE": 30,
            "SYNC_FAILED_INVALID_BALANCE": 40,
            "SYNC_SUCCESS": 50,
        }
        incoming_stage = str(body.get("checkpoint_stage") or "SETTLEMENT_DETECTED")
        existing_stage = str((existing or {}).get("checkpoint_stage") or "")
        effective_stage = incoming_stage
        if stage_rank.get(existing_stage, 0) > stage_rank.get(incoming_stage, 0):
            effective_stage = existing_stage
        incoming_submit = str(body.get("next_trade_submission_status") or "NOT_ATTEMPTED")
        existing_submit = str((existing or {}).get("next_trade_submission_status") or "")
        effective_submit = incoming_submit
        if submit_rank.get(existing_submit, 0) > submit_rank.get(incoming_submit, 0):
            effective_submit = existing_submit
        incoming_sync = str(body.get("bankroll_sync_status") or "NOT_SETTLED")
        existing_sync = str((existing or {}).get("bankroll_sync_status") or "")
        effective_sync = incoming_sync
        if bankroll_sync_rank.get(existing_sync, 0) > bankroll_sync_rank.get(incoming_sync, 0):
            effective_sync = existing_sync
        effective_ambiguous = bool(body.get("is_ambiguous", False)) or bool((existing or {}).get("is_ambiguous", False))
        effective_reason = str(body.get("reason") or (existing or {}).get("reason") or "")

        self._execute(
            """
            INSERT INTO cycle_recovery_checkpoints(
                settlement_key,
                settlement_correlation_id,
                cycle_id,
                table_id,
                strategy_context_json,
                checkpoint_stage,
                bankroll_sync_status,
                money_management_status,
                cycle_active,
                progression_allowed,
                next_stake,
                step_index,
                round_index,
                next_trade_submission_status,
                idempotency_key,
                reason,
                is_ambiguous,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(settlement_key) DO UPDATE SET
                settlement_correlation_id = excluded.settlement_correlation_id,
                cycle_id = excluded.cycle_id,
                table_id = excluded.table_id,
                strategy_context_json = excluded.strategy_context_json,
                checkpoint_stage = excluded.checkpoint_stage,
                bankroll_sync_status = excluded.bankroll_sync_status,
                money_management_status = excluded.money_management_status,
                cycle_active = excluded.cycle_active,
                progression_allowed = excluded.progression_allowed,
                next_stake = excluded.next_stake,
                step_index = excluded.step_index,
                round_index = excluded.round_index,
                next_trade_submission_status = excluded.next_trade_submission_status,
                idempotency_key = excluded.idempotency_key,
                reason = excluded.reason,
                is_ambiguous = excluded.is_ambiguous,
                updated_at = excluded.updated_at
            """,
            (
                key,
                str(body.get("settlement_correlation_id") or ""),
                str(body.get("cycle_id") or ""),
                None if body.get("table_id") in (None, "") else self._safe_int(body.get("table_id"), 0),
                self._safe_json_dumps(body.get("strategy_context") or {}),
                effective_stage,
                effective_sync,
                str(body.get("money_management_status") or "MM_STOP_CONTEXT_MISSING"),
                self._safe_bool_int(body.get("cycle_active", False)),
                self._safe_bool_int(body.get("progression_allowed", False)),
                self._safe_float(body.get("next_stake"), 0.0),
                self._safe_int(body.get("step_index"), 0),
                self._safe_int(body.get("round_index"), 0),
                effective_submit,
                str(body.get("idempotency_key") or key),
                effective_reason,
                self._safe_bool_int(effective_ambiguous),
                created_at,
                now,
            ),
        )

    def get_cycle_recovery_checkpoint(self, settlement_key: str) -> Optional[Dict[str, Any]]:
        key = str(settlement_key or "").strip()
        if not key:
            return None
        row = self._execute(
            "SELECT * FROM cycle_recovery_checkpoints WHERE settlement_key = ? LIMIT 1",
            (key,),
            fetchone=True,
            commit=False,
        )
        if not row:
            return None
        item = dict(row)
        item["strategy_context"] = self._safe_json_loads(item.get("strategy_context_json"), {})
        item["table_id"] = None if item.get("table_id") is None else self._safe_int(item.get("table_id"), 0)
        item["cycle_active"] = bool(item.get("cycle_active"))
        item["progression_allowed"] = bool(item.get("progression_allowed"))
        item["is_ambiguous"] = bool(item.get("is_ambiguous"))
        item["next_stake"] = self._safe_float(item.get("next_stake"), 0.0)
        item["step_index"] = self._safe_int(item.get("step_index"), 0)
        item["round_index"] = self._safe_int(item.get("round_index"), 0)
        return item

    def get_cycle_recovery_state(self, settlement_key: str) -> Dict[str, Any]:
        checkpoint = self.get_cycle_recovery_checkpoint(settlement_key)
        if checkpoint is None:
            return {
                "exists": False,
                "processed": False,
                "bankroll_synced": False,
                "submit_attempted": False,
                "submit_confirmed": False,
                "ambiguous": False,
                "stage": "",
                "checkpoint": None,
            }

        stage = str(checkpoint.get("checkpoint_stage") or "")
        bankroll_sync_status = str(checkpoint.get("bankroll_sync_status") or "")
        submit_status = str(checkpoint.get("next_trade_submission_status") or "")
        ambiguous = bool(checkpoint.get("is_ambiguous"))
        submit_attempted = submit_status in {"ATTEMPTED", "SUBMITTED", "CONFIRMED", "AMBIGUOUS"}
        submit_confirmed = submit_status in {"SUBMITTED", "CONFIRMED"}
        processed = stage in {
            "SETTLEMENT_DETECTED",
            "BANKROLL_SYNC_DONE",
            "MM_DECISION_DONE",
            "NEXT_TRADE_SUBMIT_ATTEMPTED",
            "NEXT_TRADE_SUBMIT_CONFIRMED",
            "CYCLE_BLOCKED",
            "CYCLE_AMBIGUOUS",
        }
        return {
            "exists": True,
            "processed": processed,
            "bankroll_synced": bankroll_sync_status == "SYNC_SUCCESS",
            "submit_attempted": submit_attempted,
            "submit_confirmed": submit_confirmed,
            "ambiguous": ambiguous or (submit_attempted and not submit_confirmed),
            "stage": stage,
            "checkpoint": checkpoint,
        }

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
    # TRADING ENGINE ORDERS CONTRACT
    # =========================================================
    def insert_order(self, payload: Dict[str, Any]) -> str:
        customer_ref = str(payload.get("customer_ref") or "")
        correlation_id = str(payload.get("correlation_id") or "")
        if not customer_ref or not correlation_id:
            raise RuntimeError("insert_order requires customer_ref and correlation_id")

        status = str(payload.get("status") or "INFLIGHT")
        created_at = float(payload.get("created_at") or time.time())
        updated_at = float(payload.get("updated_at") or created_at)

        cur = self._execute(
            """
            INSERT INTO orders(
                customer_ref, correlation_id, status, payload_json,
                response_json, outcome, reason, last_error, ambiguity_reason,
                finalized, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_ref,
                correlation_id,
                status,
                self._safe_json_dumps(payload.get("payload", {})),
                self._safe_json_dumps(payload.get("response")) if payload.get("response") is not None else None,
                payload.get("outcome"),
                payload.get("reason"),
                payload.get("last_error"),
                payload.get("ambiguity_reason"),
                int(bool(payload.get("finalized", False))),
                created_at,
                updated_at,
            ),
        )
        return str(cur.lastrowid)

    def update_order(self, order_id: str, update: Dict[str, Any]) -> None:
        current = self.get_order(order_id)
        merged = dict(current)
        merged.update(update or {})

        self._execute(
            """
            UPDATE orders
            SET status = ?,
                payload_json = ?,
                response_json = ?,
                outcome = ?,
                reason = ?,
                last_error = ?,
                ambiguity_reason = ?,
                finalized = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                str(merged.get("status") or ""),
                self._safe_json_dumps(merged.get("payload", {})),
                self._safe_json_dumps(merged.get("response")) if merged.get("response") is not None else None,
                merged.get("outcome"),
                merged.get("reason"),
                merged.get("last_error"),
                merged.get("ambiguity_reason"),
                int(bool(merged.get("finalized", False))),
                float(merged.get("updated_at") or time.time()),
                int(order_id),
            ),
        )

    def get_order(self, order_id: str) -> Dict[str, Any]:
        row = self._execute(
            "SELECT * FROM orders WHERE id = ? LIMIT 1",
            (int(order_id),),
            fetchone=True,
            commit=False,
        )
        if not row:
            raise KeyError(str(order_id))
        item = dict(row)
        item["payload"] = self._safe_json_loads(item.get("payload_json"), {})
        item["response"] = self._safe_json_loads(item.get("response_json"), None)
        item["finalized"] = bool(item.get("finalized"))
        return item

    def order_exists_inflight(
        self,
        customer_ref: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> bool:
        if not customer_ref and not correlation_id:
            return False

        identity_clauses: List[str] = []
        params: List[Any] = []
        if customer_ref:
            identity_clauses.append("customer_ref = ?")
            params.append(str(customer_ref))
        if correlation_id:
            identity_clauses.append("correlation_id = ?")
            params.append(str(correlation_id))
        sql = (
            "SELECT 1 FROM orders "
            "WHERE status IN ('INFLIGHT', 'SUBMITTED') "
            f"AND ({' OR '.join(identity_clauses)}) LIMIT 1"
        )
        row = self._execute(sql, tuple(params), fetchone=True, commit=False)
        return row is not None

    def find_duplicate_order(
        self,
        customer_ref: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Optional[str]:
        row = None
        if customer_ref:
            row = self._execute(
                "SELECT id FROM orders WHERE customer_ref = ? ORDER BY id DESC LIMIT 1",
                (str(customer_ref),),
                fetchone=True,
                commit=False,
            )
        if row is None and correlation_id:
            row = self._execute(
                "SELECT id FROM orders WHERE correlation_id = ? ORDER BY id DESC LIMIT 1",
                (str(correlation_id),),
                fetchone=True,
                commit=False,
            )
        return str(row["id"]) if row else None

    def load_pending_customer_refs(self) -> List[str]:
        rows = self._execute(
            "SELECT customer_ref FROM orders WHERE status IN ('INFLIGHT', 'SUBMITTED') ORDER BY id ASC",
            fetch=True,
            commit=False,
        ) or []
        return [str(row["customer_ref"]) for row in rows if row["customer_ref"]]

    def load_pending_correlation_ids(self) -> List[str]:
        rows = self._execute(
            "SELECT correlation_id FROM orders WHERE status IN ('INFLIGHT', 'SUBMITTED') ORDER BY id ASC",
            fetch=True,
            commit=False,
        ) or []
        return [str(row["correlation_id"]) for row in rows if row["correlation_id"]]

    def insert_audit_event(self, event: Dict[str, Any]) -> None:
        self._execute(
            "INSERT INTO audit_events(ts, event_json) VALUES (?, ?)",
            (float(time.time()), self._safe_json_dumps(event or {})),
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
            INSERT INTO order_saga(
                customer_ref, batch_id, event_key, table_id,
                market_id, selection_id, bet_type, price, stake,
                status, payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(customer_ref) DO UPDATE SET
                batch_id = excluded.batch_id,
                event_key = excluded.event_key,
                table_id = excluded.table_id,
                market_id = excluded.market_id,
                selection_id = excluded.selection_id,
                bet_type = excluded.bet_type,
                price = excluded.price,
                stake = excluded.stake,
                status = excluded.status,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
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
        if not row:
            return None
        item = dict(row)
        item["payload"] = self._safe_json_loads(item.get("payload_json"), {})
        return item

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
            item["payload"] = self._safe_json_loads(item.get("payload_json"), {})
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
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            item = dict(row)
            item["payload"] = self._safe_json_loads(item.get("payload_json"), {})
            out.append(item)
        return out

    def save_observability_snapshot(self, payload):
        sql = """
        INSERT INTO observability_snapshots (created_at, payload_json)
        VALUES (?, ?)
        """
        body = json.dumps(payload, ensure_ascii=False, default=str)

        execute = getattr(self, "execute", None)
        if callable(execute):
            execute(sql, (time.time(), body))
            return

        conn = getattr(self, "conn", None)
        if conn is not None:
            cur = conn.cursor()
            cur.execute(sql, (time.time(), body))
            conn.commit()
            return

        self._execute(sql, (time.time(), body))

    def register_diagnostics_export(self, export_path):
        sql = """
        INSERT INTO diagnostics_exports (created_at, export_path)
        VALUES (?, ?)
        """

        execute = getattr(self, "execute", None)
        if callable(execute):
            execute(sql, (time.time(), str(export_path)))
            return

        conn = getattr(self, "conn", None)
        if conn is not None:
            cur = conn.cursor()
            cur.execute(sql, (time.time(), str(export_path)))
            conn.commit()
            return

        self._execute(sql, (time.time(), str(export_path)))

    def get_recent_observability_snapshots(self, limit=100):
        rows = self._execute(
            """
            SELECT id, created_at, payload_json
            FROM observability_snapshots
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (int(limit),),
            fetch=True,
            commit=False,
        ) or []
        return [dict(row) for row in rows]

    def get_recent_orders_for_diagnostics(self, limit=200):
        for table_name in ("orders", "order_saga"):
            try:
                rows = self._execute(
                    f"""
                    SELECT *
                    FROM {table_name}
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                    fetch=True,
                    commit=False,
                ) or []
                return [dict(row) for row in rows]
            except Exception:
                continue
        return []

    def get_recent_audit_events_for_diagnostics(self, limit=500):
        for table_name in ("audit_events", "order_events", "telegram_outbox_log"):
            for ts_col in ("ts", "created_at"):
                try:
                    rows = self._execute(
                        f"""
                        SELECT *
                        FROM {table_name}
                        ORDER BY {ts_col} DESC
                        LIMIT ?
                        """,
                        (int(limit),),
                        fetch=True,
                        commit=False,
                    ) or []
                    return [dict(row) for row in rows]
                except Exception:
                    continue
        return []

    def delete_old_observability_snapshots(self, cutoff_ts):
        execute = getattr(self, "execute", None)
        if callable(execute):
            execute(
                "DELETE FROM observability_snapshots WHERE created_at < ?",
                (float(cutoff_ts),),
            )
            return

        conn = getattr(self, "conn", None)
        if conn is not None:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM observability_snapshots WHERE created_at < ?",
                (float(cutoff_ts),),
            )
            conn.commit()
            return

        self._execute(
            "DELETE FROM observability_snapshots WHERE created_at < ?",
            (float(cutoff_ts),),
        )

    def delete_old_diagnostics_exports(self, cutoff_ts):
        execute = getattr(self, "execute", None)
        if callable(execute):
            execute(
                "DELETE FROM diagnostics_exports WHERE created_at < ?",
                (float(cutoff_ts),),
            )
            return

        conn = getattr(self, "conn", None)
        if conn is not None:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM diagnostics_exports WHERE created_at < ?",
                (float(cutoff_ts),),
            )
            conn.commit()
            return

        self._execute(
            "DELETE FROM diagnostics_exports WHERE created_at < ?",
            (float(cutoff_ts),),
        )
