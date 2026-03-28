from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


class DutchingBatchManager:
    TERMINAL_BATCH_STATUSES = {"EXECUTED", "ROLLED_BACK", "FAILED", "CANCELLED"}
    ACTIVE_BATCH_STATUSES = {"PENDING", "SUBMITTING", "LIVE", "PARTIAL", "ROLLBACK_PENDING"}
    OPEN_LEG_STATUSES = {"CREATED", "SUBMITTED", "PLACED", "PARTIAL"}
    SUCCESS_LEG_STATUSES = {"PLACED", "MATCHED", "PARTIAL"}
    FAILURE_LEG_STATUSES = {"FAILED", "CANCELLED", "ROLLED_BACK"}
    KNOWN_LEG_STATUSES = {
        "CREATED",
        "SUBMITTED",
        "PLACED",
        "MATCHED",
        "PARTIAL",
        "FAILED",
        "CANCELLED",
        "ROLLED_BACK",
    }

    def __init__(self, db, bus=None):
        self.db = db
        self.bus = bus
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.db._execute(
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

        self.db._execute(
            """
            CREATE TABLE IF NOT EXISTS dutching_batch_legs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                leg_index INTEGER NOT NULL,
                customer_ref TEXT,
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

        self.db._execute(
            "CREATE INDEX IF NOT EXISTS idx_dutching_batches_status ON dutching_batches(status)"
        )
        self.db._execute(
            "CREATE INDEX IF NOT EXISTS idx_dutching_batches_market_id ON dutching_batches(market_id)"
        )
        self.db._execute(
            "CREATE INDEX IF NOT EXISTS idx_dutching_legs_batch_id ON dutching_batch_legs(batch_id)"
        )
        self.db._execute(
            "CREATE INDEX IF NOT EXISTS idx_dutching_legs_customer_ref ON dutching_batch_legs(customer_ref)"
        )

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    def _json_dumps(self, value: Any) -> str:
        try:
            return json.dumps(value if value is not None else {}, ensure_ascii=False)
        except Exception:
            return "{}"

    def _json_loads(self, value: Any, default):
        if not value:
            return default
        try:
            return json.loads(value)
        except Exception:
            return default

    def _publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        if not self.bus:
            return
        try:
            self.bus.publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish %s", event_name)

    def create_batch(
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
        legs: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        now = self._now()
        total_legs_value = len(legs) if legs is not None else int(total_legs or 0)

        self.db._execute(
            """
            INSERT INTO dutching_batches (
                batch_id, event_key, market_id, event_name, market_name,
                table_id, strategy, status, total_legs,
                batch_exposure, avg_profit, book_pct,
                payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(batch_id) DO UPDATE SET
                event_key = excluded.event_key,
                market_id = excluded.market_id,
                event_name = excluded.event_name,
                market_name = excluded.market_name,
                table_id = excluded.table_id,
                strategy = excluded.strategy,
                total_legs = excluded.total_legs,
                batch_exposure = excluded.batch_exposure,
                avg_profit = excluded.avg_profit,
                book_pct = excluded.book_pct,
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (
                str(batch_id),
                str(event_key),
                str(market_id),
                str(event_name or ""),
                str(market_name or ""),
                table_id,
                str(strategy or "DUTCHING"),
                "PENDING",
                int(total_legs_value),
                float(batch_exposure or 0.0),
                float(avg_profit or 0.0),
                float(book_pct or 0.0),
                self._json_dumps(payload or {}),
                now,
                now,
            ),
        )

        if legs:
            for idx, leg in enumerate(legs, start=1):
                self.create_leg(
                    batch_id=batch_id,
                    leg_index=idx,
                    market_id=market_id,
                    selection_id=leg.get("selectionId") or leg.get("selection_id"),
                    side=leg.get("side", "BACK"),
                    price=leg.get("price", 0.0),
                    stake=leg.get("stake", 0.0),
                    liability=leg.get("liability", 0.0),
                )

        batch = self.get_batch(batch_id)
        self._publish("DUTCHING_BATCH_CREATED", {"batch": batch})
        return batch

    def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        rows = self.db._execute(
            "SELECT * FROM dutching_batches WHERE batch_id = ? LIMIT 1",
            (str(batch_id),),
            fetch=True,
            commit=False,
        )
        if not rows:
            return None
        row = dict(rows[0])
        row["payload"] = self._json_loads(row.get("payload_json"), {})
        return row

    def get_open_batches(self) -> List[Dict[str, Any]]:
        rows = self.db._execute(
            """
            SELECT *
            FROM dutching_batches
            WHERE status IN ('PENDING', 'SUBMITTING', 'LIVE', 'PARTIAL', 'ROLLBACK_PENDING')
            ORDER BY created_at ASC, id ASC
            """,
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            item = dict(row)
            item["payload"] = self._json_loads(item.get("payload_json"), {})
            result.append(item)
        return result

    def get_all_batches(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.db._execute(
            """
            SELECT *
            FROM dutching_batches
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            item = dict(row)
            item["payload"] = self._json_loads(item.get("payload_json"), {})
            result.append(item)
        return result

    def update_batch_status(self, batch_id: str, status: str, notes: str = "") -> None:
        now = self._now()
        current = self.get_batch(batch_id)
        if not current:
            return

        status = str(status)
        notes = str(notes or "")

        if current.get("status") == status and current.get("notes", "") == notes:
            return

        closed_at = now if status in self.TERMINAL_BATCH_STATUSES else None

        if closed_at:
            self.db._execute(
                """
                UPDATE dutching_batches
                SET status = ?, notes = ?, updated_at = ?, closed_at = ?
                WHERE batch_id = ?
                """,
                (status, notes, now, closed_at, str(batch_id)),
            )
        else:
            self.db._execute(
                """
                UPDATE dutching_batches
                SET status = ?, notes = ?, updated_at = ?
                WHERE batch_id = ?
                """,
                (status, notes, now, str(batch_id)),
            )

        self._recount_batch(batch_id)
        batch = self.get_batch(batch_id)
        self._publish("DUTCHING_BATCH_STATUS_UPDATED", {"batch": batch})

    def mark_batch_failed(self, batch_id: str, reason: str = "") -> None:
        self.update_batch_status(batch_id, "FAILED", notes=reason)

    def mark_batch_rollback_pending(self, batch_id: str, reason: str = "") -> None:
        self.update_batch_status(batch_id, "ROLLBACK_PENDING", notes=reason)

    def mark_batch_rolled_back(self, batch_id: str, reason: str = "") -> None:
        self.update_batch_status(batch_id, "ROLLED_BACK", notes=reason)

    def mark_batch_cancelled(self, batch_id: str, reason: str = "") -> None:
        self.update_batch_status(batch_id, "CANCELLED", notes=reason)

    def create_leg(
        self,
        *,
        batch_id: str,
        leg_index: int,
        market_id: str,
        selection_id: Any,
        side: str,
        price: float,
        stake: float,
        liability: float = 0.0,
        customer_ref: str = "",
        status: str = "CREATED",
    ) -> None:
        now = self._now()
        self.db._execute(
            """
            INSERT INTO dutching_batch_legs (
                batch_id, leg_index, customer_ref, market_id, selection_id,
                side, price, stake, liability, status,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(batch_id, leg_index) DO UPDATE SET
                customer_ref = excluded.customer_ref,
                market_id = excluded.market_id,
                selection_id = excluded.selection_id,
                side = excluded.side,
                price = excluded.price,
                stake = excluded.stake,
                liability = excluded.liability,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (
                str(batch_id),
                int(leg_index),
                str(customer_ref or ""),
                str(market_id),
                str(selection_id),
                str(side or "BACK").upper(),
                float(price or 0.0),
                float(stake or 0.0),
                float(liability or 0.0),
                str(status).upper(),
                now,
                now,
            ),
        )
        self._recount_batch(batch_id)

    def get_batch_legs(self, batch_id: str) -> List[Dict[str, Any]]:
        rows = self.db._execute(
            """
            SELECT *
            FROM dutching_batch_legs
            WHERE batch_id = ?
            ORDER BY leg_index ASC, id ASC
            """,
            (str(batch_id),),
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            item = dict(row)
            item["raw_response"] = self._json_loads(item.get("raw_response_json"), {})
            result.append(item)
        return result

    def get_leg_by_customer_ref(self, customer_ref: str) -> Optional[Dict[str, Any]]:
        rows = self.db._execute(
            """
            SELECT *
            FROM dutching_batch_legs
            WHERE customer_ref = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(customer_ref),),
            fetch=True,
            commit=False,
        )
        if not rows:
            return None
        item = dict(rows[0])
        item["raw_response"] = self._json_loads(item.get("raw_response_json"), {})
        return item

    def update_leg_submission(
        self,
        *,
        batch_id: str,
        leg_index: int,
        customer_ref: str,
        raw_response: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = self._now()
        self.db._execute(
            """
            UPDATE dutching_batch_legs
            SET customer_ref = ?, status = 'SUBMITTED', raw_response_json = ?, updated_at = ?
            WHERE batch_id = ? AND leg_index = ?
            """,
            (
                str(customer_ref or ""),
                self._json_dumps(raw_response or {}),
                now,
                str(batch_id),
                int(leg_index),
            ),
        )
        self._recount_batch(batch_id)

    def update_leg_status(
        self,
        *,
        batch_id: str,
        leg_index: int,
        status: str,
        bet_id: str = "",
        error_text: str = "",
        raw_response: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = self._now()
        normalized_status = str(status).upper()

        self.db._execute(
            """
            UPDATE dutching_batch_legs
            SET status = ?, bet_id = ?, error_text = ?, raw_response_json = ?, updated_at = ?
            WHERE batch_id = ? AND leg_index = ?
            """,
            (
                normalized_status,
                str(bet_id or ""),
                str(error_text or ""),
                self._json_dumps(raw_response or {}),
                now,
                str(batch_id),
                int(leg_index),
            ),
        )
        self._recount_batch(batch_id)
        batch = self.recompute_batch_status(batch_id)
        self._publish(
            "DUTCHING_LEG_STATUS_UPDATED",
            {
                "batch_id": batch_id,
                "leg_index": leg_index,
                "status": normalized_status,
                "batch_status": batch.get("status") if batch else None,
            },
        )

    def _recount_batch(self, batch_id: str) -> None:
        legs = self.get_batch_legs(batch_id)

        total_legs = len(legs)
        placed_legs = sum(1 for leg in legs if str(leg["status"]).upper() in {"PLACED", "MATCHED", "PARTIAL"})
        matched_legs = sum(1 for leg in legs if str(leg["status"]).upper() == "MATCHED")
        failed_legs = sum(1 for leg in legs if str(leg["status"]).upper() == "FAILED")
        cancelled_legs = sum(
            1 for leg in legs if str(leg["status"]).upper() in {"CANCELLED", "ROLLED_BACK"}
        )

        self.db._execute(
            """
            UPDATE dutching_batches
            SET total_legs = ?, placed_legs = ?, matched_legs = ?, failed_legs = ?, cancelled_legs = ?, updated_at = ?
            WHERE batch_id = ?
            """,
            (
                int(total_legs),
                int(placed_legs),
                int(matched_legs),
                int(failed_legs),
                int(cancelled_legs),
                self._now(),
                str(batch_id),
            ),
        )

    def recompute_batch_status(self, batch_id: str) -> Optional[Dict[str, Any]]:
        batch = self.get_batch(batch_id)
        if not batch:
            return None

        legs = self.get_batch_legs(batch_id)
        if not legs:
            self.update_batch_status(batch_id, "PENDING", notes="Batch senza legs")
            return self.get_batch(batch_id)

        statuses = {str(leg["status"]).upper() for leg in legs}
        unknown_statuses = statuses - self.KNOWN_LEG_STATUSES

        target_status = batch.get("status")
        notes = batch.get("notes", "")

        if unknown_statuses:
            target_status = "LIVE"
            notes = f"Stati sconosciuti rilevati: {', '.join(sorted(unknown_statuses))}"
        elif statuses.issubset({"MATCHED", "PLACED"}):
            target_status = "EXECUTED"
            notes = "Tutte le legs eseguite"
        elif "PARTIAL" in statuses and not statuses.intersection({"FAILED", "CANCELLED", "ROLLED_BACK"}):
            target_status = "PARTIAL"
            notes = "Leg almeno parzialmente eseguita"
        elif statuses.intersection({"FAILED"}) and statuses.intersection(
            {"PLACED", "MATCHED", "PARTIAL", "SUBMITTED"}
        ):
            target_status = "PARTIAL"
            notes = "Batch sbilanciato / failure parziale"
        elif statuses.issubset({"FAILED"}):
            target_status = "FAILED"
            notes = "Tutte le legs fallite"
        elif statuses.issubset({"CANCELLED", "ROLLED_BACK"}):
            target_status = "ROLLED_BACK"
            notes = "Batch rollbackato"
        elif statuses.issubset({"SUBMITTED", "CREATED"}):
            target_status = "SUBMITTING"
            notes = "Batch ancora in submit"
        else:
            target_status = "LIVE"
            notes = "Batch live/non terminale"

        self.update_batch_status(batch_id, target_status, notes=notes)
        return self.get_batch(batch_id)

    def get_active_customer_refs(self, batch_id: str) -> List[str]:
        legs = self.get_batch_legs(batch_id)
        result = []
        active_statuses = self.OPEN_LEG_STATUSES.union(self.SUCCESS_LEG_STATUSES)
        for leg in legs:
            ref = str(leg.get("customer_ref") or "").strip()
            status = str(leg.get("status") or "").upper()
            if ref and status in active_statuses:
                result.append(ref)
        return result

    def get_terminal_batches(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.db._execute(
            """
            SELECT *
            FROM dutching_batches
            WHERE status IN ('EXECUTED', 'ROLLED_BACK', 'FAILED', 'CANCELLED')
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
            fetch=True,
            commit=False,
        )
        result = []
        for row in rows or []:
            item = dict(row)
            item["payload"] = self._json_loads(item.get("payload_json"), {})
            result.append(item)
        return result

    def release_runtime_artifacts(
        self,
        *,
        batch_id: str,
        duplication_guard=None,
        table_manager=None,
        pnl: float = 0.0,
    ) -> None:
        batch = self.get_batch(batch_id)
        if not batch:
            return

        event_key = str(batch.get("event_key") or "")
        table_id = batch.get("table_id")

        if duplication_guard and event_key:
            try:
                duplication_guard.release(event_key)
            except Exception:
                logger.exception("Errore release duplication guard batch=%s", batch_id)

        if table_manager and table_id is not None:
            try:
                if pnl != 0.0 and hasattr(table_manager, "release"):
                    table_manager.release(int(table_id), pnl=float(pnl))
                elif hasattr(table_manager, "force_unlock"):
                    table_manager.force_unlock(int(table_id))
            except Exception:
                logger.exception("Errore release table batch=%s table_id=%s", batch_id, table_id)

    def register_new_batch_from_results(
        self,
        *,
        batch_id: str,
        event_key: str,
        payload: Dict[str, Any],
        results: List[Dict[str, Any]],
        table_id: Optional[int] = None,
        avg_profit: float = 0.0,
        book_pct: float = 0.0,
        batch_exposure: float = 0.0,
    ) -> Dict[str, Any]:
        legs = []
        for item in results:
            legs.append(
                {
                    "selectionId": item.get("selectionId") or item.get("selection_id"),
                    "side": item.get("side", "BACK"),
                    "price": item.get("price", 0.0),
                    "stake": item.get("stake", 0.0),
                    "liability": item.get("liability", 0.0),
                }
            )

        return self.create_batch(
            batch_id=batch_id,
            event_key=event_key,
            market_id=str(payload.get("market_id") or ""),
            event_name=str(payload.get("event_name") or ""),
            market_name=str(payload.get("market_name") or ""),
            table_id=table_id,
            strategy="DUTCHING",
            total_legs=len(legs),
            batch_exposure=float(batch_exposure or 0.0),
            avg_profit=float(avg_profit or 0.0),
            book_pct=float(book_pct or 0.0),
            payload=payload,
            legs=legs,
        )