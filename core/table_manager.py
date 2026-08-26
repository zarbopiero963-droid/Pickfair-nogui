from __future__ import annotations

import math

from dataclasses import dataclass, asdict
from datetime import datetime
from threading import RLock
from typing import Any, Dict, List, Optional


@dataclass
class TableState:
    table_id: int
    status: str = "FREE"  # FREE / ACTIVE / RECOVERY / LOCKED
    current_event_key: str = ""
    current_exposure: float = 0.0
    loss_amount: float = 0.0
    in_recovery: bool = False
    market_id: str = ""
    selection_id: str = ""
    meta: Dict[str, Any] = None
    updated_at: str = ""

    def __post_init__(self):
        if self.meta is None:
            self.meta = {}
        if not self.updated_at:
            self.updated_at = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TableManager:
    """
    Gestione tavoli Roserpina.

    Regole:
    - ogni tavolo può essere FREE / ACTIVE / RECOVERY / LOCKED
    - ogni tavolo mantiene la propria memoria perdite
    - l'allocazione preferisce:
        1) tavolo FREE
        2) tavolo già in recovery se allow_recovery=True
    """

    def __init__(self, table_count: int = 5):
        self._lock = RLock()
        self.table_count = max(1, int(table_count or 1))
        self._tables: Dict[int, TableState] = {
            idx: TableState(table_id=idx)
            for idx in range(1, self.table_count + 1)
        }

    # =========================================================
    # INTERNAL
    # =========================================================
    def _touch(self, table: TableState) -> None:
        table.updated_at = datetime.utcnow().isoformat()

    def _get(self, table_id: int) -> Optional[TableState]:
        return self._tables.get(int(table_id))

    # =========================================================
    # ALLOCATION
    # =========================================================
    def allocate(self, *, event_key: str, allow_recovery: bool = True) -> Optional[TableState]:
        with self._lock:
            # 1) tavolo libero
            for table in self._tables.values():
                if table.status == "FREE":
                    return TableState(**table.to_dict())

            # 2) recovery consentito
            if allow_recovery:
                recovery_candidates = [
                    t for t in self._tables.values()
                    if t.status in {"RECOVERY", "LOCKED"} or t.in_recovery
                ]
                if recovery_candidates:
                    recovery_candidates.sort(key=lambda t: (t.loss_amount, t.table_id))
                    return TableState(**recovery_candidates[0].to_dict())

            return None

    def activate(
        self,
        *,
        table_id: int,
        event_key: str,
        exposure: float,
        market_id: str,
        selection_id: Any,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._lock:
            table = self._get(table_id)
            if not table:
                raise RuntimeError(f"Tavolo non trovato: {table_id}")

            table.current_event_key = str(event_key or "")
            table.current_exposure = float(exposure or 0.0)
            table.market_id = str(market_id or "")
            table.selection_id = str(selection_id or "")
            table.meta = dict(meta or {})

            if table.loss_amount > 0.0 or table.in_recovery:
                table.status = "RECOVERY"
                table.in_recovery = True
            else:
                table.status = "ACTIVE"
                table.in_recovery = False

            self._touch(table)

    # =========================================================
    # RELEASE / PNL
    # =========================================================
    def release(self, table_id: int, *, pnl: float = 0.0) -> None:
        with self._lock:
            table = self._get(table_id)
            if not table:
                return

            pnl = float(pnl or 0.0)

            if pnl < 0:
                table.loss_amount += abs(pnl)
                table.in_recovery = True
                table.status = "RECOVERY"
            else:
                # profitto usato per assorbire recovery
                if table.loss_amount > 0:
                    table.loss_amount = max(0.0, table.loss_amount - pnl)

                if table.loss_amount <= 0.0:
                    table.loss_amount = 0.0
                    table.in_recovery = False
                    table.status = "FREE"
                else:
                    table.in_recovery = True
                    table.status = "RECOVERY"

            table.current_event_key = ""
            table.current_exposure = 0.0
            table.market_id = ""
            table.selection_id = ""
            table.meta = {}
            self._touch(table)

    def force_unlock(self, table_id: int) -> None:
        with self._lock:
            table = self._get(table_id)
            if not table:
                return

            table.current_event_key = ""
            table.current_exposure = 0.0
            table.market_id = ""
            table.selection_id = ""
            table.meta = {}

            if table.loss_amount > 0.0:
                table.status = "RECOVERY"
                table.in_recovery = True
            else:
                table.status = "FREE"
                table.in_recovery = False

            self._touch(table)

    # =========================================================
    # LOOKUPS
    # =========================================================
    def find_by_event_key(self, event_key: str) -> Optional[TableState]:
        with self._lock:
            event_key = str(event_key or "")
            for table in self._tables.values():
                if table.current_event_key == event_key:
                    return TableState(**table.to_dict())
            return None

    def get_table(self, table_id: int) -> Optional[TableState]:
        with self._lock:
            table = self._get(table_id)
            if not table:
                return None
            return TableState(**table.to_dict())

    def active_tables(self) -> List[TableState]:
        with self._lock:
            return [
                TableState(**t.to_dict())
                for t in self._tables.values()
                if t.status in {"ACTIVE", "RECOVERY", "LOCKED"} and t.current_event_key
            ]

    def recovery_tables(self) -> List[TableState]:
        with self._lock:
            return [
                TableState(**t.to_dict())
                for t in self._tables.values()
                if t.in_recovery or t.loss_amount > 0.0 or t.status == "RECOVERY"
            ]

    def total_exposure(self) -> float:
        with self._lock:
            # Questa somma alimenta il cap assoluto in euro `max_open_exposure`
            # (runtime_controller, Enforcement A2 #320): non deve MAI sotto-stimare
            # l'esposizione aperta, altrimenti un ordine passerebbe oltre il cap.
            total = 0.0
            for t in self._tables.values():
                exp = t.current_exposure
                # Fail-closed su valori NON FINITI (NaN/inf) = stato corrotto: NON
                # trattarli come 0 — `max(0.0, NaN)` darebbe 0.0 e li nasconderebbe,
                # sotto-stimando il totale e facendo passare il cap. Restituisci inf
                # cosi' il cap A2 blocca ogni nuovo ordine finche' lo stato e' corrotto.
                if not math.isfinite(exp):
                    return float("inf")
                # Clamp per-tavolo a >= 0 (M-06): un `current_exposure` negativo non
                # deve DEFLAZIONARE il totale (sovra-stima al piu', mai sotto-stima).
                total += max(0.0, exp)
            return float(total)

    # =========================================================
    # RESET / SNAPSHOT
    # =========================================================
    def reset_all(self) -> None:
        with self._lock:
            for table in self._tables.values():
                table.status = "FREE"
                table.current_event_key = ""
                table.current_exposure = 0.0
                table.loss_amount = 0.0
                table.in_recovery = False
                table.market_id = ""
                table.selection_id = ""
                table.meta = {}
                self._touch(table)

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [t.to_dict() for t in self._tables.values()]