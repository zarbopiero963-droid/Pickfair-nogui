from __future__ import annotations

import time
from typing import Dict, List, Optional

from core.system_state import TableState, TableStatus


class TableManager:
    def __init__(self, table_count: int = 5):
        self.tables: List[TableState] = [
            TableState(table_id=i + 1) for i in range(max(1, int(table_count)))
        ]

    def reset_all(self) -> None:
        for table in self.tables:
            table.status = TableStatus.FREE
            table.loss_amount = 0.0
            table.current_exposure = 0.0
            table.current_event_key = ""
            table.market_id = ""
            table.selection_id = None
            table.opened_at_ts = None
            table.meta = {}

    def get(self, table_id: int) -> Optional[TableState]:
        for table in self.tables:
            if table.table_id == table_id:
                return table
        return None

    def find_by_event_key(self, event_key: str) -> Optional[TableState]:
        for table in self.tables:
            if table.current_event_key == event_key:
                return table
        return None

    def active_tables(self) -> List[TableState]:
        return [t for t in self.tables if t.status == TableStatus.ACTIVE]

    def recovery_tables(self) -> List[TableState]:
        return [t for t in self.tables if t.status == TableStatus.RECOVERY]

    def total_exposure(self) -> float:
        return sum(float(t.current_exposure or 0.0) for t in self.tables)

    def allocate(self, event_key: str, allow_recovery: bool = True) -> Optional[TableState]:
        existing = self.find_by_event_key(event_key)
        if existing:
            return None

        free_tables = [t for t in self.tables if t.status == TableStatus.FREE]
        if free_tables:
            return free_tables[0]

        if allow_recovery:
            recovery_tables = sorted(
                [t for t in self.tables if t.status == TableStatus.RECOVERY],
                key=lambda x: x.loss_amount,
            )
            if recovery_tables:
                return recovery_tables[0]

        return None

    def activate(
        self,
        table_id: int,
        event_key: str,
        exposure: float,
        market_id: str = "",
        selection_id: Optional[int] = None,
        meta: Optional[Dict] = None,
    ) -> TableState:
        table = self.get(table_id)
        if table is None:
            raise ValueError(f"table_id non valido: {table_id}")

        table.status = TableStatus.ACTIVE
        table.current_event_key = str(event_key or "")
        table.current_exposure = float(exposure or 0.0)
        table.market_id = str(market_id or "")
        table.selection_id = selection_id
        table.opened_at_ts = time.time()
        table.meta = dict(meta or {})
        return table

    def release(self, table_id: int, pnl: float = 0.0) -> TableState:
        table = self.get(table_id)
        if table is None:
            raise ValueError(f"table_id non valido: {table_id}")

        pnl = float(pnl or 0.0)

        if pnl < 0:
            table.loss_amount += abs(pnl)
            table.status = TableStatus.RECOVERY if table.loss_amount > 0 else TableStatus.FREE
        else:
            table.loss_amount = max(0.0, table.loss_amount - pnl)
            table.status = TableStatus.RECOVERY if table.loss_amount > 0 else TableStatus.FREE

        table.current_exposure = 0.0
        table.current_event_key = ""
        table.market_id = ""
        table.selection_id = None
        table.opened_at_ts = None
        table.meta = {}
        return table

    def force_unlock(self, table_id: int) -> TableState:
        table = self.get(table_id)
        if table is None:
            raise ValueError(f"table_id non valido: {table_id}")

        table.status = TableStatus.FREE
        table.current_exposure = 0.0
        table.current_event_key = ""
        table.market_id = ""
        table.selection_id = None
        table.opened_at_ts = None
        table.meta = {}
        return table

    def snapshot(self) -> List[dict]:
        return [
            {
                "table_id": t.table_id,
                "status": t.status.value,
                "loss_amount": round(float(t.loss_amount or 0.0), 2),
                "current_exposure": round(float(t.current_exposure or 0.0), 2),
                "current_event_key": t.current_event_key,
                "market_id": t.market_id,
                "selection_id": t.selection_id,
                "opened_at_ts": t.opened_at_ts,
                "meta": dict(t.meta or {}),
            }
            for t in self.tables
        ]
