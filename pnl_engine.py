from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

from core.type_helpers import safe_side


@dataclass
class PnLResult:
    market_id: str
    selection_id: int
    side: str
    entry_price: float
    exit_price: float
    size: float
    gross_pnl: float
    commission_pct: float
    commission_amount: float
    net_pnl: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PnLEngine:
    """
    Motore P&L unificato.

    Supporta:
    - BACK
    - LAY
    - calcolo chiusura trade
    - commissione exchange
    - output coerente live/sim

    NON gestisce:
    - matching
    - ordini
    - stato tavoli
    """

    def __init__(self, commission_pct: float = 4.5):
        self.commission_pct = float(commission_pct or 0.0)

    # =========================================================
    # HELPERS
    # =========================================================
    def _safe_side(self, side: Any) -> str:
        return safe_side(side)

    def _commission_amount(self, gross_pnl: float, commission_pct: Optional[float] = None) -> float:
        pct = self.commission_pct if commission_pct is None else float(commission_pct or 0.0)
        if gross_pnl <= 0:
            return 0.0
        return gross_pnl * (pct / 100.0)

    # =========================================================
    # SINGLE POSITION PNL
    # =========================================================
    def calculate_position_pnl(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        entry_price: float,
        exit_price: float,
        size: float,
        commission_pct: Optional[float] = None,
    ) -> PnLResult:
        """
        Calcolo P&L chiusura posizione semplice.

        BACK:
            gross = stake * (exit_price - entry_price) / entry_price

        LAY:
            gross = stake * (entry_price - exit_price) / entry_price

        Nota:
        questo è un modello di trading P&L continuo,
        non un settlement finale binario win/lose.
        """
        market_id = str(market_id or "")
        selection_id = int(selection_id)
        side = self._safe_side(side)
        entry_price = float(entry_price or 0.0)
        exit_price = float(exit_price or 0.0)
        size = float(size or 0.0)

        if entry_price <= 1.0:
            raise ValueError("entry_price non valido")
        if exit_price <= 1.0:
            raise ValueError("exit_price non valido")
        if size <= 0.0:
            raise ValueError("size non valido")

        if side == "BACK":
            gross = size * (exit_price - entry_price) / entry_price
        else:
            gross = size * (entry_price - exit_price) / entry_price

        commission_amount = self._commission_amount(gross, commission_pct)
        net = gross - commission_amount

        return PnLResult(
            market_id=market_id,
            selection_id=selection_id,
            side=side,
            entry_price=entry_price,
            exit_price=exit_price,
            size=size,
            gross_pnl=float(gross),
            commission_pct=float(self.commission_pct if commission_pct is None else commission_pct),
            commission_amount=float(commission_amount),
            net_pnl=float(net),
        )

    # =========================================================
    # SETTLEMENT PNL
    # =========================================================
    def calculate_settlement_pnl(
        self,
        *,
        side: str,
        price: float,
        size: float,
        won: bool,
        commission_pct: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        P&L settlement finale stile exchange.

        BACK:
            win  -> gross = size * (price - 1)
            lose -> gross = -size

        LAY:
            win  -> gross = size
            lose -> gross = -(size * (price - 1))
        """
        side = self._safe_side(side)
        price = float(price or 0.0)
        size = float(size or 0.0)

        if price <= 1.0:
            raise ValueError("price non valido")
        if size <= 0.0:
            raise ValueError("size non valido")

        if side == "BACK":
            gross = size * (price - 1.0) if won else -size
        else:
            gross = size if won else -(size * (price - 1.0))

        commission_amount = self._commission_amount(gross, commission_pct)
        net = gross - commission_amount

        return {
            "gross_pnl": float(gross),
            "commission_pct": float(self.commission_pct if commission_pct is None else commission_pct),
            "commission_amount": float(commission_amount),
            "net_pnl": float(net),
        }

    # =========================================================
    # GREEN-UP / CASHOUT HELPERS
    # =========================================================
    def calculate_green_up_size(
        self,
        *,
        entry_side: str,
        entry_price: float,
        entry_size: float,
        hedge_price: float,
    ) -> float:
        """
        Calcola size hedge per green-up base.

        BACK entry + LAY hedge:
            lay_size = (back_price * back_stake) / lay_price

        LAY entry + BACK hedge:
            back_size = (lay_price * lay_stake) / back_price
        """
        entry_side = self._safe_side(entry_side)
        entry_price = float(entry_price or 0.0)
        entry_size = float(entry_size or 0.0)
        hedge_price = float(hedge_price or 0.0)

        if entry_price <= 1.0:
            raise ValueError("entry_price non valido")
        if hedge_price <= 1.0:
            raise ValueError("hedge_price non valido")
        if entry_size <= 0.0:
            raise ValueError("entry_size non valido")

        return float((entry_price * entry_size) / hedge_price)

    def calculate_cashout_pnl(
        self,
        *,
        entry_side: str,
        entry_price: float,
        entry_size: float,
        hedge_price: float,
        commission_pct: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        Calcola pnl stimato da cashout/green-up.
        """
        hedge_size = self.calculate_green_up_size(
            entry_side=entry_side,
            entry_price=entry_price,
            entry_size=entry_size,
            hedge_price=hedge_price,
        )

        pnl = self.calculate_position_pnl(
            market_id="",
            selection_id=0,
            side=entry_side,
            entry_price=entry_price,
            exit_price=hedge_price,
            size=entry_size,
            commission_pct=commission_pct,
        )

        return {
            "hedge_size": float(hedge_size),
            "gross_pnl": float(pnl.gross_pnl),
            "commission_amount": float(pnl.commission_amount),
            "net_pnl": float(pnl.net_pnl),
        }

    # =========================================================
    # SNAPSHOT HELPERS
    # =========================================================
    def mark_to_market_pnl(
        self,
        *,
        side: str,
        entry_price: float,
        current_price: float,
        size: float,
    ) -> float:
        result = self.calculate_position_pnl(
            market_id="",
            selection_id=0,
            side=side,
            entry_price=entry_price,
            exit_price=current_price,
            size=size,
            commission_pct=0.0,
        )
        return float(result.gross_pnl)