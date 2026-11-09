"""Oracolo finanziario INDIPENDENTE di Roserpina (PF-ROSERPINA-FINANCIAL-ORACLE-CLAUDE-D-20261109).

Solo `decimal.Decimal` costruito dalla forma decimale dei valori (mai da un float binario), confronti ESATTI, nessuna
tolleranza. Non importa codice di prodotto: e' la specifica eseguibile contro cui il codice viene misurato.

Definizioni (decisioni owner e contratto del codice, vedi docs/testing/roserpina_financial_oracle.md):
- cassa (equity di cassa) = disponibile + impegnato della STESSA lettura del conto (PFC-008, F026);
- equity = cassa + P&L non realizzato;  le puntate aperte NON sono perdite (F026);
- picco = massimo storico della cassa; il drawdown assoluto e' picco - cassa (>= 0);
- regola owner «il reset non cancella le perdite»: un auto-reset del ciclo Roserpina non riduce le perdite gia'
  maturate ai fini dei limiti di arresto (perdita giornaliera, di sessione, drawdown assoluto);
- soglia di cap: un importo e' ammesso se e solo se importo <= limite (Decimal esatto);
- soglia di arresto: scatta se e solo se importo >= limite;
- BACK: rischio = stake; LAY: rischio (liability) = stake x (quota - 1);
- commissione Betfair Italia = 4,5 % del profitto NETTO del mercato se positivo, altrimenti 0;
- stake Roserpina = (bankroll x target% + chase) / (quota - 1) x moltiplicatori, PER DIFETTO al centesimo.
"""
from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Any, Iterable

CENT = Decimal("0.01")
COMMISSION_IT = Decimal("0.045")


def D(x: Any) -> Decimal:
    if isinstance(x, Decimal):
        return x
    if isinstance(x, bool) or x is None:
        raise TypeError(f"valore non monetario: {x!r}")
    return Decimal(str(x))


def admitted_by_cap(amount: Any, cap: Any) -> bool:
    return D(amount) <= D(cap)


def stop_reached(amount: Any, limit: Any) -> bool:
    return D(amount) >= D(limit)


def risk(side: str, stake: Any, price: Any) -> Decimal:
    s, p = D(stake), D(price)
    if p <= 1:
        raise ValueError("quota <= 1")
    return s if side.upper() == "BACK" else s * (p - 1)


def commission(net_market_profit: Any) -> Decimal:
    n = D(net_market_profit)
    return n * COMMISSION_IT if n > 0 else Decimal(0)


def net_after_commission(net_market_profit: Any) -> Decimal:
    return D(net_market_profit) - commission(net_market_profit)


def stake(bankroll: Any, target_pct: Any, price: Any, chase: Any = 0, risk_mult: Any = 1, desk_mult: Any = 1) -> Decimal:
    raw = (D(bankroll) * D(target_pct) / 100 + D(chase)) / (D(price) - 1) * D(risk_mult) * D(desk_mult)
    return max(Decimal(0), raw).quantize(CENT, rounding=ROUND_DOWN)


def cash(available: Any, committed: Any) -> Decimal:
    return D(available) + D(committed)


class Ledger:
    """Storia delle letture del conto: picco della cassa VERIFICATA e perdite maturate, invarianti al reset di ciclo,
    al riavvio e alla liberazione delle riserve (contratto owner PF-ROSERPINA-DRAWDOWN-AND-RECOVERY-20261109):
    drawdown_abs = max(0, verified_cash_peak - verified_cash_current)."""

    def __init__(self) -> None:
        self.peak = Decimal(0)
        self.current = Decimal(0)

    def read(self, available: Any, committed: Any = 0) -> "Ledger":
        self.current = cash(available, committed)
        self.peak = max(self.peak, self.current)
        return self

    def drawdown_abs(self) -> Decimal:
        return max(Decimal(0), self.peak - self.current)

    def drawdown_pct(self) -> Decimal:
        return Decimal(0) if self.peak <= 0 else self.drawdown_abs() / self.peak * 100


def cents(x: Any) -> Decimal:
    return D(x).quantize(CENT, rounding=ROUND_HALF_UP)


def total(values: Iterable[Any]) -> Decimal:
    return sum((D(v) for v in values), Decimal(0))
