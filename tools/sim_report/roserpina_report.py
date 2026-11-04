"""Report della simulazione Roserpina (100 EUR fittizi) dal dump dell'harness F-026 (runtime reale, confini SIM).

Solo lettura del dump: nessun calcolo dell'atteso qui (l'oracolo indipendente vive nei test). Tutte le somme in
Decimal, quantizzate al centesimo solo in uscita. Provenienza dei dati dichiarata: ``SINTETICO`` (scenario scritto a
mano, quote inventate e dichiarate tali), ``PDF_MONEYFLOW`` (dati del PDF dell'owner, non disponibile al 04/11/2026)
o ``REGISTRATO`` (storico reale). Le funzioni non coperte dal motore su ``main`` sono elencate, mai simulate.

Grandezze:
- capitale iniziale e saldo finale: ``funds.total`` (disponibile + esposizione) del primo e dell'ultimo snapshot;
- per mercato (= tavolo dell'ingresso): ingressi (righe ``orders`` d'ingresso), uscite (hedge di cashout, righe con
  ``customer_ref`` di hedge), P&L lordo e netto dal settlement dell'exchange (autorevole), commissione = lordo - netto;
- esposizione massima: massimo di ``funds.exposure`` sugli snapshot;
- drawdown massimo: dalla serie dell'equita' di cassa ``funds.total`` rispetto al suo massimo progressivo, e quello
  del desk (``drawdown_pct`` di Roserpina) riportato a parte;
- ordini non riconciliati: righe ``orders`` finali in stato incerto (INFLIGHT/AMBIGUOUS) e ordini dell'exchange
  ancora vivi (non SETTLED/LAPSED/CANCELLED/VOIDED) su mercati gia' regolati.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional

PROVENANCES = ("SINTETICO", "PDF_MONEYFLOW", "REGISTRATO")
UNCERTAIN = ("INFLIGHT", "AMBIGUOUS")
EXCHANGE_CLOSED = ("SETTLED", "LAPSED", "CANCELLED", "VOIDED")
CENT = Decimal("0.01")


def _d(value: Any) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError(f"valore non numerico: {value!r}")
    out = Decimal(str(value))
    if not out.is_finite():
        raise ValueError(f"valore non finito: {value!r}")
    return out


def _c(value: Decimal) -> str:
    return str(value.quantize(CENT, rounding=ROUND_HALF_UP))


def _snaps(dumps: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for run in dumps:
        out.extend(run.get("snaps") or [])
    if not out:
        raise ValueError("dump senza snapshot")
    return out


def build_report(dumps: List[Dict[str, Any]], *, provenance: str, scenario: str,
                 not_covered: Optional[List[str]] = None) -> Dict[str, Any]:
    """``dumps``: i dump di una o piu' esecuzioni consecutive dello stesso HOME (riavvii compresi), in ordine."""
    if provenance not in PROVENANCES:
        raise ValueError(f"provenienza sconosciuta: {provenance!r}")
    snaps = _snaps(dumps)
    first, last = snaps[0], snaps[-1]
    initial = _d(first["funds"]["total"])
    final_total, final_available = _d(last["funds"]["total"]), _d(last["funds"]["available"])

    settled: Dict[str, Dict[str, Decimal]] = {}
    for snap in snaps:                              # il settlement dell'exchange e' unico per mercato
        for row in snap.get("exch_settlements") or []:
            settled[str(row["market_id"])] = {"gross": _d(row["gross_pnl"]), "net": _d(row["net_pnl"])}

    tables: Dict[str, Dict[str, Any]] = {}
    for row in last.get("orders") or []:
        market = str(row.get("market_id") or "")
        if not market:
            continue
        t = tables.setdefault(market, {"market_id": market, "entries": 0, "exits": 0, "statuses": []})
        if row.get("hedge"):
            t["exits"] += 1
        else:
            t["entries"] += 1
        t["statuses"].append(row.get("status"))
    commission_total = Decimal(0)
    for market, t in tables.items():
        s = settled.get(market)
        if s is None:
            t.update(settled=False, gross_pnl=None, net_pnl=None, commission=None)
            continue
        commission = s["gross"] - s["net"]
        commission_total += commission
        t.update(settled=True, gross_pnl=_c(s["gross"]), net_pnl=_c(s["net"]), commission=_c(commission))

    exposure_max = max(_d(s["funds"].get("exposure") or 0) for s in snaps)
    peak = Decimal("-Infinity")
    cash_dd_max = Decimal(0)
    for s in snaps:
        total = _d(s["funds"]["total"])
        peak = max(peak, total)
        if peak > 0:
            cash_dd_max = max(cash_dd_max, (peak - total) / peak * 100)
    desk_dd_max = max(_d(s.get("drawdown_pct") or 0) for s in snaps)

    unreconciled = [{"market_id": o.get("market_id"), "status": o.get("status")}
                    for o in last.get("orders") or [] if o.get("status") in UNCERTAIN]
    unreconciled += [{"market_id": o.get("market_id"), "status": f"EXCHANGE_LIVE_ON_SETTLED:{o.get('status')}"}
                     for o in last.get("exch_orders") or []
                     if str(o.get("market_id")) in settled and str(o.get("status") or "").upper() not in EXCHANGE_CLOSED]

    net_total = sum((v["net"] for v in settled.values()), Decimal(0))
    return {
        "provenance": provenance, "scenario": scenario, "not_covered": list(not_covered or []),
        "initial_capital": _c(initial),
        "tables": sorted(tables.values(), key=lambda t: t["market_id"]),
        "entries": sum(t["entries"] for t in tables.values()),
        "exits": sum(t["exits"] for t in tables.values()),
        "realized_net_pnl": _c(net_total),
        "commission_total": _c(commission_total),
        "exposure_max": _c(exposure_max),
        "cash_drawdown_max_pct": _c(cash_dd_max),
        "desk_drawdown_max_pct": _c(desk_dd_max),
        "final_balance_total": _c(final_total),
        "final_available": _c(final_available),
        "unreconciled_orders": unreconciled,
        "restarts": max(0, len(dumps) - 1),
    }
