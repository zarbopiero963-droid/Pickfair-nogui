"""F-33 — binding L2/L3: combinazione generata -> configurazione -> runtime SIM -> operazioni -> oracolo -> report.

Per ogni caso del generatore (``tools/combi``):
1. ambiente SIM isolato (HOME dedicato, SQLite nuovo) via ``runner.run``;
2. i parametri del caso sono salvati nelle settings PRIMA dello START (None => "" = non attivo, come la GUI);
3. lo snapshot ``config``/``sim_config``/``table_count`` del child prova che il runtime ha caricato quei valori;
4. scenario finanziario deterministico (vedi SCENARIOS);
5. raccolta di fondi, riserve, esposizione, ordini, ledger, settlement;
6. oracolo Decimal indipendente + controlli coerenti con la configurazione (stake entro i cap configurati, tavoli
   entro table_count, rifiuto esplicito quando la configurazione nega l'ingresso);
7. record: seed, SHA, gruppo, caso, scenario, esito, violazioni -> ``art/fullscope/<sha>/combi_runs.json``.

Nessun limite finanziario e' introdotto qui: i soli limiti verificati sono quelli DEL CASO (configurazione scelta).
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from decimal import Decimal as D
from typing import Any, Dict, List, Optional

from tests.fullscope.harness import oracle as O
from tests.fullscope.harness import runner as H
from tests.fullscope.requirements import tracker as trk
from tools.combi import generator as G

START = D("100")
# chiavi Roserpina il cui None significa "non attivo" e si persiste come "" (rettifica owner 18-19/10/2026)
OPTIONAL_CAPS = {"roserpina.max_daily_loss", "roserpina.max_drawdown_hard_stop_pct", "roserpina.max_open_exposure",
                 "roserpina.max_order_exposure", "roserpina.max_market_exposure", "roserpina.max_event_exposure_abs",
                 "roserpina.max_drawdown_abs", "roserpina.max_session_loss", "roserpina.max_exposure_stop",
                 "roserpina.max_recovery_chase_abs", "roserpina.max_stake_abs", "roserpina.max_win"}

# scenari deterministici (mercati distinti per gruppo per non confondere gli artifact)
SCENARIOS: Dict[str, List[Dict[str, Any]]] = {
    "entry_settle_win": [{"op": "signal", "m": "1.911", "price": 2.0, "side": "BACK"},
                         {"op": "settle", "m": "1.911", "win": True}],
    "entry_settle_lose": [{"op": "signal", "m": "1.912", "price": 2.0, "side": "BACK"},
                          {"op": "settle", "m": "1.912", "win": False}],
    "lay_settle": [{"op": "signal", "m": "1.913", "price": 2.0, "side": "LAY"},
                   {"op": "settle", "m": "1.913", "win": False}],
    "partial_cashout_settle": [{"op": "mode", "m": "1.914", "mode": "partial"},
                               {"op": "signal", "m": "1.914", "price": 2.0, "side": "BACK"},
                               {"op": "signal", "m": "1.915", "price": 2.0, "side": "BACK"},
                               {"op": "cashout", "m": "1.915"},
                               {"op": "settle", "m": "1.914", "win": False},
                               {"op": "settle", "m": "1.915", "win": True}],
    "three_tables": [{"op": "signal", "m": "1.921", "price": 2.0}, {"op": "signal", "m": "1.922", "price": 1.5},
                     {"op": "signal", "m": "1.923", "price": 3.0}, {"op": "signal", "m": "1.924", "price": 2.0},
                     {"op": "settle", "m": "1.921", "win": True}],
}
GROUP_SCENARIOS = {
    "a_rischio_roserpina": ["entry_settle_win", "three_tables"],
    "b_protezioni": ["entry_settle_lose", "lay_settle"],
    "c_esecuzione_sim": ["partial_cashout_settle"],
    "d_liquidita_book": ["entry_settle_win"],
    "t_stake_cap": ["entry_settle_win"],
    "t_fase_moltiplicatori": ["entry_settle_lose"],
}


def settings_from_case(case: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in case.items():
        if v is None:
            out[k] = ""                     # non attivo, come il salvataggio della tab Roserpina
        elif isinstance(v, bool):
            out[k] = int(v)
        else:
            out[k] = v
    return out


def _num(v: Any) -> Optional[D]:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return D(str(v))


def check_applied(case: Dict[str, Any], snap: Dict[str, Any]) -> List[str]:
    """Il runtime ha caricato i valori del caso (config Roserpina, sim config, numero tavoli)."""
    problems = []
    cfg = snap.get("config") or {}
    sim = snap.get("sim_config") or {}
    for k, v in case.items():
        if k.startswith("roserpina."):
            attr = k.split(".", 1)[1]
            if attr not in cfg:
                problems.append(f"{k}: attributo assente dal config caricato")
                continue
            got = cfg[attr]
            if v is None:
                if got is not None:
                    problems.append(f"{k}: atteso None (non attivo), caricato {got!r}")
            elif isinstance(v, bool):
                if bool(got) != v:
                    problems.append(f"{k}: atteso {v}, caricato {got!r}")
            elif isinstance(v, (int, float)):
                if _num(got) is None or abs(float(got) - float(v)) > 1e-9:
                    problems.append(f"{k}: atteso {v}, caricato {got!r}")
            elif str(got) != str(v):
                problems.append(f"{k}: atteso {v!r}, caricato {got!r}")
            if attr == "table_count" and snap.get("table_count") != int(v):
                problems.append(f"table_count: config {v} ma tavoli creati {snap.get('table_count')}")
        elif k.startswith("simulation."):
            attr = k.split(".", 1)[1]
            got = sim.get(attr)
            if isinstance(v, bool):
                if bool(got) != v:
                    problems.append(f"{k}: atteso {v}, caricato {got!r}")
            elif _num(got) is None or abs(float(got) - float(v)) > 1e-9:
                problems.append(f"{k}: atteso {v}, caricato {got!r}")
        elif k.startswith("settlement."):
            pass  # letta dal poller all'avvio; l'effetto e' visibile dai settlement contabilizzati
    return problems


def config_consistency(case: Dict[str, Any], snaps: List[Dict[str, Any]], result: Dict[str, Any], start: D) -> List[str]:
    """Controlli finanziari coerenti con la configurazione DEL CASO (nessun limite esterno)."""
    v: List[str] = []
    first = snaps[0]
    last = snaps[-1]
    orders = [o for o in last.get("orders", []) if o.get("status") in ("SUBMITTED", "COMPLETED", "SETTLED", "PARTIAL", "AMBIGUOUS")]
    entries = [o for o in orders if (o.get("side") or "BACK").upper() in ("BACK", "LAY")]
    single_pct = _num(case.get("roserpina.max_single_bet_pct"))
    stake_abs = _num(case.get("roserpina.max_stake_abs"))
    min_stake = _num(case.get("roserpina.min_stake"))
    table_count = case.get("roserpina.table_count")
    avail0 = D(str(first["funds"]["available"]))
    for o in entries:
        stake = _num(o.get("stake"))
        if stake is None:
            continue
        if single_pct is not None and stake > (avail0 * single_pct / 100) + D("0.01"):
            v.append(f"stake {stake} > max_single_bet_pct {single_pct}% di {avail0} ({o.get('market_id')})")
        if stake_abs is not None and stake > stake_abs + D("0.001"):
            v.append(f"stake {stake} > max_stake_abs {stake_abs} ({o.get('market_id')})")
        if min_stake is not None and stake < min_stake - D("0.001"):
            v.append(f"stake {stake} < min_stake {min_stake} ({o.get('market_id')})")
    if table_count is not None:
        live_tables = len(last.get("tables", []))
        if live_tables > int(table_count):
            v.append(f"tavoli vivi {live_tables} > table_count {table_count}")
    # ingresso negato => rifiuto esplicito osservato (fail-closed), mai silenzio
    signal_markets = {s["m"] for s in result.get("_steps", []) if s.get("op") == "signal"}
    placed = {str(o.get("market_id")) for o in entries}
    for m in signal_markets - placed:
        rejected = any(e["e"] in ("SIGNAL_REJECTED", "QUICK_BET_FAILED", "QUICK_BET_DUPLICATE") and str(e.get("market_id") or m) == m
                       for e in result.get("events", []))
        if not rejected:
            v.append(f"segnale {m} senza ordine e senza rifiuto esplicito")
    return v


def run_case(group: str, case_id: str, case: Dict[str, Any], scenario: str, *, seed: int) -> Dict[str, Any]:
    steps = SCENARIOS[scenario]
    home = tempfile.mkdtemp(prefix=f"pf_combi_{group}_")
    settings = settings_from_case(case)
    r = H.run(steps, home=home, settings=settings, seed=seed, scenario=f"combi_{group}_{scenario}",
              requirements=[f"COMBI:{group}"])
    r["_steps"] = steps
    snaps = r["snaps"]
    applied = check_applied(case, snaps[0])
    outcomes = {s["m"]: bool(s.get("win")) for s in steps if s["op"] == "settle"}
    pre_settle = snaps[max(0, len(snaps) - 1 - len(outcomes))]
    last = snaps[-1]
    # saldo iniziale REALE dell'exchange (snapshot "start"): se differisce da quello configurato e' un problema di
    # applicazione della configurazione, non dell'oracolo
    start_exchange = D(str(snaps[0]["funds"]["available"]))
    cfg_start = _num(case.get("simulation.starting_balance"))
    if cfg_start is not None and start_exchange != cfg_start:
        applied.append(f"simulation.starting_balance: configurato {cfg_start}, saldo exchange all'avvio {start_exchange}")
    commission = D(str((last.get("sim_config") or {}).get("commission_pct", 4.5))) / 100
    poll_enabled = case.get("settlement.poll_enabled", True)
    o = O.Oracle(last, outcomes, start=start_exchange, commission=commission, extra_legs=O.exchange_legs(pre_settle))
    violations: List[str] = []
    checks = [("settlement_unico", lambda: O.check_single_settlement_per_market(last)),
              ("duplicati", lambda: O.check_no_duplicate_intents(last, sorted({s["m"] for s in steps if s["op"] == "signal"})))]
    if poll_enabled in (True, 1, "1", "true", "True"):
        checks += [("conservazione", lambda: O.check_conservation(last, o)),
                   ("realizzato", lambda: O.check_realized(last, o))]
    else:
        # poller disattivato dal caso: l'exchange regola, il bot NON contabilizza (comportamento atteso e dichiarato)
        def _not_booked():
            assert D(str(last["realized"])) == D(0), "realizzato senza poller: " + O.describe(last, o)
            exch_total = D(str(last["funds"]["available"])) + D(str(last["funds"].get("exposure") or 0))
            net = sum((D(str(x.get("net_pnl") or 0)) for x in last.get("exch_settlements", [])), D(0))
            assert abs(exch_total - (start_exchange + net)) <= D("0.01"), "exchange incoerente: " + O.describe(last, o)
        checks.append(("exchange_senza_poller", _not_booked))
    for name, fn in checks:
        try:
            fn()
        except AssertionError as exc:
            violations.append(f"{name}: {str(exc)[:300]}")
    violations.extend(config_consistency(case, snaps, r, start_exchange))
    unmet = [w for w in r.get("waits", []) if not w["met"]]
    rec = {
        "group": group, "case_id": case_id, "case": case, "scenario": scenario, "seed": seed,
        "repo_sha": trk.head_sha(), "home": home, "duration_s": r.get("_duration_s"),
        "applied_problems": applied, "oracle_violations": violations, "unmet_waits": unmet,
        "orders": last.get("orders"), "funds": last.get("funds"), "realized": last.get("realized"),
        "tables": last.get("tables"), "exch_settlements": last.get("exch_settlements"),
        "outcome": "PASS" if not (applied or violations or unmet) else "FAIL",
    }
    _append_record(rec)
    return rec


def reset_records() -> str:
    """Nuovo file di record per questa sessione (il precedente resta come .prev)."""
    path = os.path.join(trk.artifact_dir(trk.head_sha()), "combi_runs.json")
    if os.path.exists(path):
        os.replace(path, path + ".prev")
    return path


def _append_record(rec: Dict[str, Any]) -> None:
    path = os.path.join(trk.artifact_dir(trk.head_sha()), "combi_runs.json")
    data: List[Dict[str, Any]] = []
    if os.path.exists(path):
        try:
            data = json.load(open(path))
        except Exception:  # noqa: BLE001
            data = []
    data.append(rec)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1, default=str)


def cases_for(group: str, *, seed: int, limit: Optional[int]) -> List[tuple]:
    inv = G.load_inventory()
    rep = G.build_group(group, inv, seed=seed)
    params = G.pytest_params(rep)
    return params if limit is None else params[:limit]
