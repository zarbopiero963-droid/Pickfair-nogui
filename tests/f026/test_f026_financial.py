"""PF-ALL-F026 — Roserpina: bankroll, drawdown e auto-reset con posizioni vive (HeadlessApp reale, SIM, SQLite su disco).

Oracolo finanziario INDIPENDENTE (Decimal), nessun import del codice sotto test:
- P&L di un BACK abbinato: vincita = matched*(prezzo-1)*(1-c), perdita = -matched; c = 4.5% (config SIM;
  OD-PF-COMMISSION-RATE-01: in SIM vale roserpina.commission_pct).
- Equita' di cassa del conto = saldo iniziale + realizzato (+ variazioni esterne) = disponibile + impegnato.
  I fondi impegnati in posizioni aperte NON sono perdite (#426/6146717266). F026 v2: questa grandezza e' registrata
  a parte (snapshot del conto) e decide SOLO l'auto-reset; la base dello stake e dei limiti resta il disponibile.
- Drawdown = max(0, picco - equity)/picco, con il picco calcolato sul percorso dell'equity realizzata.
- Un reset non libera mai il tavolo di una posizione non regolata (#426 OWNER-SETTLEMENT-02, OC-04: «senza
  liberazioni anticipate», «nessuna liberazione di capitale ancora impegnato»).
Ogni test riporta nel messaggio: stato, saldo, esposizione, capitale a rischio, P&L, tavoli, atteso/effettivo.
"""
import json
import os
import subprocess
import sys
import tempfile
from decimal import Decimal as D, ROUND_HALF_UP

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
TREE = os.environ.get("PF_TREE") or os.path.dirname(os.path.dirname(HERE))
PY = os.environ.get("PF_PY", sys.executable)
START = D("100")
COMM = D("0.045")
AUTO_RESET_PCT = D("15")          # default di codice roserpina.auto_reset_drawdown_pct (soglia di stato)


def q(x):
    return D(str(x)).quantize(D("0.001"), rounding=ROUND_HALF_UP)


# Parametro SIM dichiarato: soglia di lockdown alta, per isolare F026/F2 dal lockdown sul drawdown del disponibile
# (base «caso peggiore», invariata: la base del drawdown e' una decisione owner aperta, D04 6.O2).
SETTINGS = {"roserpina.lockdown_drawdown_pct": 60.0}


def run(steps, home=None, settings=None, **flags):
    home = home or tempfile.mkdtemp(prefix="f026_")
    out = os.path.join(tempfile.mkdtemp(prefix="f026o_"), "o.json")
    env = dict(os.environ)
    if os.environ.get("PF_NETGUARD"):
        env["PYTHONPATH"] = os.environ["PF_NETGUARD"]
    p = subprocess.run([PY, os.path.join(HERE, "child_f026.py"), TREE, out,
                        json.dumps({"steps": steps, "settings": dict(SETTINGS, **(settings or {})), **flags}), home],
                       env=env, capture_output=True, text=True, timeout=300)
    r = json.load(open(out))
    r["_rc"] = p.returncode
    r["_home"] = home
    assert "error" not in r, r.get("error")
    return r


class Oracle:
    """Ricostruisce dal ledger SQLite (stake/prezzo scelti dal codice) e dagli esiti dichiarati il valore atteso."""

    def __init__(self, snap, outcomes, external=D(0)):
        self.orders = {o["market_id"]: o for o in snap["orders"] if o["status"] not in ("DUPLICATE_BLOCKED",)}
        self.outcomes = outcomes
        self.external = external

    def pnl(self, m):
        o = self.orders[m]
        matched = D(str(o["matched"] if o["matched"] is not None else o["stake"]))
        price = D(str(o["price"]))
        side = (o["side"] or "BACK").upper()
        win = self.outcomes[m]                                  # True = la selezione puntata vince
        if side == "BACK":
            return matched * (price - 1) * (1 - COMM) if win else -matched
        return -(matched * (price - 1)) if win else matched * (1 - COMM)

    def realized(self):
        return sum((self.pnl(m) for m in self.outcomes if m in self.orders), D(0))

    def equity(self):
        return START + self.realized() + self.external

    def open_markets(self):
        return {m for m, o in self.orders.items() if m not in self.outcomes and o["status"] in ("SUBMITTED", "AMBIGUOUS", "INFLIGHT")}

    def capital_at_risk(self):
        tot = D(0)
        for m in self.open_markets():
            o = self.orders[m]
            stake, price = D(str(o["stake"])), D(str(o["price"]))
            tot += stake if (o["side"] or "BACK").upper() == "BACK" else stake * (price - 1)
        return tot


def describe(s, oracle=None):
    f = s["funds"]
    a = s.get("account") or {}
    msg = (f"cassa={a.get('cash_equity')} ok={a.get('ok')} " + f"[{s['tag']}] saldo disp={f.get('available')} esposizione={f.get('exposure')} totale={f.get('total')} "
           f"bankroll={round(s['bankroll'], 3)} picco={round(s['peak'], 3)} dd%={round(s['drawdown_pct'], 2)} "
           f"realizzato={round(s['realized'], 3)} reset={s['auto_resets']} tavoli={[(t['market_id'], t['exposure']) for t in s['tables']]}")
    if oracle:
        msg += f" | ORACOLO equity={q(oracle.equity())} realizzato={q(oracle.realized())} capitale_a_rischio={q(oracle.capital_at_risk())} aperti={sorted(oracle.open_markets())}"
    return msg


def five_open(prices=(1.3, 1.3, 2.0, 2.0, 2.0), base=700):
    return [{"op": "signal", "m": f"1.{base + i + 1}", "price": p} for i, p in enumerate(prices)]


def table_markets(s):
    """Mercati con una posizione sul tavolo (un tavolo in RECOVERY senza mercato non ha posizioni)."""
    return {t["market_id"] for t in s["tables"] if t["market_id"]}



def account_ok(s, oracle=None):
    """Snapshot del conto valido e coerente con l'exchange (e con l'oracolo, se dato)."""
    a = s.get("account") or {}
    assert a.get("ok") is True, "snapshot del conto assente o non valido: " + describe(s, oracle)
    assert q(a["cash_equity"]) == q(s["funds"]["total"]), "equita' di cassa != exchange: " + describe(s, oracle)
    assert q(a["available"]) == q(s["funds"]["available"]), describe(s, oracle)
    if oracle is not None:
        assert q(a["cash_equity"]) == q(oracle.equity()), "equita' di cassa != oracolo: " + describe(s, oracle)


def stake_base_unchanged(r):
    """F026-R1: ogni sincronizzazione del bankroll (base di stake e limiti) usa il DISPONIBILE dell'exchange in
    quell'istante, come prima di F026: le grandezze del conto non cambiano la base del money management."""
    exchange_syncs = {"start", "_sync_bankroll_post_settlement", "get_status"}   # non i ripristini interni
    syncs = [x for x in r.get("bankroll_syncs", []) if x.get("available") is not None
             and x.get("caller") in exchange_syncs]
    assert syncs, "nessuna sincronizzazione del bankroll registrata"
    for x in syncs:
        assert q(x["value"]) == q(x["available"]), f"bankroll sincronizzato su una base diversa dal disponibile (F026-R1): {x}"


# ---------------------------------------------------------------- 1, 3, 4, 10: vincita regolata con 4 posizioni vive
def test_F026_01_winning_settlement_with_live_positions_keeps_live_tables_frees_settled():
    r = run(five_open() + [{"op": "settle", "m": "1.703", "win": True}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.703": True})
    assert s["auto_resets"] == 0, "reset spurio: " + describe(s, o)
    assert o.open_markets() == {"1.701", "1.702", "1.704", "1.705"} == table_markets(s), \
        "tavoli != posizioni aperte (F026 vive trattenute, F2 regolata liberata): " + describe(s, o)
    assert q(s["realized"]) == q(o.realized()), describe(s, o)
    assert q(s["bankroll"]) == q(s["funds"]["available"]), "base bankroll cambiata: " + describe(s, o)
    account_ok(s, o)
    live_reserve = sum(D(str(t["exposure"])) for t in s["tables"])
    assert q(live_reserve) == q(o.capital_at_risk()), describe(s, o)
    stake_base_unchanged(r)


# ---------------------------------------------------------------- 2: LAY con liability riservata
def test_F026_02_lay_liability_reserved_is_not_a_loss():
    r = run([{"op": "signal", "m": "1.711", "price": 2.0, "side": "LAY"},
             {"op": "signal", "m": "1.712", "price": 2.0},
             {"op": "signal", "m": "1.713", "price": 1.3},
             {"op": "settle", "m": "1.712", "win": True}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.712": True})
    assert s["auto_resets"] == 0, describe(s, o)
    assert o.open_markets() == table_markets(s), describe(s, o)
    account_ok(s, o)
    stake_base_unchanged(r)


# ---------------------------------------------------------------- 5: posizione perdente regolata, altre vive
def test_F026_03_losing_settlement_counts_only_the_real_loss():
    r = run(five_open() + [{"op": "settle", "m": "1.704", "win": False}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.704": False})
    assert s["auto_resets"] == 0, describe(s, o)
    assert q(s["realized"]) == q(o.realized()), describe(s, o)
    account_ok(s, o)
    assert q(D(str(s["cash_peak"]))) == START, describe(s, o)


# ---------------------------------------------------------------- 6, 7: ordine parzialmente abbinato / residuo non abbinato
def test_F026_04_partial_match_and_unmatched_remainder_keep_reservation():
    r = run([{"op": "mode", "m": "1.721", "mode": "partial"}, {"op": "signal", "m": "1.721", "price": 2.0}]
            + five_open(prices=(1.3, 2.0, 2.0), base=721) + [{"op": "settle", "m": "1.723", "win": True}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.723": True})
    assert s["auto_resets"] == 0, describe(s, o)
    assert "1.721" in table_markets(s), "tavolo del parziale liberato: " + describe(s, o)
    assert "1.723" not in table_markets(s), "tavolo regolato non liberato (F2): " + describe(s, o)
    account_ok(s)


# ---------------------------------------------------------------- 8: ordine AMBIGUOUS + regolamento di altro mercato
def test_F026_05_ambiguous_order_and_settlement_elsewhere():
    r = run([{"op": "mode", "m": "1.731", "mode": "lost"}, {"op": "signal", "m": "1.731", "price": 1.3}]
            + five_open(prices=(1.3, 2.0, 2.0), base=731) + [{"op": "settle", "m": "1.733", "win": True}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.733": True})
    assert any(x["market_id"] == "1.731" and x["status"] == "AMBIGUOUS" for x in s["orders"]), describe(s, o)
    assert s["auto_resets"] == 0, describe(s, o)
    assert {"1.731", "1.732", "1.734"} <= table_markets(s) and "1.733" not in table_markets(s), describe(s, o)
    account_ok(s)


# ---------------------------------------------------------------- 9: drawdown reale vicino alla soglia (sotto)
def test_F026_06_real_drawdown_below_threshold_no_reset():
    r = run(five_open() + [{"op": "settle", "m": "1.701", "win": False}])       # -10 su 100 => 10%
    s = r["snaps"][-1]
    o = Oracle(s, {"1.701": False})
    dd = (START - o.equity()) / START * 100
    assert dd < AUTO_RESET_PCT
    assert s["auto_resets"] == 0, f"reset con drawdown reale {q(dd)}% < {AUTO_RESET_PCT}%: " + describe(s, o)
    assert o.open_markets() == table_markets(s), describe(s, o)
    account_ok(s, o)


# ---------------------------------------------------------------- 10, 11: drawdown reale oltre soglia, reset legittimo
def test_F026_07_legit_reset_keeps_live_tables_and_rebases_cash_peak():
    r = run(five_open() + [{"op": "settle", "m": "1.701", "win": False}, {"op": "settle", "m": "1.702", "win": False}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.701": False, "1.702": False})                            # -20 => 20%
    assert s["auto_resets"] >= 1, "precondizione: drawdown reale >= soglia => reset: " + describe(s, o)
    assert table_markets(s) == o.open_markets() == {"1.703", "1.704", "1.705"}, \
        "il reset deve trattenere solo le posizioni vive: " + describe(s, o)
    account_ok(s, o)
    assert q(D(str(s["cash_peak"]))) == q(o.equity()), "il reset ribasa il picco di cassa (OC-04): " + describe(s, o)
    reset_ev = [e for e in r["events"] if e["e"] == "ROSERPINA_AUTO_RESET"]
    assert reset_ev and reset_ev[0]["drawdown_pct"] is not None
    stake_base_unchanged(r)


# ---------------------------------------------------------------- 14: settlement duplicato
def test_F026_08_duplicate_settlement_counted_once_and_frees_nothing_else():
    r = run(five_open() + [{"op": "settle", "m": "1.703", "win": True}, {"op": "resettle", "m": "1.703"}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.703": True})
    assert q(s["realized"]) == q(o.realized()), "settlement contato due volte: " + describe(s, o)
    assert o.open_markets() == table_markets(s), describe(s, o)
    account_ok(s, o)


# ---------------------------------------------------------------- 16: variazione esterna del saldo senza settlement
def test_F026_09_external_balance_change_is_not_trading_pnl_and_never_frees_live_tables():
    r = run(five_open() + [{"op": "withdraw", "amount": 20.0}, {"op": "settle", "m": "1.703", "win": True}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.703": True}, external=D("-20"))
    # Il trattamento di un prelievo nel drawdown e nell'auto-reset e' NON DECISO (#426): si verifica solo il definito.
    close = [e for e in r["events"] if e["e"] == "RUNTIME_CLOSE_POSITION" and e["market_id"] == "1.703"]
    assert close and q(close[0]["pnl"]) == q(o.pnl("1.703")), "il prelievo e' finito nel P&L di trading: " + describe(s, o)
    assert o.open_markets() == table_markets(s), "tavoli vivi liberati: " + describe(s, o)
    account_ok(s, o)


# ---------------------------------------------------------------- 15: cashout totale (RISK_STOP) poi settlement
def test_F026_10_cashout_then_settlement_pnl_matches_oracle():
    r = run(five_open(prices=(2.0, 2.0, 2.0)) + [{"op": "risk_stop"},
                                                {"op": "settle", "m": "1.701", "win": True},
                                                {"op": "settle", "m": "1.702", "win": False},
                                                {"op": "settle", "m": "1.703", "win": True}])
    outcomes = {"1.701": True, "1.702": False, "1.703": True}
    s = r["snaps"][-1]
    # Oracolo green-up: copertura LAY a 2.02, stake di copertura arrotondato al centesimo (incremento Betfair).
    back, lay, stake = D("2.00"), D("2.02"), D("3")
    hedge = (stake * back / lay).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    exp = D(0)
    for m, win in outcomes.items():
        net = (stake * (back - 1) - hedge * (lay - 1)) if win else (hedge - stake)
        exp += net * (1 - COMM) if net > 0 else net
    assert sum(1 for e in r["events"] if e["e"] == "CASHOUT_SUCCESS") == 3, describe(s)
    assert s["auto_resets"] == 0, describe(s)
    assert q(s["realized"]) == q(exp), f"realizzato cashout {s['realized']} != oracolo {q(exp)}: " + describe(s)
    assert table_markets(s) == set(), "tutti i mercati regolati: tavoli da liberare (F2): " + describe(s)
    for snap in r["snaps"]:
        if ":settle:" in snap["tag"]:
            account_ok(snap)


# ---------------------------------------------------------------- 15: cashout PARZIALE (una posizione su tre), poi settlement
def test_F026_12_partial_cashout_keeps_other_live_positions():
    r = run(five_open(prices=(2.0, 2.0, 2.0)) + [{"op": "cashout", "m": "1.701"},
                                                {"op": "settle", "m": "1.701", "win": True}])
    s = r["snaps"][-1]
    back, lay, stake = D("2.00"), D("2.02"), D("3")
    hedge = (stake * back / lay).quantize(D("0.01"), rounding=ROUND_HALF_UP)
    net = stake * (back - 1) - hedge * (lay - 1)                       # 1.701 vince: copertura LAY persa
    exp = net * (1 - COMM) if net > 0 else net
    assert sum(1 for e in r["events"] if e["e"] == "CASHOUT_SUCCESS") == 1, describe(s)
    assert s["auto_resets"] == 0, describe(s)
    assert table_markets(s) == {"1.702", "1.703"}, "cashout parziale: tavoli != posizioni vive " + describe(s)
    assert q(s["realized"]) == q(exp), f"realizzato {s['realized']} != oracolo cashout {q(exp)}: " + describe(s)
    account_ok(s)
    stake_base_unchanged(r)


# ---------------------------------------------------------------- 12, 13: crash durante il reset, riavvio, riconciliazione
def _crash_and_restart():
    home = tempfile.mkdtemp(prefix="f026crash_")
    first = run(five_open() + [{"op": "crash_after_reset"}, {"op": "settle", "m": "1.701", "win": False},
                               {"op": "settle", "m": "1.702", "win": False}], home=home)
    assert first.get("crashed_at") == "ROSERPINA_AUTO_RESET", "precondizione: crash durante il reset"
    second = run(five_open() + [{"op": "wait", "s": 2}], home=home)            # riavvio + riconsegna dei segnali
    return first, second


def test_F026_11a_crash_during_reset_restart_account_is_consistent_and_no_duplicates():
    first, second = _crash_and_restart()
    s0 = second["snaps"][0]
    s = second["snaps"][-1]
    o = Oracle(s, {"1.701": False, "1.702": False})
    account_ok(s0, o)
    assert q(s0["bankroll"]) == q(s0["funds"]["available"]), describe(s0, o)
    submitted = [x for x in s["orders"] if x["status"] == "SUBMITTED"]
    assert len({x["market_id"] for x in submitted}) == len(submitted), "ordini duplicati dopo il riavvio: " + describe(s, o)
    assert not [e for e in second["events"] if e["e"] == "QUICK_BET_ROUTED" and e["market_id"] in {"1.703", "1.704", "1.705"}], \
        "segnale riconsegnato inviato di nuovo al broker: " + describe(s, o)


def test_F026_11b_restart_rebuilds_tables_of_live_positions_F018():
    """F018: al riavvio i tavoli delle posizioni vive sono ricostruiti (DB + riconciliazione exchange)."""
    first, second = _crash_and_restart()
    s = second["snaps"][-1]
    o = Oracle(s, {"1.701": False, "1.702": False})
    assert {"1.703", "1.704", "1.705"} <= table_markets(s), \
        "dopo il riavvio i tavoli delle posizioni vive non sono ricostruiti (F018): " + describe(s, o)


def test_F026_11c_restart_with_unreadable_exchange_stays_paused_and_places_nothing():
    """F018 fail-closed: posizioni aperte da riconciliare ed exchange illeggibile all'avvio => PAUSED, nessun ingresso
    (ATOMIC-RESERVE-01 regola 7). Le posizioni restano trattenute come incerte."""
    home = tempfile.mkdtemp(prefix="f018down_")
    run(five_open(prices=(2.0, 2.0, 2.0)), home=home)
    second = run([{"op": "signal", "m": "1.799", "price": 2.0}], home=home, exchange_down_at_start=True)
    assert second["mode_after_start"] == "PAUSED", second.get("rebuild")
    assert second["rebuild"]["ok"] is False and second["rebuild"]["reason"].startswith("EXCHANGE_ORDERS_UNAVAILABLE")
    s = second["snaps"][-1]
    assert not [o for o in s["orders"] if o["market_id"] == "1.799"], "nuovo ingresso con riconciliazione incompleta"
    assert {"1.701", "1.702", "1.703"} <= table_markets(s), "posizioni non trattenute: " + describe(s)
