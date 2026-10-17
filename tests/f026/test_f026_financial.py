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


# ================================================================ PF-ALL-F027 (decisione owner, mandato CLOSURE-03)
# LOCKDOWN, PAUSE ed emergency stop bloccano i NUOVI ingressi, non la riconciliazione finanziaria delle posizioni
# esistenti: settlement confermati contabilizzati (vincite, perdite, commissione), conto e tavoli aggiornati, modo
# conservato, nessun nuovo ordine prodotto dalla riconciliazione, nessuna doppia contabilizzazione dopo un riavvio.
def _entries_after(r, tag_prefix):
    """Ordini nel ledger comparsi dopo lo snapshot con tag che inizia per tag_prefix."""
    idx = next(i for i, s in enumerate(r["snaps"]) if s["tag"].split(":", 2)[1:2] == [tag_prefix])
    before = {(x["market_id"], x["status"]) for x in r["snaps"][idx]["orders"]}
    return [x for x in r["snaps"][-1]["orders"] if (x["market_id"], x["status"]) not in before
            and x["status"] not in ("DUPLICATE_BLOCKED", "DENIED")]


@pytest.mark.parametrize("win", [True, False])
def test_F027_01_settlement_during_lockdown_is_booked_and_lockdown_stays(win):
    r = run(five_open(prices=(2.0, 2.0, 2.0)) + [{"op": "lockdown"},
                                                {"op": "settle", "m": "1.701", "win": win},
                                                {"op": "signal", "m": "1.790", "price": 2.0}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.701": win})
    assert s["mode"] == "LOCKDOWN", "la riconciliazione ha tolto il LOCKDOWN: " + describe(s, o)
    assert q(s["realized"]) == q(o.realized()), "settlement in LOCKDOWN non contabilizzato (F027): " + describe(s, o)
    assert table_markets(s) == {"1.702", "1.703"}, "tavolo del mercato regolato non liberato: " + describe(s, o)
    account_ok(s, o)
    assert not [x for x in s["orders"] if x["market_id"] == "1.790"], "nuovo ingresso in LOCKDOWN"
    assert not _entries_after(r, "lockdown"), "la riconciliazione ha prodotto nuovi ordini"
    stake_base_unchanged(r)


def test_F027_02_settlement_during_pause_is_booked_and_pause_stays():
    r = run(five_open(prices=(2.0, 2.0)) + [{"op": "pause"}, {"op": "settle", "m": "1.702", "win": False}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.702": False})
    assert s["mode"] == "PAUSED", describe(s, o)
    assert q(s["realized"]) == q(o.realized()), "settlement in PAUSED non contabilizzato (F027): " + describe(s, o)
    assert table_markets(s) == {"1.701"}, describe(s, o)
    account_ok(s, o)


def test_F027_03_settlement_during_emergency_stop_is_booked_and_emergency_stays():
    r = run(five_open(prices=(2.0, 2.0)) + [{"op": "emergency"}, {"op": "settle", "m": "1.701", "win": True},
                                           {"op": "signal", "m": "1.791", "price": 2.0}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.701": True})
    assert s["emergency"] is True and s["mode"] == "LOCKDOWN", describe(s, o)
    assert q(s["realized"]) == q(o.realized()), "settlement in emergency stop non contabilizzato (F027): " + describe(s, o)
    assert "1.701" not in table_markets(s), describe(s, o)
    account_ok(s, o)
    assert not [x for x in s["orders"] if x["market_id"] == "1.791"], "nuovo ingresso dopo emergency stop"


def test_F027_04_simultaneous_settlements_in_lockdown_each_booked_once_with_commission():
    r = run(five_open(prices=(2.0, 2.0, 2.0)) + [{"op": "lockdown"},
                                                {"op": "settle", "m": "1.701", "win": True, "nowait": True},
                                                {"op": "settle", "m": "1.702", "win": False, "nowait": True},
                                                {"op": "settle", "m": "1.703", "win": True},
                                                {"op": "resettle", "m": "1.701"}])
    s = r["snaps"][-1]
    o = Oracle(s, {"1.701": True, "1.702": False, "1.703": True})
    closes = [e for e in r["events"] if e["e"] == "RUNTIME_CLOSE_POSITION"]
    for m in ("1.701", "1.702", "1.703"):
        mine = [e for e in closes if e["market_id"] == m]
        assert mine and q(mine[0]["pnl"]) == q(o.pnl(m)), f"{m}: P&L != oracolo (commissione una volta): " + describe(s, o)
    assert q(s["realized"]) == q(o.realized()), "doppia contabilizzazione o settlement mancante: " + describe(s, o)
    assert table_markets(s) == set(), describe(s, o)
    assert s["mode"] == "LOCKDOWN"
    account_ok(s, o)


def test_F027_05_crash_after_settlement_in_emergency_restart_counts_once_and_stays_blocked():
    home = tempfile.mkdtemp(prefix="f027crash_")
    first = run(five_open(prices=(2.0, 2.0)) + [{"op": "emergency"}, {"op": "crash_after_settlement"},
                                               {"op": "settle", "m": "1.701", "win": False}], home=home)
    assert first.get("crashed_at") == "BANKROLL_SYNC_RESULT", "precondizione: crash subito dopo la contabilizzazione"
    second = run([{"op": "wait", "s": 3}, {"op": "signal", "m": "1.792", "price": 2.0}], home=home)
    s = second["snaps"][-1]
    o = Oracle(s, {"1.701": False})
    assert s["emergency"] is True, "l'emergency stop persistito deve sopravvivere al riavvio: " + describe(s, o)
    rows = [x for x in s["orders"] if x["market_id"] == "1.792"]
    assert not rows or all(x["status"] in ("DENIED", "DUPLICATE_BLOCKED") for x in rows), "ingresso dopo riavvio in emergenza"
    closes = [e for e in first["events"] + second["events"] if e["e"] == "RUNTIME_CLOSE_POSITION" and e["market_id"] == "1.701"]
    assert len(closes) == 1, f"settlement contabilizzato {len(closes)} volte tra crash e riavvio: " + describe(s, o)
    assert q(D(str(s["funds"]["total"]))) == q(o.equity()), "exchange != oracolo: " + describe(s, o)


# ================================================================ PF-ALL-F028 / F031 (decisione owner, CLOSURE-03)
# Un ordine con stato finale CONFERMATO dal broker (annullato, decaduto, chiuso) e nessun abbinato libera tavolo e
# riserva. Un esito incerto non si considera mai annullato. Un parziale abbinato resta una posizione viva (F031).
def _broker_status(s, m):
    return [x["status"] for x in s["orders"] if x["market_id"] == m]


def test_F028_01_unmatched_order_lapsed_at_market_close_frees_table_and_reserve():
    r = run([{"op": "mode", "m": "1.801", "mode": "nomatch"}, {"op": "signal", "m": "1.801", "price": 2.0},
             {"op": "signal", "m": "1.802", "price": 2.0},
             {"op": "settle", "m": "1.801", "win": True}, {"op": "wait", "s": 3}])
    s0 = next(x for x in r["snaps"] if x["tag"].startswith("1:signal"))
    assert "1.801" in table_markets(s0), "precondizione: ordine non abbinato con tavolo riservato " + describe(s0)
    s = r["snaps"][-1]
    o = Oracle(s, {})
    assert table_markets(s) == {"1.802"}, "ordine decaduto senza abbinato: tavolo ancora trattenuto (F028) " + describe(s)
    assert q(sum(D(str(t["exposure"])) for t in s["tables"])) == q(o.capital_at_risk() - D(str(
        next(x for x in s["orders"] if x["market_id"] == "1.801")["stake"]))), describe(s)
    assert q(s["realized"]) == q(D(0)), "un ordine mai abbinato non produce P&L: " + describe(s)
    # nessun settlement: il conto si verifica sull'exchange (oracolo), non sullo snapshot aggiornato ai settlement
    stake2 = D(str(next(x for x in s["orders"] if x["market_id"] == "1.802")["stake"]))
    assert q(s["funds"]["total"]) == START and q(s["funds"]["exposure"]) == q(stake2), describe(s)


def test_F028_02_unmatched_order_cancelled_by_risk_stop_frees_table_and_places_nothing():
    r = run([{"op": "mode", "m": "1.811", "mode": "nomatch"}, {"op": "signal", "m": "1.811", "price": 2.0},
             {"op": "risk_stop"}, {"op": "wait", "s": 3}, {"op": "signal", "m": "1.812", "price": 2.0}])
    s = r["snaps"][-1]
    assert "1.811" not in table_markets(s), "ordine annullato senza abbinato: tavolo trattenuto (F028) " + describe(s)
    assert not [x for x in s["orders"] if x["market_id"] == "1.812"], "ingresso dopo RISK_STOP"
    account_ok(s)


def test_F028_03_partial_match_then_cancel_keeps_live_position_until_settlement():
    r = run([{"op": "mode", "m": "1.821", "mode": "partial"}, {"op": "signal", "m": "1.821", "price": 2.0},
             {"op": "risk_stop"}, {"op": "wait", "s": 3}])
    s = r["snaps"][-1]
    assert "1.821" in table_markets(s), "parziale abbinato liberato dopo il cancel del residuo: " + describe(s)


def test_F028_04_ambiguous_unmatched_order_is_never_released_as_cancelled():
    r = run([{"op": "mode", "m": "1.831", "mode": "nomatch"}, {"op": "mode", "m": "1.831", "mode": "lost"},
             {"op": "signal", "m": "1.831", "price": 2.0}, {"op": "settle", "m": "1.831", "win": True},
             {"op": "wait", "s": 3}])
    s = r["snaps"][-1]
    assert "AMBIGUOUS" in _broker_status(s, "1.831"), "precondizione: esito incerto " + describe(s)
    assert "1.831" in table_markets(s), "esito incerto liberato come annullato: " + describe(s)


def test_F028_05_crash_between_cancel_and_release_restart_frees_and_never_replays():
    home = tempfile.mkdtemp(prefix="f028crash_")
    first = run([{"op": "mode", "m": "1.841", "mode": "nomatch"}, {"op": "signal", "m": "1.841", "price": 2.0},
                 {"op": "risk_stop"}, {"op": "crash_now"}], home=home)
    assert first.get("crashed_at") == "NOW"
    second = run([{"op": "wait", "s": 3}, {"op": "signal", "m": "1.841", "price": 2.0}], home=home)
    s = second["snaps"][-1]
    assert "1.841" not in table_markets(s), "dopo il riavvio il tavolo dell'ordine annullato e' ancora occupato: " + describe(s)
    sent = [x for x in s["orders"] if x["market_id"] == "1.841" and x["status"] in ("SUBMITTED", "AMBIGUOUS", "INFLIGHT")]
    assert len(sent) <= 1, "segnale riconsegnato rigiocato dopo il riavvio: " + describe(s)
    account_ok(s)


def test_F031_partial_then_cancel_restart_rebuilds_live_position():
    home = tempfile.mkdtemp(prefix="f031_")
    run([{"op": "mode", "m": "1.851", "mode": "partial"}, {"op": "signal", "m": "1.851", "price": 2.0},
         {"op": "risk_stop"}, {"op": "crash_now"}], home=home)
    second = run([{"op": "wait", "s": 1}], home=home)
    s = second["snaps"][-1]
    assert "1.851" in table_markets(s), "parziale abbinato vivo non ricostruito al riavvio (F031): " + describe(s)


# ================================================================ PF-ALL-F030 (decisione owner, CLOSURE-03)
# Il Liquidity Guard protegge anche gli ingressi singoli BACK/LAY: liquidita' eseguibile al prezzo dell'ordine,
# lato corretto del book; avviso/blocco come da config; dato assente => negato anche in solo-avviso.
LIQ_BLOCK = {"roserpina.liquidity_guard_enabled": True, "roserpina.liquidity_warning_only": False,
             "roserpina.liquidity_multiplier": 3.0, "roserpina.min_liquidity_absolute": 50.0}


def _row(s, m):
    rows = [x for x in s["orders"] if x["market_id"] == m]
    assert rows, f"nessuna riga per {m}: " + describe(s)
    return rows[-1]


def test_F030_01_single_back_without_executable_liquidity_is_blocked():
    r = run([{"op": "mode", "m": "1.901", "mode": "nomatch"}, {"op": "signal", "m": "1.901", "price": 2.0}],
            settings=LIQ_BLOCK)
    row = _row(r["snaps"][-1], "1.901")
    assert row["status"] == "DENIED" and row["reason"] == "RISK_LIQUIDITY_INSUFFICIENT", row


def test_F030_02_warning_only_lets_thin_market_through():
    r = run([{"op": "mode", "m": "1.902", "mode": "nomatch"}, {"op": "signal", "m": "1.902", "price": 2.0}],
            settings=dict(LIQ_BLOCK, **{"roserpina.liquidity_warning_only": True}))
    assert _row(r["snaps"][-1], "1.902")["status"] == "SUBMITTED"


def test_F030_03_single_lay_uses_lay_side_of_the_book():
    # LAY a 2.0: availableToLay del book SIM e' a 2.02, non eseguibile a 2.0 => liquidita' eseguibile 0
    r = run([{"op": "signal", "m": "1.903", "price": 2.0, "side": "LAY"}], settings=LIQ_BLOCK)
    row = _row(r["snaps"][-1], "1.903")
    assert row["status"] == "DENIED" and row["reason"] == "RISK_LIQUIDITY_INSUFFICIENT", row


def test_F030_04_sufficient_liquidity_passes_and_floor_blocks():
    ok = run([{"op": "signal", "m": "1.904", "price": 2.0}], settings=LIQ_BLOCK)
    assert _row(ok["snaps"][-1], "1.904")["status"] == "SUBMITTED"
    floor = run([{"op": "signal", "m": "1.905", "price": 2.0}],
                settings=dict(LIQ_BLOCK, **{"roserpina.min_liquidity_absolute": 500.0}))
    assert _row(floor["snaps"][-1], "1.905")["reason"] == "RISK_LIQUIDITY_INSUFFICIENT"


def test_F030_05_missing_book_is_denied_even_in_warning_only():
    r = run([{"op": "book_down"}, {"op": "signal", "m": "1.906", "price": 2.0}],
            settings=dict(LIQ_BLOCK, **{"roserpina.liquidity_warning_only": True}))
    row = _row(r["snaps"][-1], "1.906")
    assert row["status"] == "DENIED" and row["reason"] == "RISK_LIQUIDITY_UNKNOWN", row


def test_F030_06_guard_disabled_does_not_check():
    r = run([{"op": "mode", "m": "1.907", "mode": "nomatch"}, {"op": "signal", "m": "1.907", "price": 2.0}],
            settings=dict(LIQ_BLOCK, **{"roserpina.liquidity_guard_enabled": False}))
    assert _row(r["snaps"][-1], "1.907")["status"] == "SUBMITTED"


# ================================================================ famiglia A: crash prima del settlement
def test_F027_06_crash_with_open_positions_market_settles_while_down_booked_once_after_restart():
    home = tempfile.mkdtemp(prefix="f027down_")
    run(five_open(prices=(2.0, 2.0)) + [{"op": "crash_now"}], home=home)
    second = run([{"op": "settle", "m": "1.701", "win": True}, {"op": "wait", "s": 2}], home=home)
    s = second["snaps"][-1]
    o = Oracle(s, {"1.701": True})
    closes = [e for e in second["events"] if e["e"] == "RUNTIME_CLOSE_POSITION" and e["market_id"] == "1.701"]
    assert len(closes) == 1 and q(closes[0]["pnl"]) == q(o.pnl("1.701")), describe(s, o)
    assert table_markets(s) == {"1.702"}, "dopo il riavvio: tavolo regolato liberato, posizione viva ricostruita " + describe(s, o)
    account_ok(s, o)


# ================================================================ famiglia D: due mercati dello stesso evento (F033)
@pytest.mark.xfail(strict=True, reason="F033: l'esposizione per evento e' per event_key (mercato+selezione+lato); due "
                                       "mercati dello stesso incontro non si sommano nel cap evento. L'aggregazione per "
                                       "identita' canonica e' nella PR #505 (protetta): BLOCKED, nessuna copia anticipata.")
def test_F033_event_cap_aggregates_markets_of_the_same_event():
    ev = "Juventus v Inter"
    r = run([{"op": "signal", "m": "1.951", "price": 2.0, "event": ev},
             {"op": "signal", "m": "1.952", "price": 2.0, "event": ev}],
            settings={"roserpina.max_event_exposure_pct": 4.0})
    s = r["snaps"][-1]
    placed = [x for x in s["orders"] if x["status"] == "SUBMITTED"]
    assert len(placed) == 1, "cap evento superato con due mercati dello stesso incontro: " + describe(s)


# ================================================================ F032 / RSR-12 (decisioni owner CONTINUOUS-DELIVERY-04)
EXP = {"roserpina.expansion_profit_pct": 2.0, "roserpina.expansion_multiplier": 1.5}


def _stake(s, m):
    return D(str(next(x for x in s["orders"] if x["market_id"] == m and x["status"] == "SUBMITTED")["stake"]))


def test_F032_01_expansion_after_net_profit_and_after_restart():
    home = tempfile.mkdtemp(prefix="f032_")
    first = run([{"op": "signal", "m": "1.961", "price": 2.0}, {"op": "settle", "m": "1.961", "win": True},
                 {"op": "signal", "m": "1.962", "price": 2.0}, {"op": "crash_now"}], home=home, settings=EXP)
    s = first["snaps"][-1]
    o = Oracle(s, {"1.961": True})
    cash = START + o.realized()                                   # netto di commissione: 100 + 3*1*(1-0.045)
    growth = (cash - START) / START * 100
    assert growth >= 2, "precondizione: crescita netta oltre soglia"
    expected = cash * D("0.03") / (D("2.0") - 1) * D("1.5")       # target 3%, BALANCED, EXPANSION 1.5
    assert q(_stake(s, "1.962")) == q(expected), f"stake {_stake(s, '1.962')} != atteso in espansione {q(expected)}"
    second = run([{"op": "signal", "m": "1.963", "price": 2.0}], home=home)
    s2 = second["snaps"][-1]
    assert _stake(s2, "1.963") > cash * D("0.03"), "dopo il riavvio la baseline del ciclo e' persa (F032)"


def test_F032_02_below_threshold_no_expansion():
    r = run([{"op": "signal", "m": "1.971", "price": 2.0}, {"op": "settle", "m": "1.971", "win": True},
             {"op": "signal", "m": "1.972", "price": 2.0}], settings=dict(EXP, **{"roserpina.expansion_profit_pct": 5.0}))
    s = r["snaps"][-1]
    o = Oracle(s, {"1.971": True})
    cash = START + o.realized()
    assert q(_stake(s, "1.972")) == q(cash * D("0.03")), "espansione sotto soglia"


def test_F032_03_losses_never_expand():
    r = run([{"op": "signal", "m": "1.981", "price": 2.0}, {"op": "settle", "m": "1.981", "win": False},
             {"op": "signal", "m": "1.982", "price": 2.0}], settings=dict(EXP, **{"roserpina.expansion_profit_pct": 0.0}))
    s = r["snaps"][-1]
    rows = [x for x in s["orders"] if x["market_id"] == "1.982" and x["status"] == "SUBMITTED"]
    assert rows and D(str(rows[0]["stake"])) <= D("3"), "espansione dopo una perdita"


def test_RSR12_01_recovery_tables_survive_crash_and_restart():
    home = tempfile.mkdtemp(prefix="rsr12_")
    run([{"op": "signal", "m": "1.991", "price": 2.0}, {"op": "signal", "m": "1.992", "price": 2.0},
         {"op": "settle", "m": "1.991", "win": False}, {"op": "settle", "m": "1.992", "win": False},
         {"op": "crash_now"}], home=home)
    second = run([{"op": "wait", "s": 1}], home=home)
    s = second["snaps"][-1]
    rec = [t for t in s["tables"] if t["status"] == "RECOVERY"]
    assert len(rec) == 2 and all(D(str(t["loss"])) > 0 for t in rec), \
        "dopo il riavvio la recovery attiva e' sparita (RSR-12): " + str(s["tables"])


def test_RSR12_02_idle_recovery_tables_count_on_the_signal_path():
    """RSR-12 sul percorso reale del segnale (mutante MD10): due tavoli in recovery SENZA ordini aperti esauriscono
    max_recovery_tables=2; con nessun tavolo FREE il terzo segnale e' rifiutato, nessun ordine nuovo."""
    r = run([{"op": "signal", "m": "1.995", "price": 2.0}, {"op": "signal", "m": "1.996", "price": 2.0},
             {"op": "settle", "m": "1.995", "win": False}, {"op": "settle", "m": "1.996", "win": False},
             {"op": "signal", "m": "1.997", "price": 2.0}],
            settings={"roserpina.table_count": 2, "roserpina.max_recovery_tables": 2, "roserpina.allow_recovery": True})
    s = r["snaps"][-1]
    assert not [x for x in s["orders"] if x["market_id"] == "1.997"], "ordine oltre il limite dei tavoli di recovery"
    rec = [t for t in s["tables"] if t["status"] == "RECOVERY" and not t["event_key"]]
    assert len(rec) == 2, "due tavoli in recovery inattivi attesi: " + str(s["tables"])
    reasons = [e["reason"] for e in r["events"] if e["e"] == "SIGNAL_REJECTED"]
    assert any(str(x).startswith("max_recovery_tables_reached:2") for x in reasons), reasons
