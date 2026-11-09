"""Campagna 5 (PF-ROSERPINA-FINANCIAL-ORACLE-CLAUDE-D-20261109): Roserpina a 5 tavoli, 100 EUR virtuali, con la
configurazione della campagna Grok PF-ROSERPINA-5T-OFFLINE-20261109. HeadlessApp REALE (SIM), quote SINTETICHE.
Oracolo indipendente in Decimal (`tests/roserpina_oracle/oracle.py`, replicato qui per non dipendere da un altro branch):
stake = (bankroll x target%) / (quota - 1) per difetto al centesimo; rischio BACK = stake, LAY = stake x (quota - 1);
esposizione dell'exchange = somma dei rischi (anche non abbinati); netto di mercato dagli ordini abbinati; saldo finale
= 100 + somma dei netti; nessuna doppia contabilizzazione dopo un crash.
RO-DD-01 end-to-end (xfail strict): dopo un auto-reset la perdita realizzata oltre `max_drawdown_abs` non blocca i nuovi
ingressi.
"""
from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal as D

import pytest

from tests.f026.test_f026_financial import describe, run

pytestmark = pytest.mark.integration
CENT = D("0.01")
GROK = {"roserpina.table_count": 5, "roserpina.target_profit_cycle_pct": 1.0, "roserpina.max_single_bet_pct": 5.0,
        "roserpina.max_total_exposure_pct": 60.0, "roserpina.max_event_exposure_pct": 5.0,
        "roserpina.max_drawdown_abs": 10.0, "roserpina.min_stake": 0.10, "roserpina.allow_recovery": False,
        "roserpina.lockdown_drawdown_pct": 60.0}
TAVOLI = [("1.950", 1.36, "BACK"), ("1.951", 1.44, "LAY"), ("1.952", 1.5, "BACK"), ("1.953", 1.8, "BACK"),
          ("1.954", 2.2, "LAY")]


def q(x):
    return D(str(x)).quantize(CENT, rounding=ROUND_HALF_UP)


def oracle_stake(bankroll, pct, price):
    return (D(str(bankroll)) * D(str(pct)) / 100 / (D(str(price)) - 1)).quantize(CENT, rounding=ROUND_DOWN)


def risk(side, size, price):
    return D(str(size)) if side == "BACK" else D(str(size)) * (D(str(price)) - 1)


def net(orders, market, sel_wins):
    """Netto lordo dagli ordini ABBINATI della selezione 11 (BACK vinto: m(p-1); perso: -m; LAY all'opposto)."""
    g = D(0)
    for o in orders:
        if str(o["market_id"]) != market or not o.get("matched"):
            continue
        m, p = D(str(o["matched"])), D(str(o.get("avg_price") or o["price"]))
        if o["side"] == "BACK":
            g += m * (p - 1) if sel_wins else -m
        else:
            g += -m * (p - 1) if sel_wins else m
    return g - (g * D("0.045") if g > 0 else 0)


def _entries():
    return [{"op": "signal", "m": m, "price": p, "side": s} for m, p, s in TAVOLI]


def _mine(snap, market):
    return [o for o in snap["exch_orders"] if str(o["market_id"]) == market and not str(o["ref"]).startswith("hedge:")]


# ------------------------------------------------------------------ ingressi, esposizione aggregata, riserve
@pytest.fixture(scope="module")
def ingressi():
    return run(_entries(), settings=GROK)


def test_cinque_tavoli_stake_dell_oracolo_back_e_lay(ingressi) -> None:
    last = ingressi["snaps"][-1]
    assert len([t for t in last["tables"] if t["market_id"]]) == 5, describe(last)
    for m, p, s in TAVOLI:
        (o,) = _mine(last, m)
        assert o["side"] == s and D(str(o["size"])) == oracle_stake(100, 1, p), (m, o)


def test_esposizione_aggregata_uguale_ai_rischi_e_alle_riserve(ingressi) -> None:
    last = ingressi["snaps"][-1]
    attesa = sum(risk(o["side"], o["size"], o["price"]) for m, _p, _s in TAVOLI for o in _mine(last, m))
    assert q(last["funds"]["exposure"]) == q(attesa)                          # exchange (anche non abbinati)
    assert q(sum(D(str(t["exposure"] or 0)) for t in last["tables"])) == q(attesa)   # riserve dei tavoli
    assert q(last["funds"]["available"]) == q(D(100) - attesa)
    assert q(last["funds"]["total"]) == D("100.00")


def test_riserva_sull_intero_per_gli_ordini_non_abbinati(ingressi) -> None:
    last = ingressi["snaps"][-1]
    for m, p, s in TAVOLI:
        (o,) = _mine(last, m)
        t = [t for t in last["tables"] if t["market_id"] == m][0]
        assert q(t["exposure"]) == q(risk(s, o["size"], p)), (m, t, o)   # riserva worst-case, abbinato o no


# ------------------------------------------------------------------ parziale
def test_ingresso_parziale_riserva_intera_e_netto_solo_sull_abbinato() -> None:
    r = run([{"op": "mode", "m": "1.960", "mode": "partial"}, {"op": "signal", "m": "1.960", "price": 1.5},
             {"op": "settle", "m": "1.960", "win": True}], settings=GROK)
    before = [s for s in r["snaps"] if s["tag"].startswith("1:signal")][0]
    (o,) = _mine(before, "1.960")
    assert 0 < D(str(o["matched"])) < D(str(o["size"]))
    t = [t for t in before["tables"] if t["market_id"] == "1.960"][0]
    assert q(t["exposure"]) == q(o["size"])
    final = r["snaps"][-1]
    assert q(final["funds"]["total"]) == q(D(100) + net(before["exch_orders"], "1.960", True)), describe(final)
    assert q(final["funds"]["exposure"]) == D("0.00")


# ------------------------------------------------------------------ perdita simultanea, settlement, liberazione del capitale
def test_perdita_simultanea_settlement_e_capitale_liberato() -> None:
    steps = _entries() + [{"op": "settle", "m": m, "win": s == "LAY", "nowait": True} for m, _p, s in TAVOLI[:-1]] + \
        [{"op": "settle", "m": TAVOLI[-1][0], "win": True}]
    r = run(steps, settings=GROK)
    before = [s for s in r["snaps"] if s["tag"].startswith("4:signal")][0]
    attesi = {m: net(before["exch_orders"], m, s == "LAY") for m, _p, s in TAVOLI}
    final = r["snaps"][-1]
    assert q(final["funds"]["total"]) == q(D(100) + sum(attesi.values())), (attesi, describe(final))
    assert q(final["realized"]) == q(sum(attesi.values()))                    # nessuna doppia contabilizzazione
    assert q(final["funds"]["exposure"]) == D("0.00") and q(final["funds"]["available"]) == q(final["funds"]["total"])
    assert all(D(str(t["exposure"] or 0)) == 0 for t in final["tables"]), describe(final)


# ------------------------------------------------------------------ cashout poi settlement
def test_cashout_poi_settlement_netto_coerente() -> None:
    r = run(_entries()[:3] + [{"op": "price", "m": "1.950", "price": 1.2}, {"op": "cashout", "m": "1.950"},
                              {"op": "settle", "m": "1.950", "win": False}], settings=GROK)
    before = [s for s in r["snaps"] if s["tag"].endswith(":settle:1.950")]
    pre = r["snaps"][r["snaps"].index(before[0]) - 1]
    hedges = [o for o in pre["exch_orders"] if str(o["market_id"]) == "1.950" and str(o["ref"]).startswith("hedge:")
              and float(o.get("matched") or 0) > 0]
    assert len(hedges) == 1, describe(pre)
    final = r["snaps"][-1]
    atteso = net(pre["exch_orders"], "1.950", False)
    sett = [x for x in final["exch_settlements"] if str(x["market_id"]) == "1.950"]
    assert len(sett) == 1 and q(sett[0]["net_pnl"]) == q(atteso), (sett, atteso)


# ------------------------------------------------------------------ crash e riavvio
def test_crash_e_riavvio_nessuna_doppia_contabilizzazione() -> None:
    r1 = run(_entries() + [{"op": "crash_now"}], settings=GROK)
    pre = r1["snaps"][-1]
    r2 = run([{"op": "settle", "m": m, "win": True, "nowait": True} for m, _p, _s in TAVOLI[:-1]] +
             [{"op": "settle", "m": TAVOLI[-1][0], "win": True}], home=r1["_home"], settings=GROK)
    attesi = {m: net(pre["exch_orders"], m, True) for m, _p, _s in TAVOLI}
    final = r2["snaps"][-1]
    assert q(final["funds"]["total"]) == q(D(100) + sum(attesi.values())), describe(final)
    sett = {str(x["market_id"]): x for x in final["exch_settlements"]}
    assert set(sett) == {m for m, _p, _s in TAVOLI if any(o.get("matched") for o in _mine(pre, m))}
    assert all(q(sett[m]["net_pnl"]) == q(attesi[m]) for m in sett)
    assert q(final["funds"]["exposure"]) == D("0.00")


# ------------------------------------------------------------------ RO-DD-01 end-to-end
DD = dict(GROK, **{"roserpina.target_profit_cycle_pct": 5.0, "roserpina.max_single_bet_pct": 20.0,
                   "roserpina.max_event_exposure_pct": 20.0})


@pytest.mark.xfail(strict=True, reason="RO-DD-01: dopo l'auto-reset la perdita oltre max_drawdown_abs non blocca")
def test_ro_dd_01_e2e_perdita_realizzata_oltre_il_limite_blocca_anche_dopo_l_auto_reset() -> None:
    r = run([{"op": "signal", "m": m, "price": 2.0} for m in ("1.971", "1.972", "1.973")] +
            [{"op": "settle", "m": m, "win": False, "nowait": True} for m in ("1.971", "1.972")] +
            [{"op": "settle", "m": "1.973", "win": False}, {"op": "signal", "m": "1.974", "price": 2.0}], settings=DD)
    after = [s for s in r["snaps"] if s["tag"].endswith(":settle:1.973")][0]
    perdita = D(100) - D(str(after["funds"]["total"]))
    assert perdita >= 10, describe(after)                                   # precondizione: limite superato
    assert any(e["e"] == "ROSERPINA_AUTO_RESET" for e in r["events"]), "precondizione: auto-reset avvenuto"
    nuovi = [o for o in r["snaps"][-1]["orders"] if o["market_id"] == "1.974"]
    assert nuovi == [], f"ingresso ammesso con perdita realizzata {perdita} >= 10: " + describe(r["snaps"][-1])


# ------------------------------------------------------------------ recovery e capacita' (Grok B-1, B-2)
@pytest.mark.xfail(strict=True, reason="RO-REC-01 (Grok B-1): ordine eseguito sull'exchange mai riconciliato dopo il crash")
def test_ro_rec_01_crash_con_risposta_in_volo_riconciliato_per_customer_ref() -> None:
    """Crash dopo l'invio (l'exchange ha l'ordine, EXECUTION_COMPLETE, stesso customerOrderRef): al riavvio la riga non
    deve restare INFLIGHT/AMBIGUOUS a tempo indeterminato. La riserva prudente resta (sicurezza preservata)."""
    r1 = run([{"op": "crash_on_next_place"}, {"op": "signal", "m": "1.980", "price": 1.5}], settings=GROK)
    ex = [o for o in r1["snaps"][-1]["exch_orders"] if str(o["market_id"]) == "1.980"] if r1["snaps"] else []
    r2 = run([{"op": "wait", "s": 3}], home=r1["_home"], settings=GROK)
    r3 = run([{"op": "wait", "s": 3}], home=r1["_home"], settings=GROK)
    last = r3["snaps"][-1]
    exch = [o for o in last["exch_orders"] if str(o["market_id"]) == "1.980"]
    assert len(exch) == 1 and exch[0]["status"] == "EXECUTION_COMPLETE", describe(last)        # precondizione
    assert any(t["market_id"] == "1.980" for t in last["tables"]), describe(last)              # riserva mantenuta
    stati = [o["status"] for o in last["orders"] if o["market_id"] == "1.980"]
    assert stati and all(s not in ("INFLIGHT", "AMBIGUOUS") for s in stati), (stati, ex, r2["rebuild"])


@pytest.mark.xfail(strict=True, reason="RO-CAP-01 (Grok B-2, contratto owner): tavolo regolato senza esposizione non liberato")
def test_ro_cap_01_con_recovery_off_i_tavoli_in_perdita_tornano_liberi() -> None:
    """Cinque BACK abbinati, tutti persi (perdita totale ~5 EUR, sotto ogni limite): nessun limite e' superato, ma con
    recovery OFF i tavoli restano in RECOVERY e il nuovo segnale non trova un tavolo. Da decidere (owner): un tavolo
    in perdita senza recovery deve tornare libero?"""
    mercati = [f"1.99{i}" for i in range(5)]
    r = run([{"op": "signal", "m": m, "price": 2.0} for m in mercati] +
            [{"op": "settle", "m": m, "win": False, "nowait": True} for m in mercati[:-1]] +
            [{"op": "settle", "m": mercati[-1], "win": False}, {"op": "signal", "m": "1.959", "price": 2.0}],
            settings=GROK)
    final = r["snaps"][-1]
    assert D(100) - D(str(final["funds"]["total"])) < 10, describe(final)          # precondizione: sotto i limiti
    nuovo = [o for o in final["orders"] if o["market_id"] == "1.959"]
    assert nuovo, "nuovo segnale respinto per tavoli bloccati: " + describe(final)
    # contratto owner (recovery OFF): nessuna puntata di recupero, stake = target sulla cassa, senza inseguimento
    assert D(str(nuovo[0]["stake"])) == oracle_stake(final["funds"]["total"], 1, 2.0), nuovo


# ------------------------------------------------------------------ contratto owner PF-ROSERPINA-DRAWDOWN-AND-RECOVERY-20261109
DD12 = dict(DD, **{"roserpina.target_profit_cycle_pct": 6.0})       # 2 perdite da 6 EUR: 12 EUR (< auto-reset 15 %)


def _perdita_12():
    return [{"op": "signal", "m": "1.981", "price": 2.0}, {"op": "signal", "m": "1.982", "price": 2.0},
            {"op": "settle", "m": "1.981", "win": False, "nowait": True}, {"op": "settle", "m": "1.982", "win": False}]


@pytest.mark.xfail(strict=True, reason="RO-DD-07 (contratto): blocco al settlement, non al segnale successivo")
def test_ro_dd_07_e2e_blocco_alla_variazione_finanziaria() -> None:
    r = run(_perdita_12(), settings=DD12)
    after = [s for s in r["snaps"] if s["tag"].endswith(":settle:1.982")][0]
    assert D(100) - D(str(after["funds"]["total"])) >= 10, describe(after)            # precondizione
    assert after["mode"] == "LOCKDOWN", describe(after)


@pytest.mark.xfail(strict=True, reason="RO-DD-06 (contratto regola 1): picco e blocco persi al riavvio")
def test_ro_dd_06_e2e_blocco_persistente_dopo_crash_e_riavvio() -> None:
    r1 = run(_perdita_12() + [{"op": "signal", "m": "1.983", "price": 2.0}, {"op": "crash_now"}], settings=DD12)
    assert r1["snaps"][-1]["mode"] == "LOCKDOWN", describe(r1["snaps"][-1])          # precondizione: bloccato
    r2 = run([{"op": "signal", "m": "1.984", "price": 2.0}], home=r1["_home"], settings=DD12)
    assert [o for o in r2["snaps"][-1]["orders"] if o["market_id"] == "1.984"] == [], (
        "dopo il riavvio picco ribasato e ingresso ammesso: " + describe(r2["snaps"][-1]))
