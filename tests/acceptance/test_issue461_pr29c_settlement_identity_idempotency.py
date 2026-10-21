"""#461 PR29-c — un settlement produce un solo effetto economico, sulla sola riserva che identifica.

Mandato owner PF-PR29-C-FINANCIAL-IDEMPOTENCY-01; contratto PF-C29-01 .11 (rilascio esattamente una volta), .13
(rilascio legato all'identita'), .18 (settlement -> tavolo per identita' durevole). Phase 0 su main 409bcf9
(#461 c.6320051493): con 4 consegne concorrenti dello stesso settlement il debito di recovery saliva da 10 a 40
(S7); con due bet sullo stesso mercato nessun tavolo si liberava (S2); il settlement della propria bet non
chiudeva l'incertezza AMBIGUOUS (S3); il ramo con ``table_id`` liberava la riserva di un altro evento e
raddoppiava la perdita (S5/S6); dopo il riavvio la chiave appuntata da F018 restava per sempre (S9).

Componenti reali: RuntimeController, TradingEngine, SimulationBroker, Database (SQLite), TableManager,
DuplicationGuard. Confine esterno simulato e dichiarato: ``betfair_service.list_cleared_orders`` (l'exchange che
riporta le bet regolate del mercato) e ``get_account_funds``.
"""
from __future__ import annotations

import json
import threading

from core.duplication_guard import DuplicationGuard
from core.runtime_controller import RuntimeController
from core.table_manager import TableManager
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain, _signal

MERCATO = "1.234"
CHIAVE_A = "1.234:11:BACK:default"
CHIAVE_B = "1.234:12:BACK:default"


def _catena(tmp_path, db_name="pr29c.db"):
    c = _chain(tmp_path, stake=10.0, anti_duplication=True, db_name=db_name)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0}
    c.regolate = []                                   # bet regolate riportate dall'exchange (confine esterno)
    c.rc.betfair_service.list_cleared_orders = lambda bet_status=None, market_ids=None: list(c.regolate)
    return c


def _piazza(c, **sig):
    prima = len(c.bus.payloads("CMD_QUICK_BET"))
    c.rc._on_signal_received(_signal(**sig))
    nuovi = c.bus.payloads("CMD_QUICK_BET")[prima:]
    assert len(nuovi) == 1, c.bus.payloads("SIGNAL_REJECTED")
    cmd = nuovi[0]
    bet = next(str(o.bet_id) for o in c.broker.state.orders.values() if o.customer_ref == cmd["customer_ref"])
    c.regolate.append({"betId": bet, "marketId": MERCATO})
    return cmd, bet


def _seconda(c):
    return _piazza(c, selection_id=12, selection="Under 2.5", received_at="2026-10-08T10:00:05+00:00")


def _cleared(bet, net):
    gross = net if net <= 0 else round(net / 0.955, 2)
    return {"event_key": f"cleared:{MERCATO}:{bet}", "market_id": MERCATO, "table_id": None, "batch_id": "",
            "pnl": net, "gross_pnl": gross, "commission_amount": round(gross - net, 2), "net_pnl": net,
            "commission_pct": 4.5, "settlement_basis": "market_net_realized",
            "settlement_source": "betfair_cleared_orders", "settlement_kind": "realized_settlement",
            "settlement_ref": bet}


def _tavolo(c, tid):
    return c.rc.table_manager.get_table(int(tid))


def _perdite(c):
    return round(sum(float(t["loss_amount"]) for t in c.rc.table_manager.snapshot()), 2)


def _perdite_persistite(c):
    raw = (c.rc.db.get_settings() or {}).get(RuntimeController._RECOVERY_KEY) or "{}"
    return {int(k): round(float(v), 2) for k, v in json.loads(raw).items()}


def _contatore_rilasci(c):
    rilasci = []
    originale = c.rc.table_manager.release

    def conta(*a, **k):
        rilasci.append(a)
        return originale(*a, **k)

    c.rc.table_manager.release = conta
    return rilasci


def _concorrenti(fn, n):
    barriera = threading.Barrier(n)
    errori = []

    def corri():
        barriera.wait()
        try:
            fn()
        except Exception as exc:                      # pragma: no cover - riportato sotto
            errori.append(exc)

    fili = [threading.Thread(target=corri) for _ in range(n)]
    for f in fili:
        f.start()
    for f in fili:
        f.join()
    assert errori == []


# ---------------------------------------------------------------- S7 / T01 / T02: consegne concorrenti
def _settlement_concorrente(tmp_path, n):
    c = _catena(tmp_path)
    cmd, bet = _piazza(c)
    rilasci = _contatore_rilasci(c)
    realizzato_prima = float(c.rc.risk_desk.realized_pnl)
    _concorrenti(lambda: c.rc._on_close_position(_cleared(bet, -10.0)), n)
    assert len(rilasci) == 1, f"{len(rilasci)} rilasci per un solo settlement"
    assert _perdite(c) == 10.0, "debito di recovery contato piu' volte"
    assert _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0
    assert round(float(c.rc.risk_desk.realized_pnl) - realizzato_prima, 2) == -10.0
    assert _tavolo(c, cmd["table_id"]).current_event_key == ""
    # un replay successivo non cambia nulla
    c.rc._on_close_position(_cleared(bet, -10.0))
    assert len(rilasci) == 1 and _perdite(c) == 10.0
    assert round(float(c.rc.risk_desk.realized_pnl) - realizzato_prima, 2) == -10.0
    return c


def test_s7_t01_quattro_settlement_concorrenti_un_solo_effetto(tmp_path):
    _settlement_concorrente(tmp_path, 4)


def test_t02_venti_settlement_concorrenti_un_solo_effetto(tmp_path):
    _settlement_concorrente(tmp_path, 20)


# ---------------------------------------------------------------- S2 / T09: due bet sullo stesso mercato
def test_s2_t09_due_bet_sullo_stesso_mercato_chiudono_solo_la_propria_riserva(tmp_path):
    c = _catena(tmp_path)
    a, bet_a = _piazza(c)
    b, bet_b = _seconda(c)
    assert a["table_id"] != b["table_id"]
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    assert _tavolo(c, a["table_id"]).current_event_key == "", "riserva di A non liberata dal suo settlement"
    assert _tavolo(c, a["table_id"]).loss_amount == 10.0
    tb = _tavolo(c, b["table_id"])
    assert tb.current_event_key == CHIAVE_B and tb.current_exposure == 10.0 and tb.loss_amount == 0.0
    c.rc._on_close_position(_cleared(bet_b, 9.55))
    assert _tavolo(c, b["table_id"]).current_event_key == ""
    assert _tavolo(c, a["table_id"]).loss_amount == 10.0


# ---------------------------------------------------------------- T10: settlement di un'altra riserva
def test_t10_settlement_di_una_bet_non_tocca_la_riserva_di_un_altra(tmp_path):
    c = _catena(tmp_path)
    a, bet_a = _piazza(c)
    b, _ = _seconda(c)
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    c.rc._on_close_position(_cleared(bet_a, -10.0))         # replay
    tb = _tavolo(c, b["table_id"])
    assert tb.current_event_key == CHIAVE_B and tb.current_exposure == 10.0 and tb.loss_amount == 0.0
    assert c.rc.duplication_guard.is_duplicate(CHIAVE_B), "chiave anti-duplicato di B toccata"


# ---------------------------------------------------------------- S3 / T11: AMBIGUOUS poi esito certo
def test_s3_t11_settlement_della_propria_bet_chiude_l_incertezza_della_sola_riserva(tmp_path):
    c = _catena(tmp_path)
    a, bet_a = _piazza(c)
    b, _ = _seconda(c)
    c.rc._on_quick_bet_ambiguous({**a, "status": "AMBIGUOUS", "outcome": "AMBIGUOUS"})
    c.rc._on_quick_bet_ambiguous({**b, "status": "AMBIGUOUS", "outcome": "AMBIGUOUS"})
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    assert _tavolo(c, a["table_id"]).current_event_key == "", "settlement certo di A non rilascia la sua riserva"
    incerte = c.rc._uncertain_reservations_map()
    assert CHIAVE_A not in incerte, "incertezza di A non chiusa dal suo settlement"
    assert CHIAVE_B in incerte, "l'incertezza di B non va chiusa dal settlement di A"
    assert _tavolo(c, b["table_id"]).current_event_key == CHIAVE_B


# ---------------------------------------------------------------- S5 / S6: ramo con table_id esplicito
def _chiusura_con_tavolo(table_id, event_key, net, **extra):
    p = _cleared("X", net)
    p.update(table_id=table_id, event_key=event_key, settlement_source="core_pnl_engine", **extra)
    p.pop("settlement_ref", None)
    return p


def test_s5_table_id_di_un_altro_evento_non_libera_la_riserva_ne_raddoppia(tmp_path):
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    stale = _chiusura_con_tavolo(a["table_id"], "1.999:5:BACK:default", -10.0)
    c.rc._on_close_position(stale)
    c.rc._on_close_position(stale)
    t = _tavolo(c, a["table_id"])
    assert t.current_event_key == CHIAVE_A and t.current_exposure == 10.0, "liberata la riserva di un altro evento"
    assert t.loss_amount == 0.0, "perdita attribuita senza identita' dimostrata"
    assert c.rc.duplication_guard.is_duplicate(CHIAVE_A)


def test_s5_table_id_con_identita_dimostrata_rilascia_una_volta(tmp_path):
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    chiusura = _chiusura_con_tavolo(a["table_id"], CHIAVE_A, -10.0, customer_ref=a["customer_ref"])
    c.rc._on_close_position(chiusura)
    c.rc._on_close_position(chiusura)
    t = _tavolo(c, a["table_id"])
    assert t.current_event_key == "" and t.loss_amount == 10.0


def test_s5_stesso_evento_ma_altra_riserva_non_e_rilasciato_e_la_perdita_conta_una_volta(tmp_path):
    # La posizione chiusa (identita' propria, altro intento) stava sul tavolo, ora riusato per lo stesso evento da
    # un'altra riserva: la riserva corrente resta, la perdita della posizione chiusa entra una volta sola nel
    # recovery del tavolo (stessa semantica di PR29-b), anche con il replay.
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    chiusura = _chiusura_con_tavolo(a["table_id"], CHIAVE_A, -10.0, customer_ref="sig-un-altro-intento")
    c.rc._on_close_position(chiusura)
    c.rc._on_close_position(chiusura)
    t = _tavolo(c, a["table_id"])
    assert t.current_event_key == CHIAVE_A and t.current_exposure == 10.0, "liberata una riserva non identificata"
    assert t.loss_amount == 10.0 and t.status == "RECOVERY"
    assert c.rc.duplication_guard.is_duplicate(CHIAVE_A)


def test_s6_table_id_concorrente_una_sola_perdita(tmp_path):
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    rilasci = _contatore_rilasci(c)
    chiusura = _chiusura_con_tavolo(a["table_id"], CHIAVE_A, -10.0, customer_ref=a["customer_ref"])
    _concorrenti(lambda: c.rc._on_close_position(dict(chiusura)), 8)
    assert len(rilasci) == 1 and _perdite(c) == 10.0


# ---------------------------------------------------------------- S9 / T14 / T08: riavvio F018
def _riavvio(c):
    """Nuovo stato in memoria sullo stesso DB e sullo stesso exchange: tavoli e chiavi ricostruiti da F018."""
    c.rc.table_manager = TableManager(table_count=len(c.rc.table_manager.snapshot()))
    c.rc.duplication_guard = DuplicationGuard()
    c.rc._uncertain_reservations = {}
    c.rc._uncertain_tables = {}
    c.rc._processed_realized_pnl_keys = set()
    assert c.rc._restore_recovery_tables()
    esito = c.rc._rebuild_open_positions()
    assert esito["ok"], esito
    return esito


def test_s9_t14_dopo_il_riavvio_il_settlement_libera_la_chiave_appuntata_della_propria_riserva(tmp_path):
    c = _catena(tmp_path)
    a, bet_a = _piazza(c)
    b, _ = _seconda(c)
    _riavvio(c)
    guard = c.rc.duplication_guard
    assert guard.is_pinned(CHIAVE_A) and guard.is_pinned(CHIAVE_B)
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    ta = c.rc.table_manager.find_by_event_key(CHIAVE_A)
    assert ta is None, "tavolo ricostruito di A non liberato dal suo settlement"
    assert not guard.is_duplicate(CHIAVE_A), "chiave appuntata di A trattenuta per sempre"
    assert guard.is_pinned(CHIAVE_B), "protezione di B rimossa dal settlement di A"
    assert c.rc.table_manager.find_by_event_key(CHIAVE_B) is not None


def test_t08_replay_dopo_il_riavvio_non_raddoppia(tmp_path):
    c = _catena(tmp_path)
    a, bet_a = _piazza(c)
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    assert _perdite_persistite(c).get(int(a["table_id"])) == 10.0
    _riavvio(c)                                        # l'exchange riporta ancora l'ordine: F018 lo ricostruisce
    rilasci = _contatore_rilasci(c)
    realizzato = float(c.rc.risk_desk.realized_pnl)
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    assert rilasci == [], "settlement gia' contabilizzato rilasciato di nuovo dopo il riavvio"
    assert _perdite(c) == 10.0 and _perdite_persistite(c).get(int(a["table_id"])) == 10.0
    # P&L realizzato gia' applicato e marcato nel registro: il nuovo processo non lo conta di nuovo
    assert float(c.rc.risk_desk.realized_pnl) == realizzato, "P&L realizzato riapplicato dopo il riavvio"


def test_t12_settlement_gia_contabilizzato_non_produce_effetti_economici(tmp_path):
    # Il replay puo' rileggere il saldo (informativo), ma non ripete nessun effetto economico: niente rilascio,
    # niente P&L realizzato o debito di recovery in piu', nessun nuovo ordine.
    c = _catena(tmp_path)
    a, bet_a = _piazza(c)
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    rilasci = _contatore_rilasci(c)
    ordini = len(c.bus.payloads("CMD_QUICK_BET"))
    realizzato = float(c.rc.risk_desk.realized_pnl)
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    assert rilasci == [] and len(c.bus.payloads("CMD_QUICK_BET")) == ordini
    assert float(c.rc.risk_desk.realized_pnl) == realizzato
    assert _perdite(c) == 10.0 and _perdite_persistite(c).get(int(a["table_id"])) == 10.0


# ---------------------------------------------------------------- T03 / T15: due processi sullo stesso DB
def test_t03_due_processi_sullo_stesso_database_un_solo_effetto_durevole(tmp_path):
    c1 = _catena(tmp_path, db_name="condiviso.db")
    a, bet_a = _piazza(c1)
    c2 = _catena(tmp_path, db_name="condiviso.db")      # secondo processo: stesso file SQLite, memoria propria
    c2.regolate.extend(c1.regolate)
    _concorrenti(lambda: None, 1)
    barriera = threading.Barrier(2)

    def consegna(c):
        barriera.wait()
        c.rc._on_close_position(_cleared(bet_a, -10.0))

    fili = [threading.Thread(target=consegna, args=(x,)) for x in (c1, c2)]
    for f in fili:
        f.start()
    for f in fili:
        f.join()
    righe = c1.db._execute("SELECT settlement_key FROM settlement_effects", fetch=True, commit=False)
    assert [r["settlement_key"] for r in righe] == [f"cleared:{MERCATO}:{bet_a}"]
    assert _perdite_persistite(c1).get(int(a["table_id"])) == 10.0, "perdita durevole persa o contata due volte"


# ---------------------------------------------------------------- T04-T07 / T16: crash e fault injection
def _nessun_effetto(c, cmd):
    t = _tavolo(c, cmd["table_id"])
    assert t.current_event_key == CHIAVE_A and t.current_exposure == 10.0 and t.loss_amount == 0.0
    assert c.rc.db.get_settlement_effect(f"cleared:{MERCATO}:{c.bet}") is None
    assert _perdite_persistite(c).get(int(cmd["table_id"]), 0.0) == 0.0


def _con_bet(c):
    cmd, bet = _piazza(c)
    c.bet = bet
    return cmd, bet


def _guasto(*_a, **_k):
    raise RuntimeError("guasto iniettato")


def test_t04_crash_prima_della_registrazione_nessun_effetto_poi_una_volta(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "record_settlement_effect", _guasto)
        c.rc._on_close_position(_cleared(bet, -10.0))
    _nessun_effetto(c, cmd)
    c.rc._on_close_position(_cleared(bet, -10.0))          # nuova consegna: l'effetto si applica una volta
    assert _tavolo(c, cmd["table_id"]).current_event_key == "" and _perdite(c) == 10.0
    assert _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0


def test_t05_t16_guasto_dopo_la_riga_e_prima_del_commit_annulla_tutto(tmp_path, monkeypatch):
    # Registrazione ed effetti durevoli stanno nella stessa transazione: il guasto sulla scrittura dello stato
    # tavoli (dopo l'INSERT della riga) annulla anche la riga e l'effetto in memoria.
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "_set_setting", _guasto)
        c.rc._on_close_position(_cleared(bet, -10.0))
    _nessun_effetto(c, cmd)
    c.rc._on_close_position(_cleared(bet, -10.0))
    assert _perdite(c) == 10.0 and _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0


def test_t06_guasto_durante_l_effetto_sul_tavolo_nessun_effetto_parziale(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    tm = c.rc.table_manager

    def rilascio_a_meta(table_id, *, pnl=0.0):
        tavolo = tm._get(int(table_id))
        tavolo.loss_amount += abs(pnl)                     # parte dell'effetto applicata...
        raise RuntimeError("crash durante l'effetto")      # ...e poi il guasto

    with monkeypatch.context() as m:
        m.setattr(tm, "release", rilascio_a_meta)
        c.rc._on_close_position(_cleared(bet, -10.0))
    _nessun_effetto(c, cmd)
    c.rc._on_close_position(_cleared(bet, -10.0))
    assert _perdite(c) == 10.0


def test_t07_crash_dopo_il_commit_e_prima_della_conferma_poi_riavvio(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    with monkeypatch.context() as m:
        m.setattr(c.rc, "_sync_bankroll_post_settlement", _guasto)   # crash a valle dell'effetto registrato
        try:
            c.rc._on_close_position(_cleared(bet, -10.0))
        except RuntimeError:
            pass
    riga = c.rc.db.get_settlement_effect(f"cleared:{MERCATO}:{bet}")
    assert riga and riga["outcome"] == "released"
    assert _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0
    _riavvio(c)                                        # memoria persa; F018 ricostruisce il tavolo dall'exchange
    realizzato = float(c.rc.risk_desk.realized_pnl)
    _concorrenti(lambda: c.rc._on_close_position(_cleared(bet, -10.0)), 4)
    assert _perdite(c) == 10.0 and _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0, "perdita raddoppiata"
    # P&L realizzato: mai marcato prima del crash => riapplicato UNA volta nella memoria del nuovo processo
    assert round(float(c.rc.risk_desk.realized_pnl) - realizzato, 2) == -10.0


def test_t16_registro_illeggibile_nessun_effetto_sui_tavoli(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    realizzato = float(c.rc.risk_desk.realized_pnl)
    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "get_settlement_effect", _guasto)
        c.rc._on_close_position(_cleared(bet, -10.0))
    _nessun_effetto(c, cmd)
    # il P&L realizzato si applica comunque (sottostimare la perdita giornaliera non e' sicuro), una volta
    assert round(float(c.rc.risk_desk.realized_pnl) - realizzato, 2) == -10.0
    c.rc._on_close_position(_cleared(bet, -10.0))
    assert round(float(c.rc.risk_desk.realized_pnl) - realizzato, 2) == -10.0


def test_t16_marcatura_del_realizzato_fallita_non_blocca_ne_duplica(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "mark_settlement_realized", _guasto)
        c.rc._on_close_position(_cleared(bet, -10.0))
    assert _tavolo(c, cmd["table_id"]).current_event_key == "" and _perdite(c) == 10.0
    c.rc._on_close_position(_cleared(bet, -10.0))
    assert _perdite(c) == 10.0


# ---------------------------------------------------------------- S7 dopo il riavvio e fra processi
def test_s7_dopo_il_riavvio_e_fra_processi_indipendenti_un_solo_effetto(tmp_path):
    c1 = _catena(tmp_path, db_name="condiviso.db")
    a, bet_a = _piazza(c1)
    c2 = _catena(tmp_path, db_name="condiviso.db")
    c2.regolate.extend(c1.regolate)
    barriera = threading.Barrier(8)

    def consegna(c):
        barriera.wait()
        c.rc._on_close_position(_cleared(bet_a, -10.0))

    fili = [threading.Thread(target=consegna, args=(x,)) for x in (c1, c2) for _ in range(4)]
    for f in fili:
        f.start()
    for f in fili:
        f.join()
    assert _perdite_persistite(c1).get(int(a["table_id"])) == 10.0
    _riavvio(c1)
    _concorrenti(lambda: c1.rc._on_close_position(_cleared(bet_a, -10.0)), 4)
    assert _perdite(c1) == 10.0 and _perdite_persistite(c1).get(int(a["table_id"])) == 10.0


# ---------------------------------------------------------------- T15: transazioni SQLite concorrenti
def test_t15_registrazione_concorrente_su_sqlite_una_sola_riga(tmp_path):
    from database import Database

    percorso = str(tmp_path / "t15.db")
    Database(percorso)
    esiti = []
    barriera = threading.Barrier(20)

    def registra(i):
        db = Database(percorso)                        # connessione propria, come un processo distinto
        barriera.wait()
        esiti.append(db.record_settlement_effect("cleared:1.1:B1", {"outcome": "released", "table_id": 1,
                                                                    "pnl": -10.0, "instance_id": str(i)},
                                                 {"t15.contatore": str(i)}))

    fili = [threading.Thread(target=registra, args=(i,)) for i in range(20)]
    for f in fili:
        f.start()
    for f in fili:
        f.join()
    assert esiti.count(True) == 1 and esiti.count(False) == 19
    db = Database(percorso)
    vincitore = db.get_settlement_effect("cleared:1.1:B1")["instance_id"]
    assert db.get_settings()["t15.contatore"] == vincitore, "impostazioni scritte da chi non ha registrato"


# ---------------------------------------------------------------- T13: ordini abbinati in parte
def test_t13_bet_abbinata_in_parte_rilasciata_una_volta_dal_suo_settlement(tmp_path):
    c = _catena(tmp_path)
    cmd, bet = _piazza(c)
    ordine = next(o for o in c.broker.state.orders.values() if o.customer_ref == cmd["customer_ref"])
    ordine.matched_size = 5.0                          # abbinata per meta', il resto annullato prima del settlement
    c.rc._on_close_position(_cleared(bet, -5.0))
    c.rc._on_close_position(_cleared(bet, -5.0))
    assert _tavolo(c, cmd["table_id"]).current_event_key == "" and _perdite(c) == 5.0


def test_t13_bet_senza_settlement_resta_trattenuta(tmp_path):
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    _, bet_b = _seconda(c)
    c.rc._on_close_position(_cleared(bet_b, 9.55))
    assert _tavolo(c, a["table_id"]).current_event_key == CHIAVE_A, "nessun esito per A: la riserva resta"


# ---------------------------------------------------------------- INV-07 tracciabilita' e incertezza per riserva
def test_inv07_ogni_effetto_e_riconducibile_a_identificativi_durevoli(tmp_path):
    c = _catena(tmp_path)
    cmd, bet = _piazza(c)
    meta = _tavolo(c, cmd["table_id"]).meta
    assert meta["reservation_id"] and cmd["reservation_id"] == meta["reservation_id"]
    c.rc._on_close_position(_cleared(bet, -10.0))
    riga = c.rc.db.get_settlement_effect(f"cleared:{MERCATO}:{bet}")
    assert (riga["outcome"], riga["table_id"], riga["customer_ref"], riga["reservation_id"], riga["bet_id"]) == (
        "released", int(cmd["table_id"]), cmd["customer_ref"], meta["reservation_id"], bet)
    assert riga["realized_applied"] == 1 and riga["pnl"] == -10.0


def test_esito_certo_di_un_altro_intento_non_chiude_l_incertezza_della_riserva(tmp_path):
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    c.rc._on_quick_bet_ambiguous({**a, "status": "AMBIGUOUS", "outcome": "AMBIGUOUS"})
    c.rc._release_if_terminal({**a, "customer_ref": "sig-altro-intento"}, event_name="QUICK_BET_FAILED")
    assert CHIAVE_A in c.rc._uncertain_reservations_map(), "incertezza chiusa dall'esito di un altro intento"
    assert _tavolo(c, a["table_id"]).current_event_key == CHIAVE_A


def test_settlement_non_identificabile_non_libera_nulla(tmp_path):
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    c.rc._on_close_position(_cleared("BET-SCONOSCIUTA", -10.0))
    t = _tavolo(c, a["table_id"])
    assert t.current_event_key == CHIAVE_A and t.current_exposure == 10.0 and t.loss_amount == 0.0
    assert c.rc.db.get_settlement_effect(f"cleared:{MERCATO}:BET-SCONOSCIUTA")["outcome"] == "held"


def test_bet_legata_a_due_righe_orders_resta_trattenuta(tmp_path):
    # Identita' bet -> intento non univoca (due righe `orders` con la stessa bet): nessuna attribuzione alla prima.
    c = _catena(tmp_path)
    a, bet_a = _piazza(c)
    c.rc.db._execute(
        "INSERT INTO orders(customer_ref, correlation_id, status, payload_json, response_json, created_at, updated_at) "
        "SELECT 'sig-copia', correlation_id, status, payload_json, response_json, created_at, updated_at "
        "FROM orders WHERE customer_ref = ?", (a["customer_ref"],), commit=True)
    assert len(c.rc.db.find_orders_by_bet_id(bet_a)) == 2
    c.rc._on_close_position(_cleared(bet_a, -10.0))
    t = _tavolo(c, a["table_id"])
    assert t.current_event_key == CHIAVE_A and t.current_exposure == 10.0 and t.loss_amount == 0.0
    assert c.rc.db.get_settlement_effect(f"cleared:{MERCATO}:{bet_a}")["outcome"] == "held"


def test_corsa_persa_contro_un_altro_processo_nessun_effetto_ne_realizzato(tmp_path, monkeypatch):
    # La riga e' stata registrata da un altro consumer fra la lettura e la transazione: questo processo non applica
    # ne' l'effetto sul tavolo ne' il P&L realizzato (li applica chi ha registrato).
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    realizzato = float(c.rc.risk_desk.realized_pnl)
    monkeypatch.setattr(c.rc.db, "record_settlement_effect", lambda *_a, **_k: False)
    c.rc._on_close_position(_cleared(bet, -10.0))
    t = _tavolo(c, cmd["table_id"])
    assert t.current_event_key == CHIAVE_A and t.current_exposure == 10.0 and t.loss_amount == 0.0
    assert float(c.rc.risk_desk.realized_pnl) == realizzato, "realizzato applicato da chi ha perso la corsa"


# ---------------------------------------------------------------- mandato PF-PR29-C FINAL VALIDATION
def test_s7_quattro_e_venti_tutte_le_grandezze_finanziarie(tmp_path):
    """S7 con 4 e 20 consegne concorrenti: P&L, esposizione, fondi, debito di recovery, registro, tavoli, chiavi."""
    for n in (4, 20):
        c = _settlement_concorrente(tmp_path / f"n{n}", n)
        righe = c.rc.db._execute("SELECT settlement_key, outcome, realized_applied FROM settlement_effects",
                                 fetch=True, commit=False)
        assert [(r["settlement_key"].split(":")[0], r["outcome"], r["realized_applied"]) for r in righe] == [
            ("cleared", "released", 1)], f"{n}: registro {righe}"
        assert c.rc.table_manager.total_exposure() == 0.0, f"{n}: esposizione residua"
        assert not c.rc.duplication_guard.is_duplicate(CHIAVE_A), f"{n}: chiave anti-duplicato trattenuta"
        # fondi utilizzabili = conto dell'exchange, mai il P&L in memoria
        assert float(c.rc.risk_desk.bankroll_current) == 1000.0, f"{n}: bankroll {c.rc.risk_desk.bankroll_current}"


def _riavvio_processo(c):
    """Come `_riavvio`, con la memoria del risk desk di un processo nuovo (P&L realizzato da zero)."""
    esito = _riavvio(c)
    c.rc.risk_desk.realized_pnl = 0.0
    return esito


def test_vincita_non_marcata_non_si_riapplica_dopo_il_riavvio(tmp_path, monkeypatch):
    # Crash fra applicazione e marcatura del realizzato: la riga resta realized_applied=0. Una VINCITA riapplicata
    # abbasserebbe la perdita misurata (limite giornaliero/sessione fail-open): non si riapplica.
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "mark_settlement_realized", _guasto)
        c.rc._on_close_position(_cleared(bet, 9.55))
    assert c.rc.db.get_settlement_effect(f"cleared:{MERCATO}:{bet}")["realized_applied"] == 0
    _riavvio_processo(c)
    _concorrenti(lambda: c.rc._on_close_position(_cleared(bet, 9.55)), 4)
    assert float(c.rc.risk_desk.realized_pnl) == 0.0, "vincita contata di nuovo nella memoria del nuovo processo"


def test_perdita_non_marcata_si_riapplica_una_volta_dopo_il_riavvio(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "mark_settlement_realized", _guasto)
        c.rc._on_close_position(_cleared(bet, -10.0))
    _riavvio_processo(c)
    _concorrenti(lambda: c.rc._on_close_position(_cleared(bet, -10.0)), 4)
    assert float(c.rc.risk_desk.realized_pnl) == -10.0, "perdita non contata (o contata piu' volte) nel nuovo processo"
    assert _perdite(c) == 10.0 and _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0


def test_registro_illeggibile_una_vincita_non_si_applica(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "get_settlement_effect", _guasto)
        c.rc._on_close_position(_cleared(bet, 9.55))
    assert float(c.rc.risk_desk.realized_pnl) == 0.0
    c.rc._on_close_position(_cleared(bet, 9.55))           # registro di nuovo leggibile: applicata una volta
    assert round(float(c.rc.risk_desk.realized_pnl), 2) == 9.55


def test_pnl_in_memoria_non_aumenta_il_capitale_ne_autorizza_ordini_scoperti(tmp_path):
    """Punto 6: il P&L realizzato in memoria (anche gonfiato) non entra nel capitale utilizzabile ne' nel gate
    fondi: stake e ammissione leggono il conto dell'exchange."""
    c = _catena(tmp_path)
    for _ in range(2):
        c.rc._apply_realized_pnl_without_mutating_bankroll(1000.0)   # P&L duplicato in memoria
    assert float(c.rc.risk_desk.realized_pnl) == 2000.0
    assert float(c.rc.risk_desk.bankroll_current) != 2000.0 + 1000.0
    c.rc.betfair_service.get_account_funds = lambda: {"available": 5.0, "exposure": 0.0}   # meno dello stake 10
    prima = len(c.bus.payloads("CMD_QUICK_BET"))
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == prima, "ordine non coperto dai fondi dell'exchange ammesso"
    assert c.bus.payloads("SIGNAL_REJECTED"), "nessun rifiuto esplicito"


# ---------------------------------------------------------------- punto 9: held riconciliabile senza perdite
def test_held_per_lettura_fallita_si_riconcilia_alla_consegna_successiva(tmp_path):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    regolate = list(c.regolate)
    c.rc.betfair_service.list_cleared_orders = lambda **_k: (_ for _ in ()).throw(RuntimeError("exchange giu'"))
    c.rc._on_close_position(_cleared(bet, -10.0))
    chiave = f"cleared:{MERCATO}:{bet}"
    assert c.rc.db.get_settlement_effect(chiave)["outcome"] == "held"
    assert _tavolo(c, cmd["table_id"]).current_event_key == CHIAVE_A and _perdite(c) == 0.0
    assert float(c.rc.risk_desk.realized_pnl) == -10.0          # contabilizzato una volta alla registrazione
    c.rc.betfair_service.list_cleared_orders = lambda **_k: list(regolate)
    _concorrenti(lambda: c.rc._on_close_position(_cleared(bet, -10.0)), 4)
    assert c.rc.db.get_settlement_effect(chiave)["outcome"] == "released"
    assert _tavolo(c, cmd["table_id"]).current_event_key == "" and _perdite(c) == 10.0
    assert _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0
    assert float(c.rc.risk_desk.realized_pnl) == -10.0, "P&L realizzato ripetuto dalla riconciliazione"


def test_riconciliazione_esplicita_dei_held_e_idempotente(tmp_path):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    regolate = list(c.regolate)
    c.rc.betfair_service.list_cleared_orders = lambda **_k: (_ for _ in ()).throw(RuntimeError("exchange giu'"))
    c.rc._on_close_position(_cleared(bet, -10.0))
    c.rc.betfair_service.list_cleared_orders = lambda **_k: list(regolate)
    esito = c.rc.reconcile_held_settlements()
    assert esito["ok"] and esito["reconciled"] == [f"cleared:{MERCATO}:{bet}"] and esito["still_held"] == []
    assert _perdite(c) == 10.0
    assert c.rc.reconcile_held_settlements()["reconciled"] == []      # gia' riconciliato: nessun effetto
    assert _perdite(c) == 10.0 and float(c.rc.risk_desk.realized_pnl) == -10.0


def test_held_non_attribuibile_resta_held_alla_riconciliazione(tmp_path):
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    c.rc._on_close_position(_cleared("BET-SCONOSCIUTA", -10.0))
    esito = c.rc.reconcile_held_settlements()
    assert esito["still_held"] == [f"cleared:{MERCATO}:BET-SCONOSCIUTA"] and esito["reconciled"] == []
    riga = c.rc.db.get_settlement_effect(f"cleared:{MERCATO}:BET-SCONOSCIUTA")
    assert riga["outcome"] == "held" and riga["pnl"] == -10.0          # evidenza durevole conservata
    assert json.loads(riga["payload_json"])["settlement_ref"] == "BET-SCONOSCIUTA"
    assert _tavolo(c, a["table_id"]).current_event_key == CHIAVE_A


def test_held_si_riconcilia_al_riavvio_sul_tavolo_della_riserva(tmp_path):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    regolate = list(c.regolate)
    c.rc.betfair_service.list_cleared_orders = lambda **_k: (_ for _ in ()).throw(RuntimeError("exchange giu'"))
    c.rc._on_close_position(_cleared(bet, -10.0))
    c.rc.betfair_service.list_cleared_orders = lambda **_k: list(regolate)
    _riavvio(c)
    assert c.rc.reconcile_held_settlements()["reconciled"] == [f"cleared:{MERCATO}:{bet}"]
    assert _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0 and _perdite(c) == 10.0


def test_avvio_riconcilia_i_held_solo_dopo_una_ricostruzione_riuscita(tmp_path):
    import inspect

    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    regolate = list(c.regolate)
    c.rc.betfair_service.list_cleared_orders = lambda **_k: (_ for _ in ()).throw(RuntimeError("exchange giu'"))
    c.rc._on_close_position(_cleared(bet, -10.0))
    c.rc.betfair_service.list_cleared_orders = lambda **_k: list(regolate)
    assert c.rc._reconcile_held_after_rebuild({"ok": False}) == {"ok": False}   # tavoli non affidabili: nulla
    assert _perdite(c) == 0.0
    esito = c.rc._reconcile_held_after_rebuild({"ok": True})
    assert esito["ok"] and esito["held_settlements"]["reconciled"] == [f"cleared:{MERCATO}:{bet}"]
    assert _perdite(c) == 10.0
    assert "self._reconcile_held_after_rebuild(rebuild)" in inspect.getsource(RuntimeController.start)


def test_esito_senza_identita_non_libera_tavolo_ne_chiave_della_riserva(tmp_path):
    # Grok su e2bb36e: un esito terminale senza customer_ref non dimostra di quale intento sia.
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    senza_ref = {k: v for k, v in a.items() if k != "customer_ref"}
    c.rc._release_if_terminal({**senza_ref, "status": "FAILED"}, event_name="QUICK_BET_FAILED")
    c.rc._release_if_terminal({**a, "customer_ref": "sig-altro-intento"}, event_name="QUICK_BET_FAILED")
    t = _tavolo(c, a["table_id"])
    assert t.current_event_key == CHIAVE_A and t.current_exposure == 10.0
    assert c.rc.duplication_guard.is_duplicate(CHIAVE_A), "chiave della riserva viva rilasciata"
    c.rc._release_if_terminal({**a, "status": "FAILED"}, event_name="QUICK_BET_FAILED")   # esito del suo intento
    assert _tavolo(c, a["table_id"]).current_event_key == ""


# ---------------------------------------------------------------- review Codex/Grok su e2bb36e
def test_r1_avvio_in_pausa_con_held_in_attesa_di_lettura(tmp_path):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    c.rc.betfair_service.list_cleared_orders = lambda **_k: (_ for _ in ()).throw(RuntimeError("exchange giu'"))
    c.rc._on_close_position(_cleared(bet, -10.0))
    esito = c.rc._reconcile_held_after_rebuild({"ok": True, "reason": ""})        # exchange ancora giu'
    assert esito["ok"] is False and esito["reason"] == f"HELD_SETTLEMENTS_PENDING:cleared:{MERCATO}:{bet}"


def test_r1_held_non_attribuibile_non_ferma_l_avvio(tmp_path):
    import inspect

    c = _catena(tmp_path)
    _piazza(c)
    c.rc._on_close_position(_cleared("BET-SCONOSCIUTA", -10.0))
    esito = c.rc._reconcile_held_after_rebuild({"ok": True, "reason": ""})
    assert esito["ok"] is True and esito["held_settlements"]["still_held"] == [f"cleared:{MERCATO}:BET-SCONOSCIUTA"]
    assert esito["held_settlements"]["pending"] == []
    assert "self._reconcile_held_after_rebuild(\n                self._gate_rebuild_on_recovery_state(" in \
        inspect.getsource(RuntimeController.resume)


def test_r2_stesso_tavolo_stesso_evento_altra_riserva_e_un_altra_chiusura(tmp_path):
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    c.rc._on_close_position(_chiusura_con_tavolo(a["table_id"], CHIAVE_A, -10.0, customer_ref=a["customer_ref"],
                                                 reservation_id=a["reservation_id"]))
    assert _tavolo(c, a["table_id"]).current_event_key == ""
    # stesso tavolo riusato per lo stesso evento da una nuova riserva (un altro intento)
    c.rc.table_manager.activate(table_id=int(a["table_id"]), event_key=CHIAVE_A, exposure=10.0, market_id=MERCATO,
                                selection_id=11, meta={"customer_ref": "sig-seconda-riserva"})
    seconda = c.rc.table_manager.get_table(int(a["table_id"])).meta["reservation_id"]
    assert seconda != a["reservation_id"]
    c.rc._on_close_position(_chiusura_con_tavolo(a["table_id"], CHIAVE_A, -10.0, customer_ref="sig-seconda-riserva",
                                                 reservation_id=seconda))
    t = _tavolo(c, a["table_id"])
    assert t.current_event_key == "", "seconda chiusura reale trattata come replay della prima"
    assert t.loss_amount == 20.0
    # Codex su 29903af: la stessa chiave in ogni fase (sync del bankroll compreso)
    assert c.rc._last_bankroll_sync_result["bankroll_sync_status"] != "SYNC_SKIPPED_DUPLICATE"


def test_r3_stato_tavoli_non_scrivibile_nessuna_registrazione_poi_una_volta(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    with monkeypatch.context() as m:
        m.setattr(c.rc, "_prepare_recovery_persist", lambda *a, **k: None)
        c.rc._on_close_position(_cleared(bet, -10.0))
    _nessun_effetto(c, cmd)                       # nessuna riga: la riconsegna non e' un duplicato
    c.rc._on_close_position(_cleared(bet, -10.0))
    assert _tavolo(c, cmd["table_id"]).current_event_key == "" and _perdite(c) == 10.0
    assert _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0


def test_r4_ambiguous_senza_tavolo_chiuso_dal_settlement_della_stessa_riserva(tmp_path):
    c = _catena(tmp_path)
    a, bet = _piazza(c)
    c.rc._on_quick_bet_ambiguous({**a, "table_id": None, "status": "AMBIGUOUS", "outcome": "AMBIGUOUS"})
    assert CHIAVE_A in c.rc._uncertain_reservations_map()
    c.rc._on_close_position(_cleared(bet, -10.0))
    assert CHIAVE_A not in c.rc._uncertain_reservations_map(), "incertezza registrata senza tavolo mai chiusa"
    assert not c.rc.duplication_guard.is_duplicate(CHIAVE_A), "chiave trattenuta dopo il settlement"


def test_force_unlock_if_event_richiede_l_identita_della_riserva():
    tm = TableManager(table_count=2)
    tm.activate(table_id=1, event_key="EV", exposure=5.0, market_id="1.1", selection_id=7, meta={"customer_ref": "R1"})
    assert tm.force_unlock_if_event(1, "EV") is False, "esito senza identita' ha liberato la riserva"
    assert tm.force_unlock_if_event(1, "EV", customer_ref="R2") is False
    assert tm.force_unlock_if_event(1, "EV", customer_ref="R1") is True
    tm.activate(table_id=2, event_key="EV2", exposure=5.0, market_id="1.1", selection_id=7)   # riserva senza intento
    assert tm.force_unlock_if_event(2, "EV2") is True


def test_esito_senza_ref_ne_tavolo_non_chiude_l_incertezza_della_riserva(tmp_path):
    # Grok su 29903af: AMBIGUOUS dell'auto-next (senza tavolo) e poi un esito certo senza tavolo NE' customer_ref.
    c = _catena(tmp_path)
    a, _ = _piazza(c)
    c.rc._on_quick_bet_ambiguous({**a, "table_id": None, "status": "AMBIGUOUS", "outcome": "AMBIGUOUS"})
    anonimo = {k: v for k, v in a.items() if k not in ("customer_ref", "table_id")}
    c.rc._release_if_terminal({**anonimo, "status": "FAILED"}, event_name="QUICK_BET_FAILED")
    c.rc._release_if_terminal({**anonimo, "customer_ref": "sig-altro", "status": "FAILED"},
                              event_name="QUICK_BET_FAILED")
    assert CHIAVE_A in c.rc._uncertain_reservations_map(), "incertezza chiusa da un esito senza identita'"
    assert c.rc.duplication_guard.is_duplicate(CHIAVE_A), "chiave anti-duplicato riaperta: doppia puntata possibile"
    c.rc._release_if_terminal({**a, "table_id": None, "status": "FAILED"}, event_name="QUICK_BET_FAILED")
    assert CHIAVE_A not in c.rc._uncertain_reservations_map()        # esito certo dello stesso intento


def test_incertezza_per_riserva_si_chiude_solo_con_l_esito_dello_stesso_intento():
    rc = RuntimeController.__new__(RuntimeController)
    rc._mark_uncertain("E", 1, customer_ref="R1")
    assert rc._clear_uncertain("E", 1) is True and "E" in rc._uncertain_reservations_map()       # nessun ref
    assert rc._clear_uncertain("E", 1, customer_ref="R2") is True                                # altro intento
    assert rc._clear_uncertain("E", 1, customer_ref="R1") is False and "E" not in rc._uncertain_reservations_map()



def test_r5_la_chiave_del_settlement_e_la_stessa_in_ogni_fase():
    base = {"event_key": "E", "table_id": 1}
    assert RuntimeController._build_bankroll_sync_key(base) == "E|1"
    assert RuntimeController._build_bankroll_sync_key({**base, "customer_ref": "R", "reservation_id": "X"}) == "E|1|R|X"
    assert RuntimeController._build_bankroll_sync_key({"event_key": "cleared:1.1:B", "customer_ref": "R"}) == \
        "cleared:1.1:B", "un settlement per bet non cambia chiave"


def test_r6_tavolo_della_riserva_assente_resta_held_e_si_riconcilia(tmp_path):
    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    tm = c.rc.table_manager
    salvato = tm._tables.pop(int(cmd["table_id"]))           # es. riavvio con meno tavoli
    c.rc._on_close_position(_cleared(bet, -10.0))
    chiave = f"cleared:{MERCATO}:{bet}"
    assert c.rc.db.get_settlement_effect(chiave)["outcome"] == "held", "debito registrato senza un tavolo"
    esito = c.rc._reconcile_held_after_rebuild({"ok": True})
    assert esito["ok"] is False and chiave in esito["reason"]
    tm._tables[int(cmd["table_id"])] = salvato
    assert c.rc.reconcile_held_settlements()["reconciled"] == [chiave]
    assert _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0


def test_r7_auto_next_persiste_tavolo_e_riserva_nella_request(tmp_path):
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _close_payload

    c = _chain(tmp_path, db_name="r7.db")
    c.rc.risk_desk.sync_bankroll(100.0)
    c.rc._on_close_position(_close_payload("batch-r7"))
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    tavolo = c.rc.table_manager.get_table(int(cmd["reservation_table_id"]))
    assert cmd.get("table_id") is None, "semantica degli esiti dell'auto-next cambiata"
    assert cmd["reservation_id"] and tavolo.meta["reservation_id"] == cmd["reservation_id"]
    riga = c.db._execute("SELECT payload_json FROM orders WHERE customer_ref = ?", (cmd["customer_ref"],),
                         fetch=True, commit=False)
    persistita = json.loads(riga[0]["payload_json"])
    assert persistita["reservation_table_id"] == int(cmd["reservation_table_id"])
    assert persistita["reservation_id"] == cmd["reservation_id"]


def test_r8_snapshot_dello_stato_tavoli_non_sovrascrive_un_settlement_registrato(tmp_path):
    import time

    c = _catena(tmp_path)
    cmd, bet = _con_bet(c)
    db = c.rc.db
    originale = db.save_settings
    fermo, entrato = threading.Event(), threading.Event()

    def salva_lento(payload):
        if RuntimeController._RECOVERY_KEY in payload and not entrato.is_set():
            entrato.set()
            fermo.wait(5)                                     # snapshot vecchio preparato, scrittura in ritardo
        return originale(payload)

    db.save_settings = salva_lento
    persistenza = threading.Thread(target=c.rc._persist_recovery_tables)
    persistenza.start()
    assert entrato.wait(5)
    settlement = threading.Thread(target=lambda: c.rc._on_close_position(_cleared(bet, -10.0)))
    settlement.start()
    time.sleep(0.3)
    fermo.set()
    persistenza.join(5)
    settlement.join(5)
    db.save_settings = originale
    assert _perdite_persistite(c).get(int(cmd["table_id"])) == 10.0, "perdita registrata sovrascritta"
