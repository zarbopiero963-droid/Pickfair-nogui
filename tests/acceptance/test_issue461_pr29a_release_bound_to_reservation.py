"""#461 PR29-a — il rilascio di tavolo e chiave segue la riserva, mai un esito che non la chiude (PF-C29-01.03, .13).

Contratto PF-C29-01 (#506 c.6123640661): QUICK_BET_SUCCESS (fine dell'invio lato engine) NON rilascia riserva ne'
rischio di posizione. In produzione l'engine emette SUCCESS solo per DUPLICATE_BLOCKED (una richiesta NON inviata
perche' lo stesso intento e' gia' in volo): rilasciare il tavolo e la chiave dell'originale lasciava l'esposizione
viva fuori da cap e gate dei fondi. Riprodotto sul runtime reale (Phase 0 PR29, sonda s1).
"""
from __future__ import annotations

from tests.acceptance.test_issue437_pr27 import _rifiuti, _tavoli_occupati
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain, _signal


def _segnale_piazzato(tmp_path, **kw):
    c = _chain(tmp_path, stake=10.0, **kw)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0}
    c.rc._on_signal_received(_signal())
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    return c, cmd


def test_success_da_doppione_non_rilascia_tavolo_ne_chiave_dell_originale(tmp_path):
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=True)
    c.rc._on_quick_bet_success({**cmd, "status": "DUPLICATE_BLOCKED", "outcome": "SUCCESS"})
    assert _tavoli_occupati(c) == [cmd["table_id"]]
    assert c.rc.table_manager.get_table(cmd["table_id"]).current_exposure == 10.0
    assert c.rc.duplication_guard._active, "la chiave anti-duplicato dell'originale non va rilasciata"
    assert "placed_at" not in c.rc.table_manager.get_table(cmd["table_id"]).meta   # nulla e' stato inviato


def test_success_di_completamento_marca_a_mercato_e_non_rilascia(tmp_path):
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=True)
    c.rc._on_quick_bet_success({**cmd, "status": "COMPLETED", "outcome": "SUCCESS"})
    tab = c.rc.table_manager.get_table(cmd["table_id"])
    assert _tavoli_occupati(c) == [cmd["table_id"]] and tab.current_exposure == 10.0
    assert "placed_at" in tab.meta
    assert c.rc.duplication_guard._active


def test_stesso_intento_gia_riservato_e_rifiutato_prima_di_un_secondo_tavolo(tmp_path):
    # Anti-duplicazione disattivata: un segnale identico ha lo stesso customer_ref, che l'engine bloccherebbe come
    # doppione. Il runtime lo rifiuta subito: nessun secondo tavolo, nessun secondo CMD_QUICK_BET.
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=False)
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    assert "ingresso_gia_in_volo" in _rifiuti(c), _rifiuti(c)
    assert _tavoli_occupati(c) == [cmd["table_id"]]


def test_failed_certo_rilascia_ancora_la_propria_riserva(tmp_path):
    # Blocco: un esito certo di non-invio continua a liberare tavolo e chiave (PF-C29-01.07).
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=True)
    c.rc._on_quick_bet_failed({**cmd, "status": "FAILED", "outcome": "FAILURE"})
    assert _tavoli_occupati(c) == []
    assert not c.rc.duplication_guard._active


def test_riconsegna_di_un_intento_gia_consegnato_non_riserva_un_tavolo(tmp_path):
    # Review anticipata su PR29-a (P1): il messaggio e' stato inviato e poi chiuso (FAILED certo dopo la consegna
    # al trasporto, o settlement): il tavolo e' libero ma l'intento resta consumato nel DB. Una riconsegna dello
    # stesso messaggio verrebbe bloccata dall'engine come doppione, e il suo SUCCESS non rilascia piu' nulla:
    # il runtime la rifiuta prima di riservare un tavolo.
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=False)
    c.rc.db.consume_order_intent(cmd["customer_ref"], attempt_id="attempt-1")
    c.rc._on_quick_bet_failed({**cmd, "status": "FAILED", "outcome": "FAILURE"})
    assert _tavoli_occupati(c) == []
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    assert "ingresso_gia_eseguito" in _rifiuti(c), _rifiuti(c)
    assert _tavoli_occupati(c) == []


def test_verifica_dell_intento_che_fallisce_blocca_l_ingresso(tmp_path):
    c = _chain(tmp_path, stake=10.0, anti_duplication=False)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0}

    def _boom(_ref):
        raise RuntimeError("db locked")

    c.rc.db.is_order_intent_consumed = _boom
    c.rc._on_signal_received(_signal())
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert "ammissione_non_verificabile" in _rifiuti(c), _rifiuti(c)
    assert _tavoli_occupati(c) == []


def test_doppione_bloccato_da_un_altro_processo_rilascia_la_propria_riserva_mai_inviata(tmp_path):
    # Rilievo Codex su #533: due processi sullo stesso DB ammettono lo stesso intento; A lo invia, il dedup del
    # motore di B vede la riga/intento di A sul DB e blocca B come DUPLICATE. La riserva di B non e' mai partita
    # da B: va rilasciata (prima di PR29-a lo faceva il SUCCESS; ora lo fa QUICK_BET_DUPLICATE con
    # duplicate_in_process=False). Due runtime reali sullo stesso file SQLite.
    a = _chain(tmp_path, stake=10.0, anti_duplication=True, db_name="shared.db")
    b = _chain(tmp_path, stake=10.0, anti_duplication=True, db_name="shared.db")
    for c in (a, b):
        c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0}
    b.rc._consumed_intent_reason = lambda _ref: ""        # B ha controllato l'intento PRIMA che A lo consumasse
    a.rc._on_signal_received(_signal())
    assert len(a.bus.payloads("CMD_QUICK_BET")) == 1 and len(a.broker.state.orders) == 1
    b.rc._on_signal_received(_signal())
    (dup,) = b.bus.payloads("QUICK_BET_DUPLICATE")
    assert dup.get("duplicate_in_process") is False, dup
    assert b.broker.state.orders == {}                    # B non ha inviato nulla
    assert _tavoli_occupati(b) == [], "la riserva mai inviata del processo perdente resta bloccata"
    assert not b.rc.duplication_guard._active
    assert _tavoli_occupati(a) != []                      # A conserva la sua riserva viva


def test_doppione_dello_stesso_processo_conserva_la_riserva_viva(tmp_path):
    # Replay nello stesso processo: l'originale e' in volo nella memoria del motore => duplicate_in_process=True,
    # nessun rilascio (PF-C29-01.03).
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=True)
    c.bus.publish("CMD_QUICK_BET", dict(cmd))
    (dup,) = c.bus.payloads("QUICK_BET_DUPLICATE")
    assert dup.get("duplicate_in_process") is True, dup
    assert _tavoli_occupati(c) == [cmd["table_id"]]
    assert c.rc.duplication_guard._active


def test_doppione_esterno_non_rilascia_una_riserva_di_un_altro_ordine(tmp_path):
    # Il tavolo indicato dall'evento porta ora la riserva di un altro intento (ref diverso): nessun rilascio.
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=True)
    c.rc._on_quick_bet_duplicate({**cmd, "customer_ref": "ref-di-un-altro-ordine", "duplicate_in_process": False})
    assert _tavoli_occupati(c) == [cmd["table_id"]]
    c.rc._on_quick_bet_duplicate({**cmd, "duplicate_in_process": None})        # dato assente => si conserva
    assert _tavoli_occupati(c) == [cmd["table_id"]]



def _riserva_manuale(c, *, table_id, ref, event_key):
    # Riserva come la crea l'auto-next: tavolo attivato col customer_ref nel meta, CMD SENZA table_id.
    c.rc.table_manager.activate(table_id=table_id, event_key=event_key, exposure=10.0, market_id="1.234",
                                selection_id=11, meta={"customer_ref": ref})


def test_doppione_solo_per_correlation_id_rilascia_la_riserva_di_un_altro_intento(tmp_path):
    # Rilievo Codex su #533: un comando nuovo (customer_ref diverso) che riusa il correlation_id di un ordine in
    # volo e' bloccato come doppione, ma la sua riserva e' sua e non e' mai partita: va rilasciata. La proprieta'
    # della riserva la decide il customer_ref, non il predicato combinato del dedup.
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=True)
    (cid,) = c.engine._seen_correlation_ids
    _riserva_manuale(c, table_id=2, ref="at-altro-intento", event_key="1.234:11:BACK:altro")
    c.bus.publish("CMD_QUICK_BET", {**cmd, "table_id": 2, "customer_ref": "at-altro-intento",
                                    "event_key": "1.234:11:BACK:altro", "correlation_id": cid})
    (dup,) = c.bus.payloads("QUICK_BET_DUPLICATE")
    assert dup.get("duplicate_in_process") is False, dup
    assert _tavoli_occupati(c) == [cmd["table_id"]], "la riserva mai inviata dell'altro intento resta bloccata"
    assert len(c.broker.state.orders) == 1


def test_doppione_esterno_senza_table_id_risolve_il_tavolo_dal_customer_ref(tmp_path):
    # Rilievo Codex su #533: il CMD dell'auto-next non porta table_id. Due runtime sullo stesso SQLite: A invia
    # l'intento, B (che ha riservato un tavolo per lo stesso intento come fa l'auto-next) lo vede consumato dal
    # motore e deve rilasciare la propria riserva risolvendo il tavolo dal customer_ref.
    a = _chain(tmp_path, stake=10.0, anti_duplication=True, db_name="shared.db")
    b = _chain(tmp_path, stake=10.0, anti_duplication=True, db_name="shared.db")
    for c in (a, b):
        c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0}
    a.rc._on_signal_received(_signal())
    (cmd,) = a.bus.payloads("CMD_QUICK_BET")
    _riserva_manuale(b, table_id=3, ref=cmd["customer_ref"], event_key=cmd["event_key"])
    b.bus.publish("CMD_QUICK_BET", {k: v for k, v in cmd.items() if k != "table_id"})
    (dup,) = b.bus.payloads("QUICK_BET_DUPLICATE")
    assert dup.get("duplicate_in_process") is False and dup.get("table_id") is None, dup
    assert b.broker.state.orders == {}
    assert _tavoli_occupati(b) == [], "la riserva senza table_id del processo perdente resta bloccata"
    assert _tavoli_occupati(a) == [cmd["table_id"]]


def test_doppione_esterno_tardivo_non_rilascia_un_tavolo_riassegnato_a_meta_operazione(tmp_path):
    # Rilievo Codex su #533 (P1): fra la verifica dell'identita' e il rilascio un reset/sweep libera il tavolo e una
    # nuova ammissione lo riusa per lo STESSO event_key con un altro intento. Il rilascio non deve colpirla:
    # verifica e rilascio sono una sola operazione atomica del TableManager.
    c, cmd = _segnale_piazzato(tmp_path, anti_duplication=True)
    tm, tid = c.rc.table_manager, cmd["table_id"]
    stato = {"fatto": False}

    def riassegna():
        if not stato["fatto"]:
            stato["fatto"] = True
            tm.force_unlock(tid)
            _riserva_manuale(c, table_id=tid, ref="sig-nuovo-intento", event_key=cmd["event_key"])

    get_table, atomico = tm.get_table, tm.force_unlock_if_unplaced_reservation

    def get_table_poi_riassegna(table_id):
        copia = get_table(table_id)
        riassegna()                                   # la copia letta e' gia' stantia
        return copia

    def riassegna_poi_atomico(*a, **kw):
        riassegna()
        return atomico(*a, **kw)

    tm.get_table, tm.force_unlock_if_unplaced_reservation = get_table_poi_riassegna, riassegna_poi_atomico
    c.rc._on_quick_bet_duplicate({**cmd, "duplicate_in_process": False})
    assert stato["fatto"]
    tavolo = get_table(tid)
    assert tavolo.current_event_key == cmd["event_key"], "la nuova riserva sullo stesso evento e' stata liberata"
    assert tavolo.meta.get("customer_ref") == "sig-nuovo-intento"

# ---------------------------------------------------------------- runtime reale (HeadlessApp, SIM, SQLite)
from tests.f026.test_f026_financial import run  # noqa: E402


def test_runtime_replay_dello_stesso_cmd_non_libera_la_riserva_viva():
    r = run([{"op": "signal", "m": "1.951", "price": 2.0}, {"op": "replay_cmd", "m": "1.951"}, {"op": "wait", "s": 1}])
    snap = r["snaps"][-1]
    eventi = [e["e"] for e in r["events"]]
    assert "QUICK_BET_SUCCESS" in eventi, eventi             # il doppione esce come SUCCESS(DUPLICATE_BLOCKED)
    assert [t["market_id"] for t in snap["tables"]] == ["1.951"], snap["tables"]
    assert snap["tables"][0]["exposure"] > 0, snap["tables"]
