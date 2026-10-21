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


# ---------------------------------------------------------------- runtime reale (HeadlessApp, SIM, SQLite)
from tests.f026.test_f026_financial import run  # noqa: E402


def test_runtime_replay_dello_stesso_cmd_non_libera_la_riserva_viva():
    r = run([{"op": "signal", "m": "1.951", "price": 2.0}, {"op": "replay_cmd", "m": "1.951"}, {"op": "wait", "s": 1}])
    snap = r["snaps"][-1]
    eventi = [e["e"] for e in r["events"]]
    assert "QUICK_BET_SUCCESS" in eventi, eventi             # il doppione esce come SUCCESS(DUPLICATE_BLOCKED)
    assert [t["market_id"] for t in snap["tables"]] == ["1.951"], snap["tables"]
    assert snap["tables"][0]["exposure"] > 0, snap["tables"]
