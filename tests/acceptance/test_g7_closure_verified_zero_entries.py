"""#532 G7, criterio di chiusura owner (follow-up #546): con un disponibile ZERO VERIFICATO (nessun `error`, conto
che ha solo impegnato) nessun percorso di nuova entrata produce un ordine.

`get_status` non allinea il bankroll a 0 fuori dall'avvio (#546 D1, scelta prudente: niente LOCKDOWN per un conto
vuoto solo per riserve impegnate), quindi la cassa registrata resta quella vecchia. Ogni ingresso deve fermarsi
comunque, prima del broker, sulla lettura fresca del conto:

- segnale (Telegram/GUI), anche con stake sotto il minimo exchange (Microstake): gate fondi PR28-e;
- lotto Dutching e bet manuale: gate fondi del controller (#546 D2, #553);
- auto-next dopo il settlement: sync fallita (`BALANCE_ZERO_VERIFIED`) => nessuna entrata successiva.

Catena reale (`_catena`): RuntimeController, TradingEngine, SimulationBroker, money management vero.
"""
from __future__ import annotations

import pytest

from tests.acceptance.test_issue437_pr27 import _catena
from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _dutching, _manual
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]

_ZERO_VERIFICATO = {"available": 0.0, "exposure": 40.0, "total": 40.0, "simulated": True}


def _conto_a_zero(c):
    c.rc.betfair_service.get_account_funds = lambda: dict(_ZERO_VERIFICATO)
    c.rc.get_status()                                   # la GUI/headless legge lo stato: bankroll non allineato
    assert float(c.rc.risk_desk.bankroll_current) == 1000.0   # cassa vecchia: il freno deve essere la lettura fresca


def _rifiutato(res):
    """Rifiuto fail-closed: fondi freschi insufficienti, oppure cassa invalidata dalla lettura di stato
    (`get_status` con zero verificato e bankroll invariato => CASH_READING_MISMATCH)."""
    reason = str(res.get("reason", ""))
    assert res["ok"] is False and (reason.startswith("fondi_insufficienti") or reason == "cassa_non_verificata"), res


def _nessun_ordine(c):
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}


def test_segnale_con_zero_verificato_non_produce_ordini(tmp_path):
    c = _catena(tmp_path)
    _conto_a_zero(c)
    c.rc._on_signal_received(_signal())
    _nessun_ordine(c)


def test_segnale_microstake_con_zero_verificato_non_produce_ordini(tmp_path):
    c = _catena(tmp_path)
    _conto_a_zero(c)
    c.rc._on_signal_received(_signal(stake=0.5))          # sotto il minimo exchange: percorso Microstake
    _nessun_ordine(c)


def test_lotto_dutching_con_zero_verificato_non_produce_ordini(tmp_path):
    c = _catena(tmp_path)
    _conto_a_zero(c)
    _rifiutato(_dutching(c).submit_dutching(_batch(total_stake=10.0)))
    _nessun_ordine(c)


def test_bet_manuale_con_zero_verificato_non_produce_ordini(tmp_path):
    c = _catena(tmp_path)
    _conto_a_zero(c)
    _rifiutato(_dutching(c).manual_bet(_manual(stake=4.0)))
    _nessun_ordine(c)


@pytest.mark.parametrize("fondi,atteso_invio", [
    ({"available": 0.0, "exposure": 40.0, "total": 40.0}, False),   # zero verificato: disponibile 0, solo impegnato
    ({"available": 150.0, "exposure": 0.0}, True),                                       # controllo positivo
])
def test_auto_next_dopo_settlement_si_ferma_solo_sullo_zero_verificato(fondi, atteso_invio):
    """Rilievo Codex su beb1845: il test precedente si fermava su `resume_submit_disabled`, prima del gate della
    sync. Qui il ciclo e' idoneo all'invio (stesso payload del percorso felice di `test_cycle_executor`): con
    fondi validi l'auto-next parte, con lo zero verificato si ferma proprio sulla sync fallita."""
    from core.system_state import RuntimeMode
    from tests.core.test_cycle_executor import _close_payload, _make_controller

    rc, bus = _make_controller(responses=[dict(fondi), dict(fondi)])
    rc.mode = RuntimeMode.ACTIVE
    rc._on_close_position(_close_payload())
    result = rc._last_cycle_executor_result
    ordini = [e for e in bus.events if e[0] == "CMD_QUICK_BET"]
    if atteso_invio:
        assert result["submitted"] is True and len(ordini) == 1, result
    else:
        assert result["submitted"] is False and ordini == [], result
        assert result["cycle_executor_status"] == "CYCLE_SKIPPED_SYNC_FAILED", result
        assert result["reason"] == "bankroll_sync_not_success", result
        # La sync e' fallita proprio per lo zero verificato (disponibile 0 senza `error`, solo impegnato), non per
        # un altro motivo qualsiasi.
        assert rc._last_bankroll_sync_result.get("reason") == "BALANCE_ZERO_VERIFIED", rc._last_bankroll_sync_result


def test_bet_manuale_senza_get_status_si_ferma_sui_fondi_freschi(tmp_path):
    """Headless senza lettura di stato: la cassa resta quella vecchia, il freno e' il gate fondi del controller."""
    c = _catena(tmp_path)
    c.rc.betfair_service.get_account_funds = lambda: dict(_ZERO_VERIFICATO)
    res = _dutching(c).manual_bet(_manual(stake=4.0))
    assert res["ok"] is False and str(res.get("reason", "")).startswith("fondi_insufficienti"), res
    _nessun_ordine(c)


def test_quando_i_fondi_tornano_le_entrate_riprendono(tmp_path):
    """Nessun blocco permanente: la lettura di stato successiva con fondi validi riallinea cassa e bankroll."""
    c = _catena(tmp_path)
    _conto_a_zero(c)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0, "total": 1000.0,
                                                      "simulated": True}
    c.rc.get_status()
    assert _dutching(c).manual_bet(_manual(stake=4.0))["ok"] is True


@pytest.mark.parametrize("stake", [None, 0.5])
def test_controllo_positivo_con_fondi_validi_il_segnale_produce_un_ordine(tmp_path, stake):
    """Il rifiuto sopra dipende dallo zero verificato, non dal percorso: con fondi validi lo stesso segnale passa."""
    c = _catena(tmp_path)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0, "total": 1000.0,
                                                      "simulated": True}
    c.rc.get_status()
    c.rc._on_signal_received(_signal() if stake is None else _signal(stake=stake))
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1


# ----------------------------------------------------------------------------- G7: concorrenza multi-tavolo
def test_g7_piazzamenti_concorrenti_oltre_il_disponibile_ne_passa_uno_solo():
    """Due tavoli piazzano insieme 60 + 60 su 100 di saldo: la riserva del broker e' atomica, ne passa
    esattamente uno e il disponibile non scende sotto zero (nessun doppio conteggio, nessuna sovrastima)."""
    import threading

    from simulation_broker import SimulationBroker

    for giro in range(25):
        broker = SimulationBroker(starting_balance=100.0)
        partenza = threading.Barrier(2)
        esiti = []

        def piazza(i):
            partenza.wait()
            esiti.append(broker.place_bet(market_id=f"1.{i}", selection_id=i, side="BACK", price=2.0, size=60.0,
                                          customer_ref=f"g7-conc-{giro}-{i}")["status"])
        threads = [threading.Thread(target=piazza, args=(i,)) for i in (1, 2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(esiti).count("SUCCESS") == 1, (giro, esiti)
        funds = broker.get_account_funds()
        assert funds["available"] == pytest.approx(40.0) and funds["exposure"] == pytest.approx(60.0), funds
