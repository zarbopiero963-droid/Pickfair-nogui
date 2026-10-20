"""#461 PR28-e — gate esplicito dei fondi sulle NUOVE entrate (PF-CAP-09 / PF-CAP-19, PF-EXEC-04).

Decisioni owner: PF-EXEC-04 (#426 c.6235676826) «fondi insufficienti = blocco, mai ridimensionamento»;
registro cap PF-CAP-REGISTER-20261018 + addendum (#426 c.6250526960): gate sulle sole nuove entrate,
mai su hedge/uscite protettive (PF-EXEC-03), sotto `_risk_admission_lock`, al netto dell'esposizione
gia' riservata sui tavoli.

Due condizioni, entrambe obbligatorie: (1) l'esposizione della nuova entrata sta nel disponibile del
conto; (2) l'esposizione riservata su tutti i tavoli (compresa la nuova entrata) sta nel disponibile piu'
l'impegnato a mercato, cosi' gli ordini gia' piazzati non contano due volte e quelli riservati ma non
ancora a mercato consumano il disponibile. La (1) copre l'impegnato da posizioni esterne al bot.
Fondi illeggibili/non validi => nessuna nuova entrata.
"""
from __future__ import annotations

import pytest

from core.system_state import RuntimeMode
from tests.acceptance.test_issue437_pr27 import _rifiuti, _tavoli_occupati
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain, _decision, _signal


def _con_fondi(c, available, exposure=0.0, **extra):
    funds = {"available": available, "exposure": exposure, **extra}
    c.rc.betfair_service.get_account_funds = lambda: dict(funds)
    return funds


def _nessun_effetto(c):
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert _tavoli_occupati(c) == []
    assert c.rc.duplication_guard._active == {}


def test_back_stake_oltre_i_fondi_disponibili_e_rifiutato(tmp_path):
    # PF-CAP-09 riprodotto dal registro: bankroll 1000 nel runtime, 100 sul conto, BACK 150 => piazzato.
    c = _chain(tmp_path, stake=150.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)
    _nessun_effetto(c)


def test_lay_liability_oltre_i_fondi_e_rifiutata_anche_con_stake_piccolo(tmp_path):
    # LAY 50 a quota 4 => liability 150 > 100 disponibili.
    c = _chain(tmp_path, stake=50.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal(bet_type="LAY", price=4.0))
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)
    _nessun_effetto(c)


def test_back_entro_i_fondi_passa_anche_alla_soglia_esatta(tmp_path):
    c = _chain(tmp_path, stake=100.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1, _rifiuti(c)
    assert len(c.broker.state.orders) == 1


def test_secondo_tavolo_conta_l_esposizione_gia_riservata(tmp_path):
    # Conto a 100; primo ingresso 60 riservato. Il conto non riflette ancora l'ordine (stesso disponibile):
    # il secondo 60 su un altro evento supererebbe i fondi => rifiutato.
    c = _chain(tmp_path, stake=60.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    c.rc._on_signal_received(_signal(market_id="1.999", selection_id=22, event="Gamma v Delta",
                                     received_at="2026-10-08T10:05:00.000000+00:00"))
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1, "il secondo ingresso non doveva partire"
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)
    assert len(_tavoli_occupati(c)) == 1


def test_secondo_tavolo_rifiutato_anche_quando_il_conto_riflette_il_primo(tmp_path):
    # Conto aggiornato (disponibile 40, impegnato 60): l'ordine piazzato non va contato due volte, ma il
    # secondo 60 resta oltre i fondi.
    c = _chain(tmp_path, stake=60.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    _con_fondi(c, 40.0, exposure=60.0)
    c.rc._on_signal_received(_signal(market_id="1.999", selection_id=22, event="Gamma v Delta",
                                     received_at="2026-10-08T10:05:00.000000+00:00"))
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)


def test_ordine_gia_piazzato_non_e_contato_due_volte(tmp_path):
    # Conto aggiornato (40 + 60): un secondo ingresso da 40 sta esattamente nei fondi => passa.
    c = _chain(tmp_path, stake=60.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    _con_fondi(c, 40.0, exposure=60.0)
    c.rc.mm.calculate = _decision(40.0)
    c.rc._on_signal_received(_signal(market_id="1.999", selection_id=22, event="Gamma v Delta",
                                     received_at="2026-10-08T10:05:00.000000+00:00"))
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 2, _rifiuti(c)


def test_impegnato_esterno_al_bot_non_gonfia_i_fondi(tmp_path):
    # Disponibile 10, impegnato 100 da posizioni NON del bot (nessun tavolo occupato): una entrata da 50
    # non sta nel disponibile => rifiutata, anche se disponibile + impegnato (110) la coprirebbe.
    c = _chain(tmp_path, stake=50.0, anti_duplication=True)
    _con_fondi(c, 10.0, exposure=100.0)
    c.rc._on_signal_received(_signal())
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti:ordine=")], _rifiuti(c)
    _nessun_effetto(c)


@pytest.mark.parametrize("fondi", [
    None, {}, {"available": 100.0}, {"available": "abc", "exposure": 0.0},
    {"available": float("nan"), "exposure": 0.0}, {"available": -1.0, "exposure": 0.0},
    {"available": 0.0, "exposure": 0.0, "error": "SESSION_EXPIRED"},
], ids=["none", "vuoto", "senza-esposizione", "testo", "nan", "negativo", "errore-servizio"])
def test_fondi_illeggibili_o_non_validi_bloccano_le_nuove_entrate(tmp_path, fondi):
    # PF-CAP-19: il runtime conserva il bankroll (1000) ma i fondi non sono verificabili => nessun ordine.
    c = _chain(tmp_path, stake=4.0, anti_duplication=True)
    c.rc.betfair_service.get_account_funds = lambda: fondi
    c.rc._on_signal_received(_signal())
    assert [r for r in _rifiuti(c) if r.startswith("fondi_")], _rifiuti(c)
    _nessun_effetto(c)


def test_lettura_dei_fondi_che_solleva_blocca_l_ingresso(tmp_path):
    c = _chain(tmp_path, stake=4.0, anti_duplication=True)

    def _boom():
        raise RuntimeError("API down")
    c.rc.betfair_service.get_account_funds = _boom
    c.rc._on_signal_received(_signal())
    assert "fondi_non_verificabili" in _rifiuti(c)
    _nessun_effetto(c)


def test_rifiuto_per_fondi_e_ritentabile_quando_i_fondi_tornano(tmp_path):
    # Il rifiuto rilascia chiave e tavolo: lo stesso segnale, con fondi sufficienti, parte.
    c = _chain(tmp_path, stake=150.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    assert c.bus.payloads("CMD_QUICK_BET") == []
    _con_fondi(c, 200.0)
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1, _rifiuti(c)


# ---------------------------------------------------------------- auto-next dopo settlement
from tests.integration.test_runtime_controller_cycle_executor import (  # noqa: E402
    _canonical_close_payload,
    _make_controller,
)


def _auto_next(next_signal, *, funds, riservato=0.0, pct_totale=None):
    rc, bus = _make_controller(responses=[funds])
    rc.mode = RuntimeMode.ACTIVE
    if pct_totale is not None:
        # La percentuale di esposizione totale del MM non ha tetto superiore in config: oltre il 100%
        # il cap percentuale non protegge piu' la cassa, resta solo il gate dei fondi.
        rc.config.max_total_exposure_pct = pct_totale
    if riservato:
        # Un altro tavolo (id 2) tiene gia' un'entrata aperta: la sua esposizione impegna la cassa.
        rc.table_manager.activate(
            table_id=2, event_key="evt-altro", exposure=riservato, market_id="1.9", selection_id=9,
        )
    rc._on_close_position(
        _canonical_close_payload(
            auto_trade_enabled=True,
            cycle_executor_enabled=True,
            mm_context={
                "cycle_active": True,
                "cycle_id": "cycle-pr28e",
                "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": next_signal,
            },
        )
    )
    esito = [e for e in bus.events if e[0] == "AUTO_TRADE_MM_RESULT"][0][1]
    return rc, bus, esito


def test_auto_next_lay_liability_oltre_i_fondi_e_rifiutato():
    rc, bus, esito = _auto_next(
        # Il tavolo 2 ha riservato 95 non ancora a mercato (l'exchange non lo vede: exposure 0).
        # Cassa 100; liability LAY 2*(6-1)=10 entro max_single_bet (18% di 100) e, con
        # max_total_exposure_pct=200, entro il cap percentuale (105 <= 200); ma
        # 95 + 10 = 105 > 100 di cassa => la nuova entrata eccede i fondi.
        {"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0, "bet_type": "LAY"},
        funds={"available": 100.0, "exposure": 0.0},
        riservato=95.0,
        pct_totale=200.0,
    )
    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == [], esito
    assert esito["reason"].startswith("fondi_insufficienti"), esito


def test_auto_next_entro_i_fondi_passa():
    rc, bus, esito = _auto_next(
        {"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0, "bet_type": "LAY"},
        funds={"available": 150.0, "exposure": 0.0},
    )
    assert len([e for e in bus.events if e[0] == "CMD_QUICK_BET"]) == 1, esito


def test_auto_next_con_fondi_non_validi_e_rifiutato():
    rc, bus, esito = _auto_next(
        {"market_id": "1.2", "selection_id": 2, "price": 2.0, "stake": 1.0, "bet_type": "BACK"},
        funds={"available": 150.0},                     # esposizione mancante: dato non valido
    )
    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == [], esito
    assert esito["reason"] == "fondi_non_verificabili", esito
