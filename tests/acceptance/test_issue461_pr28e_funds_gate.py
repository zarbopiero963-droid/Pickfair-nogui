"""#461 PR28-e — gate esplicito dei fondi sulle NUOVE entrate (PF-CAP-09 / PF-CAP-19, PF-EXEC-04).

Decisioni owner: PF-EXEC-04 (#426 c.6235676826) «fondi insufficienti = blocco, mai ridimensionamento»;
registro cap PF-CAP-REGISTER-20261018 + addendum (#426 c.6250526960): gate sulle sole nuove entrate,
mai su hedge/uscite protettive (PF-EXEC-03), sotto `_risk_admission_lock`, al netto dell'esposizione
gia' riservata sui tavoli.

Regola: le riserve il cui ordine non risulta confermato a mercato (ACK) prima dell'inizio della lettura del
conto, COMPRESA la nuova entrata, devono stare nel disponibile. Gli ordini gia' a mercato sono gia' fuori dal
disponibile e non contano due volte; un ACK arrivato durante/dopo la lettura resta addebitato (prudente).
L'impegnato del conto non copre nulla: puo' essere di posizioni esterne al bot (rilievo Sol su #531).
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
    (primo,) = c.bus.payloads("CMD_QUICK_BET")
    c.rc._on_quick_bet_accepted(dict(primo))        # ACK a mercato del primo ordine (gestore reale)
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
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti:riserve_pendenti=")], _rifiuti(c)
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


# ---------------------------------------------------------------- servizio reale: lettura fallita
from services.betfair_service import BetfairService  # noqa: E402


class _BrokerGuasto:
    def get_account_funds(self):
        raise RuntimeError("API down")


def _servizio_reale(client):
    svc = BetfairService(settings_service=None)
    svc.simulation_mode = False
    svc.client = client
    return svc


@pytest.mark.parametrize("client", [_BrokerGuasto(), None], ids=["lettura-che-solleva", "nessun-client"])
def test_lettura_fallita_del_servizio_reale_e_fondi_non_verificabili(tmp_path, client):
    # BetfairService intercetta l'errore e restituisce zeri: il payload deve dirlo (`error`), cosi' il gate
    # risponde fondi_non_verificabili (PF-CAP-19) e non "fondi insufficienti" con un disponibile 0 inventato.
    funds = _servizio_reale(client).get_account_funds()
    assert funds.get("error"), funds
    c = _chain(tmp_path, stake=4.0, anti_duplication=True)
    c.rc.betfair_service.get_account_funds = lambda: dict(funds)
    c.rc._on_signal_received(_signal())
    assert "fondi_non_verificabili" in _rifiuti(c), _rifiuti(c)
    _nessun_effetto(c)


def test_lettura_riuscita_del_servizio_reale_non_porta_errore():
    class _Broker:
        def get_account_funds(self):
            return {"available": 50.0, "exposure": -5.0, "total": 55.0}

    funds = _servizio_reale(_Broker()).get_account_funds()
    assert "error" not in funds
    assert funds["available"] == 50.0 and funds["exposure"] == -5.0


# ---------------------------------------------------------------- riserve pendenti vs ordini a mercato
def _riserva_pendente(c, exposure, table_id=2):
    # Un ingresso concorrente ammesso (tavolo riservato) il cui ordine non e' ancora confermato a mercato.
    c.rc.table_manager.activate(table_id=table_id, event_key="evt-concorrente", exposure=exposure,
                                market_id="1.77", selection_id=7)


def test_impegnato_esterno_non_copre_le_riserve_concorrenti_non_piazzate(tmp_path):
    # Rilievo Sol su #531: disponibile 100, impegnato esterno 100, due ingressi pendenti da 80. Le riserve non
    # ancora a mercato (80 + 80 = 160) vanno addebitate al disponibile (100): il secondo e' rifiutato.
    c = _chain(tmp_path, stake=80.0, anti_duplication=True)
    _riserva_pendente(c, 80.0)
    _con_fondi(c, 100.0, exposure=100.0)
    c.rc._on_signal_received(_signal())
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)
    assert c.bus.payloads("CMD_QUICK_BET") == []


def test_ordine_confermato_a_mercato_prima_della_lettura_non_conta_due_volte(tmp_path):
    # Il tavolo 2 ha l'ordine confermato (ACK) prima della lettura: il conto lo riflette gia' (disponibile 40
    # dopo 60 impegnati), quindi l'ingresso da 40 sta nei fondi.
    c = _chain(tmp_path, stake=40.0, anti_duplication=True)
    _riserva_pendente(c, 60.0)
    c.rc.table_manager.mark_placed(2, "evt-concorrente", at=0.0)
    _con_fondi(c, 40.0, exposure=60.0)
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1, _rifiuti(c)


def test_ordine_confermato_dopo_la_lettura_resta_pendente(tmp_path):
    # ACK arrivato DOPO l'inizio della lettura del conto: la lettura puo' non rifletterlo => resta addebitato
    # al disponibile (prudente). 60 pendente + 60 nuovo > 100.
    c = _chain(tmp_path, stake=60.0, anti_duplication=True)
    _riserva_pendente(c, 60.0)
    c.rc.table_manager.mark_placed(2, "evt-concorrente", at=float("inf"))
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)


def test_ack_a_mercato_marca_il_tavolo_della_riserva_e_il_rilascio_lo_azzera():
    from core.table_manager import TableManager
    tm = TableManager(table_count=2)
    tm.activate(table_id=1, event_key="e1", exposure=10.0, market_id="1.1", selection_id=1)
    assert tm.pending_exposure(since=5.0) == 10.0
    assert tm.mark_placed(1, "altro-evento", at=1.0) is False        # riserva diversa: nessuna marca
    assert tm.mark_placed(1, "e1", at=1.0) is True
    tm.mark_placed(1, "e1", at=9.0)                                  # vale il primo ACK
    assert tm.pending_exposure(since=5.0) == 0.0
    assert tm.pending_exposure(since=0.5) == 10.0
    assert tm.pending_exposure(since=None) == 10.0                   # lettura senza istante: tutto pendente
    tm.force_unlock(1)
    tm.activate(table_id=1, event_key="e1", exposure=7.0, market_id="1.1", selection_id=1)
    assert tm.pending_exposure(since=5.0) == 7.0                     # nuova riserva: la vecchia marca non vale


def test_auto_next_con_impegnato_esterno_e_riserva_pendente_e_rifiutato():
    # Snapshot del settlement: 100 disponibili + 100 impegnati esterni; tavolo 2 pendente da 95. LAY 2@6
    # (liability 10): 95 + 10 = 105 > 100 disponibili.
    rc, bus, esito = _auto_next(
        {"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0, "bet_type": "LAY"},
        funds={"available": 100.0, "exposure": 100.0},
        riservato=95.0,
        pct_totale=200.0,
    )
    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == [], esito
    assert esito["reason"].startswith("fondi_insufficienti"), esito


# ---------------------------------------------------------------- verifiche owner prima del merge (#531)
def test_sim_usa_il_saldo_simulato_senza_fondi_reali(tmp_path):
    # Punto 1 owner: in SIM il gate legge il saldo del SimulationBroker; nessun client LIVE coinvolto.
    # Verifica SOLO la fonte del saldo: NON certifica la parita' finanziaria SIM/LIVE, che resta aperta finche'
    # il SimulationBroker non riserva i fondi sugli ordini (G7, tests/acceptance/test_g7_sim_funds_reservation.py).
    from simulation_broker import SimulationBroker

    class _LiveVietato:
        def get_account_funds(self):
            raise AssertionError("in SIM non si leggono fondi reali Betfair")

    svc = BetfairService(settings_service=None)
    svc.simulation_mode = True
    svc.simulation_broker = SimulationBroker(starting_balance=120.0)
    svc.client = _LiveVietato()
    funds = svc.get_account_funds()
    assert funds["available"] == 120.0 and funds["simulated"] is True and "error" not in funds
    c = _chain(tmp_path, stake=100.0, anti_duplication=True)
    c.rc.betfair_service.get_account_funds = svc.get_account_funds
    c.rc._on_signal_received(_signal())
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1, _rifiuti(c)
    svc.simulation_broker = SimulationBroker(starting_balance=50.0)
    c.rc._on_signal_received(_signal(market_id="1.999", selection_id=22, event="Gamma v Delta",
                                     received_at="2026-10-08T10:05:00.000000+00:00"))
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)


def test_esito_incerto_non_e_rifiuto_definitivo_e_la_riserva_resta_addebitata(tmp_path):
    # Punto 2 owner: un AMBIGUOUS (timeout / risposta persa) non rilascia tavolo e chiave e non marca l'ordine
    # a mercato: la riserva resta e continua a consumare il disponibile per le nuove entrate.
    c = _chain(tmp_path, stake=60.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    (primo,) = c.bus.payloads("CMD_QUICK_BET")
    c.rc._on_quick_bet_ambiguous(dict(primo))
    assert len(_tavoli_occupati(c)) == 1                    # riserva trattenuta, nessun rilascio
    assert c.rc.table_manager.pending_exposure(since=float("inf")) == 60.0
    assert "placed_at" not in c.rc.table_manager.get_table(primo["table_id"]).meta
    c.rc._on_signal_received(_signal(market_id="1.999", selection_id=22, event="Gamma v Delta",
                                     received_at="2026-10-08T10:05:00.000000+00:00"))
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)


def test_percorso_di_produzione_success_rilascia_e_non_addebita_due_volte(tmp_path):
    # Il TradingEngine pubblica QUICK_BET_SUCCESS dopo un piazzamento riuscito (nessun ACCEPTED: l'OrderManager
    # non e' cablato). SUCCESS rilascia il tavolo: la posizione esce dalle riserve e resta solo nel conto.
    # Conto dopo il piazzamento: 40 disponibili + 60 impegnati; il secondo ingresso da 40 sta nei fondi.
    c = _chain(tmp_path, stake=60.0, anti_duplication=True)
    _con_fondi(c, 100.0)
    c.rc._on_signal_received(_signal())
    (primo,) = c.bus.payloads("CMD_QUICK_BET")
    c.rc._on_quick_bet_success({**primo, "status": "PLACED", "outcome": "SUCCESS"})
    assert _tavoli_occupati(c) == []
    _con_fondi(c, 40.0, exposure=60.0)
    c.rc.mm.calculate = _decision(40.0)
    c.rc._on_signal_received(_signal(market_id="1.999", selection_id=22, event="Gamma v Delta",
                                     received_at="2026-10-08T10:05:00.000000+00:00"))
    assert len(c.bus.payloads("CMD_QUICK_BET")) == 2, _rifiuti(c)


def test_settlement_concorrente_durante_la_lettura_non_apre_il_gate(tmp_path):
    # Rilievo Codex su 9e6dc05: tra la lettura dei fondi (fuori dal lock) e il controllo un settlement in
    # perdita chiude una posizione e ne rilascia il tavolo, mentre il conto letto conta ancora il suo
    # impegnato. Il gate confronta solo le riserve pendenti col disponibile, che una perdita non cambia
    # (saldo ed esposizione scendono insieme): l'impegnato stantio non apre margine.
    # Disponibile 30; posizione a mercato da 50 (tavolo 2); riserva concorrente non piazzata da 20 (tavolo 3);
    # nuova entrata 20 => 40 pendenti > 30 => rifiutata (con 9e6dc05 passava: 40 <= 30 + 50).
    c = _chain(tmp_path, stake=20.0, anti_duplication=True)
    _riserva_pendente(c, 50.0)
    c.rc.table_manager.mark_placed(2, "evt-concorrente", at=float("-inf"))
    _riserva_pendente(c, 20.0, table_id=3)

    def _lettura_con_settlement_concorrente():
        c.rc.table_manager.force_unlock(2)              # settlement concorrente: tavolo rilasciato
        return {"available": 30.0, "exposure": 50.0}     # conto letto prima del settlement

    c.rc.betfair_service.get_account_funds = _lettura_con_settlement_concorrente
    c.rc._on_signal_received(_signal())
    assert [r for r in _rifiuti(c) if r.startswith("fondi_insufficienti")], _rifiuti(c)
    assert c.bus.payloads("CMD_QUICK_BET") == []


# ---------------------------------------------------------------- rilievi Codex su 17f5e53
def test_auto_next_success_marca_a_mercato_la_riserva_anche_senza_table_id():
    # Rilievo Codex r4330530577: il CMD_QUICK_BET dell'auto-next non porta table_id (il tavolo viene da
    # mm_context), quindi SUCCESS non rilascia il tavolo. L'ordine e' gia' fuori dal disponibile: non deve
    # restare addebitato come pendente alle entrate successive.
    import time as _time
    rc, bus, esito = _auto_next(
        {"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0, "bet_type": "LAY"},
        funds={"available": 150.0, "exposure": 0.0},
    )
    (cmd,) = [p for name, p in bus.events if name == "CMD_QUICK_BET"]
    assert "table_id" not in cmd
    assert rc.table_manager.pending_exposure(since=_time.monotonic()) == pytest.approx(10.0)
    rc._on_quick_bet_success({**cmd, "status": "PLACED", "outcome": "SUCCESS"})
    assert rc.table_manager.pending_exposure(since=_time.monotonic()) == 0.0


def test_success_senza_table_id_con_riserva_ambigua_per_evento_non_marca_nulla():
    # Due tavoli con la stessa event_key (anti-duplicazione disattivata): senza table_id non si sa quale
    # ordine e' a mercato => nessuna marca (resta tutto addebitato, prudente).
    from core.table_manager import TableManager
    tm = TableManager(table_count=2)
    for tid in (1, 2):
        tm.activate(table_id=tid, event_key="e1", exposure=10.0, market_id="1.1", selection_id=1)
    assert tm.mark_placed_by_event("e1", at=0.0) is False
    assert tm.pending_exposure(since=5.0) == 20.0
    tm.force_unlock(2)
    assert tm.mark_placed_by_event("e1", at=0.0) is True
    assert tm.pending_exposure(since=5.0) == 0.0


@pytest.mark.parametrize("risposta", [{"available": 100.0}, {"exposure": 0.0}, {}],
                         ids=["senza-exposure", "senza-available", "vuota"])
def test_risposta_incompleta_del_broker_e_fondi_non_verificabili(tmp_path, risposta):
    # Rilievo Codex r4330530594: BetfairService riempiva i campi mancanti con 0.0 senza errore.
    class _Broker:
        def get_account_funds(self):
            return dict(risposta)

    funds = _servizio_reale(_Broker()).get_account_funds()
    assert funds.get("error"), funds
    c = _chain(tmp_path, stake=4.0, anti_duplication=True)
    c.rc.betfair_service.get_account_funds = lambda: dict(funds)
    c.rc._on_signal_received(_signal())
    assert "fondi_non_verificabili" in _rifiuti(c), _rifiuti(c)
    _nessun_effetto(c)
