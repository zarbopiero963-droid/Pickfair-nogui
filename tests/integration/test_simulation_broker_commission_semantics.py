import math
import pytest

from simulation_broker import SimulationBroker


@pytest.mark.integration
def test_simulation_realized_commission_is_applied_only_on_positive_winnings():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    won = broker.record_realized_settlement(200.0, market_id="1.100")

    assert math.isfinite(won["gross_pnl"])
    assert math.isfinite(won["commission_amount"])
    assert math.isfinite(won["net_pnl"])
    assert won["gross_pnl"] == 200.0
    assert won["commission_amount"] == 9.0
    assert won["net_pnl"] == 191.0
    assert won["commission_pct"] == 4.5
    assert won["settlement_basis"] == "market_net_realized"
    assert won["settlement_source"] == "simulation_broker"
    assert won["settlement_kind"] == "realized_settlement"
    assert won["pnl"] == won["net_pnl"]


@pytest.mark.integration
def test_simulation_realized_commission_is_zero_on_losses():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    lost = broker.record_realized_settlement(-100.0, market_id="1.101")

    assert math.isfinite(lost["gross_pnl"])
    assert math.isfinite(lost["commission_amount"])
    assert math.isfinite(lost["net_pnl"])
    assert lost["gross_pnl"] == -100.0
    assert lost["commission_amount"] == 0.0
    assert lost["net_pnl"] == -100.0
    assert lost["commission_pct"] == 4.5
    assert lost["settlement_basis"] == "market_net_realized"
    assert lost["settlement_source"] == "simulation_broker"
    assert lost["settlement_kind"] == "realized_settlement"


@pytest.mark.integration
def test_simulation_broker_snapshot_exposes_realized_commission_accounting_contract():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    broker.record_realized_settlement(200.0, market_id="1.102")
    broker.record_realized_settlement(-100.0, market_id="1.102")
    snap = broker.snapshot()

    # Fail-closed expectation lock: simulation-facing realized accounting is explicit.
    assert "realized_pnl" in snap
    assert "realized_commission" in snap
    assert "last_settlement" in snap
    # Commission is market-net scoped per market id:
    # gross path +200 then -100 => market-net +100 => commission 4.5
    assert snap["realized_pnl"] == 95.5
    assert snap["realized_commission"] == 4.5
    assert snap["last_settlement"]["gross_pnl"] == -100.0
    # Refund leg: later negative gross reduced prior market-net commission basis.
    assert snap["last_settlement"]["commission_amount"] == -4.5
    assert snap["last_settlement"]["net_pnl"] == -95.5
    assert snap["last_settlement"]["commission_pct"] == 4.5
    assert snap["last_settlement"]["settlement_source"] == "simulation_broker"
    assert snap["last_settlement"]["settlement_kind"] == "realized_settlement"


@pytest.mark.integration
def test_simulation_broker_enforces_explicit_betfair_italy_commission_policy():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=5.0)
    with pytest.raises(ValueError):
        broker.record_realized_settlement(100.0, market_id="1.103")


@pytest.mark.integration
def test_simulation_settlement_requires_market_id():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)

    with pytest.raises(ValueError, match="market_id is required"):
        broker.record_realized_settlement(10.0, market_id="")


@pytest.mark.integration
def test_simulation_legacy_global_commission_ledger_is_migrated_to_first_explicit_market():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    broker.state.market_commission_ledger["__GLOBAL__"] = {"gross": 100.0, "commission": 4.5}

    settlement = broker.record_realized_settlement(-100.0, market_id="1.104")

    assert settlement["market_id"] == "1.104"
    assert settlement["market_net_gross"] == 0.0
    assert settlement["market_commission_amount_total"] == 0.0
    assert settlement["commission_amount"] == pytest.approx(-4.5)
    assert settlement["net_pnl"] == pytest.approx(-95.5)
    assert "__GLOBAL__" not in broker.state.market_commission_ledger
    assert broker.state.market_commission_ledger["1.104"]["gross"] == 0.0
    assert broker.state.market_commission_ledger["1.104"]["commission"] == 0.0

@pytest.mark.integration
def test_simulation_same_market_multi_leg_commission_is_market_net_positive_once():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)

    # Same market, two realized legs:
    # +100 gross then -40 gross => market-net gross +60
    first = broker.record_realized_settlement(100.0, market_id="1.777")
    second = broker.record_realized_settlement(-40.0, market_id="1.777")

    expected_market_net_gross = 60.0
    expected_market_commission = expected_market_net_gross * 0.045
    expected_market_net = expected_market_net_gross - expected_market_commission

    assert first["market_id"] == "1.777"
    assert second["market_id"] == "1.777"
    assert second["market_net_gross"] == expected_market_net_gross
    assert second["market_commission_amount_total"] == expected_market_commission
    assert second["settlement_basis"] == "market_net_realized"
    assert broker.state.realized_commission == expected_market_commission
    assert broker.state.realized_pnl == expected_market_net
    assert broker.state.balance == 1000.0 + expected_market_net


@pytest.mark.integration
def test_simulation_same_market_multi_leg_net_loss_has_zero_commission():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)

    # Same market, two realized legs:
    # +30 gross then -50 gross => market-net gross -20 => zero commission.
    broker.record_realized_settlement(30.0, market_id="1.778")
    second = broker.record_realized_settlement(-50.0, market_id="1.778")

    assert second["market_id"] == "1.778"
    assert second["market_net_gross"] == -20.0
    assert second["market_commission_amount_total"] == 0.0
    assert second["settlement_basis"] == "market_net_realized"
    assert broker.state.realized_commission == 0.0
    assert broker.state.realized_pnl == -20.0
    assert broker.state.balance == pytest.approx(980.0)


@pytest.mark.integration
def test_simulation_market_net_overcharge_detector_differs_from_per_leg_commission():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)

    # If commission were charged per positive leg only:
    # +100 leg => 4.50 commission, -40 leg => 0.00 commission => total 4.50
    # Market-net rule must be commission on (+100-40)=+60 => 2.70.
    broker.record_realized_settlement(100.0, market_id="1.779")
    broker.record_realized_settlement(-40.0, market_id="1.779")

    per_leg_commission = 100.0 * 0.045
    market_net_commission = 60.0 * 0.045

    assert per_leg_commission == pytest.approx(4.5)
    assert market_net_commission == pytest.approx(2.7)
    assert broker.state.realized_commission == pytest.approx(market_net_commission)
    assert broker.state.realized_commission < per_leg_commission


# ===========================================================================
# PR03 (#461/#437) — il broker SIM regola il mercato CHIUSO con i fill veri.
#
# Prima di questa PR il settlement SIM esisteva solo come helper
# (`record_realized_settlement`) chiamato dai test qui sopra con un lordo
# inventato e nessuna puntata: nessun codice di produzione lo chiamava, e
# usato dopo dei fill avrebbe contato due volte stake e liability (gia'
# tolti dal saldo al fill). Qui il settlement parte dal book CHIUSO, legge
# le puntate abbinate e verifica saldo, esposizione e registro contro un
# calcolo indipendente scritto a mano in ogni test.
# ===========================================================================

import copy  # noqa: E402
import json  # noqa: E402


def _aperto(market_id, prezzi):
    """prezzi: {selectionId: (miglior back, miglior lay, liquidita')}."""
    return {
        "marketId": market_id,
        "status": "OPEN",
        "runners": [
            {
                "selectionId": sel,
                "status": "ACTIVE",
                "ex": {
                    "availableToBack": [{"price": back, "size": size}],
                    "availableToLay": [{"price": lay, "size": size}],
                },
            }
            for sel, (back, lay, size) in prezzi.items()
        ],
    }


def _chiuso(market_id, stati, **extra):
    book = {
        "marketId": market_id,
        "status": "CLOSED",
        "runners": [{"selectionId": sel, "status": st} for sel, st in stati.items()],
    }
    book.update(extra)
    return book


def _fotografia(broker):
    """Tutto cio' che un settlement rifiutato NON deve toccare."""
    stato = broker.state
    return {
        "balance": stato.balance,
        "exposure": stato.exposure,
        "realized_pnl": stato.realized_pnl,
        "realized_commission": stato.realized_commission,
        "ledgers": sorted(stato.position_ledgers),
        "ordini": {k: (o.status, o.matched_size) for k, o in stato.orders.items()},
        "commissioni": copy.deepcopy(stato.market_commission_ledger),
        "settlements": copy.deepcopy(stato.settlements),
    }


def _tre_selezioni(broker, market_id="2.1"):
    broker.update_market_book(_aperto(market_id, {
        11: (3.0, 3.1, 100.0),
        22: (1.98, 2.0, 100.0),
        33: (5.0, 5.1, 100.0),
    }))
    broker.place_bet(market_id=market_id, selection_id=11, side="BACK", price=3.0, size=10.0)
    broker.place_bet(market_id=market_id, selection_id=22, side="LAY", price=2.0, size=5.0)
    broker.place_bet(market_id=market_id, selection_id=33, side="BACK", price=5.0, size=4.0)


@pytest.mark.integration
def test_settlement_sim_piu_selezioni_vinte_e_perse_netto_di_mercato():
    """Calcolo indipendente:
    BACK 10 @ 3.0 sulla 11 (WINNER) = +20; LAY 5 @ 2.0 sulla 22 (LOSER) = +5;
    BACK 4 @ 5.0 sulla 33 (LOSER) = -4. Lordo di mercato +21, commissione
    4,5% una volta sul netto = 0,945, netto 20,055. Ai fill il saldo scende
    di 10 + 5 + 4 = 19 (981); al settlement torna a 1000 + 20,055."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    assert broker.get_account_funds()["available"] == pytest.approx(981.0)
    assert broker.get_account_funds()["exposure"] == pytest.approx(19.0)

    esito = broker.update_market_book(_chiuso("2.1", {11: "WINNER", 22: "LOSER", 33: "LOSER"}))

    assert esito["settlement_status"] == "SETTLED"
    fondi = broker.get_account_funds()
    assert fondi["available"] == pytest.approx(1020.055)
    assert fondi["exposure"] == pytest.approx(0.0)
    assert not [k for k in broker.state.position_ledgers if k.startswith("2.1::")]
    assert broker.state.realized_pnl == pytest.approx(20.055)
    assert broker.state.realized_commission == pytest.approx(0.945)
    assert {o.status for o in broker.state.orders.values()} == {"SETTLED"}

    registro = broker.list_settlements()
    assert len(registro) == 1
    rec = registro[0]
    assert rec["market_id"] == "2.1"
    assert rec["settlement_id"].startswith("SIMSET-")
    assert rec["gross_pnl"] == pytest.approx(21.0)
    assert rec["commission_amount"] == pytest.approx(0.945)
    assert rec["net_pnl"] == pytest.approx(20.055)
    assert rec["commission_pct"] == pytest.approx(4.5)
    assert rec["settlement_basis"] == "market_net_realized"
    assert sorted(b["gross_pnl"] for b in rec["bets"]) == pytest.approx([-4.0, 5.0, 20.0])
    assert esito["settlement"] == rec


@pytest.mark.integration
def test_settlement_sim_mercato_in_perdita_senza_commissione():
    """BACK 10 @ 3.0 perde (-10), LAY 5 @ 2.0 sul vincitore perde la
    liability (-5): lordo -15, commissione 0, saldo finale 985 = 1000 - 15."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    broker.update_market_book(_aperto("2.2", {11: (3.0, 3.1, 100.0), 22: (1.98, 2.0, 100.0)}))
    broker.place_bet(market_id="2.2", selection_id=11, side="BACK", price=3.0, size=10.0)
    broker.place_bet(market_id="2.2", selection_id=22, side="LAY", price=2.0, size=5.0)

    broker.update_market_book(_chiuso("2.2", {11: "LOSER", 22: "WINNER"}))

    rec = broker.list_settlements()[0]
    assert rec["gross_pnl"] == pytest.approx(-15.0)
    assert rec["commission_amount"] == pytest.approx(0.0)
    assert rec["net_pnl"] == pytest.approx(-15.0)
    assert broker.get_account_funds()["available"] == pytest.approx(985.0)
    assert broker.get_account_funds()["exposure"] == pytest.approx(0.0)


@pytest.mark.integration
def test_settlement_sim_match_parziale_e_residuo_decaduto():
    """Liquidita' 4 a quota 3.0: della BACK da 10 se ne abbinano 4. Una
    seconda BACK a 3.5, sopra il miglior back, non si abbina. Vince la 11: lordo 4 x 2 = 8,
    commissione 0,36, netto 7,64. Il residuo non abbinato decade senza
    toccare il saldo."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    broker.update_market_book(_aperto("2.3", {11: (3.0, 3.1, 4.0)}))
    parziale = broker.place_bet(market_id="2.3", selection_id=11, side="BACK", price=3.0, size=10.0)
    non_abbinata = broker.place_bet(market_id="2.3", selection_id=11, side="BACK", price=3.5, size=5.0)
    id_parziale = parziale["instructionReports"][0]["betId"]
    id_libera = non_abbinata["instructionReports"][0]["betId"]
    assert broker.state.orders[id_parziale].matched_size == pytest.approx(4.0)
    assert broker.get_account_funds()["available"] == pytest.approx(996.0)

    broker.update_market_book(_chiuso("2.3", {11: "WINNER"}))

    assert broker.list_settlements()[0]["net_pnl"] == pytest.approx(7.64)
    assert broker.get_account_funds()["available"] == pytest.approx(1007.64)
    assert broker.state.orders[id_parziale].status == "SETTLED"
    assert broker.state.orders[id_libera].status == "LAPSED"
    assert broker.get_current_orders() == []


def _back_poi_lay(broker, market_id="2.4"):
    """BACK 10 @ 3.0, poi LAY 10 @ 2.0 sullo stesso runner. Il ledger
    accredita subito (3 - 2) x 10 = +10 alla chiusura della posizione:
    saldo 1010 prima del settlement."""
    broker.update_market_book(_aperto(market_id, {11: (3.0, 3.1, 100.0)}))
    broker.place_bet(market_id=market_id, selection_id=11, side="BACK", price=3.0, size=10.0)
    broker.update_market_book(_aperto(market_id, {11: (1.98, 2.0, 100.0)}))
    broker.place_bet(market_id=market_id, selection_id=11, side="LAY", price=2.0, size=10.0)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("esito", "lordo", "netto", "saldo"),
    [
        # vince: BACK +20, LAY -10 => +10, commissione 0,45
        ("WINNER", 10.0, 9.55, 1009.55),
        # perde: BACK -10, LAY +10 => 0
        ("LOSER", 0.0, 0.0, 1000.0),
    ],
)
def test_settlement_sim_back_poi_lay_corregge_il_credito_anticipato(esito, lordo, netto, saldo):
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _back_poi_lay(broker)
    assert broker.get_account_funds()["available"] == pytest.approx(1010.0)

    broker.update_market_book(_chiuso("2.4", {11: esito}))

    rec = broker.list_settlements()[0]
    assert rec["gross_pnl"] == pytest.approx(lordo)
    assert rec["net_pnl"] == pytest.approx(netto)
    assert broker.get_account_funds()["available"] == pytest.approx(saldo)
    assert broker.get_account_funds()["exposure"] == pytest.approx(0.0)


@pytest.mark.integration
def test_settlement_sim_runner_rimosso_puntata_annullata():
    """Tutte le puntate sul runner rimosso: bet annullata, lordo 0, lo stake
    torna al saldo."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    broker.update_market_book(_aperto("2.5", {11: (3.0, 3.1, 100.0), 22: (3.0, 3.1, 100.0)}))
    broker.place_bet(market_id="2.5", selection_id=22, side="BACK", price=3.0, size=10.0)

    esito = broker.update_market_book(_chiuso("2.5", {11: "WINNER", 22: "REMOVED"}))

    assert esito["settlement_status"] == "SETTLED"
    assert broker.list_settlements()[0]["gross_pnl"] == pytest.approx(0.0)
    assert broker.get_account_funds()["available"] == pytest.approx(1000.0)
    assert broker.get_account_funds()["exposure"] == pytest.approx(0.0)


@pytest.mark.integration
def test_settlement_sim_book_chiuso_duplicato_non_riapplica():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    chiuso = _chiuso("2.1", {11: "WINNER", 22: "LOSER", 33: "LOSER"})
    broker.update_market_book(chiuso)
    prima = _fotografia(broker)

    esito = broker.update_market_book(copy.deepcopy(chiuso))

    assert esito["settlement_status"] == "DUPLICATE"
    assert _fotografia(broker) == prima
    assert broker.get_account_funds()["available"] == pytest.approx(1020.055)


@pytest.mark.integration
@pytest.mark.parametrize(
    "stati",
    [
        {11: "ACTIVE", 22: "LOSER", 33: "LOSER"},
        {11: "PLACED", 22: "LOSER", 33: "LOSER"},
        {11: "HIDDEN", 22: "LOSER", 33: "LOSER"},
        {11: None, 22: "LOSER", 33: "LOSER"},
        {22: "LOSER", 33: "LOSER"},  # runner con puntate assente dal book
    ],
)
def test_settlement_sim_stato_runner_non_regolabile_nessun_effetto(stati):
    """Il settlement non inventa un esito: finche' un runner con puntate non
    ha uno stato terminale noto, le posizioni restano aperte."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    prima = _fotografia(broker)

    esito = broker.update_market_book(_chiuso("2.1", stati))

    assert esito["settlement_status"] == "UNSUPPORTED_RUNNER_STATUS"
    assert _fotografia(broker) == prima
    assert broker.list_settlements() == []


@pytest.mark.integration
def test_settlement_sim_runner_rimosso_con_puntate_su_altri_fail_closed():
    """Un runner rimosso cambia le quote degli altri (fattori di riduzione),
    che la simulazione non modella: nessun settlement."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    prima = _fotografia(broker)

    esito = broker.update_market_book(_chiuso("2.1", {11: "WINNER", 22: "REMOVED", 33: "LOSER"}))

    assert esito["settlement_status"] == "REDUCTION_FACTOR_UNSUPPORTED"
    assert _fotografia(broker) == prima


@pytest.mark.integration
def test_settlement_sim_dead_heat_fail_closed():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    prima = _fotografia(broker)

    esito = broker.update_market_book(
        _chiuso("2.1", {11: "WINNER", 22: "WINNER", 33: "LOSER"}, numberOfWinners=1)
    )

    assert esito["settlement_status"] == "DEAD_HEAT_UNSUPPORTED"
    assert _fotografia(broker) == prima


@pytest.mark.integration
@pytest.mark.parametrize("stato_mercato", ["OPEN", "SUSPENDED", "INACTIVE", None, 7])
def test_settlement_sim_mercato_non_chiuso_nessun_effetto(stato_mercato):
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    prima = _fotografia(broker)
    book = _chiuso("2.1", {11: "WINNER", 22: "LOSER", 33: "LOSER"})
    book["status"] = stato_mercato

    esito = broker.update_market_book(book)

    assert "settlement_status" not in esito
    prima_fondi = {k: prima[k] for k in ("balance", "exposure", "ledgers", "ordini", "settlements")}
    dopo = _fotografia(broker)
    assert {k: dopo[k] for k in prima_fondi} == prima_fondi


@pytest.mark.integration
def test_settlement_sim_ledger_incoerente_con_gli_ordini_fail_closed():
    """Se gli ordini persistiti non ricostruiscono l'esposizione del ledger,
    l'accredito sarebbe sbagliato: nessun settlement."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    primo = next(iter(broker.state.orders.values()))
    primo.matched_size = 3.0  # il ledger dice 10
    prima = _fotografia(broker)

    esito = broker.update_market_book(_chiuso("2.1", {11: "WINNER", 22: "LOSER", 33: "LOSER"}))

    assert esito["settlement_status"] == "LEDGER_INCONSISTENT"
    assert _fotografia(broker) == prima


@pytest.mark.integration
def test_settlement_sim_mercato_senza_posizioni_decade_senza_registro():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    broker.update_market_book(_aperto("2.6", {11: (3.0, 3.1, 100.0)}))
    # Sopra il miglior back (3.0): su Betfair resta a riposo.
    r = broker.place_bet(market_id="2.6", selection_id=11, side="BACK", price=3.5, size=5.0)
    bet_id = r["instructionReports"][0]["betId"]

    esito = broker.update_market_book(_chiuso("2.6", {11: "WINNER"}))

    assert esito["settlement_status"] == "NO_POSITIONS"
    assert broker.state.orders[bet_id].status == "LAPSED"
    assert broker.list_settlements() == []
    assert broker.get_account_funds()["available"] == pytest.approx(1000.0)


@pytest.mark.integration
def test_settlement_sim_persistito_e_idempotente_dopo_restart():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    chiuso = _chiuso("2.1", {11: "WINNER", 22: "LOSER", 33: "LOSER"})
    broker.update_market_book(chiuso)
    stato = json.loads(json.dumps(broker.state.to_dict()))  # come nel DB

    riavviato = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    riavviato.state.load_from_dict(stato)
    assert riavviato.list_settlements() == broker.list_settlements()

    esito = riavviato.update_market_book(copy.deepcopy(chiuso))

    assert esito["settlement_status"] == "DUPLICATE"
    assert riavviato.get_account_funds()["available"] == pytest.approx(1020.055)
    assert len(riavviato.list_settlements()) == 1


@pytest.mark.integration
def test_settlement_sim_restart_prima_del_settlement_saldo_esatto():
    """Il ledger ricaricato dopo un restart non conserva il realizzato gia'
    accreditato (+10 della chiusura BACK/LAY). L'accredito del settlement
    si ricostruisce dagli ordini persistiti, non dal ledger: saldo 1000."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _back_poi_lay(broker)
    stato = json.loads(json.dumps(broker.state.to_dict()))

    riavviato = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    riavviato.state.load_from_dict(stato)
    assert riavviato.get_account_funds()["available"] == pytest.approx(1010.0)

    riavviato.update_market_book(_chiuso("2.4", {11: "LOSER"}))

    assert riavviato.get_account_funds()["available"] == pytest.approx(1000.0)
    assert riavviato.list_settlements()[0]["net_pnl"] == pytest.approx(0.0)


@pytest.mark.integration
def test_settlement_sim_registro_restituisce_copie_ordinate():
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    for mercato in ("3.2", "3.1"):
        broker.update_market_book(_aperto(mercato, {11: (3.0, 3.1, 100.0)}))
        broker.place_bet(market_id=mercato, selection_id=11, side="BACK", price=3.0, size=2.0)
        broker.update_market_book(_chiuso(mercato, {11: "LOSER"}))

    registro = broker.list_settlements()
    assert [r["market_id"] for r in registro] == sorted(
        (r["market_id"] for r in registro),
        key=lambda m: broker.state.settlements[m]["settled_at"] + m,
    )
    registro[0]["net_pnl"] = 999.0
    registro[0]["bets"].clear()
    assert broker.list_settlements()[0]["net_pnl"] == pytest.approx(-2.0)
    assert broker.list_settlements()[0]["bets"]


@pytest.mark.integration
def test_settlement_sim_puntata_su_mercato_gia_regolato_non_si_abbina():
    """Dopo il settlement il mercato e' chiuso: anche se il book CHIUSO
    portasse ancora dei prezzi, l'ordine non si abbina e non resta in attesa.
    Una posizione aperta dopo il settlement non verrebbe mai regolata."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    chiuso = _chiuso("2.1", {11: "WINNER", 22: "LOSER", 33: "LOSER"})
    broker.update_market_book(chiuso)
    saldo = broker.get_account_funds()["available"]
    chiuso_con_prezzi = _aperto("2.1", {11: (3.0, 3.1, 100.0)})
    chiuso_con_prezzi["status"] = "CLOSED"
    broker.update_market_book(chiuso_con_prezzi)

    r = broker.place_bet(market_id="2.1", selection_id=11, side="BACK", price=3.0, size=10.0)

    report = r["instructionReports"][0]
    assert report["status"] == "FAILURE"
    assert report["sizeMatched"] == 0.0
    assert broker.state.orders[report["betId"]].status == "LAPSED"
    assert broker.get_account_funds()["available"] == pytest.approx(saldo)
    assert broker.get_account_funds()["exposure"] == pytest.approx(0.0)
    assert broker.get_current_orders() == []


@pytest.mark.integration
def test_settlement_sim_commissione_fuori_policy_fail_closed():
    """Commissione SIM diversa dal 4,5% di policy: il settlement non parte e
    le posizioni restano aperte, invece di regolare con un'aliquota sbagliata."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=5.0)
    _tre_selezioni(broker)
    prima = _fotografia(broker)

    esito = broker.update_market_book(_chiuso("2.1", {11: "WINNER", 22: "LOSER", 33: "LOSER"}))

    assert esito["settlement_status"] == "COMMISSION_POLICY_VIOLATION"
    assert _fotografia(broker) == prima


@pytest.mark.integration
def test_settlement_sim_piu_vincitori_senza_numero_atteso_fail_closed():
    """Fugu Ultra sulla #485: due WINNER e nessun ``numberOfWinners`` non
    distinguono un dead heat da un mercato a piu' vincitori. Pagarli entrambi
    per intero gonfierebbe il PnL: nessun settlement."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    prima = _fotografia(broker)

    esito = broker.update_market_book(_chiuso("2.1", {11: "WINNER", 22: "WINNER", 33: "LOSER"}))

    assert esito["settlement_status"] == "DEAD_HEAT_UNSUPPORTED"
    assert _fotografia(broker) == prima


@pytest.mark.integration
def test_settlement_sim_dead_heat_dal_market_definition_dello_stream_fail_closed():
    """Nei book dello stream (``StreamingFeed``) ``numberOfWinners`` sta in
    ``marketDefinition``, non al livello alto del book."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)
    prima = _fotografia(broker)

    esito = broker.update_market_book(
        _chiuso("2.1", {11: "WINNER", 22: "WINNER", 33: "LOSER"},
                marketDefinition={"status": "CLOSED", "numberOfWinners": 1})
    )

    assert esito["settlement_status"] == "DEAD_HEAT_UNSUPPORTED"
    assert _fotografia(broker) == prima


@pytest.mark.integration
def test_settlement_sim_piu_vincitori_attesi_regolati():
    """Mercato a due vincitori dichiarati (``marketDefinition``): nessun
    dead heat. Calcolo indipendente: BACK 10 @ 3.0 sulla 11 vince +20; LAY
    5 @ 2.0 sulla 22 che vince -5; BACK 4 @ 5.0 sulla 33 perde -4. Lordo +11,
    commissione 0,495, netto 10,505."""
    broker = SimulationBroker(starting_balance=1000.0, commission_pct=4.5)
    _tre_selezioni(broker)

    esito = broker.update_market_book(
        _chiuso("2.1", {11: "WINNER", 22: "WINNER", 33: "LOSER"},
                marketDefinition={"numberOfWinners": 2})
    )

    assert esito["settlement_status"] == "SETTLED"
    assert broker.list_settlements()[0]["net_pnl"] == pytest.approx(10.505)
    assert broker.get_account_funds()["available"] == pytest.approx(1010.505)
