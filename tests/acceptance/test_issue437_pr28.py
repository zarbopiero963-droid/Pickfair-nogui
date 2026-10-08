"""#461 PR28 (PKG-P24-B), first slice: owner limits from config to transport.

This slice covers LAY liability, `reload_config` and auto-next.

Card contract: every owner cap measures the RISK of the order with a single
formula, and the comparison holds at the exact threshold.
- BACK risks the stake.
- LAY risks the liability `stake * (price - 1)`.
- The threshold is compared with a tolerance of +-epsilon.
- A config patch (GUI save -> `reload_config`) does not wipe tables, exposure,
  loss memory or the reconciliation engine.

Phase 0 on main feb2302, reproduced with real classes:
- LAY in the runtime and in money management. MM, the A2 cap and the table
  exposure counted the STAKE: a LAY of 1.5 at 21 (liability 30) took 1.5 of
  exposure, and a cap of 10 let it through. No consumer read the side.
- `reload_config` (GUI save while the bot is running) built a new
  `TableManager`. ACTIVE tables, exposure and recovery memory went back to zero,
  so the aggregate caps restarted from 0. It also rebuilt the
  `ReconciliationEngine`, with a new batch lock manager.
- Auto-next (next step after a settlement) applied neither the A2 cap nor the
  event exposure (it passed `event_current_exposure=0.0`), and it activated the
  table with the stake.
- Exact threshold: 0.1 + 0.2 = 0.30000000000000004 > 0.3 denied an order that
  sits exactly at the cap. RiskGate did the same for a LAY 2 @ 1.1 against
  MAX_WIN 0.2.
- Already correct, proven by a test: A1 drawdown NaN/text in LIVE is stopped by
  the deploy gate (LIVE_HARD_STOP_CONFIG_INVALID) before the signal.
"""
from __future__ import annotations

import pytest

from core.risk_gate import RiskGate
from core.system_state import RoserpinaConfig, RuntimeMode
from core import validators
from core.money_management import RoserpinaMoneyManagement
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti, _tavoli_occupati
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]


# ==========================================================================
# 0. Formula unica e soglia +-epsilon
# ==========================================================================
@pytest.mark.parametrize("lato,stake,quota,atteso", [
    ("BACK", 5.0, 3.0, 5.0),
    ("LAY", 5.0, 3.0, 10.0),
    ("LAY", 1.5, 21.0, 30.0),
    ("lay", 2.0, 1.1, 0.2),
    ("BACK", 2.0, 1.1, 2.0),
])
def test_pass_esposizione_ordine_formula_unica(lato, stake, quota, atteso):
    assert validators.order_exposure(lato, stake, quota) == pytest.approx(atteso, abs=1e-12)


@pytest.mark.parametrize("lato", ["", None, "BOTH", True])
def test_block_esposizione_lato_sconosciuto(lato):
    with pytest.raises(ValueError):
        validators.order_exposure(lato, 2.0, 3.0)


def test_pass_soglia_esatta_con_epsilon_e_superamento():
    assert validators.exceeds_cap(0.1 + 0.2, 0.3) is False
    assert validators.exceeds_cap(2.0 * (1.1 - 1.0), 0.2) is False
    assert validators.exceeds_cap(0.31, 0.3) is True
    assert validators.exceeds_cap(10.000001, 10.0) is True


def _gate(max_win):
    class _Cfg:
        MIN_STAKE = 0.10
        MIN_PRICE = 1.02
        MAX_WIN = max_win
        BOOK_BLOCK = 110.0
        LIQUIDITY_MULTIPLIER = 3.0
        MIN_LIQUIDITY_ABSOLUTE = 50.0
        LIQUIDITY_GUARD_ENABLED = False
        LIQUIDITY_WARNING_ONLY = True
    return RiskGate(config=_Cfg())


def test_pass_risk_gate_lay_alla_soglia_esatta():
    """Parity with the consumers: liability 0.1*(4-1) = 0.30000000000000004
    is exactly MAX_WIN 0.3, not above it (the win of the LAY is 0.1)."""
    esito = _gate(0.3).check({"stake": 0.1, "price": 4.0, "bet_type": "LAY"})
    assert esito["allowed"] is True, esito


def test_block_risk_gate_lay_oltre_la_soglia():
    esito = _gate(0.3).check({"stake": 0.1, "price": 4.1, "bet_type": "LAY"})
    assert esito["allowed"] is False
    assert esito["reason"] == "RISK_MAX_EXPOSURE_EXCEEDED"


# ==========================================================================
# 1. Money management: la LAY si misura sulla liability
# ==========================================================================
def _mm(**cfg):
    return RoserpinaMoneyManagement(RoserpinaConfig(**cfg))


def _calcola(mm, *, signal, totale=0.0, evento=0.0):
    return mm.calculate(
        signal=signal,
        bankroll_current=1000.0,
        equity_peak=1000.0,
        current_total_exposure=totale,
        event_current_exposure=evento,
        table={"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
    )


def test_block_mm_lay_liability_oltre_il_tetto_singolo():
    # single cap 18% of 1000 = 180; LAY 10 @ 21 -> liability 200
    decisione = _calcola(_mm(), signal={"stake": 10.0, "price": 21.0, "bet_type": "LAY"})
    assert decisione.approved is False
    assert decisione.reason == "supera_max_single_bet"


def test_pass_mm_lay_liability_esattamente_al_tetto_singolo():
    decisione = _calcola(_mm(), signal={"stake": 9.0, "price": 21.0, "bet_type": "LAY"})
    assert decisione.approved is True
    assert decisione.recommended_stake == pytest.approx(9.0, abs=1e-12)


def test_pass_mm_back_stesso_stake_resta_ammesso():
    decisione = _calcola(_mm(), signal={"stake": 10.0, "price": 21.0, "bet_type": "BACK"})
    assert decisione.approved is True
    assert decisione.recommended_stake == pytest.approx(10.0, abs=1e-12)


@pytest.mark.parametrize("campo,base,motivo", [
    ("totale", 340.0, "supera_max_total_exposure"),   # cap 35% = 350
    ("evento", 170.0, "supera_max_event_exposure"),   # cap 18% = 180
])
def test_block_mm_lay_liability_supera_gli_aggregati(campo, base, motivo):
    # LAY 2 @ 6.5 -> liability 11: 340+11 = 351 > 350 ; 170+11 = 181 > 180
    decisione = _calcola(_mm(), signal={"stake": 2.0, "price": 6.5, "bet_type": "LAY"}, **{campo: base})
    assert decisione.approved is False
    assert decisione.reason == motivo


@pytest.mark.parametrize("campo,base", [("totale", 340.0), ("evento", 170.0)])
def test_pass_mm_lay_liability_esattamente_agli_aggregati(campo, base):
    # LAY 2 @ 6 -> liability 10: exactly 350 / 180
    decisione = _calcola(_mm(), signal={"stake": 2.0, "price": 6.0, "bet_type": "LAY"}, **{campo: base})
    assert decisione.approved is True


# ==========================================================================
# 2. Runtime: cap A2 e esposizione del tavolo con la liability
# ==========================================================================
def _lay(**extra):
    return _signal(bet_type="LAY", **extra)


def test_block_runtime_lay_liability_oltre_max_open_exposure(tmp_path):
    c = _catena(tmp_path, max_open_exposure=10.0)

    c.rc._on_signal_received(_lay(price=6.5, stake=2.0))   # liability 11

    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_open_exposure_exceeded")
    assert _tavoli_occupati(c) == []
    assert c.rc.duplication_guard._active == {}


def test_pass_runtime_lay_liability_esattamente_al_cap(tmp_path):
    c = _catena(tmp_path, max_open_exposure=10.0)

    c.rc._on_signal_received(_lay(price=6.0, stake=2.0))   # liability 10

    assert len(c.broker.state.orders) == 1
    assert _rifiuti(c) == []


def test_pass_runtime_tavolo_lay_porta_la_liability(tmp_path):
    c = _catena(tmp_path)

    c.rc._on_signal_received(_lay(price=21.0, stake=1.5))

    tavoli = [t for t in c.rc.table_manager._tables.values() if t.status != "FREE"]
    assert [t.current_exposure for t in tavoli] == [pytest.approx(30.0)]
    assert c.rc.table_manager.total_exposure() == pytest.approx(30.0)


def test_pass_runtime_cap_alla_soglia_esatta_con_somma_float(tmp_path):
    """0.1 already open + 0.2 = 0.30000000000000004: it is exactly the cap of
    0.3 and must not be denied."""
    c = _catena(tmp_path, max_open_exposure=0.3)
    altro = list(c.rc.table_manager._tables.values())[-1]
    altro.status = "ACTIVE"
    altro.current_event_key = "altro-evento"
    altro.current_exposure = 0.1

    c.rc._on_signal_received(_signal(stake=0.2))

    assert len(c.broker.state.orders) == 1, _rifiuti(c)


def test_block_runtime_cap_superato_di_un_centesimo(tmp_path):
    c = _catena(tmp_path, max_open_exposure=0.3)
    altro = list(c.rc.table_manager._tables.values())[-1]
    altro.status = "ACTIVE"
    altro.current_event_key = "altro-evento"
    altro.current_exposure = 0.1

    c.rc._on_signal_received(_signal(stake=0.21))

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_open_exposure_exceeded")


def _best_price(c, prezzo):
    def _applica(payload):
        payload["price"] = prezzo
        return True
    c.rc._apply_direct_best_price = _applica


def test_block_best_price_non_aumenta_la_liability_approvata(tmp_path):
    """The best price is applied after the caps: a LAY 2@5 (liability 8)
    rewritten to 6.5 would send 11 > the approved 8."""
    c = _catena(tmp_path, max_open_exposure=10.0)
    _best_price(c, 6.5)

    c.rc._on_signal_received(_lay(price=5.0, stake=2.0))

    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert _rifiuti(c) == ["esposizione_cresciuta_dopo_best_price"]
    assert _tavoli_occupati(c) == []
    assert c.rc.duplication_guard._active == {}


def test_pass_best_price_che_riduce_la_liability_resta_ammesso(tmp_path):
    c = _catena(tmp_path, max_open_exposure=10.0)
    _best_price(c, 4.5)

    c.rc._on_signal_received(_lay(price=5.0, stake=2.0))

    assert len(c.broker.state.orders) == 1, _rifiuti(c)


# ==========================================================================
# 3. reload_config: la patch di config non azzera lo stato
# ==========================================================================
def _stato_tavoli(rc):
    return sorted(
        (t.table_id, t.status, t.current_event_key, t.current_exposure, t.loss_amount, t.in_recovery)
        for t in rc.table_manager._tables.values()
    )


def test_block_reload_config_non_azzera_tavoli_esposizione_e_recovery(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_lay(price=21.0, stake=1.5))
    recupero = c.rc.table_manager._tables[3]
    recupero.loss_amount = 7.5
    recupero.in_recovery = True
    recupero.status = "RECOVERY"
    prima = _stato_tavoli(c.rc)
    manager = c.rc.table_manager
    riconciliazione = c.rc.reconciliation_engine

    c.rc.reload_config()

    assert _stato_tavoli(c.rc) == prima
    assert c.rc.table_manager is manager
    assert c.rc.reconciliation_engine is riconciliazione
    assert c.rc.reconciliation_engine.table_manager is c.rc.table_manager
    assert c.rc.table_manager.total_exposure() == pytest.approx(30.0)


def test_block_reload_config_non_riapre_il_cap_aggregato(tmp_path):
    c = _catena(tmp_path, max_open_exposure=40.0)
    c.rc._on_signal_received(_lay(price=21.0, stake=1.5))      # 30 open
    assert len(c.broker.state.orders) == 1

    c.rc.reload_config()
    c.rc.config.max_open_exposure = 40.0
    c.rc._on_signal_received(_lay(price=21.0, stake=1.5, market_id="1.999", event="Gamma v Delta"))

    assert len(c.broker.state.orders) == 1, "dopo il reload il cap e' ripartito da 0"
    assert _rifiuti(c)[-1].startswith("max_open_exposure_exceeded")


def test_pass_reload_config_applica_i_nuovi_limiti(tmp_path):
    c = _catena(tmp_path)

    class _NuoviLimiti:
        def load_roserpina_config(self):
            cfg = RoserpinaConfig()
            cfg.max_open_exposure = 12.5
            cfg.table_count = 7
            return cfg

    c.rc.settings_service = _NuoviLimiti()
    c.rc.reload_config()

    assert c.rc.config.max_open_exposure == 12.5
    assert c.rc.mm.config.max_open_exposure == 12.5
    assert sorted(c.rc.table_manager._tables) == [1, 2, 3, 4, 5, 6, 7]


def test_block_reload_config_con_meno_tavoli_non_scarta_quelli_occupati(tmp_path):
    c = _catena(tmp_path)
    ultimo = c.rc.table_manager._tables[5]
    ultimo.status = "ACTIVE"
    ultimo.current_event_key = "evento-aperto"
    ultimo.current_exposure = 4.0
    ultimo_recupero = c.rc.table_manager._tables[4]
    ultimo_recupero.loss_amount = 3.0
    ultimo_recupero.in_recovery = True
    ultimo_recupero.status = "RECOVERY"

    class _DueTavoli:
        def load_roserpina_config(self):
            cfg = RoserpinaConfig()
            cfg.table_count = 2
            return cfg

    c.rc.settings_service = _DueTavoli()
    c.rc.reload_config()

    tavoli = c.rc.table_manager._tables
    assert tavoli[5].current_exposure == 4.0 and tavoli[5].status == "ACTIVE"
    assert tavoli[4].loss_amount == 3.0 and tavoli[4].in_recovery is True
    assert 3 not in tavoli, "un tavolo libero e pulito oltre il nuovo numero resta"
    assert c.rc.table_manager.total_exposure() == pytest.approx(4.0)
    # A free table beyond the new number is not allocated again.
    for t in tavoli.values():
        if t.table_id > 2 and t.status == "FREE":
            pytest.fail(f"tavolo libero {t.table_id} oltre table_count")


# ==========================================================================
# 4. Auto-next dopo settlement: stessi limiti del segnale
# ==========================================================================
from tests.integration.test_runtime_controller_cycle_executor import (  # noqa: E402
    _canonical_close_payload,
    _make_controller,
)


def _auto_next(next_signal, *, cap=None):
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.config.max_open_exposure = cap
    rc._on_close_position(
        _canonical_close_payload(
            auto_trade_enabled=True,
            cycle_executor_enabled=True,
            mm_context={
                "cycle_active": True,
                "cycle_id": "cycle-pr28",
                "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": next_signal,
            },
        )
    )
    esito = [e for e in bus.events if e[0] == "AUTO_TRADE_MM_RESULT"][0][1]
    return rc, bus, esito


def test_block_auto_next_lay_oltre_max_open_exposure():
    rc, bus, esito = _auto_next(
        {"market_id": "1.2", "selection_id": 2, "price": 21.0, "stake": 1.0, "bet_type": "LAY"},
        cap=10.0,
    )
    assert esito["auto_trade_status"] != "AUTO_TRADE_SUBMITTED", esito
    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == []
    assert esito["reason"].startswith("max_open_exposure_exceeded")


def test_pass_auto_next_entro_il_cap_e_tavolo_con_liability():
    rc, bus, esito = _auto_next(
        {"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0, "bet_type": "LAY"},
        cap=10.0,
    )
    assert len([e for e in bus.events if e[0] == "CMD_QUICK_BET"]) == 1, esito
    assert rc.table_manager.get_table(1).current_exposure == pytest.approx(10.0)


def test_block_auto_next_cap_illeggibile_fail_closed():
    rc, bus, esito = _auto_next(
        {"market_id": "1.2", "selection_id": 2, "price": 2.0, "stake": 1.0, "bet_type": "BACK"},
        cap=float("nan"),
    )
    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == []
    assert esito["reason"] == "max_open_exposure_non_valido"


def test_block_auto_next_conta_l_esposizione_dell_evento():
    """It used to pass `event_current_exposure=0.0`: an event that is already
    open with 175 (cap 18% of 1000 = 180) plus a LAY with liability 10 must
    stop."""
    rc, bus = _make_controller(responses=[{"available": 1000.0}])
    rc.mode = RuntimeMode.ACTIVE
    segnale = {"market_id": "1.2", "selection_id": 2, "price": 6.0, "stake": 2.0, "bet_type": "LAY",
               "event": "Alpha v Beta"}
    chiave = rc.duplication_guard.build_event_key(segnale)
    aperto = rc.table_manager._tables[2]
    aperto.status = "ACTIVE"
    aperto.current_event_key = chiave
    aperto.current_exposure = 175.0
    rc._on_close_position(
        _canonical_close_payload(
            auto_trade_enabled=True,
            cycle_executor_enabled=True,
            mm_context={
                "cycle_active": True,
                "cycle_id": "cycle-pr28-evento",
                "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
                "next_signal": segnale,
            },
        )
    )
    esito = [e for e in bus.events if e[0] == "AUTO_TRADE_MM_RESULT"][0][1]
    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == [], esito
    assert esito["reason"] == "supera_max_event_exposure"


# ==========================================================================
# 5. A1 drawdown illeggibile in LIVE: gia' fermato dal deploy gate
# ==========================================================================
@pytest.mark.parametrize("valore", [float("nan"), "abc"])
def test_pass_gia_corretto_a1_drawdown_illeggibile_in_live_fermato_dal_gate(tmp_path, valore):
    c = _catena(tmp_path, max_drawdown_hard_stop_pct=valore)
    c.rc.execution_mode = "LIVE"
    c.rc.risk_desk.bankroll_current = 500.0
    c.rc.risk_desk.equity_peak = 1000.0

    c.rc._on_signal_received(_signal())

    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("deploy_gate_no_go")
    assert _tavoli_occupati(c) == []
    assert c.rc.duplication_guard._active == {}
