"""PR28-d, rettifica owner 18/10/2026: nessun cap monetario fisso di default.

I quattro limiti assoluti in euro (`max_order_exposure`, `max_market_exposure`,
`max_event_exposure_abs`, `max_drawdown_abs`) sono OPZIONALI:

- chiave assente / None / "" (GUI o DB) => None = NON attivo, mai un numero;
- valore presente ma invalido (0, negativo, NaN, bool, testo) => blocco
  esplicito `<nome>_non_valido` di ogni nuovo ordine (mai "nessun limite");
- valore valido configurato dall'utente => applicato esattamente, come prima;
- chiave legacy senza prefisso `roserpina.` => letta (e loggata);
- `max_drawdown_abs` configurato => LOCKDOWN come oggi; assente => nessun
  lockdown da questo cap (restano i limiti percentuali del Money Management).

Catena reale: SettingsService + Database SQLite su disco => RuntimeController
=> Money Management vero => TradingEngine => SimulationBroker. Nessun mock sui
cap. Registro audit: PF-CAP-01..04, PF-CAP-10.
"""
from __future__ import annotations

import logging
import math

import pytest

from core.system_state import RoserpinaConfig, RuntimeMode
from database import Database
from services.setting_service import SettingsService
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal
from tests.integration.test_runtime_controller_auto_trade_mm import (
    _canonical_close_payload,
    _make_controller,
)

pytestmark = [pytest.mark.integration, pytest.mark.safety]

_CAPS = ("max_order_exposure", "max_market_exposure", "max_event_exposure_abs", "max_drawdown_abs")


def _real_settings(tmp_path):
    """SettingsService REALE su DB SQLite fresco (nessuna chiave di cap)."""
    db = Database(str(tmp_path / "rectification.db"))
    return db, SettingsService(db)


def _catena_reale(tmp_path, *, bankroll=100.0, **persisted):
    """Catena SIM con config caricata dal loader reale (non da RoserpinaConfig())."""
    db, service = _real_settings(tmp_path)
    if persisted:
        db.save_settings(persisted)
    c = _catena(tmp_path)
    c.rc.settings_service = service
    c.rc.reload_config()  # passa da load_roserpina_config() vero
    c.rc.risk_desk.bankroll_current = bankroll
    c.rc.risk_desk.equity_peak = bankroll
    c.db_settings = db
    return c


# --------------------------------------------------------------------------
# PF-CAP-01..04: default None, mai un numero
# --------------------------------------------------------------------------
def test_fresh_config_without_cap_keys_loads_all_four_as_none(tmp_path):
    _db, service = _real_settings(tmp_path)
    cfg = service.load_roserpina_config()
    for field in _CAPS:
        assert getattr(cfg, field) is None, field
    # Il dataclass non deve reintrodurre un numero di default.
    for field in _CAPS:
        assert getattr(RoserpinaConfig(), field) is None, field


def test_sim_bankroll_100_price_2_mm_stake_3_is_submitted_without_any_cap(tmp_path):
    """Saldo 100 €, quota 2.0, target 3 % => stake MM = 100*3%/(2.0-1) = 3.0 €.

    Con i vecchi default (1 €/ordine) il segnale veniva rifiutato con
    `max_order_exposure_exceeded:limit=1.0€`; senza cap configurati deve
    arrivare al broker SIM con lo stake richiesto esatto (3.0)."""
    c = _catena_reale(tmp_path)
    c.rc._on_signal_received(_signal())  # nessuno stake fisso: decide il MM

    assert _rifiuti(c) == []
    cmds = c.bus.payloads("CMD_QUICK_BET")
    assert len(cmds) == 1
    assert cmds[0]["stake"] == pytest.approx(3.0)
    assert len(c.broker.state.orders) == 1
    assert c.rc.mode != RuntimeMode.LOCKDOWN


def test_absent_drawdown_cap_does_not_lockdown_but_percent_limits_still_apply(tmp_path):
    """Drawdown 12 € su picco 100 (12 %): sotto il lockdown % (20 %), ma sopra
    il vecchio default assoluto (10 €). Assente => nessun lockdown da questo cap."""
    c = _catena_reale(tmp_path, bankroll=100.0)
    c.rc.risk_desk.equity_peak = 100.0
    c.rc.risk_desk.bankroll_current = 88.0
    c.rc._on_signal_received(_signal())

    assert not any("max_drawdown_abs" in r for r in _rifiuti(c))
    assert c.rc.mode != RuntimeMode.LOCKDOWN
    assert len(c.broker.state.orders) == 1


def test_configured_drawdown_cap_keeps_lockdown_semantics(tmp_path):
    c = _catena_reale(tmp_path, bankroll=100.0, **{"roserpina.max_drawdown_abs": 10.0})
    c.rc.risk_desk.equity_peak = 100.0
    c.rc.risk_desk.bankroll_current = 88.0
    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[-1] == "max_drawdown_abs_active"
    assert c.rc.mode == RuntimeMode.LOCKDOWN


# --------------------------------------------------------------------------
# Cap opzionale configurato => applicato esattamente (protezione conservata)
# --------------------------------------------------------------------------
def test_explicit_max_order_exposure_still_rejects_mm_stake_above_cap(tmp_path):
    c = _catena_reale(tmp_path, **{"roserpina.max_order_exposure": 1.0})
    assert c.rc.config.max_order_exposure == 1.0
    c.rc._on_signal_received(_signal())  # MM chiede 3.0 €

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_order_exposure_exceeded:limit=1.0")


def test_explicit_market_and_event_caps_still_apply_from_persisted_config(tmp_path):
    c = _catena_reale(tmp_path, **{
        "roserpina.max_market_exposure": 2.0,
        "roserpina.max_event_exposure_abs": 2.0,
    })
    assert c.rc.config.max_market_exposure == 2.0
    assert c.rc.config.max_event_exposure_abs == 2.0
    c.rc._on_signal_received(_signal())  # 3.0 € > 2.0 €

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[0].startswith("max_market_exposure_exceeded")


# --------------------------------------------------------------------------
# PF-CAP-10: valore presente ma invalido => blocco esplicito, mai "off"
# --------------------------------------------------------------------------
@pytest.mark.parametrize("field", _CAPS)
@pytest.mark.parametrize("bad", [0, -1, "x", True, float("nan")])
def test_invalid_present_persisted_value_blocks_new_orders(tmp_path, field, bad):
    c = _catena_reale(tmp_path, **{f"roserpina.{field}": bad})
    loaded = getattr(c.rc.config, field)
    assert loaded is not None, "un valore presente non deve diventare opt-out"

    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert _rifiuti(c)[-1] == f"{field}_non_valido"


@pytest.mark.parametrize("field", _CAPS)
def test_invalid_present_value_blocks_auto_next_trade(field):
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    setattr(rc.config, field, 0.0)
    rc._on_close_position(_canonical_close_payload(
        auto_trade_enabled=True, cycle_executor_enabled=True,
        mm_context={
            "cycle_active": True, "cycle_id": f"cycle-invalid-{field}",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {"market_id": "1.234", "selection_id": 8, "price": 2.0,
                            "side": "BACK", "event_name": "Roma v Milan"},
        },
    ))
    assert [e for e in bus.events if e[0] == "CMD_QUICK_BET"] == []
    assert rc._last_auto_trade_result["reason"] == f"{field}_non_valido"


def test_auto_next_trade_with_no_caps_configured_is_risk_approved():
    rc, bus = _make_controller(responses=[{"available": 150.0}])
    rc.mode = RuntimeMode.ACTIVE
    rc.risk_desk.sync_bankroll(100.0)
    for field in _CAPS:
        assert getattr(rc.config, field) is None
    rc._on_close_position(_canonical_close_payload(
        auto_trade_enabled=True, cycle_executor_enabled=True,
        mm_context={
            "cycle_active": True, "cycle_id": "cycle-no-caps",
            "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False},
            "next_signal": {"market_id": "1.234", "selection_id": 8, "price": 2.0,
                            "side": "BACK", "event_name": "Roma v Milan"},
        },
    ))
    assert rc._last_auto_trade_result["risk_status"] == "RISK_APPROVED"
    assert len([e for e in bus.events if e[0] == "CMD_QUICK_BET"]) == 1


# --------------------------------------------------------------------------
# PF-CAP-10: "" (GUI / DB legacy) e None => non attivo; persistenza; legacy
# --------------------------------------------------------------------------
@pytest.mark.parametrize("field", _CAPS)
@pytest.mark.parametrize("blank", ["", "   ", None])
def test_blank_or_null_persisted_value_means_not_active(tmp_path, field, blank):
    db, service = _real_settings(tmp_path)
    db.save_settings({f"roserpina.{field}": blank})
    assert getattr(service.load_roserpina_config(), field) is None


@pytest.mark.parametrize("field", _CAPS)
def test_gui_blank_field_parses_to_none(field):
    import mini_gui

    assert mini_gui.MiniPickfairGUI._parse_hard_stop("", field) is None
    assert mini_gui.MiniPickfairGUI._hard_stop_to_str(None) == ""


def test_gui_initial_cap_fields_are_blank(monkeypatch):
    """Le var iniziali della GUI non devono proporre 1/2/2/10 €."""
    import os
    import sys

    import mini_gui

    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "integration"))
    from test_mini_gui_integration import _install_mini_gui_fakes  # noqa: E402

    _install_mini_gui_fakes(monkeypatch)
    # Senza il load iniziale restano i valori di _build_vars.
    monkeypatch.setattr(mini_gui.MiniPickfairGUI, "_load_initial_settings", lambda self: None)
    app = mini_gui.MiniPickfairGUI(test_mode=True)
    try:
        assert app.rs_max_order_exposure_var.get() == ""
        assert app.rs_max_market_exposure_var.get() == ""
        assert app.rs_max_event_exposure_abs_var.get() == ""
        assert app.rs_max_drawdown_abs_var.get() == ""
    finally:
        app.destroy()


def test_user_set_valid_values_persist_and_survive_reload_and_blank_save_deactivates(tmp_path):
    db, service = _real_settings(tmp_path)
    cfg = RoserpinaConfig(
        max_order_exposure=1.5, max_market_exposure=2.5,
        max_event_exposure_abs=3.5, max_drawdown_abs=12.5,
    )
    service.save_roserpina_config(cfg)

    reloaded = SettingsService(Database(str(tmp_path / "rectification.db"))).load_roserpina_config()
    assert (reloaded.max_order_exposure, reloaded.max_market_exposure,
            reloaded.max_event_exposure_abs, reloaded.max_drawdown_abs) == (1.5, 2.5, 3.5, 12.5)

    # PF-CAP-10b (decisione owner 18/10, riesame E1-a): campo vuoto = limite NON attivo
    # anche dopo il salvataggio. Il save con i quattro cap a None li disattiva davvero
    # (prima il valore persistito sopravviveva e l'etichetta "vuoto = non attivo" mentiva).
    service.save_roserpina_config(RoserpinaConfig())
    again = service.load_roserpina_config()
    assert (again.max_order_exposure, again.max_market_exposure,
            again.max_event_exposure_abs, again.max_drawdown_abs) == (None, None, None, None)


@pytest.mark.parametrize("field", _CAPS)
def test_legacy_prefixless_key_is_read_and_logged(tmp_path, field, caplog):
    db, service = _real_settings(tmp_path)
    db.save_settings({field: 7.0})
    with caplog.at_level(logging.INFO, logger="services.setting_service"):
        cfg = service.load_roserpina_config()
    assert getattr(cfg, field) == 7.0
    assert any(field in rec.getMessage() and "legacy" in rec.getMessage().lower()
               for rec in caplog.records), caplog.text


def test_prefixed_key_wins_over_legacy_key(tmp_path):
    db, service = _real_settings(tmp_path)
    db.save_settings({"max_order_exposure": 7.0, "roserpina.max_order_exposure": 1.25})
    assert service.load_roserpina_config().max_order_exposure == 1.25


def test_no_required_absolute_cap_loader_remains():
    """Il loader con default numerico non deve esistere piu' (nessun fallback nascosto)."""
    assert not hasattr(SettingsService, "_required_absolute_cap_value")


@pytest.mark.parametrize("field", _CAPS)
def test_text_persisted_value_loads_as_nan_not_none(tmp_path, field):
    db, service = _real_settings(tmp_path)
    db.save_settings({f"roserpina.{field}": "dieci euro"})
    value = getattr(service.load_roserpina_config(), field)
    assert value is not None and math.isnan(value)


# --------------------------------------------------------------------------
# PF-CAP-10b: campo GUI vuoto => il cap persistito viene DISATTIVATO al salvataggio
# --------------------------------------------------------------------------
@pytest.mark.parametrize("field", _CAPS)
def test_blank_save_deactivates_a_previously_persisted_cap(tmp_path, field):
    db, service = _real_settings(tmp_path)
    service.save_roserpina_config(RoserpinaConfig(**{field: 1.0}))
    assert getattr(service.load_roserpina_config(), field) == 1.0
    service.save_roserpina_config(RoserpinaConfig(**{field: None}))      # campo vuoto in GUI
    reloaded = SettingsService(Database(str(tmp_path / "rectification.db"))).load_roserpina_config()
    assert getattr(reloaded, field) is None, "campo vuoto: il limite persistito deve risultare non attivo"


@pytest.mark.parametrize("field", _CAPS)
def test_blank_save_also_shadows_a_legacy_prefixless_key(tmp_path, field):
    db, service = _real_settings(tmp_path)
    db.save_settings({field: 1.0})                                       # chiave legacy senza prefisso
    assert getattr(service.load_roserpina_config(), field) == 1.0
    service.save_roserpina_config(RoserpinaConfig(**{field: None}))
    assert getattr(service.load_roserpina_config(), field) is None


def test_explicit_cap_is_rewritten_exactly_and_other_hard_stops_keep_preserve_semantics(tmp_path):
    db, service = _real_settings(tmp_path)
    service.save_roserpina_config(RoserpinaConfig(max_order_exposure=1.0, max_daily_loss=50.0))
    service.save_roserpina_config(RoserpinaConfig(max_order_exposure=4.25, max_daily_loss=None))
    cfg = service.load_roserpina_config()
    assert cfg.max_order_exposure == 4.25
    # gli hard-stop di perdita/esposizione (non cap monetari della rettifica) conservano la
    # semantica documentata "vuoto preserva il persistito": un vuoto non rimuove una protezione.
    assert cfg.max_daily_loss == 50.0


def test_blank_save_of_order_cap_lets_the_mm_stake_through(tmp_path):
    """Cap 1 € persistito: lo stake MM 3 € e' rifiutato; dopo il salvataggio con il campo
    vuoto e il reload della config lo stesso segnale arriva al broker con 3.0 € esatti."""
    c = _catena_reale(tmp_path, **{"roserpina.max_order_exposure": 1.0})
    c.rc._on_signal_received(_signal())
    assert _rifiuti(c)[0].startswith("max_order_exposure_exceeded:limit=1.0")
    assert c.broker.state.orders == {}

    c.rc.settings_service.save_roserpina_config(
        RoserpinaConfig(**{f: getattr(c.rc.config, f) for f in ("target_profit_cycle_pct", "max_single_bet_pct",
                                                               "max_total_exposure_pct", "max_event_exposure_pct")},
                        max_order_exposure=None))
    c.rc.reload_config()
    c.rc.risk_desk.bankroll_current = 100.0
    c.rc.risk_desk.equity_peak = 100.0
    assert c.rc.config.max_order_exposure is None
    c.rc._on_signal_received(_signal(received_at="2026-10-08T10:00:05.000000+00:00"))
    cmds = c.bus.payloads("CMD_QUICK_BET")
    assert len(cmds) == 1 and cmds[0]["stake"] == pytest.approx(3.0)


# --------------------------------------------------------------------------
# Rettifica owner 19/10/2026 (chiarimento definitivo): anche max_stake_abs (RSR-15) e max_win (RSR-26),
# tetti monetari assoluti preesistenti, sono opzionali e DISATTIVATI di default.
# --------------------------------------------------------------------------
_LEGACY_CAPS = ("max_stake_abs", "max_win")


def test_preexisting_monetary_caps_default_to_not_active(tmp_path):
    assert RoserpinaConfig().max_stake_abs is None
    assert RoserpinaConfig().max_win is None
    _db, service = _real_settings(tmp_path)
    cfg = service.load_roserpina_config()
    assert cfg.max_stake_abs is None and cfg.max_win is None
    import trading_config

    assert trading_config.MAX_WIN is None


@pytest.mark.parametrize("field", _LEGACY_CAPS)
def test_legacy_default_10000_persisted_before_the_rectification_is_not_active(tmp_path, field):
    """Il vecchio default 10000.0 veniva scritto nel DB a ogni salvataggio della tab: senza il marcatore della
    rettifica e' il default storico, non una scelta dell'utente."""
    db, service = _real_settings(tmp_path)
    db.save_settings({f"roserpina.{field}": 10000.0})
    assert getattr(service.load_roserpina_config(), field) is None


@pytest.mark.parametrize("field", _LEGACY_CAPS)
def test_value_saved_after_the_rectification_is_honoured_even_if_10000(tmp_path, field):
    db, service = _real_settings(tmp_path)
    cfg = service.load_roserpina_config()
    setattr(cfg, field, 10000.0)
    service.save_roserpina_config(cfg)
    assert getattr(service.load_roserpina_config(), field) == 10000.0
    setattr(cfg, field, None)
    service.save_roserpina_config(cfg)                    # vuoto = disattiva davvero
    assert getattr(service.load_roserpina_config(), field) is None


@pytest.mark.parametrize("field", _LEGACY_CAPS)
def test_legacy_non_default_value_stays_active(tmp_path, field):
    """Un valore diverso dal default storico e' una scelta dell'utente: resta attivo."""
    db, service = _real_settings(tmp_path)
    db.save_settings({f"roserpina.{field}": 250.0})
    assert getattr(service.load_roserpina_config(), field) == 250.0


def test_gui_initial_legacy_cap_fields_are_blank(monkeypatch):
    import os
    import sys

    import mini_gui

    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "integration"))
    from test_mini_gui_integration import _install_mini_gui_fakes  # noqa: E402

    _install_mini_gui_fakes(monkeypatch)
    monkeypatch.setattr(mini_gui.MiniPickfairGUI, "_load_initial_settings", lambda self: None)
    app = mini_gui.MiniPickfairGUI(test_mode=True)
    try:
        assert app.rs_max_abs_var.get() == ""
        assert app.rs_max_win_var.get() == ""
    finally:
        app.destroy()


def test_money_management_without_max_stake_abs_uses_only_the_approved_percentages():
    from core.money_management import RoserpinaMoneyManagement

    mm = RoserpinaMoneyManagement(RoserpinaConfig())
    assert mm._max_single_stake_abs(1_000_000.0) == pytest.approx(1_000_000.0 * RoserpinaConfig().max_single_bet_pct / 100)
    mm_capped = RoserpinaMoneyManagement(RoserpinaConfig(max_stake_abs=50.0))
    assert mm_capped._max_single_stake_abs(1_000_000.0) == 50.0


@pytest.mark.parametrize("bad", [0.0, -1.0, float("nan"), float("inf")])
def test_money_management_max_stake_abs_configured_but_invalid_allows_no_stake(bad):
    from core.money_management import RoserpinaMoneyManagement

    assert RoserpinaMoneyManagement(RoserpinaConfig(max_stake_abs=bad))._max_single_stake_abs(1000.0) == 0.0


def test_sim_order_above_the_old_10000_ceiling_is_sized_by_the_approved_strategy(tmp_path):
    """Catena SIM reale con il loader vero: nessun tetto da 10000 EUR attivo di default. Lo stake e' quello
    del Money Management sul bankroll (percentuali approvate, qui 3% di 500.000 = 15.000), non tagliato da un
    cap arbitrario. Con il tetto configurato dall'utente lo stesso ordine viene invece rifiutato."""
    c = _catena_reale(tmp_path, bankroll=500_000.0)
    c.rc._on_signal_received(_signal(price=2.0))
    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    (order,) = c.broker.state.orders.values()
    assert order.size == pytest.approx(15000.0)

    capped = _catena_reale(tmp_path / "capped", bankroll=500_000.0,
                           **{"roserpina.max_stake_abs": 10000.0,
                              "roserpina.monetary_caps_rectified_20261018": 1})
    capped.rc._on_signal_received(_signal(price=2.0))
    assert capped.broker.state.orders == {}


@pytest.mark.parametrize("field", _LEGACY_CAPS)
@pytest.mark.parametrize("legacy", [0.0, -1.0])
def test_legacy_non_positive_value_saved_before_the_rectification_is_not_active(tmp_path, field, legacy):
    """Verifica indipendente P2-5(b): prima della rettifica 0 valeva "nessun tetto" (max_stake_abs) o ricadeva
    sul default (max_win). Migrato a non attivo, non a "blocca ogni ordine"."""
    db, service = _real_settings(tmp_path)
    db.save_settings({f"roserpina.{field}": legacy})
    assert getattr(service.load_roserpina_config(), field) is None


@pytest.mark.parametrize("field", _LEGACY_CAPS)
def test_non_positive_value_saved_after_the_rectification_stays_invalid(tmp_path, field):
    """Dopo la rettifica un valore <= 0 e' una configurazione non valida: resta presente e blocca."""
    db, service = _real_settings(tmp_path)
    db.save_settings({f"roserpina.{field}": 0.0, "roserpina.monetary_caps_rectified_20261018": 1})
    assert getattr(service.load_roserpina_config(), field) == 0.0
