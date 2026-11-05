"""MCP-01m: trailing stop (F-03), percentuali di F-02, impostazioni non valide (F-18) sul RuntimeController reale.

Il core usato è la fusione LOCALE (mai spinta) del ponte con `claude/d-f03-trailing-stop` di Claude D (65a84f56,
che contiene F-02 e F-04). Stati, soglia e picco persistito vengono da `core.trailing_stop`: il ponte non
ricalcola nulla.
"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any

import pytest

from control_api.views import config_view, exposure_view, protections_view
from core import sl_tp_close, trailing_stop
from core.runtime_controller import RuntimeController


def activate(rc: RuntimeController, table_id: int, ref: str, **meta: Any) -> None:
    rc.table_manager.activate(table_id=table_id, event_key=f"ev-{table_id}", exposure=10.0, market_id="1.234",
                              selection_id=str(10 + table_id),
                              meta={"customer_ref": ref, "bet_type": "BACK", "event_name": "Juventus v Inter",
                                    **meta})


def trailing(rc: RuntimeController, table_id: int) -> dict[str, Any]:
    return {t["table_id"]: t for t in exposure_view(rc)["tables"]}[table_id]["sl_tp"]["trailing"]


def test_trailing_states_from_the_table_meta(runtime: RuntimeController) -> None:
    runtime.table_manager.resize(6)
    activate(runtime, 1, "r1")                                                   # nessuna percentuale
    activate(runtime, 2, "r2", trailing_stop_pct=0.0)                             # fuori dominio: spento
    activate(runtime, 3, "r3", trailing_stop_pct=20.0, trailing_peak=None)        # nessun profitto ancora
    activate(runtime, 4, "r4", trailing_stop_pct=20.0, trailing_peak="15.00")     # armato in memoria
    activate(runtime, 5, "r5", trailing_stop_pct=20.0, trailing_peak="15.00",
             sl_tp_triggered=trailing_stop.TRAILING_REASON)                       # scattato
    activate(runtime, 6, "r6", trailing_stop_pct=20.0, trailing_peak="garbage")   # illeggibile
    t = {i: trailing(runtime, i) for i in range(1, 7)}
    assert t[1] == {"state": "OFF", "pct": None, "peak": None, "threshold": None, "peak_source": "NONE"}
    assert t[2]["state"] == "OFF"
    assert (t[3]["state"], t[3]["pct"], t[3]["peak"]) == ("WAITING_PROFIT", "20", None)
    assert t[4] == {"state": "ARMED", "pct": "20", "peak": "15", "threshold": "12", "peak_source": "MEMORY"}
    assert str(trailing_stop.threshold(Decimal("15.00"), Decimal("20"))) == "12.00"  # la soglia è del core
    assert t[5]["state"] == "TRIGGERED" and t[5]["threshold"] == "12"
    assert t[6]["state"] == "UNKNOWN" and t[6]["peak"] is None


def test_trailing_trigger_never_changes_the_sl_tp_state(runtime: RuntimeController) -> None:
    activate(runtime, 1, "r1", trailing_stop_pct=10.0, trailing_peak="5",
             sl_tp_triggered=trailing_stop.TRAILING_REASON)
    activate(runtime, 2, "r2", stop_loss=2.5, sl_tp_triggered="STOP_LOSS_REACHED")
    tables = {t["table_id"]: t["sl_tp"] for t in exposure_view(runtime)["tables"]}
    assert (tables[1]["state"], tables[1]["trailing"]["state"]) == ("NOT_ARMED", "TRIGGERED")
    assert tables[2]["state"] == "TRIGGERED" and tables[2]["trailing"]["state"] == "OFF"


def test_persisted_peak_is_used_until_the_runtime_loads_it(runtime: RuntimeController) -> None:
    activate(runtime, 1, "r1", trailing_stop_pct=25.0)  # dopo un riavvio: picco non ancora nel meta
    runtime.db.save_settings({trailing_stop.PEAK_KEY_PREFIX + "r1":
                              trailing_stop.state_record(Decimal("8.00"), Decimal("25"), time.time())})
    t = trailing(runtime, 1)
    assert t == {"state": "ARMED", "pct": "25", "peak": "8", "threshold": "6", "peak_source": "PERSISTED"}
    runtime.db.save_settings({trailing_stop.PEAK_KEY_PREFIX + "r1": "{corrupt"})
    assert trailing(runtime, 1)["state"] == "UNKNOWN"  # il core non decide su un picco illeggibile
    runtime.db.save_settings({trailing_stop.PEAK_KEY_PREFIX + "r1": ""})
    assert trailing(runtime, 1)["state"] == "WAITING_PROFIT"


def test_memory_peak_wins_over_the_persisted_one(runtime: RuntimeController) -> None:
    activate(runtime, 1, "r1", trailing_stop_pct=25.0, trailing_peak="10")
    runtime.db.save_settings({trailing_stop.PEAK_KEY_PREFIX + "r1":
                              trailing_stop.state_record(Decimal("8.00"), Decimal("25"), time.time())})
    t = trailing(runtime, 1)
    assert (t["peak"], t["peak_source"]) == ("10", "MEMORY")


def test_trailing_close_is_reported_as_trailing_stop(runtime: RuntimeController) -> None:
    activate(runtime, 1, "r1", trailing_stop_pct=20.0)
    trig, why = sl_tp_close.parse_trigger(sl_tp_close.TRAILING_TRIGGERED, {
        "table_id": 1, "market_id": "1.234", "selection_id": "11", "side": "BACK", "pnl": 3.0,
        "reason": trailing_stop.TRAILING_REASON, "customer_ref": "r1", "source": "AUTO_MONITOR"})
    assert why == ""
    runtime.db.save_settings({trig.close_key: sl_tp_close.state_record(
        trig, sl_tp_close.STATE_ROUTED, bet_ids=["1"], at=time.time(), request_ids=["req-1"])})
    close = exposure_view(runtime)["sl_tp_closes"]["closes"][0]
    assert (close["trigger"], close["close_state"]) == ("TRAILING_STOP", "ROUTED")


def test_f02_percentages_in_get_config(runtime: RuntimeController) -> None:
    runtime.config.stop_loss_pct = 25.0
    runtime.config.trailing_stop_pct = None
    s = {x["key"]: x for x in config_view(runtime)["settings"]}
    assert s["stop_loss_pct"] == {"key": "stop_loss_pct", "kind": "DECIMAL", "state": "SET", "value": "25"}
    assert s["trailing_stop_pct"]["state"] == "NOT_SET"
    assert "invalid_settings" not in config_view(runtime)["unlisted"]


def test_f18_invalid_persisted_settings_and_protection(runtime: RuntimeController) -> None:
    view = config_view(runtime)
    assert view["invalid_persisted"] == [] and "invalid_persisted" not in view["unavailable"]
    assert protections_view(runtime)["items"]["config_validity"]["state"] == "ACTIVE"
    runtime.db.save_settings({"roserpina.profit_epsilon": "abc"})  # valore persistito illeggibile
    runtime.reload_config()
    assert "roserpina.profit_epsilon" in runtime.config.invalid_settings
    assert "roserpina.profit_epsilon" in config_view(runtime)["invalid_persisted"]
    p = protections_view(runtime)["items"]["config_validity"]
    assert (p["state"], p["reason"]) == ("TRIGGERED", "CONFIGURAZIONE_NON_VALIDA")


def test_f18_sensitive_or_foreign_names_are_only_counted(runtime: RuntimeController) -> None:
    runtime.config.invalid_settings = ("roserpina.max_win", "telegram.bot_token", "roserpina.api_secret",
                                       "Weird Key", "roserpina.max_win")
    view = config_view(runtime)
    assert view["invalid_persisted"] == ["roserpina.max_win"]
    assert view["redacted"] >= 3
    assert "bot_token" not in json.dumps(view) and "api_secret" not in json.dumps(view)


def test_without_f18_or_f03_the_states_are_declared(runtime: RuntimeController,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    del runtime.config.__dict__["invalid_settings"]
    view = config_view(runtime)
    assert view["invalid_persisted"] is None and view["unavailable"] == ["invalid_persisted"]
    p = protections_view(runtime)["items"]["config_validity"]
    assert (p["state"], p["reason"]) == ("UNAVAILABLE", "CORE_PENDING_F18")
    from control_api import views

    monkeypatch.setattr(views, "_trailing_module", lambda: None)
    activate(runtime, 1, "r1", trailing_stop_pct=20.0, trailing_peak="5")
    assert trailing(runtime, 1) == {"state": "NOT_IN_CORE", "pct": "20", "peak": None, "threshold": None,
                                    "peak_source": "NONE"}


def test_views_pass_the_server_contract(runtime: RuntimeController) -> None:
    from pickfair_control_api.contract import response_schema
    from pickfair_control_api.schema_check import validate

    activate(runtime, 1, "r1", trailing_stop_pct=20.0, trailing_peak="15")
    activate(runtime, 2, "r2", trailing_stop_pct=20.0)
    runtime.db.save_settings({trailing_stop.PEAK_KEY_PREFIX + "r2":
                              trailing_stop.state_record(Decimal("4.00"), Decimal("20"), time.time())})
    validate(exposure_view(runtime), response_schema("exposure"))
    validate(config_view(runtime), response_schema("config"))
    from control_api.views import status_view

    validate(status_view(runtime), response_schema("status"))
