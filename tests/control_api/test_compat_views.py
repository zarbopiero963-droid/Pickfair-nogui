"""MCP-01k: compatibilità del ponte con le correzioni di Claude D non ancora su ``main``.

1. P0 AMBIGUOUS (``claude/pickfair-core-audit-slu9su``): la riga AMBIGUOUS conserva la risposta senza esito,
   che può contenere un report con ``sizeMatched``. Quegli importi non sono confermati e non si espongono;
   il betId resta (è l'identità usata dalla riconciliazione). Le forme sono quelle dei test di D
   (``RISPOSTE_SENZA_ESITO``), così come il client le persiste (``esito["result"]``).
2. F-16 (``claude/d-f16-funds-freshness``): il limite di freschezza dei fondi non si ricalcola nel ponte.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from control_api.views import UNCONFIRMED_STATUSES, orders_view, protections_view
from core.runtime_controller import RuntimeController

# Risposta persistita dal fix P0: {"ok": False, ..., "order_unknown": True, "result": <result Betfair>}.
P0_RESULTS = {
    "status_assente_report_success_con_betid":
        {"instructionReports": [{"status": "SUCCESS", "betId": "31415", "sizeMatched": 5.0}]},
    "report_senza_status_con_betid": {"status": "SUCCESS", "instructionReports": [{"betId": "31415"}]},
    "status_non_riconosciuto":
        {"status": "PENDING", "instructionReports": [{"status": "SUCCESS", "betId": "31415"}]},
    "report_status_non_riconosciuto":
        {"status": "SUCCESS", "instructionReports": [{"status": "PENDING", "betId": "31415",
                                                      "sizeMatched": 2.0, "sizeRemaining": 8.0,
                                                      "averagePriceMatched": 2.5}]},
}


def p0_response(result: dict[str, Any]) -> dict[str, Any]:
    return {"ok": False, "error": "BET_STATUS_UNRECOGNIZED", "classification": "UNKNOWN",
            "order_unknown": True, "result": result}


def add(rc: RuntimeController, status: str, response: Any) -> int:
    return int(rc.db.insert_order({
        "customer_ref": f"cref-{time.monotonic_ns()}", "correlation_id": "corr", "status": status,
        "payload": {"market_id": "1.234567", "selection_id": 47972, "side": "BACK", "price": 2.5,
                    "stake": 10.0, "table_id": 1},
        "response": response}))


def row(rc: RuntimeController, oid: int) -> dict[str, Any]:
    return next(o for o in orders_view(rc)["snapshot"]["orders"] if o["local_id"] == oid)


@pytest.mark.parametrize("name", sorted(P0_RESULTS))
@pytest.mark.parametrize("status", sorted(UNCONFIRMED_STATUSES))
def test_unconfirmed_rows_never_show_report_amounts(runtime: RuntimeController, name: str,
                                                    status: str) -> None:
    r = row(runtime, add(runtime, status, p0_response(P0_RESULTS[name])))
    assert (r["size_matched"], r["avg_price_matched"], r["size_remaining"]) == (None, None, None)
    assert {"size_matched", "avg_price_matched", "size_remaining"} <= set(r["unavailable"])
    assert r["bet_id"] == "31415"  # identità della riconciliazione: resta
    assert r["status"] in ("AMBIGUOUS", "PENDING")


def test_unconfirmed_set_is_exactly_the_uncertain_core_states() -> None:
    assert UNCONFIRMED_STATUSES == {"AMBIGUOUS", "HEDGE_AMBIGUOUS", "INFLIGHT", "HEDGE_PENDING"}


@pytest.mark.parametrize("status", ["SUBMITTED", "COMPLETED", "HEDGE_PLACED", "HEDGE_UNMATCHED"])
def test_confirmed_rows_keep_their_amounts(runtime: RuntimeController, status: str) -> None:
    resp = {"status": "SUCCESS", "instructionReports": [
        {"betId": "27182", "sizeMatched": 2.0, "sizeRemaining": 8.0, "averagePriceMatched": 2.5}]}
    r = row(runtime, add(runtime, status, resp))
    assert (r["size_matched"], r["avg_price_matched"]) == ("2", "2.5")
    assert r["size_remaining"] == "8"


def funds(rc: Any) -> dict[str, Any]:
    return protections_view(rc)["items"]["funds_freshness"]


def test_funds_limit_on_main_is_the_reservation_ttl(sim_runtime: Any) -> None:
    from core.table_manager import TableManager

    assert funds(sim_runtime)["limit"] == str(int(TableManager.PLACED_TOMBSTONE_TTL_S))


def test_funds_limit_not_declared_when_f16_threshold_exists_without_accessor(
        sim_runtime: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # forma di F-16: costante di classe, nessun accessore unico => nessun limite inventato (prima: 120)
    monkeypatch.setattr(RuntimeController, "LIVE_FUNDS_MAX_AGE_S", 30.0, raising=False)
    p = funds(sim_runtime)
    assert p["limit"] is None and p["state"] == "ACTIVE"


def test_funds_limit_from_the_core_accessor(sim_runtime: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(RuntimeController, "LIVE_FUNDS_MAX_AGE_S", 30.0, raising=False)
    monkeypatch.setattr(RuntimeController, "funds_max_age_s", classmethod(lambda cls: 30.0),
                        raising=False)
    assert funds(sim_runtime)["limit"] == "30"


def test_views_pass_the_server_contract(sim_runtime: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from pickfair_control_api.contract import response_schema
    from pickfair_control_api.schema_check import validate

    add(sim_runtime, "AMBIGUOUS", p0_response(P0_RESULTS["report_status_non_riconosciuto"]))
    validate(orders_view(sim_runtime), response_schema("orders"))
    monkeypatch.setattr(RuntimeController, "LIVE_FUNDS_MAX_AGE_S", 30.0, raising=False)
    from control_api.views import status_view

    validate(status_view(sim_runtime), response_schema("status"))
