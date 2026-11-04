"""Comando interno tipizzato per il Dutching (mandato owner a Claude D, 04/11/2026).

Su `main` `ada4076` la corsia `REQ_PLACE_DUTCHING -> RiskMiddleware -> CMD_PLACE_DUTCHING -> DutchingController`
e' armata ma dormiente: nessun publisher esiste (headless_main.py:306-319) e il middleware scarta ogni identificativo
del chiamante, quindi un ingresso Dutching non sarebbe tracciabile dalla richiesta agli ordini.

`core.dutching_command` e' l'ingresso esplicito: comando validato (fail-closed, importi Decimal, lato obbligatorio per
gamba, nessuna gamba duplicata), pubblicato sulla STESSA corsia (nessun percorso di trading parallelo), con un
`command_id` che arriva fino agli eventi d'audit del controller. Stake, fondi, cap, negoziabilita' del mercato e
idempotenza del lotto restano decisi dal controller: il comando non introduce limiti finanziari.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from core import dutching_command as dc


class _Bus:
    def __init__(self) -> None:
        self.pubblicati = []
        self.handlers = {}

    def subscribe(self, topic, handler) -> None:
        self.handlers.setdefault(topic, []).append(handler)

    def publish(self, topic, payload=None) -> None:
        self.pubblicati.append((topic, payload))
        for h in list(self.handlers.get(topic, [])):
            h(payload)


def _raw(**over):
    raw = {"command_id": "cmd-0001", "market_id": "1.700", "total_stake": "10.00", "event_name": "Alpha v Beta",
           "market_name": "Match Odds", "event_id": "34567",
           "legs": [{"selection_id": 10, "price": "2.0", "side": "BACK", "runner_name": "Alpha"},
                    {"selection_id": 20, "price": "3.5", "side": "BACK", "runner_name": "Beta"}]}
    raw.update(over)
    return raw


# ------------------------------------------------------------------ costruzione e validazione
def test_comando_valido_usa_decimal_e_conserva_l_identita() -> None:
    cmd = dc.build_dutching_command(_raw())

    assert cmd.command_id == "cmd-0001" and cmd.market_id == "1.700"
    assert cmd.total_stake == Decimal("10.00") and isinstance(cmd.total_stake, Decimal)
    assert [(l.selection_id, l.price, l.side) for l in cmd.legs] == [(10, Decimal("2.0"), "BACK"),
                                                                      (20, Decimal("3.5"), "BACK")]


def test_float_convertito_dalla_sua_forma_decimale_non_binaria() -> None:
    cmd = dc.build_dutching_command(_raw(total_stake=0.1 + 0.2))

    assert cmd.total_stake == Decimal("0.30000000000000004")      # repr del float, mai l'espansione binaria


@pytest.mark.parametrize("over,codice", [
    ({"command_id": ""}, "INVALID_COMMAND_ID"),
    ({"command_id": None}, "INVALID_COMMAND_ID"),
    ({"command_id": "x" * 65}, "INVALID_COMMAND_ID"),
    ({"command_id": "con spazio"}, "INVALID_COMMAND_ID"),
    ({"market_id": ""}, "INVALID_MARKET_ID"),
    ({"market_id": True}, "INVALID_MARKET_ID"),
    ({"total_stake": "0"}, "INVALID_TOTAL_STAKE"),
    ({"total_stake": "-5"}, "INVALID_TOTAL_STAKE"),
    ({"total_stake": "nan"}, "INVALID_TOTAL_STAKE"),
    ({"total_stake": "inf"}, "INVALID_TOTAL_STAKE"),
    ({"total_stake": "dieci"}, "INVALID_TOTAL_STAKE"),
    ({"total_stake": True}, "INVALID_TOTAL_STAKE"),
    ({"total_stake": None}, "INVALID_TOTAL_STAKE"),
    ({"legs": []}, "INVALID_LEGS"),
    ({"legs": "x"}, "INVALID_LEGS"),
    ({"legs": [{"selection_id": 10, "price": "2.0", "side": "BACK"}]}, "INVALID_LEGS"),     # una gamba non e' Dutching
    ({"legs": [{"selection_id": 10, "price": "2.0", "side": "BACK"},
               {"selection_id": 10, "price": "3.0", "side": "BACK"}]}, "DUPLICATE_SELECTION"),
    ({"legs": [{"selection_id": 10, "price": "2.0"},
               {"selection_id": 20, "price": "3.0", "side": "BACK"}]}, "INVALID_SIDE"),          # lato mai inventato
    ({"legs": [{"selection_id": 10, "price": "2.0", "side": "back "},
               {"selection_id": 20, "price": "3.0", "side": "BACK"}]}, "INVALID_SIDE"),        # mai normalizzato
    ({"legs": [{"selection_id": 10, "price": "2.0", "side": "BACK"},
               {"selection_id": 20, "price": "3.0", "side": "PUNTA"}]}, "INVALID_SIDE"),
    ({"simulation_mode": False}, "UNKNOWN_FIELD"),                                              # campo fuori contratto
    ({"legs": [{"selection_id": 0, "price": "2.0", "side": "BACK"},
               {"selection_id": 20, "price": "3.0", "side": "BACK"}]}, "INVALID_SELECTION_ID"),
    ({"legs": [{"selection_id": 10.5, "price": "2.0", "side": "BACK"},
               {"selection_id": 20, "price": "3.0", "side": "BACK"}]}, "INVALID_SELECTION_ID"),
    ({"legs": [{"selection_id": 10, "price": "1.0", "side": "BACK"},
               {"selection_id": 20, "price": "3.0", "side": "BACK"}]}, "INVALID_PRICE"),
    ({"legs": [{"selection_id": 10, "price": "nan", "side": "BACK"},
               {"selection_id": 20, "price": "3.0", "side": "BACK"}]}, "INVALID_PRICE"),
    ({"legs": [{"selection_id": 10, "price": "2.0", "side": "BACK"}, "x"]}, "INVALID_LEGS"),
])
def test_block_comando_non_valido_rifiutato_senza_pubblicare(over, codice: str) -> None:
    bus = _Bus()

    esito = dc.submit_dutching_command(bus, _raw(**over), simulation_mode=True)

    assert esito == {"ok": False, "command_id": esito.get("command_id"), "reason": codice}, esito
    assert bus.pubblicati == [], "comando non valido pubblicato"


def test_campo_sconosciuto_rifiutato() -> None:
    """Nessun campo fuori contratto: uno `stake` per gamba non deve poter viaggiare (il controller ricalcola)."""
    bus = _Bus()
    raw = _raw()
    raw["legs"][0]["stake"] = "7"

    esito = dc.submit_dutching_command(bus, raw, simulation_mode=True)

    assert esito["ok"] is False and esito["reason"] == "UNKNOWN_FIELD", esito
    assert bus.pubblicati == []


# ------------------------------------------------------------------ pubblicazione sulla corsia esistente
def test_comando_valido_pubblicato_su_req_place_dutching() -> None:
    bus = _Bus()

    esito = dc.submit_dutching_command(bus, _raw(), simulation_mode=True)

    assert esito == {"ok": True, "command_id": "cmd-0001", "reason": ""}
    assert [t for t, _ in bus.pubblicati] == ["REQ_PLACE_DUTCHING"]
    payload = bus.pubblicati[0][1]
    assert payload["command_id"] == "cmd-0001" and payload["source"] == "DUTCHING_COMMAND"
    assert payload["market_id"] == "1.700" and payload["total_stake"] == "10.00"
    assert payload["simulation_mode"] is True
    assert payload["results"] == [
        {"selectionId": 10, "price": "2.0", "side": "BACK", "runnerName": "Alpha"},
        {"selectionId": 20, "price": "3.5", "side": "BACK", "runnerName": "Beta"},
    ]
    assert all("stake" not in leg for leg in payload["results"])


def test_bus_assente_o_che_solleva_non_e_un_successo() -> None:
    class _Rotto:
        def publish(self, *_a, **_k):
            raise RuntimeError("bus giu'")

    assert dc.submit_dutching_command(None, _raw(), simulation_mode=True)["reason"] == "BUS_UNAVAILABLE"
    assert dc.submit_dutching_command(_Rotto(), _raw(), simulation_mode=True)["reason"] == "PUBLISH_FAILED"


def test_command_id_attraversa_il_risk_middleware() -> None:
    """Tracciabilita': il middleware della corsia conserva `command_id` nel CMD_PLACE_DUTCHING."""
    from core.risk_middleware import RiskMiddleware

    bus = _Bus()
    RiskMiddleware(bus, topics=("REQ_PLACE_DUTCHING",))
    dc.submit_dutching_command(bus, _raw(), simulation_mode=True)

    cmd = [p for t, p in bus.pubblicati if t == "CMD_PLACE_DUTCHING"]
    assert len(cmd) == 1, bus.pubblicati
    assert cmd[0]["command_id"] == "cmd-0001"
    assert cmd[0]["total_stake"] == 10.0 and [r["selectionId"] for r in cmd[0]["results"]] == [10, 20]


def test_stesso_command_id_ripubblicato_e_bloccato_dal_middleware() -> None:
    from core.risk_middleware import RiskMiddleware

    bus = _Bus()
    RiskMiddleware(bus, topics=("REQ_PLACE_DUTCHING",))
    dc.submit_dutching_command(bus, _raw(), simulation_mode=True)
    dc.submit_dutching_command(bus, _raw(), simulation_mode=True)

    assert len([p for t, p in bus.pubblicati if t == "CMD_PLACE_DUTCHING"]) == 1


# ------------------------------------------------------------------ catena reale fino agli ordini (SIM)
def test_catena_reale_comando_fino_agli_ordini_e_all_audit(tmp_path) -> None:
    """DutchingCommand -> RiskMiddleware -> DutchingController reale -> TradingEngine -> SimulationBroker: le gambe
    partono una volta, con il command_id negli eventi d'audit del lotto."""
    from core.risk_middleware import RiskMiddleware
    from tests.acceptance.test_issue437_pr27 import _catena
    from tests.acceptance.test_issue523_dutching_manual_bet import _dutching

    c = _catena(tmp_path)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0, "total": 1000.0,
                                                      "simulated": True}
    RiskMiddleware(c.bus, topics=("REQ_PLACE_DUTCHING",))
    _dutching(c).attach_bus_consumer()

    esito = dc.submit_dutching_command(c.bus, _raw(), simulation_mode=True)

    assert esito["ok"] is True
    gambe = c.bus.payloads("CMD_QUICK_BET")
    assert sorted(g["selection_id"] for g in gambe) == [10, 20], gambe
    assert all(g.get("batch_id") for g in gambe)
    approvati = c.bus.payloads("DUTCHING_BATCH_APPROVED")
    assert len(approvati) == 1 and approvati[0]["command_id"] == "cmd-0001", approvati
