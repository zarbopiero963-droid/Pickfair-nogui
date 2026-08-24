"""Corsia dutching del bus (piano «dutching agganciato», owner 2026-08-23).

Il filo: REQ_PLACE_DUTCHING -> RiskMiddleware (dedupe 2s + normalizzazione,
SOLO questa corsia sull'app reale) -> CMD_PLACE_DUTCHING ->
DutchingController._handle_cmd_place_dutching (adattamento shape) ->
submit_dutching (validate + precheck Roserpina + gates) -> una gamba per
selezione come CMD_QUICK_BET nel choke del TradingEngine.

Questi test usano un bus SINCRONO deterministico (dispatch inline): il
montaggio sul bus REALE dell'app e' asserito dal guardrail di cablaggio
(tests/guardrails/test_cablaggio_runtime.py), qui si prova la CATENA.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

from controllers.dutching_controller import DutchingController
from core.risk_middleware import RiskMiddleware
from core.system_state import RuntimeMode


class _BusSync:
    """Bus sincrono: publish esegue i callback inline (deterministico)."""

    def __init__(self) -> None:
        self.subs: Dict[str, List[Any]] = {}
        self.published: List[tuple] = []

    def subscribe(self, topic: str, callback: Any) -> None:
        self.subs.setdefault(topic, []).append(callback)

    def publish(self, topic: str, payload: Any = None) -> None:
        self.published.append((topic, payload))
        for callback in list(self.subs.get(topic, [])):
            callback(payload)

    def conta(self, topic: str) -> int:
        return len(self.subs.get(topic, []))

    def eventi(self, topic: str) -> List[Any]:
        return [p for t, p in self.published if t == topic]


class _RuntimeAttivo:
    """Runtime minimo: mode ACTIVE, il resto assente (accessor difensivi)."""

    mode = RuntimeMode.ACTIVE


def _catena(runtime=None):
    """Monta la corsia completa su bus sincrono e ritorna (bus, middleware,
    controller). Il 'motore' e' il registro degli eventi del bus stesso:
    le gambe si leggono da bus.eventi('CMD_QUICK_BET')."""
    bus = _BusSync()
    middleware = RiskMiddleware(bus, topics=("REQ_PLACE_DUTCHING",))
    controller = DutchingController(
        bus, runtime if runtime is not None else _RuntimeAttivo()
    ).attach_bus_consumer()
    return bus, middleware, controller


def _req(**over: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "market_id": "1.234",
        "event_name": "Evento Test",
        "market_name": "Match Odds",
        "bet_type": "BACK",
        "total_stake": 100.0,
        "results": [
            {"selectionId": 111, "runnerName": "Casa", "price": 2.0},
            {"selectionId": 222, "runnerName": "Ospite", "price": 4.0},
        ],
        "source": "TEST",
    }
    base.update(over)
    return base


# ---------------------------------------------------------------------------
# RiskMiddleware: corsie selezionabili (anti-doppio-consumer)
# ---------------------------------------------------------------------------
def test_middleware_default_sottoscrive_tutte_le_corsie() -> None:
    bus = _BusSync()
    RiskMiddleware(bus)
    for topic in (
        "REQ_QUICK_BET",
        "REQ_PLACE_DUTCHING",
        "REQ_EXECUTE_CASHOUT",
        "REQ_CANCEL_ORDER",
        "REQ_REPLACE_ORDER",
    ):
        assert bus.conta(topic) == 1, topic


def test_middleware_corsia_dutching_sola_niente_altre_sottoscrizioni() -> None:
    """Il cablaggio dell'app reale: SOLO la corsia dutching. Le altre corsie
    hanno gia' il loro consumer diretto (engine, cashout bridge): il
    middleware non deve toccarle, o ogni REQ verrebbe eseguita due volte."""
    bus = _BusSync()
    mw = RiskMiddleware(bus, topics=("REQ_PLACE_DUTCHING",))
    assert bus.conta("REQ_PLACE_DUTCHING") == 1
    for topic in (
        "REQ_QUICK_BET",
        "REQ_EXECUTE_CASHOUT",
        "REQ_CANCEL_ORDER",
        "REQ_REPLACE_ORDER",
    ):
        assert bus.conta(topic) == 0, topic
    assert mw.topics == ("REQ_PLACE_DUTCHING",)


def test_middleware_corsia_sconosciuta_solleva_fail_closed() -> None:
    """Un typo nel nome corsia NON deve lasciare la corsia disarmata in
    silenzio: si solleva alla costruzione."""
    with pytest.raises(ValueError, match="corsie sconosciute"):
        RiskMiddleware(_BusSync(), topics=("REQ_PLACE_DUTCHNG",))


# ---------------------------------------------------------------------------
# DutchingController: aggancio esplicito della corsia
# ---------------------------------------------------------------------------
def test_attach_bus_consumer_sottoscrive_cmd_e_ritorna_self() -> None:
    bus = _BusSync()
    controller = DutchingController(bus, _RuntimeAttivo())
    assert bus.conta("CMD_PLACE_DUTCHING") == 0, (
        "il costruttore NON deve sottoscrivere da solo (aggancio esplicito)"
    )
    tornato = controller.attach_bus_consumer()
    assert tornato is controller
    assert bus.conta("CMD_PLACE_DUTCHING") == 1


def test_attach_bus_consumer_senza_bus_solleva() -> None:
    controller = DutchingController(None, _RuntimeAttivo())
    with pytest.raises(RuntimeError, match="senza bus"):
        controller.attach_bus_consumer()


# ---------------------------------------------------------------------------
# La catena intera: REQ -> middleware -> CMD -> controller -> gambe
# ---------------------------------------------------------------------------
def test_catena_req_dutching_produce_le_gambe_ricalcolate() -> None:
    """Filo intero: un REQ con 2 selezioni produce 2 CMD_QUICK_BET con gli
    stake RICALCOLATI dal controller (equal-profit su total_stake), non
    quelli eventualmente arrivati dal wire."""
    bus, _mw, _ctrl = _catena()

    bus.publish("REQ_PLACE_DUTCHING", _req())

    gambe = bus.eventi("CMD_QUICK_BET")
    assert len(gambe) == 2
    per_selezione = {g["selection_id"]: g for g in gambe}
    assert set(per_selezione) == {111, 222}
    # Equal-profit su quote 2.0/4.0 e stake 100: pesi 1/2 e 1/4 => ~66.67/33.33.
    assert per_selezione[111]["stake"] == pytest.approx(66.67, abs=0.05)
    assert per_selezione[222]["stake"] == pytest.approx(33.33, abs=0.05)
    assert per_selezione[111]["market_id"] == "1.234"
    assert per_selezione[111]["bet_type"] == "BACK"


def test_catena_ignora_gli_stake_precomputati_dal_wire() -> None:
    """Sicurezza: stake per-gamba nel payload REQ (es. 99 e 1) vengono
    IGNORATI — il controller ricalcola dall'equal-profit. Un publisher
    compromesso o buggato non puo' imporre la distribuzione."""
    req = _req()
    req["results"][0]["stake"] = 99.0
    req["results"][1]["stake"] = 1.0
    bus, _mw, _ctrl = _catena()

    bus.publish("REQ_PLACE_DUTCHING", req)

    gambe = {g["selection_id"]: g for g in bus.eventi("CMD_QUICK_BET")}
    assert gambe[111]["stake"] == pytest.approx(66.67, abs=0.05)
    assert gambe[222]["stake"] == pytest.approx(33.33, abs=0.05)


def test_catena_dedupe_req_identica_entro_finestra() -> None:
    """Doppio click / doppio invio: la seconda REQ identica entro 2s viene
    scartata dal middleware — le gambe restano 2, non 4."""
    bus, _mw, _ctrl = _catena()

    bus.publish("REQ_PLACE_DUTCHING", _req())
    bus.publish("REQ_PLACE_DUTCHING", _req())

    assert len(bus.eventi("CMD_PLACE_DUTCHING")) == 1
    assert len(bus.eventi("CMD_QUICK_BET")) == 2


def test_catena_req_malformata_nessun_cmd_e_nessuna_gamba() -> None:
    """selectionId non intero => il middleware pubblica DUTCHING_FAILED e
    NON inoltra: nessun CMD, nessun ordine parziale (fail-closed)."""
    req = _req()
    req["results"][0]["selectionId"] = "NON-UN-ID"
    bus, _mw, _ctrl = _catena()

    bus.publish("REQ_PLACE_DUTCHING", req)

    assert bus.eventi("CMD_PLACE_DUTCHING") == []
    assert bus.eventi("CMD_QUICK_BET") == []
    assert len(bus.eventi("DUTCHING_FAILED")) == 1


def test_catena_runtime_non_attivo_batch_rigettato_niente_gambe() -> None:
    """Runtime non ACTIVE (STOPPED, emergency, mai avviato) => il precheck
    rigetta il batch: la corsia e' viva ma NON piazza nulla."""

    class _RuntimeFermo:
        mode = RuntimeMode.STOPPED

    bus, _mw, _ctrl = _catena(runtime=_RuntimeFermo())

    bus.publish("REQ_PLACE_DUTCHING", _req())

    assert len(bus.eventi("CMD_PLACE_DUTCHING")) == 1  # inoltrato...
    assert bus.eventi("CMD_QUICK_BET") == []  # ...ma nessuna gamba
    rigetti = bus.eventi("DUTCHING_BATCH_REJECTED")
    assert len(rigetti) == 1
    assert "Runtime non attivo" in str(rigetti[0].get("reason"))


def test_handler_cmd_non_solleva_mai_nel_worker() -> None:
    """Payload non-dict direttamente su CMD: il handler non solleva (il bus
    worker non deve mai morire) e non produce gambe."""
    bus, _mw, _ctrl = _catena()

    bus.publish("CMD_PLACE_DUTCHING", "spazzatura")
    bus.publish("CMD_PLACE_DUTCHING", None)

    assert bus.eventi("CMD_QUICK_BET") == []


def test_attach_bus_consumer_idempotente_un_solo_handler() -> None:
    """R1 su #442 (GPT-5.6+Fable, difesa in profondita'): due chiamate ad
    attach_bus_consumer NON devono produrre due handler (= gambe piazzate
    due volte). Sull'app reale il doppio attach non avviene (build() e'
    guardato da _built e ogni build crea un EventBus NUOVO), ma il
    contratto della classe lo garantisce comunque."""
    bus = _BusSync()
    controller = DutchingController(bus, _RuntimeAttivo())
    controller.attach_bus_consumer()
    tornato = controller.attach_bus_consumer()  # seconda chiamata: no-op
    assert tornato is controller
    assert bus.conta("CMD_PLACE_DUTCHING") == 1

    bus.publish("CMD_PLACE_DUTCHING", None)  # nessun raise, nessuna gamba
    assert bus.eventi("CMD_QUICK_BET") == []


def test_adattatore_niente_passthrough_su_selections_dal_wire() -> None:
    """R1 su #442 (Fable, fondato): un CMD gia' in shape `selections` non
    deve bypassare l'adattatore — gli stake avvelenati (99/1) vengono
    scartati e la distribuzione resta il ricalcolo equal-profit."""
    bus, _mw, _ctrl = _catena()

    bus.publish(
        "CMD_PLACE_DUTCHING",
        {
            "market_id": "1.234",
            "total_stake": 100.0,
            "bet_type": "BACK",
            "selections": [
                {"selectionId": 111, "price": 2.0, "stake": 99.0},
                {"selectionId": 222, "price": 4.0, "stake": 1.0},
            ],
        },
    )

    gambe = {g["selection_id"]: g for g in bus.eventi("CMD_QUICK_BET")}
    assert set(gambe) == {111, 222}
    assert gambe[111]["stake"] == pytest.approx(66.67, abs=0.05)
    assert gambe[222]["stake"] == pytest.approx(33.33, abs=0.05)
