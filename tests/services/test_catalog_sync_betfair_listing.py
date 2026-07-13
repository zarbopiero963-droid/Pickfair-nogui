"""Test del fix CatalogSync (#367).

Il CatalogSync chiamava `list_soccer_events()`/`list_market_catalogue(market_types=...)`
inesistenti sul client Betfair -> AttributeError, 0 eventi sincronizzati. Ora usa
`list_events`/`list_market_catalogue` reali (Betfair SportsAPING) e mappa il
formato nativo. Questi test coprono il mapping e le firme; la verifica end-to-end
con l'API Betfair reale resta sul VPS.
"""

import pytest

from betfair_client import BetfairClient
from simulation_broker import SimulationBroker
from services.catalog_sync_service import CatalogSyncService


# Formato Betfair NATIVO (come lo ritorna SportsAPING).
BF_EVENTS = [
    {"event": {"id": "30000", "name": "Team A v Team B", "openDate": "2026-07-13T18:00:00.000Z"}, "marketCount": 3}
]
BF_MARKETS = [
    {
        "marketId": "1.234",
        "marketName": "Match Odds",
        "totalMatched": 1500.0,
        "marketStartTime": "2026-07-13T18:00:00.000Z",
        "description": {"marketType": "MATCH_ODDS"},
        "competition": {"id": "55", "name": "Serie A"},
        "runners": [
            {"selectionId": 47972, "runnerName": "Team A", "handicap": 0.0, "sortPriority": 1},
            {"selectionId": 47973, "runnerName": "Team B", "handicap": 0.0, "sortPriority": 2},
        ],
    }
]


class _RecordingClient:
    """Client con SOLO i metodi REALI (list_events/list_market_catalogue).

    Non ha `list_soccer_events`: se il servizio chiamasse ancora il vecchio
    metodo inesistente, `run_sync` cadrebbe nel except e non popolerebbe nulla.
    """

    def __init__(self, events, markets):
        self._events = events
        self._markets = markets
        self.calls = []

    def list_events(self, event_type_ids, *, in_play_only=False, max_results=1000):
        self.calls.append(("list_events", list(event_type_ids), in_play_only))
        return self._events

    def list_market_catalogue(self, event_type_ids, event_ids=None, *, market_type_codes=None, max_results=1000):
        self.calls.append(("list_market_catalogue", list(event_type_ids), event_ids, market_type_codes))
        return self._markets


class _FakeDB:
    def __init__(self):
        self.events = []
        self.markets = []
        self.runners = []

    def get_sync_meta(self):
        return None

    def upsert_bf_event(self, **kw):
        self.events.append(kw)

    def upsert_bf_market(self, **kw):
        self.markets.append(kw)

    def upsert_bf_runner(self, **kw):
        self.runners.append(kw)

    def cleanup_stale_bf_data(self, sync_id):
        pass

    def update_sync_meta(self, **kw):
        pass


def _client():
    return BetfairClient(username="u", app_key="APP", cert_pem="/tmp/c.pem", key_pem="/tmp/k.pem")


# ---- CatalogSync: mapping formato Betfair -> DB (BLOCK #367) ----------------


def test_catalog_sync_maps_betfair_format_to_db():
    # BLOCK: il servizio usa list_events/list_market_catalogue reali; con un
    # client che espone SOLO quei metodi il sync deve popolare il DB (prima
    # crashava con AttributeError su list_soccer_events -> 0 eventi).
    db = _FakeDB()
    client = _RecordingClient(BF_EVENTS, BF_MARKETS)
    CatalogSyncService(db, client).run_sync(force=True)

    assert len(db.events) == 1
    ev = db.events[0]
    assert ev["event_id"] == "30000"
    assert ev["name"] == "Team A v Team B"
    assert ev["competition_id"] == "55"  # presa dal market catalogue
    assert ev["competition_name"] == "Serie A"
    assert ev["open_date"] == "2026-07-13T18:00:00.000Z"

    assert len(db.markets) == 1
    mk = db.markets[0]
    assert mk["market_id"] == "1.234"
    assert mk["market_type"] == "MATCH_ODDS"
    assert mk["total_matched"] == 1500.0

    assert len(db.runners) == 2
    assert db.runners[0]["selection_id"] == "47972"
    assert db.runners[0]["runner_name"] == "Team A"

    # market catalogue chiesto con event_ids e marketTypeCodes corretti
    mc = [c for c in client.calls if c[0] == "list_market_catalogue"][0]
    assert mc[2] == ["30000"]
    assert "MATCH_ODDS" in mc[3]


def test_catalog_sync_does_not_use_list_soccer_events():
    # Regressione esplicita: il vecchio metodo inesistente non va piu' chiamato.
    class _NoSoccer(_RecordingClient):
        def list_soccer_events(self, *a, **k):
            raise AssertionError("list_soccer_events non deve essere chiamato")

    db = _FakeDB()
    CatalogSyncService(db, _NoSoccer(BF_EVENTS, BF_MARKETS)).run_sync(force=True)
    assert len(db.events) == 1


def test_catalog_sync_skips_events_without_id():
    db = _FakeDB()
    client = _RecordingClient([{"event": {}}, {"marketCount": 1}], [])
    CatalogSyncService(db, client).run_sync(force=True)
    assert db.events == []  # nessun event_id -> nessun upsert, nessun crash


# ---- BetfairClient: costruzione filtro (BLOCK marketTypeCodes) --------------


def test_betfair_client_list_events_filter():
    c = _client()
    captured = {}

    def fake(url, method, params, **kw):
        captured.update(url=url, method=method, params=params)
        return [{"event": {"id": "1"}}]

    c._post_jsonrpc = fake
    out = c.list_events(["1"], in_play_only=True)

    assert captured["method"] == "SportsAPING/v1.0/listEvents"
    assert captured["params"]["filter"]["eventTypeIds"] == ["1"]
    assert captured["params"]["filter"]["inPlayOnly"] is True
    assert out == [{"event": {"id": "1"}}]


def test_betfair_client_market_catalogue_filter():
    c = _client()
    captured = {}

    def fake(url, method, params, **kw):
        captured["params"] = params
        return []

    c._post_jsonrpc = fake
    c.list_market_catalogue(["1"], event_ids=["30000"], market_type_codes=["MATCH_ODDS", "CORRECT_SCORE"])

    f = captured["params"]["filter"]
    assert f["eventTypeIds"] == ["1"]
    assert f["eventIds"] == ["30000"]
    # BLOCK: il filtro per tipo mercato deve arrivare a Betfair come marketTypeCodes.
    assert f["marketTypeCodes"] == ["MATCH_ODDS", "CORRECT_SCORE"]


def test_betfair_client_list_events_non_list_result():
    c = _client()
    c._post_jsonrpc = lambda *a, **k: {}  # risultato non-lista -> []
    assert c.list_events(["1"]) == []


# ---- SimulationBroker: parita' d'interfaccia (no crash in SIM) --------------


def test_simulation_broker_catalog_methods_return_empty():
    b = SimulationBroker(starting_balance=1000.0)
    assert b.list_events(["1"]) == []
    assert b.list_market_catalogue(["1"], event_ids=["x"], market_type_codes=["MATCH_ODDS"]) == []


def test_catalog_sync_in_simulation_completes_without_error():
    # In SIM il broker non ha catalogo via API: il sync completa con 0 eventi,
    # senza AttributeError (era il secondo sintomo del report #367).
    db = _FakeDB()
    CatalogSyncService(db, SimulationBroker(starting_balance=1000.0)).run_sync(force=True)
    assert db.events == []
