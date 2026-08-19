"""Test del fix CatalogSync (#367).

Il CatalogSync chiamava `list_soccer_events()`/`list_market_catalogue(market_types=...)`
inesistenti sul client Betfair -> AttributeError, 0 eventi sincronizzati. Ora usa
`list_events`/`list_market_catalogue` reali (Betfair SportsAPING), in BATCH per
evitare rate-limit, con guard fail-safe che non cancella il catalogo su sync
vuoto. La verifica end-to-end con l'API Betfair reale resta sul VPS.
"""

import pytest

from betfair_client import BetfairClient
from simulation_broker import SimulationBroker
from services.catalog_sync_service import CatalogSyncService


# Formato Betfair NATIVO. Ogni market include "event" (marketProjection EVENT):
# serve a raggruppare i mercati per evento nel sync in batch.
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
        "event": {"id": "30000"},
        "runners": [
            {"selectionId": 47972, "runnerName": "Team A", "handicap": 0.0, "sortPriority": 1},
            {"selectionId": 47973, "runnerName": "Team B", "handicap": 0.0, "sortPriority": 2},
        ],
    }
]


class _RecordingClient:
    """Client con SOLO i metodi REALI (list_events/list_market_catalogue)."""

    def __init__(self, events, markets):
        self._events = events
        self._markets = markets
        self.calls = []

    def list_events(self, event_type_ids, *, in_play_only=False):
        self.calls.append(("list_events", list(event_type_ids), in_play_only))
        return self._events

    def list_market_catalogue(self, event_type_ids, event_ids=None, *, market_type_codes=None, max_results=200):
        self.calls.append(
            ("list_market_catalogue", list(event_type_ids), list(event_ids or []), market_type_codes, max_results)
        )
        return self._markets


class _FakeDB:
    def __init__(self):
        self.events = []
        self.markets = []
        self.runners = []
        self.cleanup_called = False
        self.meta_updated = False

    def get_sync_meta(self):
        return None

    def upsert_bf_event(self, **kw):
        self.events.append(kw)

    def upsert_bf_market(self, **kw):
        self.markets.append(kw)

    def upsert_bf_runner(self, **kw):
        self.runners.append(kw)

    def cleanup_stale_bf_data(self, sync_id):
        self.cleanup_called = True

    def update_sync_meta(self, **kw):
        self.meta_updated = True


def _client():
    return BetfairClient(username="u", app_key="APP", cert_pem="/tmp/c.pem", key_pem="/tmp/k.pem")


# ---- CatalogSync: mapping formato Betfair -> DB (BLOCK #367) ----------------


def test_catalog_sync_maps_betfair_format_to_db():
    # BLOCK: con un client che espone SOLO i metodi reali il sync popola il DB
    # (prima crashava su list_soccer_events -> 0 eventi).
    db = _FakeDB()
    CatalogSyncService(db, _RecordingClient(BF_EVENTS, BF_MARKETS)).run_sync(force=True)

    assert len(db.events) == 1
    ev = db.events[0]
    assert ev["event_id"] == "30000"
    assert ev["name"] == "Team A v Team B"
    assert ev["competition_id"] == "55"
    assert ev["competition_name"] == "Serie A"
    assert ev["open_date"] == "2026-07-13T18:00:00.000Z"

    assert len(db.markets) == 1
    assert db.markets[0]["market_type"] == "MATCH_ODDS"
    assert db.markets[0]["total_matched"] == 1500.0

    assert len(db.runners) == 2
    assert db.runners[0]["selection_id"] == "47972"
    assert db.cleanup_called is True  # catalogo popolato -> cleanup consentito


def test_catalog_sync_batches_market_catalogue():
    # BLOCK N+1: con piu' eventi il market catalogue e' chiamato in UN batch
    # (eventIds multipli), non una volta per evento -> niente rate-limit 429.
    events = [{"event": {"id": str(30000 + i), "name": f"E{i}", "openDate": "x"}} for i in range(3)]
    client = _RecordingClient(events, [])
    CatalogSyncService(_FakeDB(), client).run_sync(force=True)

    mc_calls = [c for c in client.calls if c[0] == "list_market_catalogue"]
    assert len(mc_calls) == 1
    assert set(mc_calls[0][2]) == {"30000", "30001", "30002"}
    assert "MATCH_ODDS" in mc_calls[0][3]


def test_catalog_sync_batches_multiple_chunks():
    # BLOCK N+1/paginazione (Grok): >20 eventi -> piu' batch (chunk_size=20),
    # ognuno con i propri eventIds, nessun evento perso, max 20/batch.
    events = [{"event": {"id": str(30000 + i), "name": f"E{i}", "openDate": "x"}} for i in range(45)]
    client = _RecordingClient(events, [])
    CatalogSyncService(_FakeDB(), client).run_sync(force=True)

    mc_calls = [c for c in client.calls if c[0] == "list_market_catalogue"]
    assert len(mc_calls) == 3  # 45 / 20 -> chunk da 20, 20, 5
    assert all(len(c[2]) <= 20 for c in mc_calls)
    covered = set()
    for c in mc_calls:
        covered.update(c[2])
    assert len(covered) == 45  # unione eventIds = tutti gli eventi


def test_catalog_sync_uses_weight_safe_max_results():
    # BLOCK (Fable/Fugu): il sync NON deve chiedere maxResults > 200 su
    # listMarketCatalogue (peso MARKET_DESCRIPTION -> TOO_MUCH_DATA in LIVE).
    client = _RecordingClient(BF_EVENTS, BF_MARKETS)
    CatalogSyncService(_FakeDB(), client).run_sync(force=True)
    mc = [c for c in client.calls if c[0] == "list_market_catalogue"][0]
    assert mc[4] <= 200  # indice 4 = max_results


def test_catalog_sync_truncated_batch_skips_cleanup():
    # BLOCK troncamento (GPT/Fugu/Fable): se un batch raggiunge maxResults la
    # risposta puo' essere troncata -> sync INCOMPLETO. NON eseguire
    # cleanup/meta, altrimenti si cancellano mercati/runner ancora validi.
    markets = [
        {"marketId": f"1.{i}", "marketName": "MO", "event": {"id": "30000"}, "runners": []}
        for i in range(200)
    ]
    db = _FakeDB()
    CatalogSyncService(db, _RecordingClient(BF_EVENTS, markets)).run_sync(force=True)
    assert len(db.events) == 1          # eventi upsertati
    assert db.cleanup_called is False   # ma cleanup SALTATO (sync troncato)
    assert db.meta_updated is False


def test_catalog_sync_mixed_batch_failing_chunk_skips_cleanup():
    # BLOCK batch misto (CodeRabbit #369): se UN chunk fallisce (il client
    # solleva su risposta malformata) mentre altri popolano, il sync si
    # interrompe PRIMA del cleanup -> catalogo preservato, niente cancellazione
    # di mercati/runner validi del chunk mancante.
    class _FailSecondChunk(_RecordingClient):
        def list_market_catalogue(self, *a, **k):
            super().list_market_catalogue(*a, **k)  # registra la call
            if len([c for c in self.calls if c[0] == "list_market_catalogue"]) >= 2:
                raise RuntimeError("chunk market-catalogue malformato")
            return self._markets

    events = [{"event": {"id": str(30000 + i), "name": f"E{i}", "openDate": "x"}} for i in range(25)]
    db = _FakeDB()
    CatalogSyncService(db, _FailSecondChunk(events, BF_MARKETS)).run_sync(force=True)
    assert db.cleanup_called is False   # cleanup SALTATO (chunk fallito)
    assert db.meta_updated is False


def test_catalog_sync_events_but_zero_markets_skips_cleanup():
    # BLOCK fail-open (CodeRabbit Major): eventi presenti ma 0 mercati (es. una
    # risposta market-catalogue anomala degradata a [] dal client) NON deve
    # eseguire cleanup, altrimenti cancella mercati/runner ancora validi.
    events = [{"event": {"id": "30000", "name": "E", "openDate": "x"}}]
    db = _FakeDB()
    CatalogSyncService(db, _RecordingClient(events, [])).run_sync(force=True)
    assert len(db.events) == 1          # eventi upsertati
    assert db.cleanup_called is False   # ma cleanup SALTATO (0 mercati sospetto)
    assert db.meta_updated is False


def test_catalog_sync_empty_events_preserves_catalog():
    # BLOCK regressione (GPT/Grok/Fable): 0 eventi -> NON cancellare il catalogo.
    db = _FakeDB()
    CatalogSyncService(db, _RecordingClient([], [])).run_sync(force=True)
    assert db.cleanup_called is False
    assert db.meta_updated is False
    assert db.events == []


def test_catalog_sync_competition_from_first_market_with_one():
    # Il primo mercato NON ha competition -> va presa dal successivo che ce l'ha.
    markets = [
        {"marketId": "1.1", "marketName": "Over/Under", "event": {"id": "30000"}, "runners": []},
        {"marketId": "1.2", "marketName": "Match Odds", "event": {"id": "30000"},
         "competition": {"id": "55", "name": "Serie A"}, "runners": []},
    ]
    db = _FakeDB()
    CatalogSyncService(db, _RecordingClient(BF_EVENTS, markets)).run_sync(force=True)
    assert db.events[0]["competition_id"] == "55"


def test_catalog_sync_skips_runner_without_selection_id():
    markets = [{
        "marketId": "1.1", "marketName": "MO", "event": {"id": "30000"},
        "runners": [
            {"selectionId": 1, "runnerName": "A"},
            {"runnerName": "NoId"},  # senza selectionId -> skip (no chiave "")
        ],
    }]
    db = _FakeDB()
    CatalogSyncService(db, _RecordingClient(BF_EVENTS, markets)).run_sync(force=True)
    assert len(db.runners) == 1
    assert db.runners[0]["selection_id"] == "1"


def test_catalog_sync_does_not_use_list_soccer_events():
    class _NoSoccer(_RecordingClient):
        def list_soccer_events(self, *a, **k):
            raise AssertionError("list_soccer_events non deve essere chiamato")

    db = _FakeDB()
    CatalogSyncService(db, _NoSoccer(BF_EVENTS, BF_MARKETS)).run_sync(force=True)
    assert len(db.events) == 1


# ---- BetfairClient: costruzione filtro ------------------------------------


def test_betfair_client_list_events_no_max_results():
    # BLOCK: listEvents NON deve inviare maxResults (Betfair non lo accetta ->
    # APINGException in LIVE).
    c = _client()
    captured = {}

    def fake(url, method, params, **kw):
        captured["params"] = params
        return []

    c._post_jsonrpc = fake
    c.list_events(["1"], in_play_only=True)

    assert "maxResults" not in captured["params"]
    assert captured["params"]["filter"]["eventTypeIds"] == ["1"]
    assert captured["params"]["filter"]["inPlayOnly"] is True


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
    assert f["marketTypeCodes"] == ["MATCH_ODDS", "CORRECT_SCORE"]


def test_betfair_client_market_catalogue_requests_event_projection():
    # BLOCK (Fugu/Fable): il raggruppamento batch usa mk['event']['id']; il
    # client DEVE chiedere EVENT (e COMPETITION) in marketProjection, altrimenti
    # i mercati arrivano senza event/competition e vengono scartati/incompleti.
    c = _client()
    captured = {}

    def fake(url, method, params, **kw):
        captured["params"] = params
        return []

    c._post_jsonrpc = fake
    c.list_market_catalogue(["1"], event_ids=["30000"], market_type_codes=["MATCH_ODDS"])

    proj = captured["params"]["marketProjection"]
    # Il servizio dipende dal contratto completo: EVENT/COMPETITION per il
    # raggruppamento, MARKET_DESCRIPTION per description.marketType, e
    # RUNNER_DESCRIPTION per i runner (selectionId/runnerName).
    assert "EVENT" in proj
    assert "COMPETITION" in proj
    assert "MARKET_DESCRIPTION" in proj
    assert "RUNNER_DESCRIPTION" in proj


def test_betfair_client_market_catalogue_max_results_weight_safe():
    # BLOCK (Fable): default maxResults deve restare entro il limite di peso
    # dati Betfair (<=200 con MARKET_DESCRIPTION) per non causare TOO_MUCH_DATA.
    c = _client()
    captured = {}

    def fake(url, method, params, **kw):
        captured["params"] = params
        return []

    c._post_jsonrpc = fake
    c.list_market_catalogue(["1"], event_ids=["30000"])
    assert captured["params"]["maxResults"] <= 200


def test_betfair_client_list_events_non_list_result():
    c = _client()
    c._post_jsonrpc = lambda *a, **k: {}
    assert c.list_events(["1"]) == []


def test_betfair_client_market_catalogue_raises_on_non_list():
    # BLOCK fail-open (CodeRabbit #369): una risposta MALFORMATA (non-lista) NON
    # deve degradare a [] -- degradare farebbe scambiare un errore upstream per
    # "0 mercati" e innescherebbe il cleanup distruttivo. Deve sollevare.
    c = _client()
    c._post_jsonrpc = lambda *a, **k: {"error": "boom"}
    with pytest.raises(RuntimeError):
        c.list_market_catalogue(["1"], event_ids=["30000"])
    # La lista vuota GENUINA resta valida (nessun mercato, nessun errore).
    c._post_jsonrpc = lambda *a, **k: []
    assert c.list_market_catalogue(["1"], event_ids=["30000"]) == []


# ---- SimulationBroker: parita' d'interfaccia (no crash in SIM) --------------


def test_simulation_broker_catalog_methods_return_empty():
    # Passa gli STESSI kwargs di produzione (in_play_only=False, max_results=200)
    # per intercettare eventuali mismatch di firma con BetfairClient.
    b = SimulationBroker(starting_balance=1000.0)
    assert b.list_events(["1"], in_play_only=False) == []
    assert b.list_market_catalogue(
        ["1"], event_ids=["x"], market_type_codes=["MATCH_ODDS"], max_results=200
    ) == []


def test_catalog_sync_in_simulation_preserves_catalog():
    # In SIM il broker non ha catalogo via API: sync a 0 eventi, catalogo
    # (popolato dai feed) preservato, nessun AttributeError.
    db = _FakeDB()
    CatalogSyncService(db, SimulationBroker(starting_balance=1000.0)).run_sync(force=True)
    assert db.events == []
    assert db.cleanup_called is False
