"""End-to-end del parser a delimitatori di #301 — copertura REALE sotto tests/.

Ex `hard_verify_parser_v2.py` (script `__main__`, mai eseguito in CI): convertito
in test pytest veri nel follow-up #374, così la copertura del percorso
messaggio -> evento -> mercato -> selection_id VIVE nella suite invece che in uno
script che nessuno lancia. Esercita i servizi #301 REALI (`DelimiterParser`,
`DeterministicResolver`) contro un catalogo Betfair in-memory.

La Fase 6 dello script (`DutchingCalculator`) NON è portata qui: `core.dutching_calculator`
era un calcolatore dutching naive usato solo dallo script — il dutching di produzione
è `dutching_batch_manager`/`money_management` — e viene rimosso nello stesso PR.

Oltre al percorso felice, i casi fail-closed sono espliciti (invariante Pickfair:
«il parser non inventa dati mancanti»): chat non configurata / evento sconosciuto /
runner sconosciuto NON producono un risultato inventato.
"""

from __future__ import annotations

import json

import pytest

from database import Database
from services.delimiter_parser import DelimiterParser
from services.deterministic_resolver import DeterministicResolver

_MSG = (
    "\n🔔 NUOVO SEGNALE\n"
    "PARTITA: Man City v Real Madrid\n"
    "MERCATO: Risultato Esatto\n"
    "SCORE: 1-1, 2-1, 1-2 ✅\n"
)


@pytest.fixture
def catalog_db():
    """DB in-memory con provider, alias, catalogo Betfair e parser a delimitatori.

    Rispecchia le Fasi 1-3 di hard_verify_parser_v2.py con l'API DB reale.
    """
    db = Database(db_path=":memory:")

    pid = db.save_provider("TipsterPro")
    db.save_name_alias(pid, "Manchester City", "man city", "UK")
    db.save_name_alias(pid, "Real Madrid", "real madrid", "ES")
    db.save_market_alias(pid, "risultato esatto", "CORRECT_SCORE", market_name="Correct Score")

    sync_id = "test_sync_1"
    db.upsert_bf_event("E1", "Manchester City v Real Madrid", "C1", "Champions League", "1", "2024-07-11T21:00:00", sync_id)
    db.upsert_bf_market("M1", "E1", "Correct Score", "CORRECT_SCORE", 1000.0, "2024-07-11T21:00:00", sync_id)
    db.upsert_bf_runner("M1", "11", "1 - 1", 0, 1, sync_id)
    db.upsert_bf_runner("M1", "21", "2 - 1", 0, 2, sync_id)
    db.upsert_bf_runner("M1", "12", "1 - 2", 0, 3, sync_id)

    parser_def = {
        "type": "DUTCHING",
        "fields": {
            "event": [{"START_AFTER": "PARTITA:", "END_BEFORE": "\n"}],
            "market": [{"START_AFTER": "MERCATO:", "END_BEFORE": "\n"}],
        },
        "dutching": {"fields": [{"START_AFTER": "SCORE:", "END_BEFORE": "✅"}]},
    }
    db.save_parser("TestParser", pid, json.dumps(parser_def))
    # DB fresco :memory: -> unico parser salvato = id 1 (autoincrement).
    db.set_parser_for_chat("CHAT123", 1)
    return db, pid


@pytest.mark.integration
def test_delimiter_parser_extracts_event_market_selections(catalog_db):
    db, _ = catalog_db
    parsed = DelimiterParser(db).parse_message("CHAT123", _MSG)
    assert parsed is not None
    assert parsed["type"] == "DUTCHING"
    assert parsed["event"] == "Man City v Real Madrid"
    assert parsed["market"] == "Risultato Esatto"
    assert parsed["selections"] == ["1-1", "2-1", "1-2"]


@pytest.mark.integration
def test_deterministic_resolver_maps_event_market_runners(catalog_db):
    db, pid = catalog_db
    resolver = DeterministicResolver(db)

    event_id = resolver.resolve_event(pid, "Man City")
    assert event_id == "E1"

    market = resolver.resolve_market(pid, event_id, "Risultato Esatto")
    assert market is not None
    assert market["market_id"] == "M1"

    sids = [resolver.resolve_runner(market["market_id"], sel.replace("-", " - ")) for sel in ["1-1", "2-1", "1-2"]]
    assert sids == ["11", "21", "12"]


@pytest.mark.integration
def test_pipeline_message_to_selection_ids(catalog_db):
    """Percorso completo: messaggio Telegram -> selection_id Betfair."""
    db, pid = catalog_db
    parsed = DelimiterParser(db).parse_message("CHAT123", _MSG)
    resolver = DeterministicResolver(db)

    event_id = resolver.resolve_event(pid, parsed["event"].split(" v ")[0])
    assert event_id == "E1"

    market = resolver.resolve_market(pid, event_id, parsed["market"])
    assert market["market_id"] == "M1"

    selection_ids = []
    for sel in parsed["selections"]:
        sid = resolver.resolve_runner(market["market_id"], sel.replace("-", " - "))
        assert sid is not None, f"selection {sel!r} non risolta"
        selection_ids.append(sid)
    assert selection_ids == ["11", "21", "12"]


# --- Fail-closed: parser/resolver non inventano dati mancanti (BLOCK) ---

@pytest.mark.integration
def test_parser_returns_none_for_unconfigured_chat(catalog_db):
    db, _ = catalog_db
    # chat senza parser associato -> nessun segnale inventato
    assert DelimiterParser(db).parse_message("CHAT_SCONOSCIUTA", _MSG) is None


@pytest.mark.integration
def test_resolver_returns_none_for_unknown_event(catalog_db):
    db, pid = catalog_db
    assert DeterministicResolver(db).resolve_event(pid, "Squadra Inesistente") is None


@pytest.mark.integration
def test_resolver_returns_none_for_unknown_runner(catalog_db):
    db, pid = catalog_db
    resolver = DeterministicResolver(db)
    # mercato valido ma score non a catalogo -> nessun selection_id inventato
    assert resolver.resolve_runner("M1", "9 - 9") is None
