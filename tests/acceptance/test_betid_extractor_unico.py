"""F-14 (#586): un solo estrattore del betId per identita' del bot (cashout, F-04) e riconciliazione all'avvio.

Rilievo di Claude C (04/11/2026): `Database._order_bet_id` e `RuntimeController._open_order_candidates` avevano
precedenze diverse. Su `{"result": {"betId": "222"}}` il primo dava "" e la riconciliazione "222"; su
`{"betId": "1", "instructionReports": [{"betId": "2"}]}` il primo "1" e la riconciliazione "2". Una riga con due betId
diversi non ha un'identita' verificabile: nessun betId (=> incerta, riconciliazione), mai uno scelto a caso.
"""
from __future__ import annotations

import json

import pytest

from core.order_identity import response_bet_id
from database import Database

FORME = [
    ({"bet_id": "1"}, "1"),
    ({"betId": "1"}, "1"),
    ({"instructionReports": [{"betId": "7"}]}, "7"),
    ({"ok": True, "result": {"instructionReports": [{"betId": "8"}]}}, "8"),            # BetfairClient.place_bet
    ({"raw": {"instructionReports": [{"betId": "9"}]}}, "9"),
    ({"result": {"betId": "222"}}, "222"),                                             # divergenza 1
    ({"betId": "5", "instructionReports": [{"betId": "5"}]}, "5"),                     # stessa identita'
    ({"bet_id": "5", "raw": {"instructionReports": [{"betId": "5"}]}}, "5"),          # OrderRouter._result
    ({"ok": False, "error": "order_unknown", "result": {"instructionReports": [{"betId": "4"}]}}, "4"),  # P0
    ({"betId": 12345}, "12345"),
    ({}, ""),
    (None, ""),
    ("x", ""),
    ({"instructionReports": [{}]}, ""),
    ({"instructionReports": ["x"]}, ""),
    ({"betId": True}, ""),                                                              # bool non e' un betId
    ({"betId": "  "}, ""),
]
CONFLITTI = [
    {"betId": "1", "instructionReports": [{"betId": "2"}]},                            # divergenza 2
    {"instructionReports": [{"betId": "1"}, {"betId": "2"}]},                          # risposta a piu' istruzioni
    {"betId": "1", "result": {"instructionReports": [{"betId": "2"}]}},
]


@pytest.mark.parametrize("risposta,atteso", FORME)
def test_forme_note(risposta, atteso) -> None:
    assert response_bet_id(risposta) == atteso


@pytest.mark.parametrize("risposta", CONFLITTI)
def test_block_betid_in_conflitto_nessuna_identita(risposta) -> None:
    assert response_bet_id(risposta) == ""


@pytest.mark.parametrize("risposta", [r for r, _ in FORME] + CONFLITTI)
def test_database_e_riconciliazione_concordano(tmp_path, risposta) -> None:
    """Stessa riga `orders`: identita' del bot (cashout/F-04) e candidati della riconciliazione vedono lo stesso
    betId."""
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain

    c = _chain(tmp_path)
    c.db.insert_order({"customer_ref": "ref-1", "correlation_id": "c-1", "status": "SUBMITTED",
                       "payload": {"market_id": "1.1", "selection_id": 7, "event_key": "1.1:7:BACK", "bet_type": "BACK",
                                   "stake": 2.0, "price": 2.0},
                       "response": risposta if isinstance(risposta, dict) else {"v": risposta}})
    candidati, _ = c.rc._open_order_candidates()
    riga = c.db._execute("SELECT response_json FROM orders", fetch=True, commit=False)[0]
    assert candidati[0]["bet_id"] == Database._order_bet_id(json.loads(riga["response_json"]))


@pytest.mark.parametrize("stato", ["AMBIGUOUS", "INFLIGHT", "SUBMITTED"])
def test_riga_incerta_non_espone_importi_del_report(tmp_path, stato) -> None:
    """Vincolo MCP-01k: il report conservato non e' un dato confermato. Il candidato di riconciliazione porta il betId
    (identita') e lo stake della RICHIESTA, mai gli importi abbinati del report."""
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain

    c = _chain(tmp_path)
    c.db.insert_order({"customer_ref": "ref-9", "correlation_id": "c-9", "status": stato,
                       "payload": {"market_id": "1.1", "selection_id": 7, "event_key": "1.1:7:BACK", "bet_type": "BACK",
                                   "stake": 2.0, "price": 2.0},
                       "response": {"ok": False, "result": {"status": "PROCESSED_WITH_ERRORS", "instructionReports": [
                           {"status": "SUCCESS", "betId": "77", "sizeMatched": 999.0, "averagePriceMatched": 9.9}]}}})
    candidato = c.rc._open_order_candidates()[0][0]

    assert candidato["bet_id"] == "77" and candidato["stake"] == 2.0 and candidato["price"] == 2.0
    testo = json.dumps(candidato, default=str)
    assert "999" not in testo and "9.9" not in testo, candidato
