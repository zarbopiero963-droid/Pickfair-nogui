"""Gate A — T2b batch b2, componente market_stream (services/streaming_feed.StreamingFeed).

Protocollo stream Betfair (P35, oracolo tecnico): delta dei ladder level-based. Nessun client, nessuna rete.
"""
from __future__ import annotations

from services.streaming_feed import StreamingFeed


def _feed() -> StreamingFeed:
    return StreamingFeed(client_getter=lambda: None,
                         config={"enabled": True, "market_data_mode": "stream", "market_ids": ["1.1"]},
                         on_market_book=lambda _b: None)


def test_trg_628_b_0826():
    """ID: TRG-628-B-0826 / PF-B1-STREAM-DELTA

    Delta con livello [level, price, size]: aggiorna solo quel livello, size <= 0 lo rimuove, gli altri restano.
    Delta senza livello [price, size]: sostituisce l'intero ladder. Delta vuoto: ladder invariato. Il percorso
    reale _merge_runner_change applica le stesse regole a batb/batl."""
    feed = _feed()
    base = [{"level": 0, "price": 2.0, "size": 10.0}, {"level": 1, "price": 1.98, "size": 5.0},
            {"level": 2, "price": 1.96, "size": 7.0}]

    upd = feed._merge_ladder_points(base, [{"level": 0, "price": 2.02, "size": 3.0}])
    assert upd == [{"price": 2.02, "size": 3.0}, {"price": 1.98, "size": 5.0}, {"price": 1.96, "size": 7.0}]

    removed = feed._merge_ladder_points(base, [{"level": 2, "price": 1.96, "size": 0}])
    assert removed == [{"price": 2.0, "size": 10.0}, {"price": 1.98, "size": 5.0}]
    neg = feed._merge_ladder_points(base, [{"level": 1, "price": 1.98, "size": -1}])
    assert {"price": 1.98, "size": 5.0} not in neg and len(neg) == 2

    replaced = feed._merge_ladder_points(base, [{"price": 3.0, "size": 1.0}])
    assert replaced == [{"price": 3.0, "size": 1.0}]
    assert feed._merge_ladder_points(base, []) == base

    # Percorso reale: runner change level-based su batb e batl.
    state = {"runners_by_id": {}}
    feed._merge_runner_change(state, {"id": 5, "batb": [[0, 2.0, 10], [1, 1.98, 5]], "batl": [[0, 2.04, 8]]})
    feed._merge_runner_change(state, {"id": 5, "batb": [[1, 1.98, 0]], "batl": [[0, 2.06, 4]]})
    ex = state["runners_by_id"][next(iter(state["runners_by_id"]))]["ex"]
    assert ex["availableToBack"] == [{"price": 2.0, "size": 10}]
    assert ex["availableToLay"] == [{"price": 2.06, "size": 4}]
