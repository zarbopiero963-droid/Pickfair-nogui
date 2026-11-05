"""G7-R06 (#586): ricostruzione F018 e prova di settlement. Catena reale (RuntimeController + SQLite) senza exchange."""
from __future__ import annotations

import time

import pytest

from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain


def _ambiguous(c, ref="ref-r06", market="1.5"):
    c.db.insert_order({"customer_ref": ref, "correlation_id": f"c-{ref}", "status": "AMBIGUOUS",
                       "payload": {"market_id": market, "selection_id": 7, "event_key": f"{market}:7:BACK",
                                   "bet_type": "BACK", "stake": 3.0, "price": 2.0}})


def _effect(c, key, ref, outcome):
    c.db._execute("INSERT INTO settlement_effects(settlement_key, outcome, customer_ref, market_id, pnl, recorded_at) "
                  "VALUES (?, ?, ?, ?, ?, ?)", (key, outcome, ref, "1.5", 2.865, time.time()))


def test_effetto_applicato_chiude_l_ambiguous_con_la_chiave_citata(tmp_path) -> None:
    c = _chain(tmp_path)
    _ambiguous(c)
    _effect(c, "stl-1", "ref-r06", "released")

    out = c.rc._rebuild_open_positions()

    assert out["closed"] == ["ref-r06"] and out["settled"] == {"ref-r06": "stl-1"}
    assert out["uncertain"] == [] and c.rc.table_manager.total_exposure() == 0.0


@pytest.mark.parametrize("outcome", ["held"])
def test_block_effetto_trattenuto_non_e_una_prova(tmp_path, outcome) -> None:
    c = _chain(tmp_path)
    _ambiguous(c)
    _effect(c, "stl-2", "ref-r06", outcome)

    out = c.rc._rebuild_open_positions()

    assert out["uncertain"] == ["ref-r06"] and c.rc.table_manager.total_exposure() == 3.0


def test_block_effetto_di_un_altro_intento_non_chiude(tmp_path) -> None:
    c = _chain(tmp_path)
    _ambiguous(c)
    _effect(c, "stl-3", "ref-altro", "released")

    out = c.rc._rebuild_open_positions()

    assert out["uncertain"] == ["ref-r06"] and "settled" not in out


def test_block_registro_illeggibile_resta_incerto(tmp_path, monkeypatch) -> None:
    c = _chain(tmp_path)
    _ambiguous(c)
    _effect(c, "stl-4", "ref-r06", "released")

    def rotto(_refs):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(c.db, "settled_customer_refs", rotto)
    out = c.rc._rebuild_open_positions()

    assert out["uncertain"] == ["ref-r06"] and c.rc.table_manager.total_exposure() == 3.0


def test_settled_customer_refs_sola_lettura_e_filtri(tmp_path) -> None:
    c = _chain(tmp_path)
    _effect(c, "a", "r1", "released")
    _effect(c, "b", "r2", "held")
    _effect(c, "c", "", "released")
    assert c.db.settled_customer_refs(["r1", "r2", "", None]) == {"r1": "a"}
    assert c.db.settled_customer_refs([]) == {}
