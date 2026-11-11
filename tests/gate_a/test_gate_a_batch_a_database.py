"""Gate A — batch A, componente database (de-dup durevole degli intenti, #461 PR26).

Database SQLite reale su tmp_path, nessun doppio.
"""
from __future__ import annotations

from database import Database


def test_trg_628_b_0803(tmp_path):
    """ID: TRG-628-B-0803 / PF-B1-DB-INTENT-RELEASE

    release_order_intent cancella la marcatura SOLO se customer_ref E attempt_id
    coincidono con quelli della riga; ogni altra combinazione non tocca nulla.
    """
    db = Database(str(tmp_path / "g0803.db"))
    assert db.consume_order_intent("REF-A", attempt_id="ATT-A", correlation_id="C-A") is True
    assert db.consume_order_intent("REF-B", attempt_id="ATT-B", correlation_id="C-B") is True

    # attempt_id di un altro tentativo sullo stesso ref: nessuna cancellazione
    assert db.release_order_intent("REF-A", attempt_id="ATT-B") is False
    assert db.is_order_intent_consumed("REF-A") is True
    # attempt_id giusto ma customer_ref diverso: nessuna cancellazione di REF-B
    assert db.release_order_intent("REF-X", attempt_id="ATT-B") is False
    assert db.is_order_intent_consumed("REF-B") is True
    # input vuoti: nessuna cancellazione
    assert db.release_order_intent("", attempt_id="ATT-A") is False
    assert db.release_order_intent("REF-A", attempt_id="") is False
    assert db.is_order_intent_consumed("REF-A") is True

    # coppia esatta: cancella solo la propria riga
    assert db.release_order_intent("REF-A", attempt_id="ATT-A") is True
    assert db.is_order_intent_consumed("REF-A") is False
    assert db.is_order_intent_consumed("REF-B") is True
    # una seconda release della stessa coppia non trova piu' nulla
    assert db.release_order_intent("REF-A", attempt_id="ATT-A") is False
