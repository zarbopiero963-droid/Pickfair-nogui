"""Gate A — batch B, signals: coda dei segnali attivi (core/signal_queue.py), logica pura."""
from core.signal_queue import APPEND_ACTIVE, QUEUE_UNTIL_CONFIRMED, SignalQueue


def test_trg_628_b_0869():
    """ID: TRG-628-B-0869 / PF-B1-QUEUE-CAP

    A capienza max_active un NUOVO segnale e' rifiutato (None) senza mutare i segnali attivi."""
    for mode in (APPEND_ACTIVE, QUEUE_UNTIL_CONFIRMED):
        q = SignalQueue(mode=mode, default_timeout=60, max_active=2)
        assert q.add({"m": "1.1"}, signal_id="a", now=100.0) == "a"
        assert q.add({"m": "1.2"}, signal_id="b", now=101.0) == "b"
        before = q.state()
        assert q.add({"m": "1.3"}, signal_id="c", now=102.0) is None
        assert q.add({"m": "1.4"}, now=103.0) is None
        assert q.state() == before, "coda mutata da un segnale rifiutato"
        assert q.active_ids() == ["a", "b"]
        # l'aggiornamento di un segnale gia' attivo non e' un nuovo segnale: resta ammesso
        assert q.add({"m": "1.1x"}, signal_id="a", now=104.0) == "a"
        assert len(q.active_ids()) == 2


def test_trg_628_b_0872():
    """ID: TRG-628-B-0872 / PF-B1-QUEUE-RESTORE

    restore_state ripristina le voci valide e scarta quelle che sollevano KeyError/TypeError/ValueError."""
    q = SignalQueue(mode=APPEND_ACTIVE, default_timeout=60)
    q.add({"m": "old"}, signal_id="old", now=1.0)
    data = [
        {"signal_id": "s1", "row": {"m": "1.1"}, "added_at": 10.0, "timeout": 30.0, "dedup_key": "k1"},
        {"row": {"m": "x"}, "added_at": 10.0, "timeout": 30.0},                              # KeyError
        {"signal_id": "bad-row", "row": None, "added_at": 10.0, "timeout": 30.0},            # TypeError
        {"signal_id": "bad-to", "row": {}, "added_at": 10.0, "timeout": -5},                 # ValueError
        {"signal_id": "bad-now", "row": {}, "added_at": float("nan"), "timeout": 30.0},     # ValueError
        {"signal_id": "s2", "row": {"m": "1.2"}, "added_at": 11.0, "timeout": 20.0},
    ]
    q.restore_state(data)
    assert q.active_ids() == ["s1", "s2"]
    st = q.state()
    assert st[0] == {"signal_id": "s1", "row": {"m": "1.1"}, "added_at": 10.0, "timeout": 30.0, "dedup_key": "k1"}
    assert st[1]["dedup_key"] == ""
