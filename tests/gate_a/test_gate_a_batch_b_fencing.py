"""Gate A — batch B, fencing: barriera CASHOUT e fence d'ammissione per scope (core/runtime_controller.py).

Si esercitano i metodi reali `_begin_cashout_barrier`, `_capture_admission_fence`, `_admission_fence_valid` e
`_end_cashout_barrier` su un RuntimeController non avviato (nessuna rete, nessun ordine)."""
from core.runtime_controller import RuntimeController


def _rc():
    return RuntimeController.__new__(RuntimeController)


def test_trg_628_b_0847():
    """ID: TRG-628-B-0847 / PF-B1-PR505-FENCE-SCOPE

    La barriera crea lo scope globale (CASHOUT_ALL), di partita, di mercato o di selezione e incrementa l'epoca
    di ogni chiave dello scope."""
    rc = _rc()
    _bid, scope = rc._begin_cashout_barrier({"signal_type": "CASHOUT_ALL"})
    assert scope == frozenset({("global",)})
    assert rc._admission_epochs[("global",)] == 1

    _bid, scope = rc._begin_cashout_barrier({"signal_type": "CASHOUT", "market_id": "1.5", "selection_id": 7})
    assert scope == frozenset({("selection", "1.5", 7)})
    assert rc._admission_epochs[("selection", "1.5", 7)] == 1

    _bid, scope = rc._begin_cashout_barrier({"signal_type": "CASHOUT", "market_id": "1.6"})
    assert scope == frozenset({("market", "1.6")})

    _bid, scope = rc._begin_cashout_barrier({"signal_type": "CASHOUT", "event_name": "  Inter - Milan "})
    assert scope == frozenset({("event", "inter - milan")})

    rc._begin_cashout_barrier({"signal_type": "CASHOUT_ALL"})
    assert rc._admission_epochs[("global",)] == 2, "epoca globale non incrementata alla seconda barriera"
    assert len(rc._cashout_barriers) == 5


def test_trg_628_b_0848():
    """ID: TRG-628-B-0848 / PF-B1-PR505-FENCE-CHECK

    Il fence d'ammissione e' rifiutato se interseca uno scope di barriera attivo o se un'epoca e' cambiata dalla
    cattura; un fence su scope indipendente resta valido."""
    rc = _rc()
    sig_a = {"market_id": "1.5", "selection_id": 7}
    sig_b = {"market_id": "1.9", "selection_id": 3}
    fence_a_before = rc._capture_admission_fence(sig_a)
    fence_b = rc._capture_admission_fence(sig_b)
    assert rc._admission_fence_valid(fence_a_before) and rc._admission_fence_valid(fence_b)

    bid, _ = rc._begin_cashout_barrier({"signal_type": "CASHOUT", "market_id": "1.5", "selection_id": 7})
    # catturato DURANTE la barriera: epoca coerente ma scope attivo intersecato
    fence_a_during = rc._capture_admission_fence(sig_a)
    assert rc._admission_fence_valid(fence_a_during) is False, "fence ammesso con barriera attiva sullo scope"
    assert rc._admission_fence_valid(fence_b) is True, "scope indipendente bloccato"

    rc._end_cashout_barrier(bid)
    # barriera chiusa: il fence catturato prima resta invalido perche' l'epoca e' cambiata
    assert rc._admission_fence_valid(fence_a_before) is False, "fence con epoca superata ammesso"
    assert rc._admission_fence_valid(rc._capture_admission_fence(sig_a)) is True
