"""PR-C programma test hedge-fund grade: TTL, chiavi degeneri e seed restart.

Gap chiusi (dedup verificato — storm concorrente, stabilita' chiavi e
dedup E2E sono GIA' coperti in test_duplication_guard*.py e nel full
chain E2E; qui solo signal nuovo):
- TTL: la chiave scade e ammette un nuovo ordine SOLO dopo la scadenza
  (clock finto, zero sleep)
- acquire("") / chiave vuota: fail-closed, mai esecuzione
- payload senza campi identificativi: collassa sulla chiave degenere
  condivisa -> al massimo UN passaggio per TTL (bounded), gli altri
  bloccati; la difesa completa sta nello schema downstream che richiede
  market_id
- register_startup_order: il seed da ordini vivi su exchange al restart
  protegge la chiave (niente doppia bet post-riavvio)
"""
from __future__ import annotations

import pytest

from core.duplication_guard import DuplicationGuard


@pytest.mark.unit
@pytest.mark.concurrency
def test_ttl_expiry_allows_new_order_only_after_deadline(monkeypatch):
    """La chiave si libera SOLO oltre il TTL: prima e' duplicato."""
    fake_now = {"t": 1_000_000.0}
    monkeypatch.setattr("core.duplication_guard.time.time", lambda: fake_now["t"])

    guard = DuplicationGuard(ttl_seconds=120)
    key = "1.234:5678:BACK:telegram"

    assert guard.acquire(key) is True

    # Dentro il TTL (anche all'ultimo secondo): bloccato.
    fake_now["t"] += 119.0
    assert guard.acquire(key) is False

    # Esattamente al confine (delta == ttl NON e' ancora scaduto: > ttl).
    fake_now["t"] += 1.0
    assert guard.acquire(key) is False

    # Oltre il TTL: la chiave e' stata ripulita, nuovo ordine ammesso.
    fake_now["t"] += 1.0
    assert guard.acquire(key) is True


@pytest.mark.unit
@pytest.mark.concurrency
def test_release_frees_key_before_ttl(monkeypatch):
    fake_now = {"t": 2_000_000.0}
    monkeypatch.setattr("core.duplication_guard.time.time", lambda: fake_now["t"])

    guard = DuplicationGuard(ttl_seconds=3600)
    key = "1.234:5678:LAY:copy"

    assert guard.acquire(key) is True
    assert guard.acquire(key) is False
    guard.release(key)
    # Rilascio esplicito (es. ordine fallito): riacquisibile subito.
    assert guard.acquire(key) is True


@pytest.mark.unit
@pytest.mark.parametrize("empty", ["", "   ", None])
def test_acquire_empty_key_is_fail_closed(empty):
    """Chiave vuota = MAI autorizzazione a eseguire (fail-closed)."""
    guard = DuplicationGuard()
    assert guard.acquire(empty) is False
    assert guard.acquire(empty) is False  # idempotente, mai True


@pytest.mark.unit
def test_keyless_payloads_collapse_to_single_bounded_passage():
    """Payload senza market/selection: la chiave degenera in
    '::BACK:default' CONDIVISA -> al massimo un passaggio per TTL,
    tutti gli altri payload degeneri sono bloccati. (La difesa completa
    e' nello schema downstream: market_id e' campo obbligatorio.)"""
    guard = DuplicationGuard()

    degenerate_key = guard.build_event_key({})
    assert degenerate_key == "::BACK:default"

    assert guard.acquire(degenerate_key) is True
    # Qualsiasi altro payload degenere collassa sulla stessa chiave: bloccato.
    assert guard.acquire(guard.build_event_key({"note": "junk"})) is False
    assert guard.acquire(guard.build_event_key({"stake": 10})) is False


@pytest.mark.unit
@pytest.mark.recovery
def test_startup_seed_protects_live_orders_after_restart():
    """Riavvio con ordini vivi su Betfair: il seed registra le chiavi e
    lo stesso segnale ri-arrivato NON produce una seconda bet."""
    guard = DuplicationGuard()

    live_order = {
        "market_id": "1.234",
        "selection_id": 5678,
        "bet_type": "BACK",
        "source": "telegram",
    }
    assert guard.register_startup_order(live_order) is True

    # Il segnale equivalente post-restart e' un duplicato.
    same_signal_key = guard.build_event_key({
        "marketId": "1.234",          # alias camelCase: stessa chiave
        "selectionId": "5678",
        "side": "back",
        "strategy": "TELEGRAM",
    })
    assert guard.acquire(same_signal_key) is False

    # Ri-seed dello stesso ordine: False (gia' presente), nessun errore.
    assert guard.register_startup_order(live_order) is False


@pytest.mark.unit
@pytest.mark.recovery
def test_startup_seed_with_explicit_event_key_wins_over_fields():
    guard = DuplicationGuard()
    payload = {
        "event_key": "EXPLICIT:KEY:1",
        "market_id": "1.999",
        "selection_id": 1,
    }
    assert guard.register_startup_order(payload) is True
    assert guard.acquire("EXPLICIT:KEY:1") is False
    # I campi NON sono stati usati: la chiave costruita resta libera.
    assert guard.acquire(guard.build_event_key(payload)) is True


@pytest.mark.unit
@pytest.mark.recovery
def test_startup_seed_expires_with_ttl_like_any_key(monkeypatch):
    """Il seed non e' eterno: oltre il TTL la chiave si libera (l'ordine
    vivo nel frattempo e' gestito dalla reconciliation, non dal guard)."""
    fake_now = {"t": 3_000_000.0}
    monkeypatch.setattr("core.duplication_guard.time.time", lambda: fake_now["t"])

    guard = DuplicationGuard(ttl_seconds=60)
    assert guard.register_startup_order({"market_id": "1.1", "selection_id": 2}) is True

    fake_now["t"] += 61.0
    assert guard.acquire(guard.build_event_key(
        {"market_id": "1.1", "selection_id": 2}
    )) is True
