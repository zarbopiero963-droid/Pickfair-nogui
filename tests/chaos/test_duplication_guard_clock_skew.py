"""PR-L programma test hedge-fund grade: chaos/stress — clock skew sul dedup.

Phase 0 dedup (verificato): dei 6 scenari di PR-L cinque sono GIA' coperti
in modo forte (partition post-submit/ghost, DB locked/sqlite busy, kill a
meta' ordine, 100 segnali/5s, restart con INFLIGHT — vedi tests/chaos,
tests/recovery, tests/reconciliation, tests/failure). L'UNICO gap
netto-nuovo era il clock skew sul DuplicationGuard.

Gap reale chiuso (fix autorizzato dall'owner su core/duplication_guard.py):
il TTL del guard usava time.time() (wall-clock). Un salto del wall-clock
in AVANTI oltre il TTL (correzione NTP, sleep/resume del VPS, set manuale)
faceva considerare scaduta una chiave ancora viva -> _cleanup_locked la
rimuoveva -> il duplicato passava acquire() = DOPPIA BET. Il fix porta il
TTL su time.monotonic(), immune ai salti del wall-clock; il timestamp
leggibile in _registered_at resta wall-clock (solo audit).

Questi test pilotano DUE orologi separati (time.time e time.monotonic) in
core.duplication_guard, cosi' da provare che la decisione di dedup dipende
SOLO dal monotonico.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from core.duplication_guard import DuplicationGuard


class _Clock:
    """Due sorgenti di tempo indipendenti, pilotabili a mano."""

    def __init__(self, *, mono: float = 10_000.0, wall: float = 1_700_000_000.0):
        self.mono = mono
        self.wall = wall

    def install(self, monkeypatch):
        monkeypatch.setattr("core.duplication_guard.time.monotonic", lambda: self.mono)
        monkeypatch.setattr("core.duplication_guard.time.time", lambda: self.wall)


# ---------------------------------------------------------------------------
# BLOCK: il salto in AVANTI del wall-clock NON deve liberare la chiave
# ---------------------------------------------------------------------------

@pytest.mark.chaos
@pytest.mark.concurrency
def test_wallclock_forward_jump_does_not_free_dedup_key(monkeypatch):
    """Regressione del bug: con il wall-clock il salto avanti >TTL liberava
    la chiave e ammetteva il duplicato (doppia bet). Col TTL monotonico,
    un balzo enorme del wall-clock a monotonico fermo NON deve liberarla."""
    clock = _Clock()
    clock.install(monkeypatch)

    guard = DuplicationGuard(ttl_seconds=120)
    key = "1.234:5678:BACK:telegram"

    assert guard.acquire(key) is True

    # Il wall-clock salta avanti di un'ORA (>> TTL), ma il monotonico avanza
    # di un solo secondo: la chiave e' ancora dentro il TTL reale.
    clock.wall += 3600.0
    clock.mono += 1.0

    # DEVE restare un duplicato: nessuna seconda bet.
    assert guard.acquire(key) is False
    assert guard.is_duplicate(key) is True


@pytest.mark.chaos
def test_wallclock_forward_jump_keeps_startup_seed_protected(monkeypatch):
    """Stessa difesa lato seed da restart: un ordine vivo seedato resta
    protetto anche se il wall-clock salta avanti oltre il TTL."""
    clock = _Clock()
    clock.install(monkeypatch)

    guard = DuplicationGuard(ttl_seconds=120)
    assert guard.register_startup_order({"market_id": "1.1", "selection_id": 2}) is True

    clock.wall += 10_000.0   # salto enorme del wall-clock
    clock.mono += 5.0        # monotonico quasi fermo

    dup_key = guard.build_event_key({"market_id": "1.1", "selection_id": 2})
    assert guard.acquire(dup_key) is False


# ---------------------------------------------------------------------------
# PASS: il TTL monotonico continua a scadere correttamente
# ---------------------------------------------------------------------------

@pytest.mark.chaos
@pytest.mark.concurrency
def test_monotonic_ttl_still_expires_after_real_elapsed(monkeypatch):
    """Controllo PASS: il fix non rompe la scadenza. Avanzando il
    monotonico oltre il TTL la chiave si libera (delta > ttl)."""
    clock = _Clock()
    clock.install(monkeypatch)

    guard = DuplicationGuard(ttl_seconds=120)
    key = "1.5:9:LAY:copy"

    assert guard.acquire(key) is True

    clock.mono += 120.0          # esattamente al confine: NON ancora scaduto
    assert guard.acquire(key) is False

    clock.mono += 1.0            # oltre il TTL: liberata
    assert guard.acquire(key) is True


# ---------------------------------------------------------------------------
# PASS: il salto INDIETRO del wall-clock non corrompe il TTL
# ---------------------------------------------------------------------------

@pytest.mark.chaos
def test_wallclock_backward_jump_does_not_corrupt_ttl(monkeypatch):
    """Controllo PASS: un wall-clock che torna indietro (delta negativo nel
    vecchio codice -> chiave mai scaduta / leak) non ha effetto: il TTL
    dipende solo dal monotonico, che e' sempre crescente."""
    clock = _Clock()
    clock.install(monkeypatch)

    guard = DuplicationGuard(ttl_seconds=60)
    key = "1.9:3:BACK:telegram"

    assert guard.acquire(key) is True

    # Il wall-clock torna indietro di un'ora; il monotonico avanza oltre il TTL.
    clock.wall -= 3600.0
    clock.mono += 61.0

    # La chiave scade normalmente sul monotonico: riacquisibile.
    assert guard.acquire(key) is True


# ---------------------------------------------------------------------------
# PASS: l'audit (_registered_at / snapshot) resta wall-clock leggibile
# ---------------------------------------------------------------------------

@pytest.mark.chaos
def test_registered_at_audit_stays_wallclock(monkeypatch):
    """Lo scope del fix e' chirurgico: solo il TTL passa a monotonic. Il
    timestamp di audit resta un wall-clock ISO leggibile (UTC), non un
    float monotonico."""
    clock = _Clock(wall=1_700_000_000.0)
    clock.install(monkeypatch)

    guard = DuplicationGuard()
    guard.acquire("1.1:2:BACK:telegram")

    snap = guard.snapshot()
    assert snap["active_count"] == 1
    registered_at = snap["active_keys"][0]["registered_at"]
    # ISO wall-clock (es. "2023-11-14T..."), non lo stub monotonico "10000".
    assert "T" in registered_at
    assert str(int(clock.mono)) not in registered_at
    # Deve restare un datetime ISO parsabile: il test fallisce se l'audit
    # smette di usare una rappresentazione wall-clock leggibile.
    assert isinstance(datetime.fromisoformat(registered_at), datetime)
