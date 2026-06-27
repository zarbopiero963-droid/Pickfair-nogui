"""B6.3.2a — registry in-memory dei bet_id DIRECT in RuntimeController.

Verifica il lifecycle del registry ``customer_ref → bet_id`` alimentato dagli
eventi quick-bet: popolato solo su ack (ACCEPTED/PARTIAL) di un ordine con
``best_price_source`` (percorso best-price DIRECT) + ``bet_id`` + ``customer_ref``;
pulito su eventi terminali/cancel per ``customer_ref``. Nessun poller, nessun
cancel, nessun side-effect broker: solo tracking. Registry vuoto al restart.
"""
from __future__ import annotations

import pytest

from core.runtime_controller import RuntimeController

pytestmark = pytest.mark.unit


class _Config:
    table_count = 1
    anti_duplication_enabled = False
    allow_recovery = False
    auto_reset_drawdown_pct = 90
    defense_drawdown_pct = 7.5
    lockdown_drawdown_pct = 95

    def __getattr__(self, _name):
        return 0


class _Settings:
    @staticmethod
    def load_roserpina_config():
        return _Config()


class _Bus:
    @staticmethod
    def subscribe(*_a, **_k):
        return None

    @staticmethod
    def publish(*_a, **_k):
        return None


class _Db:
    @staticmethod
    def _execute(*_a, **_k):
        return None

    @staticmethod
    def get_pending_sagas():
        return []


class _Betfair:
    _session_invalid = False

    @staticmethod
    def set_simulation_mode(*_a, **_k):
        return None

    @staticmethod
    def get_live_client():
        return None

    @staticmethod
    def connect(**_k):
        return {}

    @staticmethod
    def disconnect():
        return None

    @staticmethod
    def get_account_funds():
        return {"available": 0.0}

    @staticmethod
    def status():
        return {"connected": False}


class _Telegram:
    @staticmethod
    def start():
        return {}

    @staticmethod
    def stop():
        return None

    @staticmethod
    def status():
        return {}


def _make_rc():
    return RuntimeController(
        bus=_Bus(), db=_Db(), settings_service=_Settings(),
        betfair_service=_Betfair(), telegram_service=_Telegram(),
    )


def _ack(customer_ref="C1", bet_id="B1", direct=True, **extra):
    p = {"customer_ref": customer_ref, "bet_id": bet_id,
         "market_id": "1.1", "selection_id": 55}
    if direct:
        p["best_price_source"] = "LIVE_BOOK_DIRECT"
    p.update(extra)
    return p


# =========================================================
# Restart fail-safe
# =========================================================
def test_registry_empty_on_construction():
    rc = _make_rc()
    assert rc.direct_unmatched_bet_ids == set()


# =========================================================
# Popolamento solo su ack DIRECT con bet_id + customer_ref
# =========================================================
def test_accepted_direct_order_is_tracked():
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(customer_ref="C1", bet_id="B1"))
    assert rc.direct_unmatched_bet_ids == {"B1"}


def test_partial_direct_order_is_tracked():
    rc = _make_rc()
    rc._on_quick_bet_partial(_ack(customer_ref="C2", bet_id="B2"))
    assert rc.direct_unmatched_bet_ids == {"B2"}


def test_non_direct_order_not_tracked():
    # Senza best_price_source (flag OFF / percorso non best-price) => non tracciato.
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(direct=False))
    assert rc.direct_unmatched_bet_ids == set()


def test_ack_without_bet_id_not_tracked():
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(bet_id=""))
    assert rc.direct_unmatched_bet_ids == set()


def test_ack_without_customer_ref_not_tracked():
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(customer_ref=""))
    assert rc.direct_unmatched_bet_ids == set()


# =========================================================
# Pulizia su eventi terminali / cancel (per customer_ref)
# =========================================================
@pytest.mark.parametrize("handler", [
    "_on_quick_bet_filled", "_on_quick_bet_failed",
])
def test_terminal_event_clears_registry(handler):
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(customer_ref="C1", bet_id="B1"))
    assert rc.direct_unmatched_bet_ids == {"B1"}
    getattr(rc, handler)({"customer_ref": "C1", "bet_id": "B1"})
    assert rc.direct_unmatched_bet_ids == set()


@pytest.mark.parametrize("handler", [
    "_on_quick_bet_success", "_on_quick_bet_ambiguous",
])
def test_success_or_ambiguous_keeps_live_order_tracked(handler):
    # Greptile P2: un place riuscito/incerto può lasciare un ordine non abbinato
    # vivo => NON deve togliere il bet_id dall'allowlist del poller TTL.
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(customer_ref="C1", bet_id="B1"))
    getattr(rc, handler)({"customer_ref": "C1", "bet_id": "B1"})
    assert rc.direct_unmatched_bet_ids == {"B1"}


def test_rollback_without_bet_id_still_clears_by_customer_ref():
    # ROLLBACK_DONE non porta il bet_id: la pulizia avviene per customer_ref.
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(customer_ref="C9", bet_id="B9"))
    rc._on_quick_bet_rollback_done({"customer_ref": "C9", "reason": "rolled back"})
    assert rc.direct_unmatched_bet_ids == set()


def test_terminal_for_other_customer_ref_is_noop():
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(customer_ref="C1", bet_id="B1"))
    rc._on_quick_bet_filled({"customer_ref": "OTHER", "bet_id": "ZZ"})
    assert rc.direct_unmatched_bet_ids == {"B1"}


# =========================================================
# Più ordini + aggiornamento
# =========================================================
def test_multiple_orders_tracked_and_pruned_independently():
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(customer_ref="C1", bet_id="B1"))
    rc._on_quick_bet_accepted(_ack(customer_ref="C2", bet_id="B2"))
    assert rc.direct_unmatched_bet_ids == {"B1", "B2"}
    rc._on_quick_bet_filled({"customer_ref": "C1", "bet_id": "B1"})
    assert rc.direct_unmatched_bet_ids == {"B2"}


def test_property_returns_copy_not_live_view():
    rc = _make_rc()
    rc._on_quick_bet_accepted(_ack(customer_ref="C1", bet_id="B1"))
    snap = rc.direct_unmatched_bet_ids
    snap.add("HACK")
    assert rc.direct_unmatched_bet_ids == {"B1"}
