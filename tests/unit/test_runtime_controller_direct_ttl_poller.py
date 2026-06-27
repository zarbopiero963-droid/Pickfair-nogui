"""B6.3.2b — poller TTL/cancel ordini DIRECT non abbinati in RuntimeController.

Test di sicurezza sul cancel REALE (mockato): default-OFF, solo ordini DIRECT
noti (allowlist dal registry B6.3.2a), NO RETRY, fail-closed su fetch/cancel.
Nessun ordine fuori da ``direct_bet_ids`` deve mai essere cancellato.
"""
from __future__ import annotations

import time

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
    # Flag/parametri TTL come attributi REALI (il __getattr__ ritorna 0 per gli
    # ignoti, che falserebbe ttl/intervallo). Default-OFF.
    direct_unmatched_ttl_enabled = False
    direct_unmatched_ttl_sec = 120.0
    direct_unmatched_ttl_poll_sec = 30.0

    def __getattr__(self, _name):
        return 0


class _Settings:
    @staticmethod
    def load_roserpina_config():
        return _Config()


class _Bus:
    def __init__(self):
        self.events = []

    @staticmethod
    def subscribe(*_a, **_k):
        return None

    def publish(self, name, payload=None):
        self.events.append((name, payload or {}))


class _Db:
    @staticmethod
    def _execute(*_a, **_k):
        return None

    @staticmethod
    def get_pending_sagas():
        return []


class _CancelClient:
    def __init__(self, *, ok=True, raises=False):
        self.calls = []
        self._ok = ok
        self._raises = raises

    def cancel_orders(self, *, market_id, bet_ids=None):
        self.calls.append((market_id, tuple(bet_ids or ())))
        if self._raises:
            raise RuntimeError("cancel boom")
        return {"ok": self._ok, "cancelled_count": len(bet_ids or ())}


class _Betfair:
    _session_invalid = False

    def __init__(self, *, orders=None, orders_raises=False, cancel_client=None):
        self._orders = orders if orders is not None else []
        self._orders_raises = orders_raises
        self._cancel_client = cancel_client

    def list_current_orders(self, market_ids=None):
        if self._orders_raises:
            raise RuntimeError("list boom")
        return list(self._orders)

    def get_simulation_broker(self):
        return self._cancel_client

    def get_live_client(self):
        return self._cancel_client

    @staticmethod
    def set_simulation_mode(*_a, **_k):
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


def _make_rc(*, orders=None, orders_raises=False, cancel_client=None, enabled=True):
    bf = _Betfair(orders=orders, orders_raises=orders_raises, cancel_client=cancel_client)
    rc = RuntimeController(
        bus=_Bus(), db=_Db(), settings_service=_Settings(),
        betfair_service=bf, telegram_service=_Telegram(),
    )
    rc.simulation_mode = True
    rc.config.direct_unmatched_ttl_enabled = enabled
    rc._last_direct_ttl_poll_at = 0.0
    return rc  # bus/betfair accessibili via rc.bus / rc.betfair_service


def _order(bet_id, *, market_id="1.1", matched=0.0, remaining=5.0, age=1000.0):
    return {"betId": bet_id, "marketId": market_id, "sizeMatched": matched,
            "sizeRemaining": remaining, "placed_epoch": time.time() - age}


def _track(rc, customer_ref, bet_id):
    rc._on_quick_bet_accepted({
        "customer_ref": customer_ref, "bet_id": bet_id,
        "best_price_source": "LIVE_BOOK_DIRECT",
    })


def _events(bus, name):
    return [p for n, p in bus.events if n == name]


# =========================================================
# Default-OFF
# =========================================================
def test_flag_off_is_total_noop():
    cc = _CancelClient()
    rc = _make_rc(orders=[_order("B1")], cancel_client=cc, enabled=False)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert cc.calls == []
    assert rc.direct_unmatched_bet_ids == {"B1"}  # registry intatto


# =========================================================
# Cancel di un ordine DIRECT scaduto
# =========================================================
def test_expired_direct_order_is_cancelled_and_cleaned():
    cc = _CancelClient(ok=True)
    rc = _make_rc(orders=[_order("B1", market_id="1.5")], cancel_client=cc)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert cc.calls == [("1.5", ("B1",))]
    assert _events(rc.bus, "DIRECT_UNMATCHED_CANCELLED")
    # NO RETRY: bet_id rimosso dal registry.
    assert rc.direct_unmatched_bet_ids == set()


# =========================================================
# SICUREZZA: mai cancellare ordini fuori da direct_bet_ids
# =========================================================
def test_never_cancels_orders_outside_registry():
    cc = _CancelClient()
    # B1 DIRECT (nel registry), CASHOUT9 non DIRECT (non nel registry): entrambi
    # scaduti e non abbinati, ma solo B1 deve essere cancellato.
    rc = _make_rc(
        orders=[_order("B1"), _order("CASHOUT9", market_id="1.9")], cancel_client=cc)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert cc.calls == [("1.1", ("B1",))]  # CASHOUT9 mai toccato


def test_empty_registry_cancels_nothing():
    cc = _CancelClient()
    rc = _make_rc(orders=[_order("B1")], cancel_client=cc)
    # nessun _track => allowlist vuota
    rc._poll_direct_unmatched_ttl()
    assert cc.calls == []


def test_not_yet_expired_not_cancelled():
    cc = _CancelClient()
    rc = _make_rc(orders=[_order("B1", age=10.0)], cancel_client=cc)  # 10s < ttl 120
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert cc.calls == []
    assert rc.direct_unmatched_bet_ids == {"B1"}  # ancora tracciato


def test_matched_order_not_cancelled():
    cc = _CancelClient()
    rc = _make_rc(orders=[_order("B1", matched=5.0, remaining=0.0)], cancel_client=cc)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert cc.calls == []


# =========================================================
# Fail-closed
# =========================================================
def test_list_current_orders_failure_aborts_without_cancel():
    cc = _CancelClient()
    rc = _make_rc(orders_raises=True, cancel_client=cc)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()  # non deve sollevare
    assert cc.calls == []
    assert rc.direct_unmatched_bet_ids == {"B1"}  # registry intatto (fail-closed)


def test_cancel_exception_notifies_and_removes_no_crash():
    cc = _CancelClient(raises=True)
    rc = _make_rc(orders=[_order("B1")], cancel_client=cc)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert _events(rc.bus, "DIRECT_UNMATCHED_CANCEL_FAILED")
    assert rc.direct_unmatched_bet_ids == set()  # NO RETRY anche su errore


def test_cancel_ok_false_notifies_failed_and_removes():
    cc = _CancelClient(ok=False)
    rc = _make_rc(orders=[_order("B1")], cancel_client=cc)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    assert _events(rc.bus, "DIRECT_UNMATCHED_CANCEL_FAILED")
    assert rc.direct_unmatched_bet_ids == set()


def test_no_cancel_client_notifies_and_removes():
    rc = _make_rc(orders=[_order("B1")], cancel_client=None)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    failed = _events(rc.bus, "DIRECT_UNMATCHED_CANCEL_FAILED")
    assert failed and failed[0]["reason"] == "no_cancel_client"
    assert rc.direct_unmatched_bet_ids == set()


# =========================================================
# NO RETRY end-to-end + interval gate
# =========================================================
def test_no_auto_retry_after_one_attempt():
    cc = _CancelClient(ok=False)  # cancel fallisce
    rc = _make_rc(orders=[_order("B1")], cancel_client=cc)
    _track(rc, "C1", "B1")
    rc._poll_direct_unmatched_ttl()
    rc._last_direct_ttl_poll_at = 0.0  # bypassa il gate per un secondo giro
    rc._poll_direct_unmatched_ttl()
    assert len(cc.calls) == 1  # nessun ri-tentativo automatico


def test_interval_gate_blocks_rapid_second_poll():
    cc = _CancelClient()
    rc = _make_rc(orders=[_order("B1"), _order("B2", market_id="1.2")],
                           cancel_client=cc)
    _track(rc, "C1", "B1")
    _track(rc, "C2", "B2")
    rc._poll_direct_unmatched_ttl()       # primo giro: cancella B1, B2
    rc._on_quick_bet_accepted({"customer_ref": "C3", "bet_id": "B3",
                               "best_price_source": "LIVE_BOOK_DIRECT"})
    rc._poll_direct_unmatched_ttl()       # subito dopo: bloccato dal gate
    assert "B3" in rc.direct_unmatched_bet_ids  # B3 non ancora processato
