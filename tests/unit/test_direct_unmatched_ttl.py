"""Unit test per direct_unmatched_ttl.select_expired_unmatched (B6.3.1)."""
import pytest

from direct_unmatched_ttl import select_expired_unmatched

pytestmark = pytest.mark.unit


def _order(*, bet_id="1", market_id="1.1", matched=0.0, remaining=5.0,
           placed_epoch=1000.0, **extra):
    row = {
        "betId": bet_id,
        "marketId": market_id,
        "sizeMatched": matched,
        "sizeRemaining": remaining,
        "placed_epoch": placed_epoch,
    }
    row.update(extra)
    return row


_ALL = object()


def _bet_ids(orders):
    if not isinstance(orders, list):
        return {"1"}  # non-list: allowlist non vuota per non corto-circuitare
    return {str(o.get("betId") or o.get("bet_id"))
            for o in orders if isinstance(o, dict)} or {"1"}


def _select(orders, *, now=2000.0, ttl=300.0, scope=None, direct=_ALL):
    # Default: tutti i bet_id presenti sono trattati come DIRECT (i test
    # esistenti verificano la restante logica); i test dedicati passano `direct`.
    direct_ids = _bet_ids(orders) if direct is _ALL else direct
    return select_expired_unmatched(
        current_orders=orders, now_epoch=now, ttl_seconds=ttl,
        direct_bet_ids=direct_ids, scope_market_ids=scope,
    )


# =========================================================
# Happy path
# =========================================================
def test_unmatched_older_than_ttl_is_selected():
    # età = 2000 - 1000 = 1000 > ttl 300 => cancella.
    r = _select([_order(bet_id="B1", market_id="1.5")])
    assert r == [{"market_id": "1.5", "bet_id": "B1"}]


def test_unmatched_within_ttl_not_selected():
    # età = 2000 - 1900 = 100 <= ttl 300 => resta.
    assert _select([_order(placed_epoch=1900.0)]) == []


def test_age_exactly_ttl_not_selected():
    # (now - placed) == ttl non è "oltre": niente cancellazione.
    assert _select([_order(placed_epoch=1700.0)], now=2000.0, ttl=300.0) == []


# =========================================================
# Definizione conservativa di "non abbinato"
# =========================================================
def test_partially_matched_not_selected():
    assert _select([_order(matched=2.0, remaining=3.0)]) == []


def test_fully_matched_not_selected():
    assert _select([_order(matched=5.0, remaining=0.0)]) == []


def test_zero_remaining_not_selected():
    assert _select([_order(matched=0.0, remaining=0.0)]) == []


# =========================================================
# Sorgenti del timestamp
# =========================================================
def test_placed_date_iso_with_z():
    # placedDate ISO con suffisso Z (UTC). 2026-06-27T00:00:00Z = epoch noto.
    import datetime as _dt
    placed = _dt.datetime(2026, 6, 27, tzinfo=_dt.timezone.utc).timestamp()
    row = {"betId": "B", "marketId": "1.1", "sizeMatched": 0.0,
           "sizeRemaining": 1.0, "placedDate": "2026-06-27T00:00:00Z"}
    assert _select([row], now=placed + 1000, ttl=300.0) == [
        {"market_id": "1.1", "bet_id": "B"}]


def test_missing_placed_date_is_fail_closed():
    row = {"betId": "B", "marketId": "1.1", "sizeMatched": 0.0,
           "sizeRemaining": 1.0}  # niente placed_epoch/placedDate
    assert _select([row]) == []


def test_unparseable_placed_date_is_fail_closed():
    row = {"betId": "B", "marketId": "1.1", "sizeMatched": 0.0,
           "sizeRemaining": 1.0, "placedDate": "not-a-date"}
    assert _select([row]) == []


# =========================================================
# TTL invalido => nessuna cancellazione (fail-closed)
# =========================================================
def test_invalid_ttl_selects_nothing():
    order = _order()
    for bad in (0.0, -1.0, float("nan"), float("inf"), None, "x"):
        assert select_expired_unmatched(
            current_orders=[order], now_epoch=2000.0, ttl_seconds=bad,
            direct_bet_ids={"1"}) == []


def test_invalid_now_selects_nothing():
    order = _order()
    for bad in (float("nan"), None, "x"):
        assert select_expired_unmatched(
            current_orders=[order], now_epoch=bad, ttl_seconds=300.0,
            direct_bet_ids={"1"}) == []


# =========================================================
# Scope per market_id
# =========================================================
def test_scope_filters_out_other_markets():
    orders = [_order(bet_id="A", market_id="1.1"),
              _order(bet_id="B", market_id="1.2")]
    r = _select(orders, scope=["1.2"])
    assert r == [{"market_id": "1.2", "bet_id": "B"}]


# =========================================================
# Robustezza / fail-closed schema
# =========================================================
def test_malformed_rows_skipped_not_raise():
    orders = [5, None, {"betId": "", "marketId": "1.1"},  # bet_id vuoto
              {"betId": "X", "marketId": ""},             # market vuoto
              _order(bet_id="OK", market_id="1.9")]
    assert _select(orders) == [{"market_id": "1.9", "bet_id": "OK"}]


def test_non_list_orders_returns_empty():
    assert _select("nope") == []
    assert _select(None) == []


def test_dedup_by_bet_id():
    orders = [_order(bet_id="DUP", market_id="1.1"),
              _order(bet_id="DUP", market_id="1.1")]
    assert _select(orders) == [{"market_id": "1.1", "bet_id": "DUP"}]


def test_snake_case_field_aliases():
    row = {"bet_id": "B", "market_id": "1.3", "sizeMatched": 0.0,
           "sizeRemaining": 2.0, "placed_epoch": 1000.0}
    assert _select([row]) == [{"market_id": "1.3", "bet_id": "B"}]


# =========================================================
# Scope DIRECT obbligatorio (Greptile P1)
# =========================================================
def test_only_direct_bet_ids_are_candidates():
    # Lista mista: solo i bet_id in direct_bet_ids vengono cancellati.
    orders = [_order(bet_id="DIRECT1", market_id="1.1"),
              _order(bet_id="CASHOUT9", market_id="1.1")]
    r = _select(orders, direct={"DIRECT1"})
    assert r == [{"market_id": "1.1", "bet_id": "DIRECT1"}]


def test_empty_or_missing_direct_allowlist_selects_nothing():
    orders = [_order(bet_id="X")]
    assert _select(orders, direct=set()) == []
    assert _select(orders, direct=None) == []


# =========================================================
# Fail-closed numerico (Codex P2)
# =========================================================
def test_bool_ttl_rejected():
    # ttl_seconds=True non deve diventare 1.0.
    assert _select([_order()], ttl=True) == []


def test_non_finite_remaining_not_selected():
    for bad in (float("nan"), float("inf")):
        assert _select([_order(remaining=bad)]) == []


def test_non_finite_matched_not_selected():
    assert _select([_order(matched=float("nan"), remaining=5.0)]) == []


def test_oversized_int_does_not_raise():
    # int gigante (OverflowError in float()) => skip, no raise.
    huge = 10 ** 400
    assert _select([_order(remaining=huge)]) == []
    assert _select([_order()], ttl=huge) == []  # ttl overflow => vuoto, no raise


# =========================================================
# placed_epoch malformato => fallback a placedDate (Greptile P2)
# =========================================================
def test_invalid_placed_epoch_falls_back_to_placed_date():
    import datetime as _dt
    placed = _dt.datetime(2026, 6, 27, tzinfo=_dt.timezone.utc)
    row = {"betId": "B", "marketId": "1.1", "sizeMatched": 0.0,
           "sizeRemaining": 1.0,
           "placed_epoch": float("nan"),            # malformato
           "placedDate": "2026-06-27T00:00:00Z"}    # valido => usato
    assert _select([row], now=placed.timestamp() + 1000, ttl=300.0) == [
        {"market_id": "1.1", "bet_id": "B"}]
