"""Gate A — batch B, market stream: limiti della sottoscrizione Betfair stream (services/streaming_feed.py).

Test offline: nessun client reale, nessuna rete; si esercita `_validate_subscription_bounds` e `start()` reali.
"""
import pytest

from services.streaming_feed import StreamingConfigError, StreamingFeed


def _feed(**cfg):
    base = {"enabled": True, "market_data_mode": "stream"}
    base.update(cfg)
    return StreamingFeed(client_getter=lambda: None, config=base, on_market_book=lambda _b: None)


def test_trg_628_b_0823():
    """ID: TRG-628-B-0823 / PF-B1-STREAM-BOUNDED

    start() e la costruzione della sottoscrizione rifiutano: scope illimitato, troppi mercati, full ladder senza
    opt-in; nessun thread di stream parte per una config rifiutata."""
    bad = [
        ({"market_ids": []}, "STREAM_SUBSCRIPTION_UNBOUNDED"),
        ({"market_ids": [f"1.{i}" for i in range(26)], "max_markets": 25}, "STREAM_TOO_MANY_MARKETS"),
        ({"market_ids": ["1.1"], "fields": ["EX_ALL_OFFERS"]}, "EX_ALL_OFFERS_REQUIRES_OPT_IN"),
    ]
    for cfg, code in bad:
        feed = _feed(**cfg)
        try:
            with pytest.raises(StreamingConfigError, match=code):
                feed.start()
            assert feed._run_thread is None, f"thread di stream avviato con config rifiutata ({code})"
            with pytest.raises(StreamingConfigError, match=code):
                feed._build_subscribe_kwargs()
        finally:
            feed.stop()


def test_trg_628_b_0911():
    """ID: TRG-628-B-0911 / PF-B1-STREAM-BOUNDED-MODE

    Una modalita' market data sconosciuta e' rifiutata, anche con lo stream disabilitato."""
    for mode, enabled in (("websocket", True), ("STREAMING", True), ("websocket", False)):
        feed = _feed(market_data_mode=mode, enabled=enabled, market_ids=["1.1"])
        with pytest.raises(StreamingConfigError, match="INVALID_MARKET_DATA_MODE"):
            feed._validate_subscription_bounds()
    for mode in ("poll", "stream", "hybrid", " Stream "):
        _feed(market_data_mode=mode, market_ids=["1.1"])._validate_subscription_bounds()


def test_trg_628_b_0912():
    """ID: TRG-628-B-0912 / PF-B1-STREAM-BOUNDED-BYPASS

    Se lo stream non e' usato (modalita' poll o enabled=False) i limiti di sottoscrizione non si applicano."""
    unbounded_full = {"market_ids": [], "fields": ["EX_ALL_OFFERS"]}
    _feed(market_data_mode="poll", **unbounded_full)._validate_subscription_bounds()
    _feed(market_data_mode="stream", enabled=False, **unbounded_full)._validate_subscription_bounds()
    _feed(market_data_mode="hybrid", enabled=False, **unbounded_full)._validate_subscription_bounds()
    # controllo: la stessa config con lo stream attivo e' rifiutata
    with pytest.raises(StreamingConfigError):
        _feed(market_data_mode="stream", **unbounded_full)._validate_subscription_bounds()


def test_trg_628_b_0914():
    """ID: TRG-628-B-0914 / PF-B1-STREAM-BOUNDED-COUNT

    Un numero di market_ids oltre max_markets e' rifiutato; al limite e' accettato."""
    with pytest.raises(StreamingConfigError, match="STREAM_TOO_MANY_MARKETS"):
        _feed(market_ids=[f"1.{i}" for i in range(4)], max_markets=3)._validate_subscription_bounds()
    with pytest.raises(StreamingConfigError, match="STREAM_TOO_MANY_MARKETS"):
        _feed(market_ids=[f"1.{i}" for i in range(26)])._validate_subscription_bounds()   # default 25
    _feed(market_ids=[f"1.{i}" for i in range(3)], max_markets=3)._validate_subscription_bounds()
    _feed(market_ids=[f"1.{i}" for i in range(25)])._validate_subscription_bounds()


def test_trg_628_b_0915():
    """ID: TRG-628-B-0915 / PF-B1-STREAM-BOUNDED-LADDER

    EX_ALL_OFFERS (full ladder) senza opt-in use_full_ladder e' rifiutato; con l'opt-in e' accettato."""
    with pytest.raises(StreamingConfigError, match="EX_ALL_OFFERS_REQUIRES_OPT_IN"):
        _feed(market_ids=["1.1"], fields=["EX_BEST_OFFERS", "EX_ALL_OFFERS"])._validate_subscription_bounds()
    feed = _feed(market_ids=["1.1"], fields=["EX_BEST_OFFERS", "EX_ALL_OFFERS"], use_full_ladder=True)
    feed._validate_subscription_bounds()
    assert "EX_ALL_OFFERS" in feed._build_subscribe_kwargs()["market_data_filter"]["fields"]
