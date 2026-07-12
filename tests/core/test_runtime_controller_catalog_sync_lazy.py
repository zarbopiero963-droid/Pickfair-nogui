from __future__ import annotations

"""Hard test H-01: costruzione LAZY del CatalogSyncService in RuntimeController.

Regressione: `RuntimeController.__init__` chiamava eagerly
`betfair_service.get_client()` per costruire il CatalogSyncService. I fake
`_Betfair` della suite non espongono `get_client`, quindi la sola costruzione
del controller sollevava
`AttributeError: '_Betfair' object has no attribute 'get_client'`
facendo fallire ~285 test. Il fix rende la costruzione del CatalogSyncService
lazy (property `catalog_sync`), risolvendo il client solo alla prima esecuzione
del sync.
"""

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig
from services.catalog_sync_service import CatalogSyncService


class _Bus:
    def subscribe(self, *_args) -> None:
        return None

    def publish(self, _topic, _payload=None) -> None:
        return None


class _DB:
    def _execute(self, *_args, **_kwargs):
        return None


class _Settings:
    def load_roserpina_config(self):
        cfg = RoserpinaConfig()
        cfg.anti_duplication_enabled = False
        return cfg

    def load_market_data_config(self):
        return {"market_data_mode": "poll", "enabled": False, "market_ids": []}


class _Telegram:
    def start(self):
        return {"started": True}

    def stop(self):
        return None

    def status(self):
        return {"connected": True}


class _BetfairNoGetClient:
    """Fake come quelli della suite: NON espone get_client → riproduce H-01."""

    def set_simulation_mode(self, _enabled) -> None:
        return None

    def status(self):
        return {"connected": True}

    def get_live_client(self):
        return object()

    def get_market_book_snapshot(self, _market_id):
        return None

    def ensure_stream_session_ready(self):
        return True


class _BetfairSpyGetClient(_BetfairNoGetClient):
    """Come sopra ma con get_client spia: conta le chiamate, ritorna un sentinel."""

    def __init__(self, sentinel):
        self._sentinel = sentinel
        self.get_client_calls = 0

    def get_client(self):
        self.get_client_calls += 1
        return self._sentinel


def _make_rc(betfair_service):
    return RuntimeController(
        bus=_Bus(),
        db=_DB(),
        settings_service=_Settings(),
        betfair_service=betfair_service,
        telegram_service=_Telegram(),
    )


def test_construct_without_get_client_does_not_raise():
    # BLOCK/repro: sul codice PRE-fix questa costruzione solleva
    # AttributeError: '...' object has no attribute 'get_client'.
    rc = _make_rc(_BetfairNoGetClient())
    assert rc is not None


def test_catalog_sync_not_resolved_at_construction():
    bf = _BetfairSpyGetClient(sentinel=object())
    rc = _make_rc(bf)
    # get_client NON deve essere chiamato in __init__ (costruzione lazy).
    assert bf.get_client_calls == 0
    # Alla prima lettura della property viene risolto una sola volta.
    _ = rc.catalog_sync
    assert bf.get_client_calls == 1


def test_catalog_sync_builds_and_caches_client():
    sentinel = object()
    bf = _BetfairSpyGetClient(sentinel=sentinel)
    rc = _make_rc(bf)
    cs = rc.catalog_sync
    assert isinstance(cs, CatalogSyncService)
    # Il client è quello risolto via get_client() al primo accesso.
    assert cs.client is sentinel
    # Cache: stessa istanza su accessi ripetuti, get_client chiamato una volta.
    assert rc.catalog_sync is cs
    assert bf.get_client_calls == 1
