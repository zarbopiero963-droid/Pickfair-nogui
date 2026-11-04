"""Runtime Pickfair REALE per i test della Control API (nessun mock del RuntimeController).

Componenti reali: Database (SQLite su file temporaneo), EventBus, SettingsService,
BetfairService (mai connesso), TelegramService (mai avviato), SafeModeManager.
La rete è vietata salvo loopback: un tentativo verso l'esterno fa fallire il test.
"""

from __future__ import annotations

import socket
from typing import Any

import pytest

from core.event_bus import EventBus
from core.runtime_controller import RuntimeController
from database import Database
from safe_mode_manager import get_safe_mode_manager
from services.betfair_service import BetfairService
from services.settings_service import SettingsService
from services.telegram_service import TelegramService


class _RecordingBus(EventBus):
    """EventBus reale che registra ogni publish (per provare l'assenza di effetti)."""

    def __init__(self) -> None:
        super().__init__()
        self.published: list[str] = []

    def publish(self, event_name: str, payload: Any = None) -> Any:  # type: ignore[override]
        self.published.append(event_name)
        return super().publish(event_name, payload)


@pytest.fixture(autouse=True)
def loopback_only(monkeypatch: pytest.MonkeyPatch) -> None:
    real_connect = socket.socket.connect

    def guarded(self: socket.socket, address: Any) -> Any:
        host = address[0] if isinstance(address, tuple) else address
        if host not in ("127.0.0.1", "::1"):
            raise AssertionError(f"rete esterna vietata nei test: {host!r}")
        return real_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded)


@pytest.fixture
def runtime(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> RuntimeController:
    monkeypatch.chdir(tmp_path)
    db = Database(str(tmp_path / "pickfair.db"))
    bus = _RecordingBus()
    settings = SettingsService(db)
    rc = RuntimeController(
        bus=bus,
        db=db,
        settings_service=settings,
        betfair_service=BetfairService(settings),
        telegram_service=TelegramService(settings, db, bus),
        safe_mode=get_safe_mode_manager(),
    )
    return rc


@pytest.fixture
def sim_runtime(runtime: RuntimeController) -> Any:
    """Runtime reale AVVIATO in SIM: SimulationBroker, saldo fittizio, riconciliazione d'avvio."""
    result = runtime.start(simulation_mode=True)
    assert isinstance(result, dict) and result.get("started") is True, result
    try:
        yield runtime
    finally:
        runtime.stop()
