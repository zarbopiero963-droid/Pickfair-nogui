"""Harness Claude D (copia versionata da tests/roserpina_oracle/harness.py) per gli oracoli drawdown della PR-T3.
RuntimeController REALE (SQLite su tmp_path, SimulationBroker) pilotato con le stesse chiamate del runtime per una
lettura del conto: `_record_account_snapshot` (picco di cassa, auto-reset) + `_apply_funds_reading` (RiskDesk)."""
from __future__ import annotations

import time
from types import SimpleNamespace

from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from core.trading_engine import TradingEngine
from database import Database
from simulation_broker import SimulationBroker
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _Executor, _Telegram
from tests.risk_config_fixtures import legacy_test_config


class Bus:
    def __init__(self):
        self._subscribers, self.events = {}, []

    def subscribe(self, topic, handler):
        self._subscribers.setdefault(topic, []).append(handler)

    def publish(self, topic, payload=None):
        self.events.append((topic, dict(payload) if isinstance(payload, dict) else payload))
        for h in list(self._subscribers.get(topic, [])):
            h(payload)


class Settings:
    def __init__(self, cfg):
        self.cfg = cfg

    def load_roserpina_config(self):
        return self.cfg

    def load_market_data_config(self):
        return {"market_data_mode": "poll", "enabled": False, "market_ids": [], "snapshot_fallback_enabled": True,
                "snapshot_fallback_interval_sec": 1}


class Service:
    def __init__(self, broker):
        self.simulation_broker, self.funds = broker, {"available": 100.0, "exposure": 0.0, "ok": True}

    def get_client(self):
        return self.simulation_broker

    def set_simulation_mode(self, _e):
        return None

    def is_simulation_mode(self):
        return True

    def get_simulation_broker(self):
        return self.simulation_broker

    def get_live_client(self):
        return None

    def get_account_funds(self):
        return dict(self.funds)

    def list_current_orders(self, market_ids=None):
        return list(self.simulation_broker.get_current_orders(market_ids) or [])

    def get_market_book_snapshot(self, market_id, include_prices=False):
        return self.simulation_broker.get_market_book(market_id)

    def status(self):
        return {"connected": True}

    def ensure_stream_session_ready(self):
        return True


def build(tmp_path, **cfg_over):
    """Runtime reale con la configurazione Roserpina data (default: quella della campagna Grok 5 tavoli)."""
    cfg = legacy_test_config(**{
        "table_count": 5, "target_profit_cycle_pct": 1.0, "max_single_bet_pct": 5.0, "max_total_exposure_pct": 60.0,
        "max_event_exposure_pct": 5.0, "max_drawdown_abs": 10.0, "min_stake": 0.10, "commission_pct": 4.5,
        "allow_recovery": False, "max_recovery_tables": 0, "lockdown_drawdown_pct": 60.0,
        "auto_reset_drawdown_pct": 15.0, **cfg_over})
    bus = Bus()
    broker = SimulationBroker(starting_balance=100.0)
    svc = Service(broker)
    db = Database(str(tmp_path / "ro.db"))
    engine = TradingEngine(bus=bus, db=db, client_getter=svc.get_client, executor=_Executor())
    rc = RuntimeController(bus=bus, db=db, settings_service=Settings(cfg), betfair_service=svc,
                           telegram_service=_Telegram(), trading_engine=engine)
    engine.runtime_controller = rc
    rc.config = cfg
    from core.money_management import RoserpinaMoneyManagement
    rc.mm = RoserpinaMoneyManagement(cfg)
    rc.mode = RuntimeMode.ACTIVE
    return SimpleNamespace(bus=bus, broker=broker, svc=svc, db=db, rc=rc, cfg=cfg)


def read(c, available, committed=0.0):
    """Una lettura del conto applicata come fa il runtime (snapshot + RiskDesk), con `read_at` monotonic fresco."""
    funds = {"available": float(available), "exposure": -float(committed), "ok": True}
    c.svc.funds = funds
    t = time.monotonic()
    c.rc._record_account_snapshot(funds, source="ORACLE", read_at=t)
    return c.rc._apply_funds_reading(funds, source="ORACLE", read_at=t)
