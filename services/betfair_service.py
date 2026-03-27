from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from betfair_client import BetfairClient
from simulation_broker import SimulationBroker

logger = logging.getLogger(__name__)


class BetfairService:
    """
    Service unificato broker:
    - LIVE  -> BetfairClient
    - SIM   -> SimulationBroker

    Il broker attivo dipende da simulation_mode.
    """

    def __init__(self, settings_service):
        self.settings_service = settings_service

        self.client: Optional[BetfairClient] = None
        self.simulation_broker: Optional[SimulationBroker] = None

        self.connected = False
        self.last_error = ""
        self.simulation_mode = False

    # =========================================================
    # MODE
    # =========================================================
    def set_simulation_mode(self, enabled: bool) -> None:
        self.simulation_mode = bool(enabled)

    def is_simulation_mode(self) -> bool:
        return bool(self.simulation_mode)

    # =========================================================
    # BROKER GETTERS
    # =========================================================
    def get_client(self):
        if self.simulation_mode:
            return self.simulation_broker
        return self.client

    def get_live_client(self) -> Optional[BetfairClient]:
        return self.client

    def get_simulation_broker(self) -> Optional[SimulationBroker]:
        return self.simulation_broker

    # =========================================================
    # CONNECT / DISCONNECT
    # =========================================================
    def connect(
        self,
        password: str | None = None,
        force: bool = False,
        simulation_mode: bool | None = None,
    ) -> dict:
        if simulation_mode is not None:
            self.set_simulation_mode(simulation_mode)

        if self.simulation_mode:
            return self._connect_simulation(force=force)

        return self._connect_live(password=password, force=force)

    def _connect_live(self, password: str | None = None, force: bool = False) -> dict:
        if self.connected and self.client and not force and not self.simulation_mode:
            return {
                "connected": True,
                "reason": "already_connected",
                "simulated": False,
            }

        if force:
            try:
                self.disconnect()
            except Exception:
                pass

        cfg = self.settings_service.load_betfair_config()
        if (
            not cfg.username
            or not cfg.app_key
            or not cfg.certificate
            or not cfg.private_key
        ):
            self.last_error = "Configurazione Betfair incompleta"
            raise RuntimeError(self.last_error)

        password = (
            password
            if password is not None
            else self.settings_service.load_password()
        )
        if not password:
            self.last_error = "Password Betfair mancante"
            raise RuntimeError(self.last_error)

        try:
            client = BetfairClient(
                username=cfg.username,
                app_key=cfg.app_key,
                cert_pem=cfg.certificate,
                key_pem=cfg.private_key,
            )

            session_info = client.login(password=password)

            self.client = client
            self.simulation_broker = None
            self.connected = True
            self.last_error = ""
            self.simulation_mode = False

            db = getattr(self.settings_service, "db", None)
            if db and hasattr(db, "save_session"):
                db.save_session(
                    session_info.get("session_token", ""),
                    session_info.get("expiry", ""),
                )

            return {
                "connected": True,
                "session": session_info,
                "simulated": False,
            }

        except Exception as exc:
            self.client = None
            self.connected = False
            self.last_error = str(exc)
            logger.exception("Errore connect LIVE Betfair: %s", exc)
            raise

    def _connect_simulation(self, force: bool = False) -> dict:
        if self.connected and self.simulation_broker and not force and self.simulation_mode:
            return {
                "connected": True,
                "reason": "already_connected",
                "simulated": True,
            }

        if force:
            try:
                self.disconnect()
            except Exception:
                pass

        try:
            sim_cfg = self._load_simulation_config()

            broker = SimulationBroker(
                starting_balance=float(sim_cfg.get("starting_balance", 1000.0) or 1000.0),
                commission_pct=float(sim_cfg.get("commission_pct", 4.5) or 4.5),
                partial_fill_enabled=bool(sim_cfg.get("partial_fill_enabled", True)),
                consume_liquidity=bool(sim_cfg.get("consume_liquidity", True)),
                db=getattr(self.settings_service, "db", None),
            )

            if bool(sim_cfg.get("persist_state", True)):
                persisted_state = self._load_persisted_simulation_state()
                if persisted_state:
                    try:
                        broker.state.load_from_dict(persisted_state)
                    except Exception:
                        logger.exception("Errore load persisted simulation state")

            session_info = broker.login(password="SIMULATION")

            self.simulation_broker = broker
            self.client = None
            self.connected = True
            self.last_error = ""
            self.simulation_mode = True

            return {
                "connected": True,
                "session": session_info,
                "simulated": True,
                "starting_balance": float(sim_cfg.get("starting_balance", 1000.0) or 1000.0),
                "commission_pct": float(sim_cfg.get("commission_pct", 4.5) or 4.5),
                "partial_fill_enabled": bool(sim_cfg.get("partial_fill_enabled", True)),
                "consume_liquidity": bool(sim_cfg.get("consume_liquidity", True)),
                "persist_state": bool(sim_cfg.get("persist_state", True)),
            }

        except Exception as exc:
            self.simulation_broker = None
            self.connected = False
            self.last_error = str(exc)
            logger.exception("Errore connect SIMULATION broker: %s", exc)
            raise

    def disconnect(self) -> None:
        self._persist_simulation_state_if_needed()

        if self.simulation_broker:
            try:
                self.simulation_broker.logout()
            except Exception as exc:
                logger.warning("Errore logout SimulationBroker: %s", exc)

        if self.client:
            try:
                self.client.logout()
            except Exception as exc:
                logger.warning("Errore logout Betfair: %s", exc)

        self.simulation_broker = None
        self.client = None
        self.connected = False

        db = getattr(self.settings_service, "db", None)
        if db and hasattr(db, "clear_session"):
            try:
                db.clear_session()
            except Exception:
                pass

    def ensure_connected(
        self,
        password: str | None = None,
        simulation_mode: bool | None = None,
    ):
        broker = self.get_client()
        if self.connected and broker is not None:
            if simulation_mode is None or bool(simulation_mode) == self.simulation_mode:
                return broker

        self.connect(password=password, simulation_mode=simulation_mode)
        return self.get_client()

    # =========================================================
    # ACCOUNT FUNDS / STATUS
    # =========================================================
    def get_account_funds(self) -> dict:
        broker = self.get_client()
        if not broker:
            return {
                "available": 0.0,
                "exposure": 0.0,
                "total": 0.0,
                "simulated": bool(self.simulation_mode),
            }

        try:
            funds = broker.get_account_funds() or {}
            return {
                "available": float(funds.get("available", 0.0) or 0.0),
                "exposure": float(funds.get("exposure", 0.0) or 0.0),
                "total": float(funds.get("total", 0.0) or 0.0),
                "simulated": bool(funds.get("simulated", self.simulation_mode)),
            }
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Errore get_account_funds: %s", exc)
            return {
                "available": 0.0,
                "exposure": 0.0,
                "total": 0.0,
                "simulated": bool(self.simulation_mode),
            }

    def status(self) -> dict:
        broker = self.get_client()
        has_client = broker is not None
        simulated = bool(self.simulation_mode)

        return {
            "connected": bool(self.connected and has_client),
            "last_error": self.last_error,
            "has_client": has_client,
            "simulated": simulated,
            "simulation_mode": simulated,  # compatibilità con market_tracker e vecchio codice
            "broker_type": "SIMULATION" if simulated else "LIVE",
            "live_execution_only": not simulated,
        }

    # =========================================================
    # SIMULATION MARKET FEED
    # =========================================================
    def update_simulation_market_book(self, *args, **kwargs) -> dict:
        """
        Firma compatibile con entrambi gli stili:
        - update_simulation_market_book(market_book)
        - update_simulation_market_book(market_id, market_book)

        Questo chiude il mismatch trovato nel repository.
        """
        if not self.simulation_mode or not self.simulation_broker:
            return {
                "ok": False,
                "reason": "simulation_not_active",
                "simulated": False,
            }

        market_id = ""
        market_book: Dict[str, Any] = {}

        # stile nuovo: (market_book,)
        if len(args) == 1 and isinstance(args[0], dict):
            market_book = dict(args[0] or {})
            market_id = str(
                market_book.get("marketId")
                or market_book.get("market_id")
                or ""
            ).strip()

        # stile vecchio: (market_id, market_book)
        elif len(args) >= 2:
            market_id = str(args[0] or "").strip()
            market_book = dict(args[1] or {})

        # kwargs fallback
        if not market_book:
            market_book = dict(kwargs.get("market_book") or {})
        if not market_id:
            market_id = str(
                kwargs.get("market_id")
                or market_book.get("marketId")
                or market_book.get("market_id")
                or ""
            ).strip()

        if not market_id or not isinstance(market_book, dict):
            return {
                "ok": False,
                "reason": "invalid_market_book",
                "simulated": True,
            }

        normalized = dict(market_book)
        normalized["marketId"] = market_id
        normalized["market_id"] = market_id

        try:
            result = self.simulation_broker.update_market_book(market_id, normalized)
            self._persist_simulation_state_if_needed()
            return result
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Errore update_simulation_market_book: %s", exc)
            return {
                "ok": False,
                "reason": str(exc),
                "simulated": True,
            }

    def simulation_snapshot(self) -> dict:
        if not self.simulation_broker:
            return {
                "connected": False,
                "simulated": True,
                "state": {},
            }

        try:
            return self.simulation_broker.snapshot()
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Errore simulation_snapshot: %s", exc)
            return {
                "connected": False,
                "simulated": True,
                "state": {},
                "error": str(exc),
            }

    def reset_simulation(self, starting_balance: float | None = None) -> dict:
        if not self.simulation_broker:
            return {
                "ok": False,
                "reason": "simulation_not_initialized",
                "simulated": True,
            }

        try:
            result = self.simulation_broker.reset(starting_balance=starting_balance)
            self._persist_simulation_state_if_needed(force=True)
            return result
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Errore reset_simulation: %s", exc)
            return {
                "ok": False,
                "reason": str(exc),
                "simulated": True,
            }

    # =========================================================
    # INTERNAL SETTINGS HELPERS
    # =========================================================
    def _load_simulation_config(self) -> dict:
        if hasattr(self.settings_service, "load_simulation_config"):
            try:
                return self.settings_service.load_simulation_config() or {}
            except Exception:
                logger.exception("Errore load_simulation_config")
        return {
            "enabled": True,
            "starting_balance": 1000.0,
            "commission_pct": 4.5,
            "partial_fill_enabled": True,
            "consume_liquidity": True,
            "persist_state": True,
        }

    def _persist_simulation_state_if_needed(self, force: bool = False) -> None:
        if not self.simulation_broker:
            return

        sim_cfg = self._load_simulation_config()
        if not force and not bool(sim_cfg.get("persist_state", True)):
            return

        if hasattr(self.settings_service, "save_simulation_state"):
            try:
                self.settings_service.save_simulation_state(
                    self.simulation_broker.state.to_dict(),
                    state_key="default",
                )
            except Exception:
                logger.exception("Errore save_simulation_state")

    def _load_persisted_simulation_state(self) -> dict:
        if hasattr(self.settings_service, "load_simulation_state"):
            try:
                return self.settings_service.load_simulation_state(state_key="default") or {}
            except Exception:
                logger.exception("Errore load_simulation_state")
        return {}