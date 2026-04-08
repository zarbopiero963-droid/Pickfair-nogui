from __future__ import annotations

from typing import Any, Dict

from core.system_state import (
    BetfairConfig,
    RiskProfile,
    RoserpinaConfig,
    TelegramRuntimeConfig,
)


class SettingsService:
    """
    Service centrale impostazioni.

    Responsabilità:
    - caricare/salvare config Betfair
    - caricare/salvare config Telegram
    - caricare/salvare config Roserpina
    - gestire parametri simulation
    """

    def __init__(self, db):
        self.db = db

    # =========================================================
    # GENERIC
    # =========================================================
    def get_all_settings(self) -> Dict[str, Any]:
        getter = getattr(self.db, "get_settings", None)
        if callable(getter):
            return getter() or {}
        return {}

    def save_settings(self, data: Dict[str, Any]) -> None:
        if hasattr(self.db, "save_settings"):
            self.db.save_settings(data or {})

    def _f(self, data: Dict[str, Any], key: str, default: float) -> float:
        try:
            return float(data.get(key, default))
        except Exception:
            return float(default)

    def _i(self, data: Dict[str, Any], key: str, default: int) -> int:
        try:
            return int(data.get(key, default))
        except Exception:
            return int(default)

    def _b(self, data: Dict[str, Any], key: str, default: bool) -> bool:
        value = data.get(key, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    # =========================================================
    # BETFAIR CONFIG
    # =========================================================
    def load_betfair_config(self) -> BetfairConfig:
        data = self.get_all_settings()
        return BetfairConfig(
            username=str(data.get("username", "") or ""),
            app_key=str(data.get("app_key", "") or ""),
            certificate=str(data.get("certificate", "") or ""),
            private_key=str(data.get("private_key", "") or ""),
        )

    def save_betfair_config(self, config: BetfairConfig, password: str | None = None) -> None:
        self.db.save_credentials(
            username=config.username,
            app_key=config.app_key,
            certificate=config.certificate,
            private_key=config.private_key,
        )
        if password is not None and hasattr(self.db, "save_password"):
            self.db.save_password(password)

    def load_password(self) -> str:
        data = self.get_all_settings()
        return str(data.get("password", "") or "")

    def save_password(self, password: str) -> None:
        if hasattr(self.db, "save_password"):
            self.db.save_password(password)

    # =========================================================
    # TELEGRAM CONFIG
    # =========================================================
    def load_telegram_config(self) -> TelegramRuntimeConfig:
        row = self.db.get_telegram_settings() if hasattr(self.db, "get_telegram_settings") else {}
        chats = self.db.get_telegram_chats() if hasattr(self.db, "get_telegram_chats") else []

        chat_ids = []
        for item in chats or []:
            try:
                if bool(item.get("is_active", True)):
                    chat_ids.append(int(item.get("chat_id")))
            except Exception:
                continue

        return TelegramRuntimeConfig(
            api_id=int(row.get("api_id") or 0),
            api_hash=str(row.get("api_hash", "") or ""),
            session_string=str(row.get("session_string", "") or ""),
            phone_number=str(row.get("phone_number", "") or ""),
            enabled=bool(row.get("enabled", False)),
            auto_bet=bool(row.get("auto_bet", False)),
            require_confirmation=bool(row.get("require_confirmation", True)),
            auto_stake=float(row.get("auto_stake", 1.0) or 1.0),
            monitored_chat_ids=chat_ids,
        )

    def save_telegram_config(self, config: TelegramRuntimeConfig) -> None:
        existing = self.db.get_telegram_settings() if hasattr(self.db, "get_telegram_settings") else {}
        existing = dict(existing or {})

        existing.update(
            {
                "api_id": str(config.api_id or ""),
                "api_hash": config.api_hash,
                "session_string": config.session_string,
                "phone_number": config.phone_number,
                "enabled": int(bool(config.enabled)),
                "auto_bet": int(bool(config.auto_bet)),
                "require_confirmation": int(bool(config.require_confirmation)),
                "auto_stake": float(config.auto_stake or 1.0),
            }
        )

        if hasattr(self.db, "save_telegram_settings"):
            self.db.save_telegram_settings(existing)

    # =========================================================
    # TELEGRAM ALERT SETTINGS
    # =========================================================
    def load_telegram_config_row(self) -> Dict[str, Any]:
        row = self.db.get_telegram_settings() if hasattr(self.db, "get_telegram_settings") else {}
        row = row or {}

        return {
            "api_id": int(row.get("api_id") or 0),
            "api_hash": str(row.get("api_hash", "") or ""),
            "session_string": str(row.get("session_string", "") or ""),
            "phone_number": str(row.get("phone_number", "") or ""),
            "enabled": bool(row.get("enabled", False)),
            "auto_bet": bool(row.get("auto_bet", False)),
            "require_confirmation": bool(row.get("require_confirmation", True)),
            "auto_stake": float(row.get("auto_stake", 1.0) or 1.0),
            "alerts_enabled": bool(row.get("alerts_enabled", False)),
            "alerts_chat_id": row.get("alerts_chat_id"),
            "alerts_chat_name": str(row.get("alerts_chat_name", "") or ""),
            "min_alert_severity": str(row.get("min_alert_severity", "WARNING") or "WARNING").upper(),
            "alert_cooldown_sec": int(row.get("alert_cooldown_sec", 300) or 300),
            "alert_dedup_enabled": bool(row.get("alert_dedup_enabled", True)),
            "alert_format_rich": bool(row.get("alert_format_rich", True)),
        }

    def save_telegram_alert_settings(
        self,
        *,
        alerts_enabled: bool,
        alerts_chat_id: Any,
        alerts_chat_name: str,
        min_alert_severity: str = "WARNING",
        alert_cooldown_sec: int = 300,
        alert_dedup_enabled: bool = True,
        alert_format_rich: bool = True,
    ) -> None:
        row = self.db.get_telegram_settings() if hasattr(self.db, "get_telegram_settings") else {}
        row = dict(row or {})

        row["alerts_enabled"] = int(bool(alerts_enabled))
        row["alerts_chat_id"] = str(alerts_chat_id or "")
        row["alerts_chat_name"] = str(alerts_chat_name or "")
        row["min_alert_severity"] = str(min_alert_severity or "WARNING").upper()
        row["alert_cooldown_sec"] = int(alert_cooldown_sec or 300)
        row["alert_dedup_enabled"] = int(bool(alert_dedup_enabled))
        row["alert_format_rich"] = int(bool(alert_format_rich))

        if hasattr(self.db, "save_telegram_settings"):
            self.db.save_telegram_settings(row)

    # =========================================================
    # ROSERPINA CONFIG
    # =========================================================
    def load_roserpina_config(self) -> RoserpinaConfig:
        data = self.get_all_settings()

        risk_profile_raw = str(
            data.get("roserpina.risk_profile", "BALANCED") or "BALANCED"
        ).upper()
        if risk_profile_raw not in {p.value for p in RiskProfile}:
            risk_profile_raw = RiskProfile.BALANCED.value

        return RoserpinaConfig(
            target_profit_cycle_pct=self._f(data, "roserpina.target_profit_cycle_pct", 3.0),
            max_single_bet_pct=self._f(data, "roserpina.max_single_bet_pct", 18.0),
            max_total_exposure_pct=self._f(data, "roserpina.max_total_exposure_pct", 35.0),
            max_event_exposure_pct=self._f(data, "roserpina.max_event_exposure_pct", 18.0),
            auto_reset_drawdown_pct=self._f(data, "roserpina.auto_reset_drawdown_pct", 15.0),
            defense_drawdown_pct=self._f(data, "roserpina.defense_drawdown_pct", 7.5),
            lockdown_drawdown_pct=self._f(data, "roserpina.lockdown_drawdown_pct", 20.0),
            expansion_profit_pct=self._f(data, "roserpina.expansion_profit_pct", 5.0),
            expansion_multiplier=self._f(data, "roserpina.expansion_multiplier", 1.10),
            defense_multiplier=self._f(data, "roserpina.defense_multiplier", 0.80),
            risk_profile=RiskProfile(risk_profile_raw),
            table_count=self._i(data, "roserpina.table_count", 5),
            max_recovery_tables=self._i(data, "roserpina.max_recovery_tables", 2),
            allow_recovery=self._b(data, "roserpina.allow_recovery", True),
            anti_duplication_enabled=self._b(data, "roserpina.anti_duplication_enabled", True),
            commission_pct=self._f(data, "roserpina.commission_pct", 4.5),
            min_stake=self._f(data, "roserpina.min_stake", 0.10),
            max_stake_abs=self._f(data, "roserpina.max_stake_abs", 10000.0),
        )

    def save_roserpina_config(self, config: RoserpinaConfig) -> None:
        self.db.save_settings(
            {
                "roserpina.target_profit_cycle_pct": config.target_profit_cycle_pct,
                "roserpina.max_single_bet_pct": config.max_single_bet_pct,
                "roserpina.max_total_exposure_pct": config.max_total_exposure_pct,
                "roserpina.max_event_exposure_pct": config.max_event_exposure_pct,
                "roserpina.auto_reset_drawdown_pct": config.auto_reset_drawdown_pct,
                "roserpina.defense_drawdown_pct": config.defense_drawdown_pct,
                "roserpina.lockdown_drawdown_pct": config.lockdown_drawdown_pct,
                "roserpina.expansion_profit_pct": config.expansion_profit_pct,
                "roserpina.expansion_multiplier": config.expansion_multiplier,
                "roserpina.defense_multiplier": config.defense_multiplier,
                "roserpina.risk_profile": config.risk_profile.value,
                "roserpina.table_count": config.table_count,
                "roserpina.max_recovery_tables": config.max_recovery_tables,
                "roserpina.allow_recovery": int(bool(config.allow_recovery)),
                "roserpina.anti_duplication_enabled": int(bool(config.anti_duplication_enabled)),
                "roserpina.commission_pct": config.commission_pct,
                "roserpina.min_stake": config.min_stake,
                "roserpina.max_stake_abs": config.max_stake_abs,
            }
        )

    # =========================================================
    # SIMULATION CONFIG
    # =========================================================
    def load_simulation_config(self) -> Dict[str, Any]:
        data = self.get_all_settings()
        return {
            "enabled": self._b(data, "simulation.enabled", True),
            "starting_balance": self._f(data, "simulation.starting_balance", 1000.0),
            "commission_pct": self._f(data, "simulation.commission_pct", self._f(data, "roserpina.commission_pct", 4.5)),
            "partial_fill_enabled": self._b(data, "simulation.partial_fill_enabled", True),
            "consume_liquidity": self._b(data, "simulation.consume_liquidity", True),
            "persist_state": self._b(data, "simulation.persist_state", True),
        }

    def save_simulation_config(self, config: Dict[str, Any]) -> None:
        self.db.save_settings(
            {
                "simulation.enabled": int(bool(config.get("enabled", True))),
                "simulation.starting_balance": float(config.get("starting_balance", 1000.0) or 1000.0),
                "simulation.commission_pct": float(config.get("commission_pct", 4.5) or 4.5),
                "simulation.partial_fill_enabled": int(bool(config.get("partial_fill_enabled", True))),
                "simulation.consume_liquidity": int(bool(config.get("consume_liquidity", True))),
                "simulation.persist_state": int(bool(config.get("persist_state", True))),
            }
        )

    def load_simulation_starting_balance(self) -> float:
        cfg = self.load_simulation_config()
        return float(cfg.get("starting_balance", 1000.0) or 1000.0)

    def load_simulation_commission_pct(self) -> float:
        cfg = self.load_simulation_config()
        return float(cfg.get("commission_pct", 4.5) or 4.5)

    def load_simulation_partial_fill_enabled(self) -> bool:
        cfg = self.load_simulation_config()
        return bool(cfg.get("partial_fill_enabled", True))

    def load_simulation_consume_liquidity(self) -> bool:
        cfg = self.load_simulation_config()
        return bool(cfg.get("consume_liquidity", True))

    def load_simulation_persist_state(self) -> bool:
        cfg = self.load_simulation_config()
        return bool(cfg.get("persist_state", True))

    # =========================================================
    # SIMULATION STATE PERSISTENCE
    # =========================================================
    def save_simulation_state(self, state: Dict[str, Any], state_key: str = "default") -> None:
        if hasattr(self.db, "save_simulation_state"):
            self.db.save_simulation_state(state_key=state_key, state=state or {})

    def load_simulation_state(self, state_key: str = "default") -> Dict[str, Any]:
        if hasattr(self.db, "get_simulation_state"):
            return self.db.get_simulation_state(state_key=state_key) or {}
        return {}

    def clear_simulation_state(self, state_key: str = "default") -> None:
        if hasattr(self.db, "clear_simulation_state"):
            self.db.clear_simulation_state(state_key=state_key)

    # =========================================================
    # ANOMALY ESCALATION TOGGLES
    # =========================================================
    def load_anomaly_toggles(self) -> Dict[str, bool]:
        data = self.get_all_settings()
        return {
            "anomaly_enabled": self._b(data, "anomaly_enabled", False),
            "anomaly_alerts_enabled": self._b(data, "anomaly_alerts_enabled", False),
            "anomaly_actions_enabled": self._b(data, "anomaly_actions_enabled", False),
        }

    def save_anomaly_toggles(
        self,
        *,
        anomaly_enabled: bool,
        anomaly_alerts_enabled: bool,
        anomaly_actions_enabled: bool,
    ) -> None:
        self.save_settings(
            {
                "anomaly_enabled": int(bool(anomaly_enabled)),
                "anomaly_alerts_enabled": int(bool(anomaly_alerts_enabled)),
                "anomaly_actions_enabled": int(bool(anomaly_actions_enabled)),
            }
        )
