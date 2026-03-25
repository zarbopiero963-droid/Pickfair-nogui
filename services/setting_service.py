from __future__ import annotations

from typing import Any, Dict

from core.system_state import (
    BetfairConfig,
    RiskProfile,
    RoserpinaConfig,
    TelegramRuntimeConfig,
)


class SettingsService:
    def __init__(self, db):
        self.db = db

    # =========================================================
    # GENERIC SETTINGS
    # =========================================================
    def get_all_settings(self) -> Dict[str, Any]:
        getter = getattr(self.db, "get_settings", None)
        if callable(getter):
            return getter() or {}
        return {}

    # =========================================================
    # BETFAIR
    # =========================================================
    def load_betfair_config(self) -> BetfairConfig:
        data = self.get_all_settings()
        return BetfairConfig(
            username=str(data.get("username", "") or ""),
            app_key=str(data.get("app_key", "") or ""),
            certificate=str(data.get("certificate", "") or ""),
            private_key=str(data.get("private_key", "") or ""),
        )

    def save_betfair_config(
        self,
        config: BetfairConfig,
        password: str | None = None,
    ) -> None:
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

    def load_session(self) -> Dict[str, str]:
        data = self.get_all_settings()
        return {
            "session_token": str(data.get("session_token", "") or ""),
            "session_expiry": str(data.get("session_expiry", "") or ""),
        }

    def clear_session(self) -> None:
        if hasattr(self.db, "clear_session"):
            self.db.clear_session()

    # =========================================================
    # TELEGRAM
    # =========================================================
    def load_telegram_config(self) -> TelegramRuntimeConfig:
        row = (
            self.db.get_telegram_settings()
            if hasattr(self.db, "get_telegram_settings")
            else {}
        )

        chats = (
            self.db.get_telegram_chats()
            if hasattr(self.db, "get_telegram_chats")
            else []
        )

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
        if hasattr(self.db, "save_telegram_settings"):
            self.db.save_telegram_settings(
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

    # =========================================================
    # ROSERPINA
    # =========================================================
    def load_roserpina_config(self) -> RoserpinaConfig:
        data = self.get_all_settings()

        def _f(key: str, default: float) -> float:
            try:
                return float(data.get(key, default))
            except Exception:
                return float(default)

        def _i(key: str, default: int) -> int:
            try:
                return int(data.get(key, default))
            except Exception:
                return int(default)

        def _b(key: str, default: bool) -> bool:
            value = data.get(key, default)
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in {"1", "true", "yes", "on"}

        risk_profile_raw = str(
            data.get("roserpina.risk_profile", "BALANCED") or "BALANCED"
        ).upper()

        if risk_profile_raw not in {p.value for p in RiskProfile}:
            risk_profile_raw = RiskProfile.BALANCED.value

        return RoserpinaConfig(
            target_profit_cycle_pct=_f("roserpina.target_profit_cycle_pct", 3.0),
            max_single_bet_pct=_f("roserpina.max_single_bet_pct", 18.0),
            max_total_exposure_pct=_f("roserpina.max_total_exposure_pct", 35.0),
            max_event_exposure_pct=_f("roserpina.max_event_exposure_pct", 18.0),
            auto_reset_drawdown_pct=_f("roserpina.auto_reset_drawdown_pct", 15.0),
            defense_drawdown_pct=_f("roserpina.defense_drawdown_pct", 7.5),
            lockdown_drawdown_pct=_f("roserpina.lockdown_drawdown_pct", 20.0),
            expansion_profit_pct=_f("roserpina.expansion_profit_pct", 5.0),
            expansion_multiplier=_f("roserpina.expansion_multiplier", 1.10),
            defense_multiplier=_f("roserpina.defense_multiplier", 0.80),
            risk_profile=RiskProfile(risk_profile_raw),
            table_count=_i("roserpina.table_count", 5),
            max_recovery_tables=_i("roserpina.max_recovery_tables", 2),
            allow_recovery=_b("roserpina.allow_recovery", True),
            anti_duplication_enabled=_b("roserpina.anti_duplication_enabled", True),
            commission_pct=_f("roserpina.commission_pct", 4.5),
            min_stake=_f("roserpina.min_stake", 0.10),
            max_stake_abs=_f("roserpina.max_stake_abs", 10000.0),
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