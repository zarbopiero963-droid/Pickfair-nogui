from __future__ import annotations

from typing import Any, Dict, Optional


class TelegramSignalProcessor:
    """
    Processore puro, senza GUI.
    Converte un segnale Telegram grezzo in un payload pulito per il runtime.
    """

    def normalize_action(self, signal: Dict[str, Any]) -> str:
        action = (
            signal.get("action")
            or signal.get("side")
            or signal.get("bet_type")
            or "BACK"
        )
        action = str(action).upper().strip()
        if action not in ("BACK", "LAY"):
            action = "BACK"
        return action

    def parse_price(self, signal: Dict[str, Any]) -> Optional[float]:
        raw = signal.get("price", signal.get("odds", 2.0))
        try:
            return float(raw)
        except Exception:
            return None

    def parse_selection_id(self, signal: Dict[str, Any]) -> Optional[int]:
        raw = signal.get("selection_id", signal.get("selectionId"))
        try:
            if raw in (None, ""):
                return None
            return int(raw)
        except Exception:
            return None

    def parse_market_id(self, signal: Dict[str, Any]) -> Optional[str]:
        raw = signal.get("market_id", signal.get("marketId"))
        if raw in (None, ""):
            return None
        try:
            return str(raw).strip()
        except Exception:
            return None

    def parse_event_name(self, signal: Dict[str, Any]) -> str:
        return str(
            signal.get("match")
            or signal.get("event")
            or signal.get("event_name")
            or "Segnale Telegram"
        )

    def parse_market_name(self, signal: Dict[str, Any]) -> str:
        return str(
            signal.get("market")
            or signal.get("market_name")
            or "Scommessa da Segnale"
        )

    def parse_market_type(self, signal: Dict[str, Any]) -> str:
        return str(signal.get("market_type") or "MATCH_ODDS")

    def parse_selection_name(self, signal: Dict[str, Any], selection_id: Optional[int]) -> str:
        return str(
            signal.get("selection")
            or signal.get("runner_name")
            or signal.get("runnerName")
            or selection_id
            or "Unknown"
        )

    def build_runtime_signal(
        self,
        signal: Dict[str, Any],
        stake: float,
        simulation_mode: bool = False,
    ) -> Optional[Dict[str, Any]]:
        action = self.normalize_action(signal)
        selection_id = self.parse_selection_id(signal)
        market_id = self.parse_market_id(signal)
        original_price = self.parse_price(signal)

        if selection_id is None or not market_id or original_price is None:
            return None

        selection_name = self.parse_selection_name(signal, selection_id)
        event_name = self.parse_event_name(signal)
        market_name = self.parse_market_name(signal)
        market_type = self.parse_market_type(signal)

        forced_price = 1.01 if action == "BACK" else 1000.0

        return {
            "market_id": market_id,
            "market_type": market_type,
            "event_name": event_name,
            "event": event_name,
            "market_name": market_name,
            "market": market_name,
            "selection_id": int(selection_id),
            "selectionId": int(selection_id),
            "runner_name": selection_name,
            "runnerName": selection_name,
            "selection": selection_name,
            "bet_type": action,
            "action": action,
            "price": float(forced_price),
            "odds": float(original_price),
            "master_price": float(original_price),
            "stake": float(stake),
            "simulation_mode": bool(simulation_mode),
            "source": "TELEGRAM",
            "forced_execution": True,
        }