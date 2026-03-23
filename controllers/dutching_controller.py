"""
DutchingController - Orchestratore unificato per dutching
Coordina UI -> validazioni -> AI -> dutching -> EventBus (RiskGate)
Entry point unico per tutto il flusso di dutching.
Zero esecuzione ordini diretta: solo validazione, calcolo, preflight e publish REQ_PLACE_DUTCHING.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ai.ai_guardrail import get_guardrail
from ai.ai_pattern_engine import AIPatternEngine
from ai.wom_engine import get_wom_engine
from automation_engine import AutomationEngine
from dutching import calculate_dutching_stakes, calculate_mixed_dutching
from market_validator import MarketValidator
from safe_mode import get_safe_mode_manager
from safety_logger import get_safety_logger
from trading_config import (
    BOOK_BLOCK,
    BOOK_WARNING,
    LIQUIDITY_GUARD_ENABLED,
    LIQUIDITY_MULTIPLIER,
    LIQUIDITY_WARNING_ONLY,
    MAX_SPREAD_TICKS,
    MAX_STAKE_PCT,
    MIN_LIQUIDITY,
    MIN_LIQUIDITY_ABSOLUTE,
    MIN_PRICE,
    MIN_STAKE,
)

logger = logging.getLogger(__name__)


@dataclass
class PreflightResult:
    is_valid: bool = True
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    liquidity_ok: bool = True
    liquidity_guard_ok: bool = True
    spread_ok: bool = True
    stake_ok: bool = True
    price_ok: bool = True
    book_ok: bool = True
    details: Dict = field(default_factory=dict)


class DutchingController:
    def __init__(self, broker=None, pnl_engine=None, bus=None, simulation: bool = False):
        self.broker = broker
        self.pnl_engine = pnl_engine
        self.bus = bus
        self.simulation = bool(simulation)
        self.auto_green_enabled = True
        self.ai_enabled = True
        self.preset_stake_pct = 1.0

        self.ai_engine = AIPatternEngine()
        self.wom_engine = get_wom_engine()
        self.guardrail = get_guardrail()
        self.market_validator = MarketValidator()
        self.automation = AutomationEngine(controller=self)
        self.safety_logger = get_safety_logger()
        self.safe_mode = get_safe_mode_manager()

        self.current_event_name = ""
        self.current_market_name = ""
        self.client = None

    def _safe_float(self, value, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def _safe_int(self, value, default: int = 0) -> int:
        try:
            if value in (None, ""):
                return int(default)
            return int(value)
        except Exception:
            return int(default)

    def _normalize_mode(self, mode: str) -> str:
        value = str(mode or "BACK").upper().strip()
        return value if value in {"BACK", "LAY", "MIXED"} else "BACK"

    def _safe_event_name(self) -> str:
        return str(getattr(self, "current_event_name", "") or "Event")

    def _safe_market_name(self) -> str:
        return str(getattr(self, "current_market_name", "") or "Market")

    def submit_dutching(
        self,
        market_id: str,
        market_type: str,
        selections: List[Dict],
        total_stake: float,
        mode: str = "BACK",
        event_name: Optional[str] = None,
        market_name: Optional[str] = None,
        ai_enabled: bool = False,
        ai_wom_enabled: bool = False,
        auto_green: bool = False,
        commission: float = 4.5,
        use_best_price: bool = False,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        trailing: Optional[float] = None,
        dry_run: bool = False,
        **kwargs,
    ) -> Dict:
        market_id = str(market_id or "").strip()
        market_type = str(market_type or "").strip()
        selections = list(selections or [])
        total_stake = self._safe_float(total_stake, 0.0)
        commission = self._safe_float(commission, 4.5)
        mode = self._normalize_mode(mode)

        # Salva i valori runtime per event_name e market_name
        if event_name is not None:
            self.current_event_name = str(event_name)

        if market_name is not None:
            self.current_market_name = str(market_name)

        if self.safe_mode.is_safe_mode_active:
            raise RuntimeError("SAFE MODE attivo: dutching bloccato")

        if not market_id:
            raise ValueError("market_id mancante")

        validation_errors = self.validate_selections(selections)
        if validation_errors:
            return {
                "status": "VALIDATION_FAILED",
                "orders": [],
                "errors": validation_errors,
                "simulation": self.simulation,
                "mode": mode,
                "dry_run": bool(dry_run),
            }

        if ai_enabled or ai_wom_enabled:
            tick_count = 10
            wom_confidence = 0.5
            volatility = 0.0

            if selections and hasattr(self, "wom_engine") and self.wom_engine:
                first_sel_id = selections[0].get("selectionId")
                if first_sel_id:
                    wom_result = self.wom_engine.calculate_enhanced_wom(first_sel_id)
                    if wom_result:
                        tick_count = self._safe_int(
                            getattr(wom_result, "tick_count", 10), 10
                        )
                        wom_confidence = self._safe_float(
                            getattr(wom_result, "confidence", 0.5), 0.5
                        )
                        volatility = self._safe_float(
                            getattr(wom_result, "volatility", 0.0), 0.0
                        )

            guardrail_result = self.check_guardrail(
                market_type=market_type,
                tick_count=tick_count,
                wom_confidence=wom_confidence,
                volatility=volatility,
            )
            if not guardrail_result.get("can_proceed", True):
                return {
                    "status": "GUARDRAIL_BLOCKED",
                    "orders": [],
                    "simulation": self.simulation,
                    "mode": mode,
                    "guardrail": guardrail_result,
                    "dry_run": bool(dry_run),
                }

        if ai_enabled:
            if not self.market_validator.is_dutching_ready(market_type):
                raise ValueError(f"Mercato {market_type} non compatibile")

            ai_sides = self.ai_engine.decide(selections) or {}
            for sel in selections:
                side = str(ai_sides.get(sel.get("selectionId"), "BACK")).upper().strip()
                if side not in {"BACK", "LAY"}:
                    side = "BACK"
                sel["side"] = side
                sel["effectiveType"] = side
            mode = "MIXED"

        try:
            if mode == "MIXED":
                results, profit, implied_prob = calculate_mixed_dutching(
                    selections,
                    total_stake,
                    commission=commission,
                )
            else:
                results, profit, implied_prob = calculate_dutching_stakes(
                    selections,
                    total_stake,
                    bet_type=mode,
                    commission=commission,
                )
        except Exception as e:
            try:
                self.safe_mode.report_error("DutchingCalcError", str(e), market_id)
            except Exception:
                logger.exception("Errore report_error safe_mode")
            raise

        try:
            self.safe_mode.report_success()
        except Exception:
            logger.exception("Errore report_success safe_mode")

        preflight = self.preflight_check(selections, total_stake, mode)

        for r in results:
            stake = self._safe_float(r.get("stake", 0), 0.0)
            side = str(r.get("side", r.get("effectiveType", mode))).upper().strip()
            price = self._safe_float(r.get("price", 0), 0.0)

            if side == "BACK" and stake < MIN_STAKE:
                preflight.is_valid = False
                preflight.stake_ok = False
                preflight.errors.append(
                    f"{r.get('runnerName', 'Runner')}: stake BACK sotto minimo"
                )

            if side == "LAY" and stake * max(price - 1.0, 0.0) < MIN_STAKE:
                preflight.is_valid = False
                preflight.stake_ok = False
                preflight.errors.append(
                    f"{r.get('runnerName', 'Runner')}: liability LAY sotto minimo"
                )

        results_with_ladders = self._merge_ladders_to_results(results, selections)
        liq_ok, liq_msgs = self._check_liquidity_guard(
            results_with_ladders,
            mode,
            market_id,
        )
        if not liq_ok:
            preflight.liquidity_guard_ok = False
            preflight.is_valid = False
            preflight.errors.extend(liq_msgs)

        if not preflight.is_valid and not dry_run:
            return {
                "status": "PREFLIGHT_FAILED",
                "orders": [],
                "preflight": {
                    "is_valid": False,
                    "errors": preflight.errors,
                    "warnings": preflight.warnings,
                    "details": preflight.details,
                },
            }

        payload = {
            "source": "DUTCHING_CONTROLLER",
            "market_id": market_id,
            "market_type": market_type,
            "event_name": self._safe_event_name(),
            "market_name": self._safe_market_name(),
            "results": results,
            "bet_type": mode,
            "total_stake": total_stake,
            "use_best_price": bool(use_best_price),
            "simulation_mode": bool(self.simulation),
            "auto_green": bool(auto_green),
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing": trailing,
            "preflight": {
                "is_valid": preflight.is_valid,
                "warnings": preflight.warnings,
                "errors": preflight.errors,
                "details": preflight.details,
            },
            "analytics": {
                "potential_profit": profit,
                "implied_probability": implied_prob,
            },
        }

        if dry_run:
            placed = [
                {
                    "betId": f"DRY_{r['selectionId']}",
                    "selectionId": r["selectionId"],
                    "side": r.get("side", r.get("effectiveType", mode)),
                    "price": r["price"],
                    "size": r["stake"],
                    "status": "DRY_RUN",
                    "dry_run": True,
                }
                for r in results
            ]
            return {
                "status": "DRY_RUN",
                "orders": placed,
                "payload": payload,
            }

        if not self.bus:
            raise RuntimeError("EventBus mancante nel DutchingController")

        self.bus.publish("REQ_PLACE_DUTCHING", payload)
        return {
            "status": "SUBMITTED",
            "async": True,
            "orders": [],
            "preflight": {
                "is_valid": preflight.is_valid,
                "warnings": preflight.warnings,
                "errors": preflight.errors,
                "details": preflight.details,
            },
        }

    def validate_selections(self, selections: List[Dict]) -> List[str]:
        errors = []
        selections = list(selections or [])

        if not selections:
            return ["Nessuna selezione"]

        for sel in selections:
            runner_name = sel.get("runnerName", "Runner")
            price = self._safe_float(sel.get("price"), 0.0)
            selection_id = sel.get("selectionId")

            if price <= 1.0:
                errors.append(f"{runner_name}: prezzo non valido")

            if not selection_id:
                errors.append(f"{runner_name}: selectionId mancante")

        return errors

    def set_simulation(self, enabled: bool):
        self.simulation = bool(enabled)

    def get_ai_analysis(self, selections: List[Dict]) -> List[Dict]:
        try:
            return self.ai_engine.get_wom_analysis(selections or [])
        except Exception:
            logger.exception("Errore get_ai_analysis")
            return []

    def preflight_check(
        self,
        selections: List[Dict],
        total_stake: float,
        mode: str = "BACK",
    ) -> PreflightResult:
        result = PreflightResult()
        selections = list(selections or [])
        total_stake = self._safe_float(total_stake, 0.0)
        mode = self._normalize_mode(mode)

        num_selections = len(selections)
        if num_selections == 0:
            result.is_valid = False
            result.errors.append("Nessuna selezione")
            return result

        min_total = MIN_STAKE * num_selections
        if total_stake < min_total:
            result.is_valid = False
            result.stake_ok = False
            result.errors.append(
                f"Stake totale €{total_stake:.2f} insufficiente (min €{min_total:.2f})"
            )

        total_liquidity = 0.0
        total_implied_prob = 0.0

        for sel in selections:
            runner_name = sel.get("runnerName", f"ID {sel.get('selectionId', '?')}")
            price = self._safe_float(sel.get("price", 0), 0.0)
            back_ladder = list(sel.get("back_ladder", []) or [])
            lay_ladder = list(sel.get("lay_ladder", []) or [])

            if 0 < price < MIN_PRICE:
                result.price_ok = False
                result.warnings.append(
                    f"{runner_name}: quota {price:.2f} troppo bassa (min {MIN_PRICE:.2f})"
                )

            if price > 1:
                total_implied_prob += 1.0 / price

            back_liq = sum(self._safe_float(p.get("size", 0), 0.0) for p in back_ladder)
            lay_liq = sum(self._safe_float(p.get("size", 0), 0.0) for p in lay_ladder)
            side = str(sel.get("side", sel.get("effectiveType", mode))).upper().strip()
            relevant_liq = back_liq if side == "BACK" else lay_liq
            total_liquidity += relevant_liq

            if relevant_liq < MIN_LIQUIDITY:
                result.liquidity_ok = False
                result.warnings.append(
                    f"{runner_name}: liquidità {side} bassa (€{relevant_liq:.0f})"
                )

            if back_ladder and lay_ladder:
                best_back = self._safe_float(back_ladder[0].get("price", 0), 0.0)
                best_lay = self._safe_float(lay_ladder[0].get("price", 0), 0.0)

                if best_back > 0 and best_lay > 0:
                    spread = best_lay - best_back
                    tick_size = 0.02 if best_back < 2 else 0.05 if best_back < 4 else 0.1
                    spread_ticks = spread / tick_size if tick_size > 0 else 0.0

                    if spread_ticks > MAX_SPREAD_TICKS:
                        result.spread_ok = False
                        result.warnings.append(
                            f"{runner_name}: spread largo ({spread_ticks:.0f} tick)"
                        )

                    result.details[sel.get("selectionId")] = {
                        "back_liq": back_liq,
                        "lay_liq": lay_liq,
                        "best_back": best_back,
                        "best_lay": best_lay,
                        "spread_ticks": spread_ticks,
                    }

        if total_liquidity > 0:
            stake_pct = total_stake / total_liquidity
            if stake_pct > MAX_STAKE_PCT:
                result.warnings.append(
                    f"Stake alto rispetto a liquidità ({stake_pct * 100:.0f}% > {MAX_STAKE_PCT * 100:.0f}%)"
                )

        book_pct = total_implied_prob * 100
        if book_pct > BOOK_BLOCK:
            result.book_ok = False
            result.is_valid = False
            result.errors.append(
                f"Book {book_pct:.1f}% troppo alto (blocco a {BOOK_BLOCK:.0f}%)"
            )
        elif book_pct > BOOK_WARNING:
            result.book_ok = False
            result.warnings.append(
                f"Book {book_pct:.1f}% elevato (warning a {BOOK_WARNING:.0f}%)"
            )

        result.details["book_pct"] = book_pct
        if result.errors:
            result.is_valid = False
        return result

    def _check_liquidity_guard(
        self,
        selections: List[Dict],
        mode: str = "BACK",
        market_id: str = "",
    ) -> Tuple[bool, List[str]]:
        if not LIQUIDITY_GUARD_ENABLED:
            return True, []

        messages = []
        mode = self._normalize_mode(mode)

        for sel in selections or []:
            selection_id = sel.get("selectionId", 0)
            runner_name = sel.get("runnerName", f"ID {selection_id}")

            stake = self._safe_float(sel.get("stake", 0), 0.0)
            price = self._safe_float(sel.get("price", 1), 1.0)
            side = str(sel.get("side", sel.get("effectiveType", mode))).upper().strip()

            back_ladder = sel.get("back_ladder")
            lay_ladder = sel.get("lay_ladder")

            if back_ladder is None and lay_ladder is None:
                continue

            back_ladder = back_ladder or []
            lay_ladder = lay_ladder or []

            back_liq = sum(self._safe_float(p.get("size", 0), 0.0) for p in back_ladder)
            lay_liq = sum(self._safe_float(p.get("size", 0), 0.0) for p in lay_ladder)

            if side == "BACK":
                available = back_liq
                required = stake * LIQUIDITY_MULTIPLIER
            else:
                liability = stake * (price - 1) if price > 1 else stake
                available = lay_liq
                required = liability * LIQUIDITY_MULTIPLIER

            if available < MIN_LIQUIDITY_ABSOLUTE:
                return False, [
                    f"{runner_name}: liquidità troppo bassa (€{available:.0f} < €{MIN_LIQUIDITY_ABSOLUTE:.0f})"
                ]

            if available < required:
                messages.append(
                    f"{runner_name}: liquidità insufficiente (€{available:.0f} < €{required:.0f} richiesti)"
                )
                if not LIQUIDITY_WARNING_ONLY:
                    return False, messages

        return len(messages) == 0, messages

    def _merge_ladders_to_results(
        self,
        results: List[Dict],
        selections: List[Dict],
    ) -> List[Dict]:
        sel_by_id = {s.get("selectionId"): s for s in (selections or [])}
        merged = []

        for r in results or []:
            original = sel_by_id.get(r.get("selectionId"), {})
            merged_item = dict(r)

            if "back_ladder" in original:
                merged_item["back_ladder"] = list(original.get("back_ladder") or [])
            else:
                merged_item["back_ladder"] = None

            if "lay_ladder" in original:
                merged_item["lay_ladder"] = list(original.get("lay_ladder") or [])
            else:
                merged_item["lay_ladder"] = None

            merged.append(merged_item)

        return merged

    def record_market_tick(
        self,
        selection_id: int,
        back_price: float,
        back_volume: float,
        lay_price: float,
        lay_volume: float,
    ):
        self.wom_engine.record_tick(
            selection_id,
            back_price,
            back_volume,
            lay_price,
            lay_volume,
        )

    def get_wom_analysis(
        self,
        selections: List[Dict],
        use_historical: bool = True,
    ) -> List[Dict]:
        try:
            if use_historical:
                return self.ai_engine.get_enhanced_analysis(
                    selections or [],
                    self.wom_engine,
                )
            return self.ai_engine.get_wom_analysis(selections or [])
        except Exception:
            logger.exception("Errore get_wom_analysis")
            return []

    def get_wom_stats(self) -> Dict:
        try:
            return self.wom_engine.get_stats()
        except Exception:
            logger.exception("Errore get_wom_stats")
            return {}

    def check_guardrail(
        self,
        market_type: str,
        tick_count: int = 10,
        wom_confidence: float = 0.5,
        volatility: float = 0.0,
    ) -> Dict:
        return self.guardrail.full_check(
            market_type=market_type,
            tick_count=tick_count,
            wom_confidence=wom_confidence,
            volatility=volatility,
        )

    def check_auto_green_ready(self, bet_id: str):
        return self.guardrail.check_auto_green_grace(bet_id)

    def register_for_auto_green(self, bet_id: str):
        self.guardrail.register_order_for_auto_green(bet_id)

    def get_time_window_signal(self, selection_id: int) -> Dict:
        return self.wom_engine.get_time_window_signal(selection_id)

    def get_guardrail_status(self) -> Dict:
        return self.guardrail.get_status()