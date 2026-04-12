"""
WoM Engine - Weight of Money Time-Window Analysis
Analisi storica dei tick per calcolare la pressione di mercato.

Versione blindata:
- input sporchi tollerati
- lock coerenti sullo stato condiviso
- snapshot atomici per letture/calcoli
- nessun side effect nei metodi di analisi
- storico limitato e controllato
"""

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from core.type_helpers import safe_float

WOM_WINDOW_SIZE = 50
WOM_TIME_WINDOW_SEC = 30.0
WOM_TIME_WINDOWS = [5.0, 15.0, 30.0, 60.0]
EDGE_THRESHOLDS = {
    "strong_back": 0.65,
    "back": 0.55,
    "neutral_high": 0.50,
    "neutral_low": 0.45,
    "lay": 0.45,
    "strong_lay": 0.35,
}
DELTA_THRESHOLD = 0.05


@dataclass
class TickData:
    """Singolo tick di mercato."""

    timestamp: float
    selection_id: int
    back_price: float
    back_volume: float
    lay_price: float
    lay_volume: float


@dataclass
class WoMResult:
    """Risultato analisi WoM per un runner."""

    selection_id: int
    wom: float
    wom_trend: float
    edge_score: float
    suggested_side: str
    confidence: float
    tick_count: int
    time_span: float
    wom_5s: float = 0.5
    wom_15s: float = 0.5
    wom_30s: float = 0.5
    wom_60s: float = 0.5
    delta_pressure: float = 0.0
    momentum: float = 0.0
    volatility: float = 0.0


@dataclass
class SelectionWoMHistory:
    """Storage storico tick per una selezione."""

    selection_id: int
    maxlen: int = WOM_WINDOW_SIZE
    ticks: deque[TickData] = field(init=False)

    def __post_init__(self):
        self.ticks = deque(maxlen=max(2, int(self.maxlen or WOM_WINDOW_SIZE)))

    def add_tick(self, tick: TickData):
        self.ticks.append(tick)

    def get_recent_from_snapshot(
        self,
        ticks_snapshot: list[TickData],
        max_age_sec: float,
        now: float | None = None,
    ) -> list[TickData]:
        ref_now = time.time() if now is None else float(now)
        max_age = max(0.1, float(max_age_sec or WOM_TIME_WINDOW_SEC))
        return [t for t in ticks_snapshot if ref_now - t.timestamp <= max_age]

    def get_recent(self, max_age_sec: float = WOM_TIME_WINDOW_SEC) -> list[TickData]:
        return self.get_recent_from_snapshot(list(self.ticks), max_age_sec)

    def clear(self):
        self.ticks.clear()


class WoMEngine:
    """
    Engine per analisi Weight of Money su finestra temporale.
    """

    def __init__(
        self,
        window_size: int = WOM_WINDOW_SIZE,
        time_window: float = WOM_TIME_WINDOW_SEC,
    ):
        self._window_size = max(2, int(window_size or WOM_WINDOW_SIZE))
        self._time_window = max(1.0, float(time_window or WOM_TIME_WINDOW_SEC))
        self._histories: dict[int, SelectionWoMHistory] = {}
        self._lock = threading.RLock()

    # =========================================================
    # SAFE PARSERS
    # =========================================================

    def _safe_int(self, value, default: int | None = None) -> int | None:
        try:
            if value in (None, ""):
                return default
            return int(value)
        except Exception:
            return default

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        return safe_float(value, default)

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, float(value)))

    # =========================================================
    # INTERNAL STATE HELPERS
    # =========================================================

    def _get_or_create_history(self, selection_id: int) -> SelectionWoMHistory:
        history = self._histories.get(selection_id)
        if history is None:
            history = SelectionWoMHistory(
                selection_id=selection_id,
                maxlen=self._window_size,
            )
            self._histories[selection_id] = history
        return history

    def _get_ticks_snapshot(
        self,
        selection_id: int,
        max_age_sec: float | None = None,
    ) -> list[TickData]:
        with self._lock:
            history = self._histories.get(selection_id)
            if history is None:
                return []
            ticks_snapshot = list(history.ticks)

        if max_age_sec is None:
            return ticks_snapshot

        now = time.time()
        max_age = max(0.1, self._safe_float(max_age_sec, self._time_window))
        return [t for t in ticks_snapshot if now - t.timestamp <= max_age]

    def _calculate_wom_from_ticks(self, ticks: list[TickData]) -> float | None:
        if len(ticks) < 2:
            return None

        total_back_vol = sum(max(0.0, self._safe_float(t.back_volume, 0.0)) for t in ticks)
        total_lay_vol = sum(max(0.0, self._safe_float(t.lay_volume, 0.0)) for t in ticks)
        total_vol = total_back_vol + total_lay_vol

        if total_vol <= 0:
            return None

        return total_back_vol / total_vol

    # =========================================================
    # PUBLIC WRITE API
    # =========================================================

    def record_tick(
        self,
        selection_id: int,
        back_price: float,
        back_volume: float,
        lay_price: float,
        lay_volume: float,
    ):
        sel_id = self._safe_int(selection_id)
        if sel_id is None:
            return

        tick = TickData(
            timestamp=time.time(),
            selection_id=sel_id,
            back_price=max(0.0, self._safe_float(back_price, 0.0)),
            back_volume=max(0.0, self._safe_float(back_volume, 0.0)),
            lay_price=max(0.0, self._safe_float(lay_price, 0.0)),
            lay_volume=max(0.0, self._safe_float(lay_volume, 0.0)),
        )

        with self._lock:
            history = self._get_or_create_history(sel_id)
            history.add_tick(tick)

    # =========================================================
    # PUBLIC READ / ANALYSIS API
    # =========================================================

    def calculate_wom(self, selection_id: int) -> WoMResult | None:
        sel_id = self._safe_int(selection_id)
        if sel_id is None:
            return None

        ticks = self._get_ticks_snapshot(sel_id, self._time_window)
        if len(ticks) < 2:
            return None

        wom = self._calculate_wom_from_ticks(ticks)
        if wom is None:
            return None

        mid_idx = len(ticks) // 2
        if mid_idx > 0:
            first_half = ticks[:mid_idx]
            second_half = ticks[mid_idx:]
            first_wom = self._calculate_wom_from_ticks(first_half)
            second_wom = self._calculate_wom_from_ticks(second_half)

            if first_wom is None or second_wom is None:
                wom_trend = 0.0
            else:
                wom_trend = second_wom - first_wom
        else:
            wom_trend = 0.0

        edge_score = self._calculate_edge_score(wom, wom_trend)
        suggested_side = self._determine_side(wom, wom_trend)
        confidence = self._calculate_confidence(wom, len(ticks), wom_trend)
        time_span = ticks[-1].timestamp - ticks[0].timestamp if len(ticks) > 1 else 0.0

        return WoMResult(
            selection_id=sel_id,
            wom=wom,
            wom_trend=wom_trend,
            edge_score=edge_score,
            suggested_side=suggested_side,
            confidence=confidence,
            tick_count=len(ticks),
            time_span=max(0.0, time_span),
        )

    def get_ai_edge_score(self, selections: list[dict]) -> dict[int, WoMResult]:
        results: dict[int, WoMResult] = {}

        for sel in selections or []:
            sel_id = sel.get("selectionId", sel.get("selection_id"))
            parsed_id = self._safe_int(sel_id)
            if parsed_id is None:
                continue

            wom_result = self.calculate_wom(parsed_id)
            if wom_result:
                results[parsed_id] = wom_result

        return results

    def get_mixed_suggestions(self, selections: list[dict]) -> list[dict]:
        selections = selections or []
        if not selections:
            return []

        results = []
        edge_data = self.get_ai_edge_score(selections)

        valid_probs = []
        for s in selections:
            price = self._safe_float(s.get("price", 2.0), 2.0)
            if price > 1.0:
                valid_probs.append(1.0 / price)

        avg_prob = sum(valid_probs) / len(valid_probs) if valid_probs else 0.5

        for sel in selections:
            sel_id = self._safe_int(sel.get("selectionId", sel.get("selection_id")))
            price = self._safe_float(sel.get("price", 2.0), 2.0)
            implied_prob = 1.0 / price if price > 1 else 1.0

            if sel_id is not None and sel_id in edge_data:
                wom_result = edge_data[sel_id]
                results.append(
                    {
                        "selectionId": sel_id,
                        "runnerName": sel.get("runnerName", f"Runner {sel_id}"),
                        "price": price,
                        "implied_prob": implied_prob,
                        "suggested_side": wom_result.suggested_side,
                        "edge_score": wom_result.edge_score,
                        "confidence": wom_result.confidence,
                        "wom": wom_result.wom,
                        "wom_trend": wom_result.wom_trend,
                        "has_wom_data": True,
                    }
                )
            else:
                suggested_side = "BACK" if implied_prob < avg_prob else "LAY"
                results.append(
                    {
                        "selectionId": sel_id,
                        "runnerName": sel.get("runnerName", f"Runner {sel_id}"),
                        "price": price,
                        "implied_prob": implied_prob,
                        "suggested_side": suggested_side,
                        "edge_score": 0.5,
                        "confidence": 0.0,
                        "wom": 0.5,
                        "wom_trend": 0.0,
                        "has_wom_data": False,
                    }
                )

        if len(results) > 1:
            back_count = sum(1 for r in results if r["suggested_side"] == "BACK")
            lay_count = len(results) - back_count

            if back_count == 0:
                best_for_back = max(results, key=lambda r: r["wom"])
                best_for_back["suggested_side"] = "BACK"
                best_for_back["forced"] = True
            elif lay_count == 0:
                best_for_lay = min(results, key=lambda r: r["wom"])
                best_for_lay["suggested_side"] = "LAY"
                best_for_lay["forced"] = True

        return results

    # =========================================================
    # CORE ANALYTICS
    # =========================================================

    def _calculate_edge_score(self, wom: float, trend: float) -> float:
        base_edge = (float(wom) - 0.5) * 2.0
        trend_boost = float(trend) * 0.5
        edge = base_edge + trend_boost
        return self._clamp(edge, -1.0, 1.0)

    def _determine_side(self, wom: float, trend: float) -> str:
        if wom >= EDGE_THRESHOLDS["strong_back"]:
            return "BACK"
        if wom >= EDGE_THRESHOLDS["back"]:
            return "BACK"
        if wom <= EDGE_THRESHOLDS["strong_lay"]:
            return "LAY"
        if wom <= EDGE_THRESHOLDS["lay"]:
            return "LAY"
        return "BACK" if trend > 0.05 else ("LAY" if trend < -0.05 else "BACK")

    def _calculate_confidence(self, wom: float, tick_count: int, trend: float) -> float:
        wom_distance = abs(float(wom) - 0.5) * 2.0
        tick_factor = min(1.0, max(0.0, float(tick_count) / 30.0))
        trend_coherence = 1.0 if (wom > 0.5 and trend > 0) or (wom < 0.5 and trend < 0) else 0.7
        confidence = wom_distance * 0.4 + tick_factor * 0.4 + trend_coherence * 0.2
        return self._clamp(confidence, 0.0, 1.0)

    # =========================================================
    # HOUSEKEEPING
    # =========================================================

    def clear_history(self, selection_id: int | None = None):
        with self._lock:
            if selection_id is not None:
                sel_id = self._safe_int(selection_id)
                if sel_id is not None and sel_id in self._histories:
                    self._histories[sel_id].clear()
            else:
                self._histories.clear()

    def get_stats(self) -> dict:
        with self._lock:
            total_ticks = sum(len(h.ticks) for h in self._histories.values())
            selections_tracked = len(self._histories)

        return {
            "selections_tracked": selections_tracked,
            "total_ticks": total_ticks,
            "window_size": self._window_size,
            "time_window": self._time_window,
        }

    # =========================================================
    # TIME WINDOW METHODS
    # =========================================================

    def calculate_wom_window(self, selection_id: int, window_sec: float) -> float:
        sel_id = self._safe_int(selection_id)
        if sel_id is None:
            return 0.5

        ticks = self._get_ticks_snapshot(
            sel_id,
            max(0.1, self._safe_float(window_sec, WOM_TIME_WINDOW_SEC)),
        )
        wom = self._calculate_wom_from_ticks(ticks)
        return wom if wom is not None else 0.5

    def calculate_multi_window_wom(self, selection_id: int) -> dict[str, float]:
        sel_id = self._safe_int(selection_id)
        if sel_id is None:
            return {
                "wom_5s": 0.5,
                "wom_15s": 0.5,
                "wom_30s": 0.5,
                "wom_60s": 0.5,
            }

        ticks_snapshot = self._get_ticks_snapshot(sel_id)
        if not ticks_snapshot:
            return {
                "wom_5s": 0.5,
                "wom_15s": 0.5,
                "wom_30s": 0.5,
                "wom_60s": 0.5,
            }

        now = time.time()

        def calc_window(window_sec: float) -> float:
            recent = [t for t in ticks_snapshot if now - t.timestamp <= window_sec]
            wom = self._calculate_wom_from_ticks(recent)
            return wom if wom is not None else 0.5

        return {
            "wom_5s": calc_window(5.0),
            "wom_15s": calc_window(15.0),
            "wom_30s": calc_window(30.0),
            "wom_60s": calc_window(60.0),
        }

    def calculate_delta_pressure(self, selection_id: int) -> float:
        wom_5s = self.calculate_wom_window(selection_id, 5.0)
        wom_30s = self.calculate_wom_window(selection_id, 30.0)
        delta = wom_5s - wom_30s
        return self._clamp(delta * 2.0, -1.0, 1.0)

    def calculate_momentum(self, selection_id: int) -> float:
        sel_id = self._safe_int(selection_id)
        if sel_id is None:
            return 0.0

        ticks = self._get_ticks_snapshot(sel_id, 30.0)
        if len(ticks) < 4:
            return 0.0

        q_size = len(ticks) // 4
        if q_size < 1:
            return 0.0

        quarters = [ticks[i * q_size : (i + 1) * q_size] for i in range(4)]
        wom_values: list[float] = []

        for q in quarters:
            wom = self._calculate_wom_from_ticks(q)
            if wom is not None:
                wom_values.append(wom)

        if len(wom_values) < 2:
            return 0.0

        deltas = [wom_values[i + 1] - wom_values[i] for i in range(len(wom_values) - 1)]
        momentum = sum(deltas) / len(deltas) if deltas else 0.0
        return self._clamp(momentum * 4.0, -1.0, 1.0)

    def calculate_volatility(self, selection_id: int) -> float:
        sel_id = self._safe_int(selection_id)
        if sel_id is None:
            return 0.0

        ticks = self._get_ticks_snapshot(sel_id, 30.0)
        if len(ticks) < 3:
            return 0.0

        spreads: list[float] = []
        for t in ticks:
            if t.lay_price > 0 and t.back_price > 0:
                spreads.append(max(0.0, t.lay_price - t.back_price))

        if len(spreads) < 2:
            return 0.0

        avg_spread = sum(spreads) / len(spreads)
        variance = sum((s - avg_spread) ** 2 for s in spreads) / len(spreads)
        std_dev = variance**0.5
        volatility = min(1.0, std_dev / 0.05)
        return max(0.0, volatility)

    def calculate_enhanced_wom(self, selection_id: int) -> WoMResult | None:
        base_result = self.calculate_wom(selection_id)
        if not base_result:
            return None

        multi_wom = self.calculate_multi_window_wom(selection_id)
        delta_pressure = self.calculate_delta_pressure(selection_id)
        momentum = self.calculate_momentum(selection_id)
        volatility = self.calculate_volatility(selection_id)

        return WoMResult(
            selection_id=base_result.selection_id,
            wom=base_result.wom,
            wom_trend=base_result.wom_trend,
            edge_score=base_result.edge_score,
            suggested_side=base_result.suggested_side,
            confidence=base_result.confidence,
            tick_count=base_result.tick_count,
            time_span=base_result.time_span,
            wom_5s=multi_wom["wom_5s"],
            wom_15s=multi_wom["wom_15s"],
            wom_30s=multi_wom["wom_30s"],
            wom_60s=multi_wom["wom_60s"],
            delta_pressure=delta_pressure,
            momentum=momentum,
            volatility=volatility,
        )

    def get_time_window_signal(self, selection_id: int) -> dict:
        result = self.calculate_enhanced_wom(selection_id)
        if not result:
            return {
                "signal": "NO_DATA",
                "strength": 0.0,
                "side": "NEUTRAL",
                "reasoning": "Dati insufficienti",
            }

        short_term = result.wom_5s
        long_term = result.wom_30s
        convergence = abs(short_term - 0.5) * abs(long_term - 0.5) * 4.0

        signal_strength = (
            abs(result.delta_pressure) * 0.4
            + abs(result.momentum) * 0.3
            + abs(result.wom - 0.5) * 0.3
        )

        if result.delta_pressure > DELTA_THRESHOLD and result.momentum > 0:
            signal = "STRONG_BACK"
            side = "BACK"
            reasoning = f"Pressione BACK in aumento (delta={result.delta_pressure:.2f})"
        elif result.delta_pressure < -DELTA_THRESHOLD and result.momentum < 0:
            signal = "STRONG_LAY"
            side = "LAY"
            reasoning = f"Pressione LAY in aumento (delta={result.delta_pressure:.2f})"
        elif result.wom > EDGE_THRESHOLDS["back"]:
            signal = "BACK"
            side = "BACK"
            reasoning = f"WoM favorisce BACK ({result.wom:.2f})"
        elif result.wom < EDGE_THRESHOLDS["lay"]:
            signal = "LAY"
            side = "LAY"
            reasoning = f"WoM favorisce LAY ({result.wom:.2f})"
        else:
            signal = "NEUTRAL"
            side = "NEUTRAL"
            reasoning = "Mercato in equilibrio"

        if result.volatility > 0.7:
            reasoning += " [ALTA VOLATILITA']"
            signal_strength *= 0.8

        return {
            "signal": signal,
            "strength": self._clamp(signal_strength * convergence, 0.0, 1.0),
            "side": side,
            "reasoning": reasoning,
            "wom_data": {
                "wom_5s": result.wom_5s,
                "wom_15s": result.wom_15s,
                "wom_30s": result.wom_30s,
                "delta": result.delta_pressure,
                "momentum": result.momentum,
                "volatility": result.volatility,
            },
        }


_global_wom_engine: WoMEngine | None = None


def get_wom_engine() -> WoMEngine:
    """Ritorna istanza globale WoM Engine."""
    global _global_wom_engine
    if _global_wom_engine is None:
        _global_wom_engine = WoMEngine()
    return _global_wom_engine
