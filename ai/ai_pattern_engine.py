"""
AIPatternEngine - Analisi Weight of Money per auto-entry BACK/LAY
"""

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ai.wom_engine import WoMEngine

logger = logging.getLogger(__name__)


class AIPatternEngine:
    def __init__(self, wom_back_threshold: float = 0.55, wom_lay_threshold: float = 0.45):
        self.wom_back_threshold = wom_back_threshold
        self.wom_lay_threshold = wom_lay_threshold

    def calculate_wom(self, selection: dict) -> float:
        back_vol = sum(p.get("size", 0) for p in selection.get("back_ladder", []))
        lay_vol = sum(p.get("size", 0) for p in selection.get("lay_ladder", []))
        total = back_vol + lay_vol
        return back_vol / total if total > 0 else 0.5

    def decide(self, selections: list[dict]) -> dict[int, str]:
        decisions = {}
        wom_values = {}
        MIN_REQUIRED_EDGE = 0.04

        for sel in selections:
            sel_id = sel.get("selectionId")
            if not sel_id:
                continue
            wom = self.calculate_wom(sel)
            wom_values[sel_id] = wom
            if wom > self.wom_back_threshold:
                decisions[sel_id] = "BACK"
            elif wom < self.wom_lay_threshold:
                decisions[sel_id] = "LAY"
            else:
                decisions[sel_id] = "BACK"

        sides = set(decisions.values())
        if len(sides) == 1 and len(selections) > 1:
            avg_wom = sum(wom_values.values()) / len(wom_values) if wom_values else 0.5
            best_candidate = None
            best_distance = -1

            for sel_id, wom in wom_values.items():
                distance = abs(wom - avg_wom)
                if distance > best_distance:
                    best_distance = distance
                    best_candidate = sel_id

            if best_candidate is None and wom_values:
                best_candidate = list(wom_values.keys())[0]
            if best_candidate:
                weakest_wom = wom_values[best_candidate]
                if abs(weakest_wom - 0.5) <= MIN_REQUIRED_EDGE:
                    decisions[best_candidate] = (
                        "LAY" if decisions[best_candidate] == "BACK" else "BACK"
                    )

        return decisions

    def get_wom_analysis(self, selections: list[dict]) -> list[dict]:
        analysis = []
        for sel in selections:
            sel_id = sel.get("selectionId")
            if not sel_id:
                continue
            wom = self.calculate_wom(sel)
            side = (
                "BACK"
                if wom > self.wom_back_threshold
                else "LAY"
                if wom < self.wom_lay_threshold
                else "NEUTRAL"
            )
            analysis.append(
                {
                    "selectionId": sel_id,
                    "runnerName": sel.get("runnerName", ""),
                    "wom": round(wom, 3),
                    "suggested_side": side,
                    "confidence": round(abs(wom - 0.5) * 2, 2),
                }
            )
        return analysis

    def get_enhanced_analysis(
        self, selections: list[dict], wom_engine: Optional["WoMEngine"] = None
    ) -> list[dict]:
        instant_analysis = self.get_wom_analysis(selections)
        if wom_engine is None:
            for item in instant_analysis:
                item["edge_score"], item["has_history"] = (item["wom"] - 0.5) * 2, False
            return instant_analysis

        enhanced = []
        for idx, sel in enumerate(selections):
            sel_id = sel.get("selectionId")
            instant = instant_analysis[idx] if idx < len(instant_analysis) else {}
            hist_result = wom_engine.calculate_wom(sel_id) if sel_id else None

            if hist_result:
                combined_wom = instant.get("wom", 0.5) * 0.4 + hist_result.wom * 0.6
                side = (
                    "BACK"
                    if combined_wom > self.wom_back_threshold
                    else "LAY"
                    if combined_wom < self.wom_lay_threshold
                    else ("BACK" if hist_result.wom_trend > 0 else "LAY")
                )
                enhanced.append(
                    {
                        "selectionId": sel_id,
                        "runnerName": sel.get("runnerName", ""),
                        "wom_instant": round(instant.get("wom", 0.5), 3),
                        "wom_historical": round(hist_result.wom, 3),
                        "wom_combined": round(combined_wom, 3),
                        "wom_trend": round(hist_result.wom_trend, 3),
                        "edge_score": round(
                            hist_result.edge_score * 0.7
                            + (instant.get("wom", 0.5) - 0.5) * 2 * 0.3,
                            3,
                        ),
                        "suggested_side": side,
                        "confidence": round(
                            max(instant.get("confidence", 0), hist_result.confidence), 2
                        ),
                        "has_history": True,
                        "tick_count": hist_result.tick_count,
                    }
                )
            else:
                instant["edge_score"], instant["has_history"] = (
                    round((instant.get("wom", 0.5) - 0.5) * 2, 3),
                    False,
                )
                enhanced.append(instant)
        return enhanced
