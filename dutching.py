from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List

from trading_config import enforce_betfair_italy_commission_pct

TWOPLACES = Decimal("0.01")
EPS = Decimal("0.0000001")
_NON_AUTHORITATIVE_SETTLEMENT_KEYS = ("settlement_source", "settlement_kind", "settlement_basis")


def _d(value: Any, default: str = "0") -> Decimal:
    try:
        if value in (None, ""):
            return Decimal(default)
        return Decimal(str(value).replace(",", "."))
    except Exception:
        return Decimal(default)


def _round_step(value: Decimal, step: Decimal = TWOPLACES) -> Decimal:
    if step <= 0:
        return value
    return value.quantize(step, rounding=ROUND_HALF_UP)


def _apply_commission(profit: Decimal, commission: float | Decimal = 4.5) -> Decimal:
    commission_d = _d(commission, "0")
    if profit <= Decimal("0") or commission_d <= Decimal("0"):
        return profit
    return profit * (Decimal("1") - (commission_d / Decimal("100")))


def _resolve_policy_commission_pct(commission: float | Decimal) -> Decimal:
    commission_d = _d(commission, "0")
    if commission_d <= Decimal("0"):
        # Helper surfaces can explicitly disable commission for gross-only previews.
        # This module is preview-only and NOT a realized settlement authority.
        return Decimal("0")
    enforced = enforce_betfair_italy_commission_pct(
        float(commission_d),
        context="dutching_helper",
    )
    return _d(enforced, "0")


def _profit_for_outcome(
    stakes: List[Decimal],
    odds_d: List[Decimal],
    winner_idx: int,
) -> Decimal:
    gross_return = stakes[winner_idx] * odds_d[winner_idx]
    gross_profit = gross_return - sum(stakes)
    return gross_profit


def _net_profit_for_outcome(
    stakes: List[Decimal],
    odds_d: List[Decimal],
    winner_idx: int,
    commission: Decimal,
) -> Decimal:
    gross_profit = _profit_for_outcome(stakes, odds_d, winner_idx)
    return _apply_commission(gross_profit, commission)


def _equalize_stakes_post_rounding(
    stakes: List[Decimal],
    odds_d: List[Decimal],
    total_stake_d: Decimal,
    commission: Decimal,
    iterations: int = 500,
) -> List[Decimal]:
    """
    Equalizzazione post-rounding:
    - mantiene la somma stake uguale al totale
    - riduce lo spread dei profitti netti
    - usa micro-aggiustamenti da 0.01
    """
    if not stakes or len(stakes) != len(odds_d):
        return stakes

    stakes = list(stakes)

    def profit_spread(current: List[Decimal]) -> Decimal:
        profits = [
            _net_profit_for_outcome(current, odds_d, i, commission)
            for i in range(len(current))
        ]
        return max(profits) - min(profits)

    current_spread = profit_spread(stakes)

    for _ in range(iterations):
        profits = [
            _net_profit_for_outcome(stakes, odds_d, i, commission)
            for i in range(len(stakes))
        ]
        min_idx = profits.index(min(profits))
        max_idx = profits.index(max(profits))

        improved = False

        candidate = list(stakes)
        if candidate[max_idx] > TWOPLACES:
            candidate[max_idx] = _round_step(candidate[max_idx] - TWOPLACES)
            candidate[min_idx] = _round_step(candidate[min_idx] + TWOPLACES)

            diff = total_stake_d - sum(candidate)
            if diff != Decimal("0"):
                candidate[min_idx] = _round_step(candidate[min_idx] + diff)

            if all(s > Decimal("0") for s in candidate):
                new_spread = profit_spread(candidate)
                if new_spread + EPS < current_spread:
                    stakes = candidate
                    current_spread = new_spread
                    improved = True

        if not improved:
            break

    diff = total_stake_d - sum(stakes)
    if stakes and diff != Decimal("0"):
        stakes[-1] = _round_step(stakes[-1] + diff)

    return stakes


def calculate_dutching_stakes(
    odds: List[float],
    total_stake: float,
    commission: float = 0.0,
    equalize: bool = True,
    commission_aware: bool = True,
) -> Dict[str, Any]:
    """
    Dutching helper (preview-only, non-authoritative settlement surface):
    - stake distribution corretta
    - equalizzazione post-rounding
    - commission-aware opzionale

    Ritorna:
    - stakes
    - profits (lordi)
    - net_profits
    - book_pct
    - avg_profit
    - avg_net_profit
    """
    odds_d = [_d(x, "0") for x in (odds or [])]
    total_stake_d = _d(total_stake, "0")
    commission_d = _resolve_policy_commission_pct(commission)

    if not odds_d or total_stake_d <= Decimal("0"):
        return {
            "stakes": [],
            "profits": [],
            "net_profits": [],
            "book_pct": 0.0,
            "avg_profit": 0.0,
            "avg_net_profit": 0.0,
        }

    if any(o <= Decimal("1.0") for o in odds_d):
        return {
            "stakes": [],
            "profits": [],
            "net_profits": [],
            "book_pct": 0.0,
            "avg_profit": 0.0,
            "avg_net_profit": 0.0,
            "error": "Invalid odds <= 1.0",
        }

    inv_sum = sum((Decimal("1") / o) for o in odds_d)
    if inv_sum <= Decimal("0"):
        return {
            "stakes": [],
            "profits": [],
            "net_profits": [],
            "book_pct": 0.0,
            "avg_profit": 0.0,
            "avg_net_profit": 0.0,
            "error": "Invalid inverse odds sum",
        }

    stakes: List[Decimal] = []
    for odd in odds_d:
        stake = total_stake_d * ((Decimal("1") / odd) / inv_sum)
        stakes.append(_round_step(stake))

    diff = total_stake_d - sum(stakes)
    if stakes:
        stakes[-1] = _round_step(stakes[-1] + diff)

    if equalize and len(stakes) >= 2:
        stakes = _equalize_stakes_post_rounding(
            stakes=stakes,
            odds_d=odds_d,
            total_stake_d=total_stake_d,
            commission=commission_d if commission_aware else Decimal("0"),
        )

    profits: List[Decimal] = []
    net_profits: List[Decimal] = []

    for idx in range(len(stakes)):
        gross_profit = _round_step(_profit_for_outcome(stakes, odds_d, idx))
        net_profit = _round_step(_apply_commission(gross_profit, commission_d))
        profits.append(gross_profit)
        net_profits.append(net_profit)

    avg_profit = sum(profits) / Decimal(len(profits)) if profits else Decimal("0")
    avg_net_profit = (
        sum(net_profits) / Decimal(len(net_profits))
        if net_profits
        else Decimal("0")
    )
    book_pct = inv_sum * Decimal("100")

    result = {
        "stakes": [float(s) for s in stakes],
        "profits": [float(p) for p in profits],
        "net_profits": [float(p) for p in net_profits],
        "book_pct": float(_round_step(book_pct)),
        "avg_profit": float(_round_step(avg_profit)),
        "avg_net_profit": float(_round_step(avg_net_profit)),
    }
    for key in _NON_AUTHORITATIVE_SETTLEMENT_KEYS:
        result.pop(key, None)
    return result


def _normalize_selection_side(selection: Dict[str, Any]) -> str:
    item = selection or {}
    raw_side = item.get("side")
    if not raw_side:
        raw_side = item.get("effectiveType")
    side = str(raw_side or "BACK").upper().strip()
    return side if side in {"BACK", "LAY"} else "BACK"


def calculate_dutching(
    selections: List[Dict[str, Any]],
    total_stake: float,
    commission: float = 4.5,
) -> tuple[List[Dict[str, Any]], float, float, float]:
    """
    Explicit dutching contract dispatcher.

    Supported models:
    - BACK_EQUAL_PROFIT_FIXED_TOTAL_STAKE
    - LAY_EQUAL_PROFIT_FIXED_TOTAL_STAKE

    LAY model semantics:
    - fixed total lay stake budget (sum(stake_i) == total_stake)
    - equalized gross/net outcome across selected mutually-exclusive outcomes
    - if laid outcome i wins against us:
        gross_i = sum(stake_j for j!=i) - liability_i
                = total_stake - stake_i * odds_i
      liability_i = stake_i * (odds_i - 1)
    """
    selections = list(selections or [])
    if not selections:
        return [], 0.0, 0.0, 0.0

    side_set = {_normalize_selection_side(s) for s in selections}
    if len(side_set) != 1:
        raise ValueError(
            "Unsupported dutching contract: mixed BACK/LAY selections are not supported"
        )

    mode = next(iter(side_set))
    odds = [float((s or {}).get("price", 0.0) or 0.0) for s in selections]
    preview = calculate_dutching_stakes(
        odds=odds,
        total_stake=float(total_stake),
        commission=float(commission),
        commission_aware=True,
    )
    if preview.get("error"):
        raise ValueError(str(preview.get("error")))

    stakes = [float(x) for x in (preview.get("stakes") or [])]
    profits = [float(x) for x in (preview.get("profits") or [])]
    net_profits = [float(x) for x in (preview.get("net_profits") or [])]
    avg_profit = float(preview.get("avg_profit", 0.0) or 0.0)
    avg_net_profit = float(preview.get("avg_net_profit", avg_profit) or avg_profit)
    book_pct = float(preview.get("book_pct", 0.0) or 0.0)

    total_stake_d = _d(total_stake, "0")
    odds_d = [_d(o, "0") for o in odds]
    commission_d = _resolve_policy_commission_pct(commission)
    lay_gross: List[float] = []
    lay_net: List[float] = []

    if mode == "LAY":
        lay_gross = []
        lay_net = []
        for idx, stake in enumerate(stakes):
            stake_d = _d(stake, "0")
            gross_d = _round_step(total_stake_d - (stake_d * odds_d[idx]))
            net_d = _round_step(_apply_commission(gross_d, commission_d))
            lay_gross.append(float(gross_d))
            lay_net.append(float(net_d))

    results: List[Dict[str, Any]] = []
    for idx, selection in enumerate(selections):
        stake_f = float(stakes[idx]) if idx < len(stakes) else 0.0
        profit_f = float(profits[idx]) if idx < len(profits) else 0.0
        net_profit_f = float(net_profits[idx]) if idx < len(net_profits) else 0.0
        item = {
            "selectionId": int(selection["selectionId"]),
            "price": float(selection["price"]),
            "stake": stake_f,
            "side": mode,
            "runnerName": selection.get("runnerName", ""),
            "profitIfWins": profit_f,
            "profitIfWinsNet": net_profit_f,
            "dutchingModel": f"{mode}_EQUAL_PROFIT_FIXED_TOTAL_STAKE",
        }
        if mode == "LAY":
            item["liability"] = round(stake_f * max(0.0, float(item["price"]) - 1.0), 2)
            if idx < len(lay_gross):
                item["profitIfWins"] = lay_gross[idx]
            if idx < len(lay_net):
                item["profitIfWinsNet"] = lay_net[idx]
        results.append(item)

    return results, avg_profit, book_pct, avg_net_profit


def dynamic_cashout_single(
    matched_stake: float = None,
    matched_price: float = None,
    current_price: float = None,
    commission: float = 4.5,
    side: str = "BACK",
    **kwargs,
) -> dict:
    side_mode = str(side).upper().strip()
    if side_mode not in {"BACK", "LAY"}:
        side_mode = "BACK"

    if matched_stake is None:
        matched_stake = (
            kwargs.get("back_stake", 0.0)
            if side_mode == "BACK"
            else kwargs.get("lay_stake", 0.0)
        )

    if matched_price is None:
        matched_price = (
            kwargs.get("back_price", 0.0)
            if side_mode == "BACK"
            else kwargs.get("lay_price", 0.0)
        )

    if current_price is None:
        current_price = (
            kwargs.get("lay_price", 0.0)
            if side_mode == "BACK"
            else kwargs.get("back_price", 0.0)
        )

    ms = _d(matched_stake, "0")
    mp = _d(matched_price, "0")
    cp = _d(current_price, "0")

    if ms <= Decimal("0") or mp <= Decimal("1.01") or cp <= Decimal("1.01"):
        side_to_place = "LAY" if side_mode == "BACK" else "BACK"
        return {
            "cashout_stake": 0.0,
            "green_up": 0.0,
            "net_profit": 0.0,
            "profit_if_win": 0.0,
            "profit_if_lose": 0.0,
            "side_to_place": side_to_place,
            "lay_stake": 0.0 if side_mode == "BACK" else None,
            "back_stake": 0.0 if side_mode == "LAY" else None,
        }

    cashout_stake = _round_step((ms * mp) / cp)

    if side_mode == "BACK":
        profit_if_win = ms * (mp - Decimal("1")) - cashout_stake * (cp - Decimal("1"))
        profit_if_lose = cashout_stake - ms
        side_to_place = "LAY"
    else:
        profit_if_win = cashout_stake * (cp - Decimal("1")) - ms * (mp - Decimal("1"))
        profit_if_lose = ms - cashout_stake
        side_to_place = "BACK"

    profit_if_win = _round_step(profit_if_win)
    profit_if_lose = _round_step(profit_if_lose)

    raw_green = _round_step((profit_if_win + profit_if_lose) / Decimal("2"))
    commission_d = _resolve_policy_commission_pct(commission)
    net_green = _round_step(_apply_commission(raw_green, commission_d))

    result = {
        "cashout_stake": float(cashout_stake),
        "green_up": float(raw_green),
        "net_profit": float(net_green),
        "profit_if_win": float(profit_if_win),
        "profit_if_lose": float(profit_if_lose),
        "side_to_place": side_to_place,
        "lay_stake": None,
        "back_stake": None,
    }

    if side_mode == "BACK":
        result["lay_stake"] = float(cashout_stake)
    else:
        result["back_stake"] = float(cashout_stake)

    return result


def calculate_cashout(
    original_stake: float,
    original_odds: float,
    current_odds: float,
    side: str = "BACK",
) -> Dict[str, float]:
    original_stake_d = _d(original_stake, "0")
    original_odds_d = _d(original_odds, "0")
    current_odds_d = _d(current_odds, "0")
    side_mode = str(side).upper().strip()
    if side_mode not in {"BACK", "LAY"}:
        side_mode = "BACK"

    if (
        original_stake_d <= Decimal("0")
        or original_odds_d <= Decimal("1.0")
        or current_odds_d <= Decimal("1.0")
    ):
        return {
            "cashout_stake": 0.0,
            "profit_if_win": 0.0,
            "profit_if_lose": 0.0,
            "guaranteed_profit": 0.0,
            "side_to_place": "LAY" if side_mode == "BACK" else "BACK",
        }

    cashout_stake = _round_step((original_stake_d * original_odds_d) / current_odds_d)

    if side_mode == "BACK":
        profit_if_win = (
            original_stake_d * (original_odds_d - Decimal("1"))
            - cashout_stake * (current_odds_d - Decimal("1"))
        )
        profit_if_lose = cashout_stake - original_stake_d
        side_to_place = "LAY"
    else:
        profit_if_win = (
            cashout_stake * (current_odds_d - Decimal("1"))
            - original_stake_d * (original_odds_d - Decimal("1"))
        )
        profit_if_lose = original_stake_d - cashout_stake
        side_to_place = "BACK"

    profit_if_win = _round_step(profit_if_win)
    profit_if_lose = _round_step(profit_if_lose)
    guaranteed_profit = _round_step(min(profit_if_win, profit_if_lose))

    return {
        "cashout_stake": float(cashout_stake),
        "profit_if_win": float(profit_if_win),
        "profit_if_lose": float(profit_if_lose),
        "guaranteed_profit": float(guaranteed_profit),
        "side_to_place": side_to_place,
    }
