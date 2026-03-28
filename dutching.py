from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List


TWOPLACES = Decimal("0.01")
EPS = Decimal("0.0000001")


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

        # Prova a spostare 0.01 dalla stake che genera profitto più alto
        # verso quella che genera profitto più basso.
        candidate = list(stakes)
        if candidate[max_idx] > TWOPLACES:
            candidate[max_idx] = _round_step(candidate[max_idx] - TWOPLACES)
            candidate[min_idx] = _round_step(candidate[min_idx] + TWOPLACES)

            # riallinea total stake
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

    # sicurezza finale sul totale
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
    Dutching pro:
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
    commission_d = _d(commission, "0")

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

    # Base dutching
    stakes: List[Decimal] = []
    for odd in odds_d:
        stake = total_stake_d * ((Decimal("1") / odd) / inv_sum)
        stakes.append(_round_step(stake))

    # riallinea totale
    diff = total_stake_d - sum(stakes)
    if stakes:
        stakes[-1] = _round_step(stakes[-1] + diff)

    # Equalizzazione pro
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

    return {
        "stakes": [float(s) for s in stakes],
        "profits": [float(p) for p in profits],
        "net_profits": [float(p) for p in net_profits],
        "book_pct": float(_round_step(book_pct)),
        "avg_profit": float(_round_step(avg_profit)),
        "avg_net_profit": float(_round_step(avg_net_profit)),
    }


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
    net_green = _round_step(_apply_commission(raw_green, commission))

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