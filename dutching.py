from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional


TWOPLACES = Decimal("0.01")


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
    quant = step
    return value.quantize(quant, rounding=ROUND_HALF_UP)


def _apply_commission(profit: Decimal, commission: float | Decimal = 4.5) -> Decimal:
    commission_d = _d(commission, "0")
    if profit <= Decimal("0") or commission_d <= Decimal("0"):
        return profit
    return profit * (Decimal("1") - (commission_d / Decimal("100")))


def calculate_dutching_stakes(
    odds: List[float],
    total_stake: float,
) -> Dict[str, Any]:
    odds_d = [_d(x, "0") for x in (odds or [])]
    total_stake_d = _d(total_stake, "0")

    if not odds_d or total_stake_d <= Decimal("0"):
        return {
            "stakes": [],
            "profits": [],
            "book_pct": 0.0,
            "avg_profit": 0.0,
        }

    if any(o <= Decimal("1.0") for o in odds_d):
        return {
            "stakes": [],
            "profits": [],
            "book_pct": 0.0,
            "avg_profit": 0.0,
            "error": "Invalid odds <= 1.0",
        }

    inv_sum = sum((Decimal("1") / o) for o in odds_d)
    if inv_sum <= Decimal("0"):
        return {
            "stakes": [],
            "profits": [],
            "book_pct": 0.0,
            "avg_profit": 0.0,
            "error": "Invalid inverse odds sum",
        }

    stakes = []
    for o in odds_d:
        stake = total_stake_d * ((Decimal("1") / o) / inv_sum)
        stakes.append(_round_step(stake))

    total_alloc = sum(stakes)
    diff = total_stake_d - total_alloc
    if stakes:
        stakes[-1] = _round_step(stakes[-1] + diff)

    profits = []
    for stake, odd in zip(stakes, odds_d):
        gross_return = stake * odd
        gross_profit = gross_return - total_stake_d
        profits.append(_round_step(gross_profit))

    avg_profit = sum(profits) / Decimal(len(profits)) if profits else Decimal("0")
    book_pct = (inv_sum * Decimal("100"))

    return {
        "stakes": [float(s) for s in stakes],
        "profits": [float(p) for p in profits],
        "book_pct": float(_round_step(book_pct)),
        "avg_profit": float(_round_step(avg_profit)),
    }


def dynamic_cashout_single(
    matched_stake: float = None,
    matched_price: float = None,
    current_price: float = None,
    commission: float = 4.5,
    side: str = "BACK",
    **kwargs,
) -> dict:
    """
    Calculate green-up (cashout) stake and resulting equal profit.

    Supporta sia:
    - BACK iniziale -> cashout con LAY
    - LAY iniziale  -> cashout con BACK

    Compatibilità legacy:
    - BACK:
        back_stake, back_price, lay_price
    - LAY:
        lay_stake, lay_price, back_price
    """
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

    # Formula universale green-up
    cashout_stake = _round_step((ms * mp) / cp)

    if side_mode == "BACK":
        # Uscita con LAY
        profit_if_win = ms * (mp - Decimal("1")) - cashout_stake * (cp - Decimal("1"))
        profit_if_lose = cashout_stake - ms
        side_to_place = "LAY"
    else:
        # Uscita con BACK
        profit_if_win = cashout_stake * (cp - Decimal("1")) - ms * (mp - Decimal("1"))
        profit_if_lose = ms - cashout_stake
        side_to_place = "BACK"

    profit_if_win = _round_step(profit_if_win)
    profit_if_lose = _round_step(profit_if_lose)

    # idealmente uguali; usiamo la media per green-up lordo
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

    # compatibilità con vecchi caller
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
    """
    Calcola cashout / green-up equal profit.

    - side="BACK": scommessa iniziale BACK, uscita con LAY
    - side="LAY": scommessa iniziale LAY, uscita con BACK
    """
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