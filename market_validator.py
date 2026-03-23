"""
Market Validator - Verifica mercati compatibili con AI Dutching.
Solo mercati winner-takes-all sono dutching-ready.
"""

from typing import List, Set


class MarketValidationError(Exception):
    """Errore quando mercato non è dutching-ready."""

    pass


class MarketValidator:
    """Validatore per mercati Betfair compatibili con dutching."""

    DUTCHING_READY_MARKETS: Set[str] = {
        "MATCH_ODDS",
        "WINNER",
        "MONEYLINE",
        "WIN",
        "PLACE",
        # FIX #28: EACH_WAY, FORECAST, TRICAST removed.
        # These are NOT winner-takes-all markets:
        #   EACH_WAY  — two sub-bets (win + place); proportional payouts
        #   FORECAST  — ordered finish prediction; multiple selections matter
        #   TRICAST   — top-3 ordered prediction; multiple selections matter
        # Including them in the dutching-ready set allowed the system to
        # attempt equal-profit dutching on markets where the payout structure
        # is fundamentally different, producing incorrect stake calculations.
        "HALF_TIME",
        "HALF_TIME_FULL_TIME",
        "FIRST_GOAL_SCORER",
        "LAST_GOAL_SCORER",
        "ANYTIME_GOALSCORER",
        "CORRECT_SCORE",
        "FIRST_HALF_GOALS",
        "SECOND_HALF_GOALS",
        "MATCH_RESULT_AND_BTTS",
    }

    NON_DUTCHING_MARKETS: Set[str] = {
        # FIX #28: explicitly mark the removed markets as non-dutching so the
        # fallback pattern-matching ("ODDS" in name, "WIN" in name) cannot
        # accidentally re-admit them.
        "EACH_WAY",
        "FORECAST",
        "TRICAST",
        "OVER_UNDER_05",
        "OVER_UNDER_15",
        "OVER_UNDER_25",
        "OVER_UNDER_35",
        "OVER_UNDER_45",
        "ASIAN_HANDICAP",
        "HANDICAP",
        "CORNER_MATCH_BET",
        "CORNER_ODDS",
        "BOOKING_MATCH_BET",
        "BOOKING_ODDS",
        "BOTH_TEAMS_TO_SCORE",
        "DRAW_NO_BET",
        "DOUBLE_CHANCE",
        "TO_QUALIFY",
        "TO_WIN_NOT_TO_WIN",
    }

    @classmethod
    def is_dutching_ready(cls, market_type: str) -> bool:
        """
        Verifica se il mercato è compatibile con dutching.

        Args:
            market_type: Tipo mercato Betfair (es. MATCH_ODDS, OVER_UNDER_25)

        Returns:
            True se mercato è winner-takes-all (dutching-ready)
        """
        if not market_type:
            return False

        market_upper = market_type.upper().replace(" ", "_").replace("-", "_")

        if market_upper in cls.DUTCHING_READY_MARKETS:
            return True

        if market_upper in cls.NON_DUTCHING_MARKETS:
            return False

        if "WINNER" in market_upper or "WIN" in market_upper:
            return True
        if (
            "ODDS" in market_upper
            and "CORNER" not in market_upper
            and "BOOKING" not in market_upper
        ):
            return True

        return False

    @classmethod
    def assert_dutching_ready(cls, market_type: str) -> None:
        """
        Verifica mercato e solleva eccezione se non dutching-ready.

        Args:
            market_type: Tipo mercato Betfair

        Raises:
            MarketValidationError: Se mercato non compatibile con dutching
        """
        if not cls.is_dutching_ready(market_type):
            raise MarketValidationError(
                f"Mercato '{market_type}' non compatibile con AI Dutching. "
                f"Solo mercati winner-takes-all sono supportati."
            )

    @classmethod
    def get_market_warning(cls, market_type: str) -> str:
        """
        Restituisce messaggio warning per mercato non compatibile.

        Args:
            market_type: Tipo mercato

        Returns:
            Messaggio warning o stringa vuota se compatibile
        """
        if cls.is_dutching_ready(market_type):
            return ""

        return "Mercato NON DUTCHING-READY\nAI disabilitata automaticamente"

    @classmethod
    def get_compatible_markets(cls) -> List[str]:
        """Restituisce lista mercati compatibili."""
        return sorted(list(cls.DUTCHING_READY_MARKETS))

