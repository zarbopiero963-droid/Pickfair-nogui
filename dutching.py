"""
Betfair API client using betfairlightweight library.
Handles SSL certificate authentication for Betfair Italy.
Includes Streaming API for real-time price updates.
"""

import atexit
import logging
import os
import shutil
import tempfile
import threading
import time
from datetime import datetime, timedelta

import betfairlightweight
from betfairlightweight import filters
from betfairlightweight.streaming import StreamListener

# --- HEDGE-FUND STABLE FIX ---
from circuit_breaker import CircuitBreaker, TransientError, PermanentError

# -----------------------------

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # seconds


def with_retry(func):
    """
    Decorator to add retry logic for IDEMPOTENT API calls.
    NOTE: Do NOT use this decorator on non-idempotent operations like place_orders or replace_orders!
    """

    def wrapper(*args, **kwargs):
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                # Retry on network/server errors
                if any(
                    x in error_str
                    for x in ["502", "503", "504", "timeout", "connection", "network"]
                ):
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                # Don't retry on other errors
                raise
        raise last_error

    return wrapper


FOOTBALL_ID = "1"

MARKET_TYPES = {
    "MATCH_ODDS": "Esito Finale (1X2)",
    "CORRECT_SCORE": "Risultato Esatto",
    "OVER_UNDER_05": "Over/Under 0.5 Goal",
    "OVER_UNDER_15": "Over/Under 1.5 Goal",
    "OVER_UNDER_25": "Over/Under 2.5 Goal",
    "OVER_UNDER_35": "Over/Under 3.5 Goal",
    "OVER_UNDER_45": "Over/Under 4.5 Goal",
    "OVER_UNDER_55": "Over/Under 5.5 Goal",
    "OVER_UNDER_65": "Over/Under 6.5 Goal",
    "OVER_UNDER_75": "Over/Under 7.5 Goal",
    "BOTH_TEAMS_TO_SCORE": "Goal/No Goal",
    "DOUBLE_CHANCE": "Doppia Chance",
    "DRAW_NO_BET": "Draw No Bet",
    "HALF_TIME": "Primo Tempo",
    "HALF_TIME_SCORE": "Risultato Primo Tempo",
    "HALF_TIME_FULL_TIME": "Primo Tempo/Finale",
    "SECOND_HALF_CORRECT_SCORE": "Risultato Secondo Tempo",
    "ASIAN_HANDICAP": "Handicap Asiatico",
    "HANDICAP": "Handicap Europeo",
    "FIRST_GOAL_SCORER": "Primo Marcatore",
    "LAST_GOAL_SCORER": "Ultimo Marcatore",
    "ANYTIME_SCORER": "Marcatore",
    "TOTAL_GOALS": "Totale Goal",
    "TEAM_A_TOTAL_GOALS": "Goal Casa",
    "TEAM_B_TOTAL_GOALS": "Goal Trasferta",
    "TEAM_TOTAL_GOALS": "Goal Squadra",
    "ODD_OR_EVEN": "Pari/Dispari Goal",
    "WINNING_MARGIN": "Margine Vittoria",
    "NEXT_GOAL": "Prossimo Goal",
    "CLEAN_SHEET": "Clean Sheet",
    "WIN_TO_NIL": "Vince a Zero",
    "CORNER_ODDS": "Corner",
    "CORNER_MATCH_BET": "Corner Vincente",
    "TOTAL_CORNERS": "Totale Corner",
    "BOOKING_ODDS": "Cartellini",
    "TOTAL_BOOKINGS": "Totale Cartellini",
    "FIRST_HALF_GOALS_05": "Goal 1T O/U 0.5",
    "FIRST_HALF_GOALS_15": "Goal 1T O/U 1.5",
    "FIRST_HALF_GOALS_25": "Goal 1T O/U 2.5",
    "PENALTY_TAKEN": "Rigore",
    "TO_SCORE_BOTH_HALVES": "Segna in Entrambi i Tempi",
    "WIN_BOTH_HALVES": "Vince Entrambi i Tempi",
    "HIGHEST_SCORING_HALF": "Tempo con Piu Goal",
    "METHOD_OF_VICTORY": "Tipo di Vittoria",
    "SENDING_OFF": "Espulsione",
}


class PriceStreamListener(StreamListener):
    """Custom listener for processing streaming price updates."""

    def __init__(self, price_callback):
        super().__init__()
        self.price_callback = price_callback
        self.market_cache = {}

    def on_data(self, raw_data):
        """Called when new data arrives from stream."""
        try:
            if hasattr(raw_data, "data"):
                data = raw_data.data
            else:
                data = raw_data

            if isinstance(data, dict) and "mc" in data:
                for market_change in data["mc"]:
                    market_id = market_change.get("id")
                    if market_id and "rc" in market_change:
                        runners_data = []
                        for rc in market_change["rc"]:
                            runner_info = {
                                "selectionId": rc.get("id"),
                                "backPrices": rc.get("atb", []),
                                "layPrices": rc.get("atl", []),
                            }
                            runners_data.append(runner_info)

                        if self.price_callback:
                            self.price_callback(market_id, runners_data)
        except Exception as e:
            print(f"Stream data error: {e}")


class BetfairClient:
    def __init__(self, username, app_key, cert_pem, key_pem):
        # Aggressive cleaning of all string values to remove newlines/whitespace
        self.username = self._clean_string(username)
        self.app_key = self._clean_string(app_key)
        self.cert_pem = cert_pem.strip() if cert_pem else cert_pem
        self.key_pem = key_pem.strip() if key_pem else key_pem
        self.client = None
        self.temp_certs_dir = None
        self.stream = None
        self.stream_thread = None
        self.streaming_active = False
        self.price_callbacks = {}

        # --- HEDGE-FUND STABLE FIX ---
        self._cb = CircuitBreaker(max_failures=3, reset_timeout=30)
        # -----------------------------

        # Ensure certs are cleaned up if process exits unexpectedly
        atexit.register(self._cleanup_temp_files)

    @staticmethod
    def _clean_string(value):
        """Remove all whitespace, newlines, and control characters from a string."""
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        # Remove all whitespace and newlines
        return "".join(value.split())

    def _create_temp_cert_files(self):
        """Create temporary certificate directory for betfairlightweight."""
        self._cleanup_temp_files()  # Clean any existing first
        self.temp_certs_dir = tempfile.mkdtemp(prefix="betfair_certs_")

        cert_file_path = os.path.join(self.temp_certs_dir, "client-2048.crt")
        with open(cert_file_path, "w") as f:
            f.write(self.cert_pem)

        key_file_path = os.path.join(self.temp_certs_dir, "client-2048.key")
        with open(key_file_path, "w") as f:
            f.write(self.key_pem)

        return self.temp_certs_dir

    def _cleanup_temp_files(self):
        """Clean up temporary certificate directory securely."""
        if self.temp_certs_dir and os.path.exists(self.temp_certs_dir):
            try:
                shutil.rmtree(self.temp_certs_dir)
            except Exception as e:
                logger.error(f"Failed to cleanup temp certs: {e}")
            finally:
                self.temp_certs_dir = None

    def login(self, password):
        """
        Login to Betfair Italy using SSL certificate authentication.
        Uses locale="italy" for Italian Exchange endpoints.
        """
        certs_dir = self._create_temp_cert_files()

        try:
            self.client = betfairlightweight.APIClient(
                username=self.username,
                password=password,
                app_key=self.app_key,
                certs=certs_dir,
                locale="italy",
            )

            self.client.login()

            if not self.client.session_token:
                raise Exception("Nessun token ricevuto - verifica credenziali")

            return {
                "session_token": self.client.session_token,
                "expiry": (datetime.now() + timedelta(hours=8)).isoformat(),
            }
        except betfairlightweight.exceptions.LoginError as e:
            self._cleanup_temp_files()
            raise Exception(f"Credenziali errate o account bloccato: {str(e)}")
        except betfairlightweight.exceptions.CertsError as e:
            self._cleanup_temp_files()
            raise Exception(
                f"Errore certificato SSL - verifica che .crt e .key siano corretti: {str(e)}"
            )
        except betfairlightweight.exceptions.APIError as e:
            self._cleanup_temp_files()
            raise Exception(f"Errore API Betfair: {str(e)}")
        except Exception as e:
            self._cleanup_temp_files()
            error_msg = str(e)
            if "SSL" in error_msg.upper() or "CERTIFICATE" in error_msg.upper():
                raise Exception(
                    f"Errore SSL - il certificato potrebbe non essere valido: {error_msg}"
                )
            elif "timeout" in error_msg.lower():
                raise Exception("Timeout connessione - riprova")
            else:
                raise Exception(f"Login fallito: {error_msg}")

    def logout(self):
        """Logout from Betfair and stop streaming."""
        self.stop_streaming()
        if self.client:
            try:
                self.client.logout()
            except Exception:
                pass
        self._cleanup_temp_files()
        self.client = None

    @with_retry
    def get_account_funds(self):
        """Get account balance."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        account = self.client.account.get_account_funds()
        return {
            "available": account.available_to_bet_balance,
            "exposure": account.exposure,
            "total": account.available_to_bet_balance + abs(account.exposure),
        }

    @with_retry
    def get_football_events(self, include_inplay=True):
        """Get upcoming and in-play football events."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        time_filter = filters.time_range(
            from_=datetime.now(), to=datetime.now() + timedelta(days=2)
        )

        # Get upcoming events
        events = self.client.betting.list_events(
            filter=filters.market_filter(
                event_type_ids=[FOOTBALL_ID], market_start_time=time_filter
            )
        )

        # Also get in-play events
        inplay_events = []
        if include_inplay:
            try:
                inplay_events = self.client.betting.list_events(
                    filter=filters.market_filter(
                        event_type_ids=[FOOTBALL_ID], in_play_only=True
                    )
                )
            except Exception:
                pass

        # Combine events (avoid duplicates)
        event_ids = set()
        result = []

        # Add in-play events first (marked as LIVE)
        for event in inplay_events:
            event_ids.add(event.event.id)
            result.append(
                {
                    "id": event.event.id,
                    "name": event.event.name,
                    "countryCode": event.event.country_code,
                    "openDate": (
                        event.event.open_date.isoformat()
                        if event.event.open_date
                        else None
                    ),
                    "marketCount": event.market_count,
                     "inPlay": True,
                }
            )

        return result


# =============================================================================
# Dutching math — restored exports required by current callers.
#
# Required by:
#   controllers/dutching_controller.py: calculate_dutching_stakes,
#                                        calculate_mixed_dutching
#   app_modules/betting_module.py:       calculate_dutching_stakes,
#                                        format_currency, validate_selections
#   dutching_ui.py:                      calculate_ai_mixed_stakes,
#                                        calculate_dutching_stakes,
#                                        calculate_mixed_dutching,
#                                        validate_selections
#   pnl_engine.py:                       dynamic_cashout_single
#
# Issue #3 fix: _lay_dutching uses correct inverse-price weighting so that
# stake_i * price_i = constant, producing equal profit on every outcome
# rather than the flat-stake bug that was present in the lost commit.
# =============================================================================

from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, getcontext as _getcontext
from typing import Dict, List, Tuple

_getcontext().prec = 18

_MIN_STAKE = Decimal("0.10")
_STEP = Decimal("0.01")
_MAX_WIN = Decimal("10000.00")


def _d(value, default: str = "0") -> Decimal:
    """Safe Decimal coercion."""
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def _round_step(value: Decimal) -> Decimal:
    return (value / _STEP).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * _STEP


def _norm_price(value) -> Decimal:
    price = _d(value, "0")
    if price <= Decimal("1.01"):
        raise ValueError(f"Quota non valida (<= 1.01): {value}")
    return price


def _norm_side(selection: Dict, default: str = "BACK") -> str:
    side = (
        selection.get("effectiveType")
        or selection.get("side")
        or default
    )
    side = str(side).upper().strip()
    return side if side in ("BACK", "LAY") else "BACK"


def _apply_commission(net_profit: Decimal, commission: float) -> Decimal:
    """Betfair charges commission only on positive net profit."""
    if net_profit <= Decimal("0"):
        return net_profit
    mult = Decimal("1") - (_d(commission, "0") / Decimal("100"))
    return net_profit * mult


def format_currency(value) -> str:
    amount = _d(value, "0")
    return f"\u20ac{amount:.2f}"


def validate_selections(results: List[Dict], bet_type: str = "BACK") -> List[str]:
    errors: List[str] = []
    for r in results:
        runner = str(r.get("runnerName", str(r.get("selectionId", "?"))))
        stake = _d(r.get("stake", 0), "0")
        if stake < _MIN_STAKE:
            errors.append(f"{runner}: stake troppo basso ({stake:.2f} EUR)")
        price = _d(r.get("price", 0), "0")
        if price <= Decimal("1.01"):
            errors.append(f"{runner}: quota non valida ({price})")
        win = _d(r.get("profitIfWins", r.get("profit_if_win", 0)), "0")
        if win > _MAX_WIN:
            errors.append(f"{runner}: vincita massima superata ({win:.2f})")
    return errors


def _back_dutching(
    selections: List[Dict],
    total_stake: float,
    commission: float,
) -> Tuple[List[Dict], float, float]:
    """
    BACK dutching: proportional stake allocation by implied probability.
    stake_i = total_stake * (1/price_i) / sum(1/price_j)
    All winning scenarios produce the same net profit.
    """
    total_dec = _d(total_stake, "0")
    if total_dec <= 0:
        return [], 0.0, 0.0

    prices = [_norm_price(s.get("price", 0)) for s in selections]
    implied_probs = [Decimal("1") / p for p in prices]
    book_value = sum(implied_probs)
    if book_value <= 0:
        raise ValueError("Book value non valido")

    raw_stakes = []
    for prob in implied_probs:
        stake = _round_step((total_dec * prob) / book_value)
        raw_stakes.append(max(stake, _MIN_STAKE))

    total_actual = sum(raw_stakes)
    delta = _round_step(total_dec - total_actual)
    if raw_stakes:
        raw_stakes[-1] = _round_step(max(raw_stakes[-1] + delta, _MIN_STAKE))
    total_actual = sum(raw_stakes)

    results = []
    scenario_profits = []
    for i, sel in enumerate(selections):
        stake = raw_stakes[i]
        price = prices[i]
        gross_return = stake * price
        raw_profit = gross_return - total_actual
        net_profit = _round_step(_apply_commission(raw_profit, commission))
        scenario_profits.append(net_profit)
        results.append({
            "selectionId": sel["selectionId"],
            "runnerName": str(sel.get("runnerName", str(sel["selectionId"]))),
            "price": float(price),
            "stake": float(stake),
            "side": "BACK",
            "effectiveType": "BACK",
            "profitIfWins": float(net_profit),
            "grossReturn": float(_round_step(gross_return)),
        })

    avg_profit = (
        sum(scenario_profits) / Decimal(str(len(scenario_profits)))
        if scenario_profits else Decimal("0")
    )
    return results, float(_round_step(avg_profit)), float(book_value * 100)


def _lay_dutching(
    selections: List[Dict],
    total_stake: float,
    commission: float,
) -> Tuple[List[Dict], float, float]:
    """
    LAY dutching — FIXED (issue #3).

    Equal-profit condition for LAY dutching requires:
        stake_i * price_i = constant  C

    Derivation:
        profit when selection i loses = total_stake - stake_i * price_i
        For this to be equal across all i:  stake_i * price_i = C (constant)
        => stake_i = C / price_i
        => sum(C / price_i) = total_stake
        => C = total_stake / sum(1 / price_i)

    OLD BUG: every selection received the same flat stake
    (equal to rounded target profit), which only holds when all prices are
    identical. With mixed odds this produces unequal per-outcome profit.
    """
    total_dec = _d(total_stake, "0")
    if total_dec <= 0:
        return [], 0.0, 0.0

    prices = [_norm_price(s.get("price", 0)) for s in selections]
    inv_prices = [Decimal("1") / p for p in prices]
    sum_inv = sum(inv_prices)
    if sum_inv <= 0:
        raise ValueError("Book value non valido")

    # C = total_stake / sum(1/price_i)
    C = total_dec / sum_inv

    raw_stakes = []
    for price in prices:
        stake = _round_step(C / price)
        raw_stakes.append(max(stake, _MIN_STAKE))

    total_actual = sum(raw_stakes)
    delta = _round_step(total_dec - total_actual)
    if raw_stakes:
        raw_stakes[-1] = _round_step(max(raw_stakes[-1] + delta, _MIN_STAKE))
    total_actual = sum(raw_stakes)

    # profit when selection i loses = total_actual - price_i * stake_i
    constants = [p * s for p, s in zip(prices, raw_stakes)]
    raw_profits = [total_actual - c for c in constants]
    net_profits = [_round_step(_apply_commission(p, commission)) for p in raw_profits]

    results = []
    for i, sel in enumerate(selections):
        price = prices[i]
        stake = raw_stakes[i]
        liability = _round_step(stake * (price - Decimal("1")))
        results.append({
            "selectionId": sel["selectionId"],
            "runnerName": str(sel.get("runnerName", str(sel["selectionId"]))),
            "price": float(price),
            "stake": float(stake),
            "side": "LAY",
            "effectiveType": "LAY",
            "liability": float(liability),
            "profitIfWins": float(net_profits[i]),
        })

    avg_profit = (
        sum(net_profits) / Decimal(str(len(net_profits)))
        if net_profits else Decimal("0")
    )
    return results, float(_round_step(avg_profit)), float(sum_inv * 100)


def calculate_dutching_stakes(
    selections: List[Dict],
    total_stake: float,
    bet_type: str = "BACK",
    commission: float = 4.5,
    side: str = None,
    **kwargs,
) -> Tuple[List[Dict], float, float]:
    """
    Main dutching entry point.
    Returns (results, avg_profit, implied_book_pct).
    """
    if not selections:
        return [], 0.0, 0.0
    mode = str(side or bet_type or "BACK").upper().strip()
    if mode == "BACK":
        return _back_dutching(selections, total_stake, commission)
    if mode == "LAY":
        return _lay_dutching(selections, total_stake, commission)
    raise ValueError(f"bet_type/side non supportato: {mode}")


def calculate_dutching(
    selections,
    total_stake: float,
    bet_type: str = "BACK",
    commission: float = 4.5,
    side: str = None,
    **kwargs,
):
    """
    Legacy entry point.
    Accepts either a plain list of odds (e.g. [2.0, 3.0]) or the standard
    selections-dict format used by calculate_dutching_stakes.
    Returns a plain dict {"stakes": [...], "profits": [...]} for the legacy
    plain-odds path, or delegates to calculate_dutching_stakes for dict input.
    """
    # Legacy mode: plain list of odds
    if selections and isinstance(selections[0], (int, float)):
        odds_list = [float(o) for o in selections]
        for o in odds_list:
            if o <= 1.0:
                raise ValueError(f"Odds non valide: {o}")
        total = float(total_stake)
        inv_sum = sum(1.0 / o for o in odds_list)
        stakes = [round((total * (1.0 / o)) / inv_sum, 2) for o in odds_list]
        diff = round(total - sum(stakes), 2)
        if stakes:
            stakes[-1] = round(stakes[-1] + diff, 2)
        profits = [round(s * (o - 1) - (total - s), 2) for s, o in zip(stakes, odds_list)]
        return {"stakes": stakes, "profits": profits}

    return calculate_dutching_stakes(
        selections=selections,
        total_stake=total_stake,
        bet_type=bet_type,
        commission=commission,
        side=side,
        **kwargs,
    )


def calculate_mixed_dutching(
    selections: List[Dict],
    amount: float,
    commission: float = 4.5,
    **kwargs,
) -> Tuple[List[Dict], float, float]:
    """
    Mixed BACK/LAY dutching.
    Returns (results, min_scenario_profit, total_weight_pct).
    """
    if not selections:
        return [], 0.0, 0.0
    total_dec = _d(amount, "0")
    if total_dec <= 0:
        return [], 0.0, 0.0

    sides = []
    prices_list = []
    weights = []
    for sel in selections:
        side = _norm_side(sel, "BACK")
        price = _norm_price(sel.get("price", 0))
        sides.append(side)
        prices_list.append(price)
        if side == "BACK":
            weights.append(Decimal("1") / price)
        else:
            denom = price - Decimal("1")
            if denom <= 0:
                raise ValueError("Quota LAY non valida per mixed dutching")
            weights.append(Decimal("1") / denom)

    total_weight = sum(weights)
    if total_weight <= 0:
        raise ValueError("Peso totale non valido")

    raw_stakes = []
    for w in weights:
        stake = _round_step((total_dec * w) / total_weight)
        raw_stakes.append(max(stake, _MIN_STAKE))

    total_actual = sum(raw_stakes)
    delta = _round_step(total_dec - total_actual)
    if raw_stakes:
        raw_stakes[-1] = _round_step(max(raw_stakes[-1] + delta, _MIN_STAKE))

    results = []
    for i, sel in enumerate(selections):
        price = prices_list[i]
        stake = raw_stakes[i]
        side = sides[i]
        row = {
            "selectionId": sel["selectionId"],
            "runnerName": str(sel.get("runnerName", str(sel["selectionId"]))),
            "price": float(price),
            "stake": float(stake),
            "side": side,
            "effectiveType": side,
        }
        if side == "LAY":
            row["liability"] = float(_round_step(stake * (price - Decimal("1"))))
        results.append(row)

    scenario_profits = []
    for winner in results:
        winner_id = winner["selectionId"]
        pnl = Decimal("0")
        for r in results:
            p = _d(r["price"], "0")
            s = _d(r["stake"], "0")
            if r["effectiveType"] == "BACK":
                pnl += s * (p - Decimal("1")) if r["selectionId"] == winner_id else -s
            else:
                pnl -= s * (p - Decimal("1")) if r["selectionId"] == winner_id else Decimal("0")
                pnl += s if r["selectionId"] != winner_id else Decimal("0")
        scenario_profits.append(_apply_commission(pnl, commission))

    min_profit = min(scenario_profits) if scenario_profits else Decimal("0")
    return results, float(_round_step(min_profit)), float(total_weight * 100)


def calculate_ai_mixed_stakes(
    selections: List[Dict],
    amount: float = None,
    commission: float = 4.5,
    total_stake: float = None,
    **kwargs,
) -> Tuple[List[Dict], float, float]:
    if amount is None:
        amount = total_stake
    if amount is None:
        amount = kwargs.get("stake", 0.0)
    return calculate_mixed_dutching(selections, amount, commission)


def calculate_ai_mixed_dutching(
    selections: List[Dict],
    amount: float,
    commission: float = 4.5,
    **kwargs,
) -> Tuple[List[Dict], float, float]:
    return calculate_mixed_dutching(selections, amount, commission)


def dynamic_cashout_single(
    matched_stake: float = None,
    matched_price: float = None,
    current_price: float = None,
    commission: float = 4.5,
    **kwargs,
) -> dict:
    """
    Calculate the green-up (cashout) lay stake and resulting equal profit.

    Accepts both explicit parameters and legacy kwargs
    (back_stake, back_price, lay_price) used by pnl_engine.
    """
    if matched_stake is None:
        matched_stake = kwargs.get("back_stake", 0.0)
    if matched_price is None:
        matched_price = kwargs.get("back_price", 0.0)
    if current_price is None:
        current_price = kwargs.get("lay_price", 0.0)

    ms = _d(matched_stake, "0")
    mp = _d(matched_price, "0")
    cp = _d(current_price, "0")

    if ms <= Decimal("0") or mp <= Decimal("1.01") or cp <= Decimal("1.01"):
        return {"lay_stake": 0.0, "green_up": 0.0, "net_profit": 0.0}

    cashout_stake = _round_step((ms * mp) / cp)
    profit_win = ms * (mp - Decimal("1")) - cashout_stake * (cp - Decimal("1"))
    profit_lose = cashout_stake - ms
    green = _round_step((profit_win + profit_lose) / Decimal("2"))

    return {
        "lay_stake": float(cashout_stake),
        "green_up": float(green),
        "net_profit": float(green),
    }

    @with_retry
    def get_available_markets(self, event_id):
        """Get all available markets for an event (no type restriction)."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        # Fetch ALL markets without type restriction
        markets = self.client.betting.list_market_catalogue(
            filter=filters.market_filter(event_ids=[event_id]),
            market_projection=["MARKET_START_TIME", "MARKET_DESCRIPTION"],
            max_results=100,
        )

        market_ids = [m.market_id for m in markets]
        in_play_status = {}

        # FIX: API Limit is usually 40/50 per call. We must chunk.
        if market_ids:
            try:
                chunk_size = 40
                for i in range(0, len(market_ids), chunk_size):
                    chunk = market_ids[i : i + chunk_size]
                    market_books = self.client.betting.list_market_book(
                        market_ids=chunk
                    )
                    for book in market_books:
                        in_play_status[book.market_id] = (
                            book.inplay if hasattr(book, "inplay") else False
                        )
            except Exception as e:
                logger.error(f"Error fetching market_books in-play status: {e}")

        result = []
        for market in markets:
            market_type = market.market_type if hasattr(market, "market_type") else None
            display_name = MARKET_TYPES.get(market_type, market.market_name)
            is_inplay = in_play_status.get(market.market_id, False)

            result.append(
                {
                    "marketId": market.market_id,
                    "marketName": market.market_name,
                    "marketType": market_type,
                    "displayName": display_name,
                    "startTime": (
                        market.market_start_time.isoformat()
                        if market.market_start_time
                        else None
                    ),
                    "inPlay": is_inplay,
                }
            )

        return result

    @with_retry
    def get_market_with_prices(self, market_id):
        """Get a specific market with runner details and prices."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        markets = self.client.betting.list_market_catalogue(
            filter=filters.market_filter(market_ids=[market_id]),
            market_projection=["RUNNER_DESCRIPTION", "MARKET_START_TIME"],
            max_results=1,
        )

        if not markets:
            raise Exception("Mercato non trovato")

        market = markets[0]

        price_data = self.client.betting.list_market_book(
            market_ids=[market_id],
            price_projection=filters.price_projection(price_data=["EX_BEST_OFFERS"]),
        )

        if not price_data:
            raise Exception("Quote non disponibili")

        runners = []
        price_book = price_data[0]

        for runner in market.runners:
            runner_prices = None
            for pb_runner in price_book.runners:
                if pb_runner.selection_id == runner.selection_id:
                    runner_prices = pb_runner
                    break

            back_price = None
            lay_price = None
            back_size = None
            lay_size = None

            if runner_prices and runner_prices.ex:
                if runner_prices.ex.available_to_back:
                    back_price = runner_prices.ex.available_to_back[0].price
                    back_size = runner_prices.ex.available_to_back[0].size
                if runner_prices.ex.available_to_lay:
                    lay_price = runner_prices.ex.available_to_lay[0].price
                    lay_size = runner_prices.ex.available_to_lay[0].size

            runners.append(
                {
                    "selectionId": runner.selection_id,
                    "runnerName": runner.runner_name,
                    "sortPriority": runner.sort_priority,
                    "backPrice": back_price,
                    "layPrice": lay_price,
                    "backSize": back_size,
                    "laySize": lay_size,
                    "status": runner_prices.status if runner_prices else "ACTIVE",
                }
            )

        market_status = "OPEN"
        if hasattr(price_book, "status"):
            market_status = price_book.status

        is_inplay = False
        if hasattr(price_book, "inplay"):
            is_inplay = price_book.inplay

        return {
            "marketId": market_id,
            "marketName": market.market_name,
            "startTime": (
                market.market_start_time.isoformat()
                if market.market_start_time
                else None
            ),
            "runners": runners,
            "status": market_status,
            "inPlay": is_inplay,
        }

    def get_market_book(self, market_id):
        """Get current market prices (for refreshing best prices before placing bets)."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        price_data = self.client.betting.list_market_book(
            market_ids=[market_id],
            price_projection=filters.price_projection(price_data=["EX_BEST_OFFERS"]),
        )

        if not price_data:
            return None

        price_book = price_data[0]
        runners = []

        for pb_runner in price_book.runners:
            back_price = None
            lay_price = None

            if pb_runner.ex:
                if pb_runner.ex.available_to_back:
                    back_price = pb_runner.ex.available_to_back[0].price
                if pb_runner.ex.available_to_lay:
                    lay_price = pb_runner.ex.available_to_lay[0].price

            runners.append(
                {
                    "selectionId": pb_runner.selection_id,
                    "ex": {
                        "availableToBack": (
                            [{"price": back_price}] if back_price else []
                        ),
                        "availableToLay": [{"price": lay_price}] if lay_price else [],
                    },
                }
            )

        return {"runners": runners}

    def get_correct_score_market(self, event_id):
        """Get correct score market for an event (legacy method)."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        markets = self.client.betting.list_market_catalogue(
            filter=filters.market_filter(
                event_ids=[event_id], market_type_codes=["CORRECT_SCORE"]
            ),
            market_projection=["RUNNER_DESCRIPTION", "MARKET_START_TIME"],
            max_results=1,
        )

        if not markets:
            raise Exception("Mercato Risultato Esatto non trovato")

        return self.get_market_with_prices(markets[0].market_id)

    def start_streaming(self, market_ids, price_callback):
        """
        Start streaming price updates for specified markets.
        """
        if not self.client:
            raise Exception("Non connesso a Betfair")

        self.stop_streaming()

        try:
            self.stream = self.client.streaming.create_stream(
                listener=PriceStreamListener(price_callback)
            )

            market_filter = filters.streaming_market_filter(market_ids=market_ids)

            market_data_filter = filters.streaming_market_data_filter(
                fields=["EX_BEST_OFFERS", "EX_TRADED"]
            )

            self.stream.subscribe_to_markets(
                market_filter=market_filter, market_data_filter=market_data_filter
            )

            self.streaming_active = True
            self.stream_thread = threading.Thread(target=self._run_stream, daemon=True)
            self.stream_thread.start()

            return True

        except Exception as e:
            self.streaming_active = False
            raise Exception(f"Errore avvio streaming: {str(e)}")

    def _run_stream(self):
        """Run the stream in a background thread."""
        try:
            if self.stream:
                self.stream.start()
        except Exception as e:
            print(f"Stream error: {e}")
        finally:
            self.streaming_active = False

    def stop_streaming(self):
        """Stop the active stream."""
        self.streaming_active = False
        if self.stream:
            try:
                self.stream.stop()
            except Exception:
                pass
            self.stream = None
        self.stream_thread = None

    def is_streaming(self):
        """Check if streaming is active."""
        return self.streaming_active and self.stream is not None

    def place_bet(
        self, market_id, selection_id, side, price, size, persistence_type="LAPSE"
    ):
        """Place a single bet on Betfair."""
        instructions = [
            {"selectionId": selection_id, "side": side, "price": price, "size": size}
        ]
        return self.place_bets(market_id, instructions)

    def place_bets(self, market_id, instructions):
        """
        Place bets on Betfair. Protected by Circuit Breaker.
        Wrappa errori tecnici/transitori come TransientError.
        """
        if not self.client:
            raise Exception("Non connesso a Betfair")

        try:
            limit_order_factory = getattr(filters, "limit_order", None)
            place_instruction_factory = getattr(filters, "place_instruction", None)

            limit_orders = []
            for inst in instructions:
                if callable(limit_order_factory):
                    limit_orders.append(
                        limit_order_factory(
                            size=inst["size"],
                            price=inst["price"],
                            persistence_type="LAPSE",
                        )
                    )
                else:
                    limit_orders.append(
                        {
                            "size": inst["size"],
                            "price": inst["price"],
                            "persistence_type": "LAPSE",
                        }
                    )

            place_instructions = []
            for i, inst in enumerate(instructions):
                if callable(place_instruction_factory):
                    place_instructions.append(
                        place_instruction_factory(
                            selection_id=inst["selectionId"],
                            side=inst["side"],
                            order_type="LIMIT",
                            limit_order=limit_orders[i],
                        )
                    )
                else:
                    place_instructions.append(
                        {
                            "selection_id": inst["selectionId"],
                            "side": inst["side"],
                            "order_type": "LIMIT",
                            "limit_order": limit_orders[i],
                        }
                    )

            result = self._cb.call(
                self.client.betting.place_orders,
                market_id=market_id,
                instructions=place_instructions,
            )
        except TransientError:
            raise
        except Exception as e:
            raise TransientError(f"Errore Temporaneo: {e}") from e

        instruction_reports = (
            getattr(result, "instruction_reports", None)
            or getattr(result, "instructionReports", None)
            or []
        )

        reports = []
        for ir in instruction_reports:
            reports.append(
                {
                    "status": getattr(ir, "status", "UNKNOWN"),
                    "betId": getattr(ir, "bet_id", None) or getattr(ir, "betId", None),
                    "placedDate": (
                        ir.placed_date.isoformat()
                        if getattr(ir, "placed_date", None)
                        else None
                    ),
                    "averagePriceMatched": getattr(ir, "average_price_matched", None)
                    or getattr(ir, "averagePriceMatched", None),
                    "sizeMatched": getattr(ir, "size_matched", 0)
                    or getattr(ir, "sizeMatched", 0),
                }
            )

        return {
            "status": getattr(result, "status", "UNKNOWN"),
            "marketId": getattr(result, "market_id", None)
            or getattr(result, "marketId", market_id),
            "instructionReports": reports,
        }

    @with_retry
    def get_current_orders(self, market_ids=None):
        """Get current unmatched and partially matched orders."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        order_filter = {}
        if market_ids:
            order_filter["market_ids"] = market_ids

        orders = self.client.betting.list_current_orders(**order_filter)

        result = {"matched": [], "unmatched": [], "partiallyMatched": []}

        for order in orders.orders if orders.orders else []:
            order_data = {
                "betId": order.bet_id,
                "marketId": order.market_id,
                "selectionId": order.selection_id,
                "side": order.side,
                "price": order.price_size.price if order.price_size else None,
                "size": order.price_size.size if order.price_size else None,
                "sizeMatched": order.size_matched,
                "sizeRemaining": order.size_remaining,
                "averagePriceMatched": order.average_price_matched,
                "status": order.status,
                "placedDate": (
                    order.placed_date.isoformat() if order.placed_date else None
                ),
            }

            if order.size_remaining == 0 and order.size_matched > 0:
                result["matched"].append(order_data)
            elif order.size_remaining > 0 and order.size_matched > 0:
                result["partiallyMatched"].append(order_data)
            elif order.size_remaining > 0:
                result["unmatched"].append(order_data)

        return result

    def cancel_orders(self, market_id, bet_ids=None):
        """Cancel unmatched orders."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        try:
            instructions = []
            if bet_ids:
                for bet_id in bet_ids:
                    instructions.append(
                        betfairlightweight.filters.cancel_instruction(bet_id=bet_id)
                    )

            result = self._cb.call(
                self.client.betting.cancel_orders,
                market_id=market_id,
                instructions=instructions if instructions else None,
            )
        except TransientError:
            raise
        except Exception as e:
            raise TransientError(f"Errore Temporaneo: {e}") from e

        instruction_reports = (
            getattr(result, "instruction_reports", None)
            or getattr(result, "instructionReports", None)
            or []
        )

        reports = []
        for ir in instruction_reports:
            reports.append(
                {
                    "status": getattr(ir, "status", "UNKNOWN"),
                    "sizeCancelled": getattr(ir, "size_cancelled", 0)
                    or getattr(ir, "sizeCancelled", 0),
                }
            )

        return {
            "status": getattr(result, "status", "UNKNOWN"),
            "instructionReports": reports,
        }

    def get_markets(self, event_id):
        """Alias for get_available_markets - get all markets for an event."""
        return self.get_available_markets(event_id)

    def place_back_bet(self, market_id, selection_id, price, size):
        return self.place_bet(market_id, selection_id, "BACK", price, size)

    def place_lay_bet(self, market_id, selection_id, price, size):
        return self.place_bet(market_id, selection_id, "LAY", price, size)

    def replace_orders(self, market_id, bet_id, new_price):
        """Replace an existing order with a new price."""
        if not self.client:
            raise Exception("Non connesso a Betfair")

        try:
            instructions = [
                betfairlightweight.filters.replace_instruction(
                    bet_id=bet_id,
                    new_price=new_price,
                )
            ]

            result = self._cb.call(
                self.client.betting.replace_orders,
                market_id=market_id,
                instructions=instructions,
            )
        except TransientError:
            raise
        except Exception as e:
            raise TransientError(f"Errore Temporaneo: {e}") from e

        instruction_reports = (
            getattr(result, "instruction_reports", None)
            or getattr(result, "instructionReports", None)
            or []
        )

        reports = []
        for ir in instruction_reports:
            reports.append(
                {
                    "status": getattr(ir, "status", "UNKNOWN"),
                    "cancelInstructionReport": getattr(
                        ir, "cancel_instruction_report", None
                    ),
                    "placeInstructionReport": getattr(
                        ir, "place_instruction_report", None
                    ),
                }
            )

        return {
            "status": getattr(result, "status", "UNKNOWN"),
            "instructionReports": reports,
        }

    def cashout(self, market_id, selection_id, side, price, size):
        opposite_side = "LAY" if side == "BACK" else "BACK"
        return self.place_bet(market_id, selection_id, opposite_side, price, size)

    def calculate_cashout(
        self, original_stake, original_odds, current_odds, side="BACK"
    ):
        # FIX: Guard against division by zero
        if current_odds <= 1.0:
            return {
                "cashout_stake": 0.0,
                "profit_if_win": 0.0,
                "profit_if_lose": 0.0,
                "guaranteed_profit": 0.0,
            }

        if side == "BACK":
            potential_profit = original_stake * (original_odds - 1)

            # Guard against division by zero
            if current_odds - 1 <= 0:
                return {
                    "cashout_stake": 0.0,
                    "profit_if_win": 0.0,
                    "profit_if_lose": 0.0,
                    "guaranteed_profit": 0.0,
                }

            cashout_stake = potential_profit / (current_odds - 1)

            if current_odds < original_odds:
                guaranteed = original_stake - cashout_stake
            else:
                guaranteed = potential_profit - (cashout_stake * (current_odds - 1))

            return {
                "cashout_stake": round(cashout_stake, 2),
                "profit_if_win": round(
                    potential_profit - (cashout_stake * (current_odds - 1)), 2
                ),
                "profit_if_lose": round(cashout_stake - original_stake, 2),
                "guaranteed_profit": round(guaranteed, 2) if guaranteed > 0 else 0,
            }
        else:
            liability = original_stake * (original_odds - 1)
            potential_profit = original_stake

            # Guard against division by zero
            if current_odds - 1 <= 0:
                return {
                    "cashout_stake": 0.0,
                    "profit_if_win": 0.0,
                    "profit_if_lose": 0.0,
                    "guaranteed_profit": 0.0,
                }

            cashout_stake = liability / (current_odds - 1)

            return {
                "cashout_stake": round(cashout_stake, 2),
                "profit_if_win": round(cashout_stake - liability, 2),
                "profit_if_lose": round(potential_profit - cashout_stake, 2),
                "guaranteed_profit": 0,
            }

    def get_position(self, market_id, selection_id):
        if not self.client:
            raise Exception("Non connesso a Betfair")

        orders = self.get_current_orders(market_ids=[market_id])
        pnl = self.get_market_profit_and_loss([market_id])

        # FIX: Compute size-weighted average instead of overwriting
        position = {
            "market_id": market_id,
            "selection_id": selection_id,
            "back_stake": 0,
            "back_total_price": 0,  # For weighted average
            "lay_stake": 0,
            "lay_total_price": 0,  # For weighted average
            "net_position": 0,
            "profit_loss": 0,
        }

        for order_list in [orders["matched"], orders["partiallyMatched"]]:
            for order in order_list:
                if order["selectionId"] == selection_id:
                    size_matched = float(order.get("sizeMatched", 0) or 0)
                    avg_price = float(order.get("averagePriceMatched", 0) or 0)

                    if order["side"] == "BACK":
                        position["back_stake"] += size_matched
                        position["back_total_price"] += size_matched * avg_price
                    else:
                        position["lay_stake"] += size_matched
                        position["lay_total_price"] += size_matched * avg_price

        # Compute size-weighted average
        if position["back_stake"] > 0:
            position["back_avg_odds"] = (
                position["back_total_price"] / position["back_stake"]
            )
        else:
            position["back_avg_odds"] = 0

        if position["lay_stake"] > 0:
            position["lay_avg_odds"] = (
                position["lay_total_price"] / position["lay_stake"]
            )
        else:
            position["lay_avg_odds"] = 0

        position["net_position"] = position["back_stake"] - position["lay_stake"]

        # FIX: pnl[market_id] is a list, not a dict with "runners"
        if pnl and market_id in pnl:
            for runner_pnl in pnl[market_id]:
                if runner_pnl.get("selectionId") == selection_id:
                    position["profit_loss"] = runner_pnl.get("ifWin", 0)

        return position

    @with_retry
    def get_settled_bets(self, days=7):
        if not self.client:
            raise Exception("Non connesso a Betfair")

        from datetime import datetime, timedelta

        settled_from = datetime.utcnow() - timedelta(days=days)
        settled_to = datetime.utcnow()

        time_range = betfairlightweight.filters.time_range(
            from_=settled_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
            to=settled_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        result = self.client.betting.list_cleared_orders(
            bet_status="SETTLED", settled_date_range=time_range
        )

        bets = []
        for order in result.cleared_orders if result.cleared_orders else []:
            bets.append(
                {
                    "betId": order.bet_id,
                    "marketId": order.market_id,
                    "selectionId": order.selection_id,
                    "side": order.side,
                    "price": order.price_requested,
                    "priceMatched": order.price_matched,
                    "size": order.size_settled,
                    "profit": order.profit,
                    "settledDate": (
                        order.settled_date.isoformat() if order.settled_date else None
                    ),
                    "eventName": getattr(order, "event_type_id", "") or "",
                    "itemDescription": getattr(order, "item_description", None),
                }
            )

        return bets

    @with_retry
    def get_market_profit_and_loss(self, market_ids):
        if not self.client:
            raise Exception("Non connesso a Betfair")

        result = self.client.betting.list_market_profit_and_loss(
            market_ids=market_ids, include_settled_bets=False, include_bsp_bets=False
        )

        market_pnl = {}
        for market in result:
            runners_pnl = []
            for runner in market.profit_and_losses if market.profit_and_losses else []:
                runners_pnl.append(
                    {
                        "selectionId": runner.selection_id,
                        "ifWin": runner.if_win,
                        "ifLose": (
                            runner.if_lose if hasattr(runner, "if_lose") else None
                        ),
                    }
                )
            market_pnl[market.market_id] = runners_pnl

        return market_pnl

    def _get_fresh_price(self, market_id, selection_id, side):
        try:
            price_data = self.client.betting.list_market_book(
                market_ids=[market_id],
                price_projection=filters.price_projection(
                    price_data=["EX_BEST_OFFERS"]
                ),
            )

            if not price_data or not price_data[0].runners:
                return None

            for runner in price_data[0].runners:
                if runner.selection_id == selection_id:
                    if side == "BACK":
                        if runner.ex and runner.ex.available_to_back:
                            return runner.ex.available_to_back[0].price
                    else:  # LAY
                        if runner.ex and runner.ex.available_to_lay:
                            return runner.ex.available_to_lay[0].price
                    break
            return None
        except Exception:
            return None

    def _adjust_price_with_slippage(self, price, side, slippage_ticks=1):
        if price < 2:
            increment = 0.01
        elif price < 3:
            increment = 0.02
        elif price < 4:
            increment = 0.05
        elif price < 6:
            increment = 0.1
        elif price < 10:
            increment = 0.2
        elif price < 20:
            increment = 0.5
        elif price < 30:
            increment = 1.0
        elif price < 50:
            increment = 2.0
        elif price < 100:
            increment = 5.0
        else:
            increment = 10.0

        if side == "BACK":
            adjusted = price - (increment * slippage_ticks)
            return max(1.01, round(adjusted, 2))
        else:  # LAY
            adjusted = price + (increment * slippage_ticks)
            return min(1000, round(adjusted, 2))

    def execute_cashout(
        self,
        market_id,
        selection_id,
        cashout_side,
        cashout_stake,
        cashout_price,
        max_retries=3,
        slippage_ticks=1,
        use_fresh_price=True,
    ):
        if not self.client:
            raise Exception("Non connesso a Betfair")

        stake = max(1.0, round(cashout_stake, 2))

        last_error = None
        last_status = None

        for attempt in range(max_retries):
            try:
                if use_fresh_price:
                    fresh_price = self._get_fresh_price(
                        market_id, selection_id, cashout_side
                    )
                    if fresh_price:
                        price_to_use = fresh_price
                    else:
                        price_to_use = cashout_price
                else:
                    price_to_use = cashout_price

                if attempt > 0 and slippage_ticks > 0:
                    price_to_use = self._adjust_price_with_slippage(
                        price_to_use, cashout_side, slippage_ticks * attempt
                    )

                instructions = [
                    betfairlightweight.filters.place_instruction(
                        order_type="LIMIT",
                        selection_id=selection_id,
                        side=cashout_side,
                        limit_order=betfairlightweight.filters.limit_order(
                            size=stake, price=price_to_use, persistence_type="LAPSE"
                        ),
                    )
                ]

                # --- HEDGE-FUND STABLE FIX ---
                result = self._cb.call(
                    self.client.betting.place_orders,
                    market_id=market_id,
                    instructions=instructions,
                )
                # -----------------------------

                parsed = self._parse_cashout_result(result)

                if parsed.get("status") == "SUCCESS":
                    parsed["price_used"] = price_to_use
                    parsed["attempts"] = attempt + 1
                    return parsed

                last_status = parsed.get("status")
                error_code = parsed.get("error_code", "")

                permanent_errors = [
                    "MARKET_SUSPENDED",
                    "MARKET_NOT_OPEN_FOR_BETTING",
                    "INSUFFICIENT_FUNDS",
                    "INVALID_ACCOUNT_STATE",
                ]
                if error_code in permanent_errors:
                    return parsed

                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue

                return parsed

            except Exception as e:
                last_error = str(e)
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue

        return {
            "status": "ERROR",
            "error": last_error
            or f"Fallito dopo {max_retries} tentativi. Ultimo stato: {last_status}",
            "betId": None,
            "sizeMatched": 0,
            "averagePriceMatched": None,
            "attempts": max_retries,
        }

    def _parse_cashout_result(self, result):
        try:
            if isinstance(result, list) and len(result) > 0:
                result = result[0]

            instruction_reports = None
            for attr_name in ["instruction_reports", "instructionReports"]:
                try:
                    instruction_reports = getattr(result, attr_name, None)
                    if instruction_reports:
                        break
                except Exception:
                    pass

            if not instruction_reports and hasattr(result, "__getitem__"):
                try:
                    instruction_reports = result.get(
                        "instructionReports"
                    ) or result.get("instruction_reports")
                except Exception:
                    pass

            bet_id = None
            size_matched = 0
            avg_price = None
            error_code = None
            error_msg = None
            status = getattr(result, "status", None) or "UNKNOWN"

            for ec_attr in ["error_code", "errorCode"]:
                error_code = getattr(result, ec_attr, None)
                if error_code:
                    break

            if instruction_reports and len(instruction_reports) > 0:
                ir = instruction_reports[0]
                for bid_attr in ["bet_id", "betId"]:
                    bet_id = getattr(ir, bid_attr, None)
                    if bet_id:
                        break
                for sm_attr in ["size_matched", "sizeMatched"]:
                    size_matched = getattr(ir, sm_attr, 0)
                    if size_matched:
                        break
                for ap_attr in ["average_price_matched", "averagePriceMatched"]:
                    avg_price = getattr(ir, ap_attr, None)
                    if avg_price:
                        break

                if not error_code:
                    for ec_attr in ["error_code", "errorCode"]:
                        error_code = getattr(ir, ec_attr, None)
                        if error_code:
                            break

            return {
                "status": status,
                "betId": bet_id,
                "sizeMatched": size_matched or 0,
                "averagePriceMatched": avg_price,
                "error_code": error_code,
                "error": error_msg,
            }
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
                "error_code": None,
                "betId": None,
                "sizeMatched": 0,
                "averagePriceMatched": None,
            }

    @with_retry
    def get_live_events(self, event_type_id="1"):
        if not self.client:
            raise Exception("Non connesso a Betfair")

        inplay_events = self.client.betting.list_events(
            filter=filters.market_filter(
                event_type_ids=[event_type_id], in_play_only=True
            )
        )

        result = []
        for event in inplay_events:
            result.append(
                {
                    "id": event.event.id,
                    "name": event.event.name,
                    "countryCode": event.event.country_code,
                    "openDate": (
                        event.event.open_date.isoformat()
                        if event.event.open_date
                        else None
                    ),
                    "marketCount": event.market_count,
                    "inPlay": True,
                }
            )

        return result

    def get_live_events_only(self):
        return self.get_live_events(FOOTBALL_ID)

    @with_retry
    def get_live_markets(self, event_id=None):
        if not self.client:
            raise Exception("Non connesso a Betfair")

        market_filter_params = {"event_type_ids": [FOOTBALL_ID], "in_play_only": True}
        if event_id:
            market_filter_params["event_ids"] = [event_id]

        markets = self.client.betting.list_market_catalogue(
            filter=filters.market_filter(**market_filter_params),
            market_projection=["RUNNER_DESCRIPTION", "MARKET_START_TIME", "EVENT"],
            max_results=100,
        )

        result = []
        for market in markets:
            market_type = market.market_type if hasattr(market, "market_type") else None
            display_name = MARKET_TYPES.get(market_type, market.market_name)
            event_name = (
                market.event.name if hasattr(market, "event") and market.event else ""
            )

            result.append(
                {
                    "marketId": market.market_id,
                    "marketName": market.market_name,
                    "marketType": market_type,
                    "displayName": display_name,
                    "eventId": (
                        market.event.id
                        if hasattr(market, "event") and market.event
                        else None
                    ),
                    "eventName": event_name,
                    "startTime": (
                        market.market_start_time.isoformat()
                        if market.market_start_time
                        else None
                    ),
                    "inPlay": True,
                }
            )

        return result