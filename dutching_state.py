"""
Dutching State Manager - Gestisce stato delle selezioni per UI reattiva.
Separa logica di stato dalla UI per architettura pulita.
"""

import threading
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional


def _tick_step(price: float) -> float:
    """Return the Betfair tick step size for a given price."""
    if price < 2.0:
        return 0.01
    if price < 3.0:
        return 0.02
    if price < 4.0:
        return 0.05
    if price < 6.0:
        return 0.10
    if price < 10.0:
        return 0.20
    if price < 20.0:
        return 0.50
    if price < 30.0:
        return 1.00
    if price < 50.0:
        return 2.00
    if price < 100.0:
        return 5.00
    return 10.0


def _apply_tick_offset(base_odds: float, offset: int) -> float:
    """
    Apply an integer tick offset to a base price using the Betfair tick ladder.
    Positive offset moves the price up; negative moves it down.
    """
    price = _snap_to_betfair_tick(max(1.01, float(base_odds or 1.01)))
    steps = int(offset)
    direction = 1 if steps > 0 else -1
    for _ in range(abs(steps)):
        step = _tick_step(price)
        price = round(price + direction * step, 10)
        price = max(1.01, price)
    return round(price, 2)


def _snap_to_betfair_tick(price: float) -> float:
    """
    Snap a price to the nearest valid Betfair tick ladder step.

    Betfair tick ladder:
      1.01 – 2.00 : 0.01 increments
      2.00 – 3.00 : 0.02 increments
      3.00 – 4.00 : 0.05 increments
      4.00 – 6.00 : 0.10 increments
      6.00 – 10.00: 0.20 increments
     10.00 – 20.00: 0.50 increments
     20.00 – 30.00: 1.00 increments
     30.00 – 50.00: 2.00 increments
     50.00 – 100.0: 5.00 increments
    100.00 – 1000 : 10.0 increments
    """
    if price <= 1.0:
        return 1.01
    if price < 2.0:
        return round(round(price / 0.01) * 0.01, 2)
    if price < 3.0:
        return round(round(price / 0.02) * 0.02, 2)
    if price < 4.0:
        return round(round(price / 0.05) * 0.05, 2)
    if price < 6.0:
        return round(round(price / 0.10) * 0.10, 2)
    if price < 10.0:
        return round(round(price / 0.20) * 0.20, 2)
    if price < 20.0:
        return round(round(price / 0.50) * 0.50, 2)
    if price < 30.0:
        return round(round(price / 1.00) * 1.00, 2)
    if price < 50.0:
        return round(round(price / 2.00) * 2.00, 2)
    if price < 100.0:
        return round(round(price / 5.00) * 5.00, 2)
    return round(round(price / 10.0) * 10.0, 2)


class DutchingMode(Enum):
    STAKE_AVAILABLE = "stake"
    REQUIRED_PROFIT = "profit"


@dataclass
class RunnerState:
    """Stato di un singolo runner nel dutching."""

    selection_id: int
    runner_name: str
    odds: float
    included: bool = True
    swap: bool = False  # True = LAY, False = BACK
    offset: int = 0  # Tick offset per quota
    stake: float = 0.0
    profit_if_wins: float = 0.0
    liability: float = 0.0

    @property
    def effective_odds(self) -> float:
        """
        Quota effettiva con offset applicato sul tick ladder Betfair.

        FIX #9: the old code used a flat 0.01 increment per tick regardless
        of price range.  Betfair's tick ladder has variable step sizes
        (e.g. 0.02 between 2.0–3.0, 0.05 between 3.0–4.0, ...).
        We snap the base price to the ladder then step tick-by-tick.
        """
        return _apply_tick_offset(self.odds, self.offset)

    @property
    def effective_type(self) -> str:
        """BACK o LAY basato su swap."""
        return "LAY" if self.swap else "BACK"

    def to_dict(self) -> Dict:
        """Converti a dizionario per engine."""
        return {
            "selectionId": self.selection_id,
            "runnerName": self.runner_name,
            "price": self.effective_odds,
            "effectiveType": self.effective_type,
            "stake": self.stake,
        }


class DutchingState:
    """
    Gestisce lo stato completo della finestra Dutching.
    Thread-safe e con callback per aggiornamenti UI.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._runners: List[RunnerState] = []
        self._mode: DutchingMode = DutchingMode.STAKE_AVAILABLE
        self._total_stake: float = 100.0
        self._target_profit: float = 10.0
        self._auto_ratio: bool = True
        self._global_offset: int = 0
        self._live_odds: bool = True
        self._commission: float = 4.5

        # Market info
        self._market_id: str = ""
        self._market_name: str = ""
        self._market_type: str = ""
        self._market_status: str = "OPEN"
        self._event_name: str = ""
        self._start_time: str = ""

        # Simulation mode
        self._simulation_mode: bool = False

        # Callback per aggiornamento UI
        self._on_change: Optional[Callable] = None

    def set_callback(self, callback: Callable):
        """Imposta callback chiamato ad ogni modifica stato."""
        self._on_change = callback

    def _notify(self):
        """Notifica UI di cambiamento."""
        if self._on_change:
            self._on_change()

    # === MARKET INFO ===

    def set_market_info(
        self,
        market_id: str,
        market_name: str,
        event_name: str,
        start_time: str,
        status: str = "OPEN",
    ):
        """Imposta info mercato."""
        with self._lock:
            self._market_id = market_id
            self._market_name = market_name
            self._event_name = event_name
            self._start_time = start_time
            self._market_status = status
        self._notify()

    @property
    def market_id(self) -> str:
        with self._lock:
            return self._market_id

    @property
    def market_display(self) -> str:
        """Stringa display mercato."""
        with self._lock:
            return f"{self._event_name} | {self._start_time} | {self._market_name}"

    @property
    def market_status(self) -> str:
        with self._lock:
            return self._market_status

    @property
    def market_type(self) -> str:
        with self._lock:
            return self._market_type

    @market_type.setter
    def market_type(self, value: str):
        with self._lock:
            self._market_type = value

    @property
    def simulation_mode(self) -> bool:
        with self._lock:
            return self._simulation_mode

    @simulation_mode.setter
    def simulation_mode(self, value: bool):
        with self._lock:
            self._simulation_mode = value

    # === RUNNERS ===

    def load_runners(self, runners: List[Dict]):
        """
        Carica runners dal mercato.
        runners: [{'selectionId': int, 'runnerName': str, 'price': float}]
        """
        with self._lock:
            self._runners = []
            for r in runners:
                state = RunnerState(
                    selection_id=r["selectionId"],
                    runner_name=r["runnerName"],
                    odds=r.get("price", 0) or 0,
                    included=r.get("price", 0) > 1.0,  # Auto-exclude senza quota
                )
                self._runners.append(state)
        self._notify()

    def update_odds(self, selection_id: int, new_odds: float):
        """Aggiorna quota singolo runner."""
        with self._lock:
            for r in self._runners:
                if r.selection_id == selection_id:
                    r.odds = new_odds
                    break
        self._notify()

    def update_all_odds(self, odds_map: Dict[int, float]):
        """Aggiorna quote multiple."""
        with self._lock:
            for r in self._runners:
                if r.selection_id in odds_map:
                    r.odds = odds_map[r.selection_id]
        self._notify()

    def toggle_included(self, selection_id: int):
        """Toggle inclusione runner."""
        with self._lock:
            for r in self._runners:
                if r.selection_id == selection_id:
                    r.included = not r.included
                    if not r.included:
                        r.stake = 0
                        r.profit_if_wins = 0
                    break
        self._notify()

    def toggle_swap(self, selection_id: int):
        """Toggle BACK/LAY per runner."""
        with self._lock:
            for r in self._runners:
                if r.selection_id == selection_id:
                    r.swap = not r.swap
                    break
        self._notify()

    def set_offset(self, selection_id: int, offset: int):
        """Imposta offset quota per runner."""
        with self._lock:
            for r in self._runners:
                if r.selection_id == selection_id:
                    r.offset = offset
                    break
        self._notify()

    def set_odds(self, selection_id: int, odds: float):
        """Imposta quota manuale per runner."""
        with self._lock:
            for r in self._runners:
                if r.selection_id == selection_id:
                    r.odds = odds
                    break
        self._notify()

    def select_all(self):
        """Seleziona tutti i runner con quota valida."""
        with self._lock:
            for r in self._runners:
                if r.odds > 1.0:
                    r.included = True
        self._notify()

    def select_none(self):
        """Deseleziona tutti."""
        with self._lock:
            for r in self._runners:
                r.included = False
                r.stake = 0
                r.profit_if_wins = 0
        self._notify()

    def swap_all(self):
        """Inverte BACK/LAY per tutti."""
        with self._lock:
            for r in self._runners:
                r.swap = not r.swap
        self._notify()

    @property
    def runners(self) -> List[RunnerState]:
        """Lista runner (copia)."""
        with self._lock:
            return list(self._runners)

    @property
    def included_runners(self) -> List[RunnerState]:
        """Solo runner inclusi."""
        with self._lock:
            return [r for r in self._runners if r.included]

    # === DUTCHING MODE ===

    @property
    def mode(self) -> DutchingMode:
        with self._lock:
            return self._mode

    @mode.setter
    def mode(self, value: DutchingMode):
        with self._lock:
            self._mode = value
        self._notify()

    @property
    def total_stake(self) -> float:
        with self._lock:
            return self._total_stake

    @total_stake.setter
    def total_stake(self, value: float):
        with self._lock:
            self._total_stake = max(0, value)
        self._notify()

    @property
    def target_profit(self) -> float:
        with self._lock:
            return self._target_profit

    @target_profit.setter
    def target_profit(self, value: float):
        with self._lock:
            self._target_profit = max(0, value)
        self._notify()

    @property
    def auto_ratio(self) -> bool:
        with self._lock:
            return self._auto_ratio

    @auto_ratio.setter
    def auto_ratio(self, value: bool):
        with self._lock:
            self._auto_ratio = value
        self._notify()

    @property
    def global_offset(self) -> int:
        with self._lock:
            return self._global_offset

    @global_offset.setter
    def global_offset(self, value: int):
        with self._lock:
            self._global_offset = value
            for r in self._runners:
                r.offset = value
        self._notify()

    @property
    def live_odds(self) -> bool:
        with self._lock:
            return self._live_odds

    @live_odds.setter
    def live_odds(self, value: bool):
        with self._lock:
            self._live_odds = value
        self._notify()

    @property
    def commission(self) -> float:
        with self._lock:
            return self._commission

    @commission.setter
    def commission(self, value: float):
        with self._lock:
            self._commission = value
        self._notify()

    # === CALCULATIONS ===

    def get_book_value(self) -> float:
        """Calcola book value (somma probabilità implicite)."""
        included = self.included_runners
        if not included:
            return 0
        return sum(1 / r.effective_odds for r in included if r.effective_odds > 1) * 100

    def get_total_stake(self) -> float:
        """Somma stake calcolate."""
        return sum(r.stake for r in self._runners)

    def get_selections_for_engine(self) -> List[Dict]:
        """Ritorna selezioni formattate per dutching engine."""
        return [r.to_dict() for r in self.included_runners]

    def apply_calculation_results(self, results: List[Dict]):
        """Applica risultati calcolo alle righe."""
        with self._lock:
            results_map = {r["selectionId"]: r for r in results}
            for runner in self._runners:
                if runner.selection_id in results_map:
                    res = results_map[runner.selection_id]
                    runner.stake = res.get("stake", 0)
                    runner.profit_if_wins = res.get("profitIfWins", 0)
                    runner.liability = res.get("liability", 0)
                elif not runner.included:
                    runner.stake = 0
                    runner.profit_if_wins = -self.get_total_stake()
        # Non notifica - chiamante gestisce refresh UI

    def get_orders_to_place(self) -> List[Dict]:
        """Ritorna ordini pronti per placement."""
        orders = []
        for r in self.included_runners:
            if r.stake > 0:
                orders.append(
                    {
                        "selectionId": r.selection_id,
                        "runnerName": r.runner_name,
                        "side": r.effective_type,
                        "price": r.effective_odds,
                        "size": r.stake,
                    }
                )
        return orders

