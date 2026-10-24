"""Microstake saga: policy, coordinator and native executor (SIM e LIVE, stesso motore).

Ruolo reale (rilievo Codex #512 su 034af59, la vecchia docstring lo negava): questo modulo
E' nel percorso di esecuzione. `core/microstake_sim_port.py` collega la saga al `SimulationBroker` di
produzione e `core/microstake_live_port.py` (PF-MICROSTAKE-OWNER-FINAL-RECTIFICATION-01) al `BetfairClient`
reale; `TradingEngine` la raggiunge per ogni invio sotto il minimo exchange (`core.stake_precision.
EXCHANGE_MIN_ORDER`, riferimento owner 1 EUR). Una policy LIVE (`live_adapter=True`) e' accettata solo sopra una
porta che si dichiara LIVE (`is_live_port is True`): ogni altra combinazione e' BLOCKED.

Ricostruisce la saga PLACE -> riduzione di size -> REPLACE dietro una policy esplicita;
`StatefulExchangeSimulator` e `JsonSagaStore` restano l'exchange e lo store dei test offline.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from core.stake_precision import EXCHANGE_MIN_ORDER


D = Decimal


class Side(str, Enum):
    BACK = "BACK"
    LAY = "LAY"


class Phase(str, Enum):
    PREPARED = "PREPARED"
    STUB_ACCEPTED = "STUB_ACCEPTED"
    REDUCED = "REDUCED"
    ACTIVE = "ACTIVE"
    AMBIGUOUS = "AMBIGUOUS"
    ROLLBACK_PENDING = "ROLLBACK_PENDING"
    ROLLED_BACK = "ROLLED_BACK"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"


class Fault(str, Enum):
    MATCH_ON_PLACE = "MATCH_ON_PLACE"
    MATCH_BEFORE_CANCEL = "MATCH_BEFORE_CANCEL"
    LOST_PLACE_RESPONSE = "LOST_PLACE_RESPONSE"
    LOST_CANCEL_RESPONSE = "LOST_CANCEL_RESPONSE"
    LOST_REPLACE_RESPONSE = "LOST_REPLACE_RESPONSE"
    TIMEOUT_PLACE = "TIMEOUT_PLACE"
    FAIL_REPLACE = "FAIL_REPLACE"
    SUSPEND = "SUSPEND"


class ExecutionKind(str, Enum):
    """Automatic route selected by the shared smart-execution preflight."""

    DIRECT = "DIRECT"
    MICROSTAKE = "MICROSTAKE"
    BLOCKED = "BLOCKED"


class LostResponse(RuntimeError):
    """Esito ignoto dopo un invio. `bet_id`: identita' gia' nota dell'ordine creato (es. replace accettato con la
    lettura successiva fallita), da persistire perche' il recovery lo ritrovi per id (rilievo Codex P2 su #561)."""

    def __init__(self, message: str = "", *, bet_id: str = "") -> None:
        super().__init__(message)
        self.bet_id = str(bet_id or "")


@dataclass(frozen=True)
class MicrostakePolicy:
    """`exchange_stub_size` = importo TEMPORANEO dello stub: il minimo exchange (fonte unica, 1 EUR), cioe' il
    piu' piccolo piazzamento che l'exchange accetta. Non e' un minimo di Pickfair: e' riservato come esposizione
    reale (`order_exposure`) entro i fondi disponibili e rilasciato quando la saga si chiude.
    `live_adapter` abilita la saga sull'exchange reale, ma solo sopra una porta LIVE (`is_live_port`)."""
    simulation_only: bool = True
    minimum_economic_stake: Decimal = D("0.10")
    exchange_stub_size: Decimal = EXCHANGE_MIN_ORDER
    max_transient_exposure: Decimal = D("0")
    commission_rate: Decimal = D("0.045")
    live_adapter: bool = False


@dataclass(frozen=True)
class MicrostakeRequest:
    market_id: str
    selection_id: int
    side: Side
    economic_stake: Decimal
    target_price: Decimal
    customer_ref: str


@dataclass
class SimOrder:
    bet_id: str
    market_id: str
    selection_id: int
    side: Side
    price: Decimal
    size: Decimal
    matched: Decimal
    remaining: Decimal
    customer_ref: str


@dataclass(frozen=True)
class MicrostakeResult:
    phase: Phase
    reason: str = ""
    bet_id: str = ""
    matched_size: Decimal = D("0")
    final_unmatched_size: Decimal = D("0")
    residual_exposure: Decimal = D("0")
    max_transient_exposure: Decimal = D("0")
    reservation_held: bool = False
    commission_rate: Decimal = D("0.045")


@dataclass(frozen=True)
class SmartExecutionPlan:
    kind: ExecutionKind
    economic_size: Decimal
    api_size: Decimal
    api_price: Decimal
    transient_exposure: Decimal
    reason: str = ""


class SmartExecutionEngine:
    """Pure, automatic route planner shared by SIM and future LIVE adapters.

    This is deliberately not a feature flag.  The requested economic amount
    alone selects DIRECT or MICROSTAKE.  A microstake plan is emitted only
    after the intermediate API order has passed book-crossing and transient
    exposure checks; otherwise the only valid result is BLOCKED.
    """

    def __init__(self, *, exchange_minimum: Decimal = EXCHANGE_MIN_ORDER) -> None:
        minimum = self._decimal(exchange_minimum)
        if minimum <= 0:
            raise ValueError("exchange_minimum must be positive")
        self.exchange_minimum = minimum

    @staticmethod
    def _decimal(value: Any) -> Decimal:
        try:
            result = value if isinstance(value, Decimal) else D(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("invalid monetary value") from exc
        if not result.is_finite():
            raise ValueError("non-finite monetary value")
        return result

    def plan(
        self,
        request: MicrostakeRequest,
        *,
        passive_stub_price: Optional[Decimal],
        best_counterparty_price: Optional[Decimal],
        transient_exposure_cap: Decimal,
    ) -> SmartExecutionPlan:
        economic = self._decimal(request.economic_stake)
        target = self._decimal(request.target_price)
        cap = self._decimal(transient_exposure_cap)
        if economic <= 0 or target < 1 or cap < 0:
            return SmartExecutionPlan(
                ExecutionKind.BLOCKED, economic, D("0"), target, D("0"),
                "invalid_economics",
            )
        if economic >= self.exchange_minimum:
            exposure = order_exposure(request.side, economic, target)
            return SmartExecutionPlan(
                ExecutionKind.DIRECT, economic, economic, target, exposure,
            )
        if passive_stub_price is None:
            return SmartExecutionPlan(
                ExecutionKind.BLOCKED, economic, D("0"), target, D("0"),
                "passive_stub_price_unverified",
            )

        stub = self._decimal(passive_stub_price)
        if stub < 1:
            return SmartExecutionPlan(
                ExecutionKind.BLOCKED, economic, D("0"), stub, D("0"),
                "invalid_stub_price",
            )
        if best_counterparty_price is not None:
            best = self._decimal(best_counterparty_price)
            crosses = (
                request.side is Side.BACK and best >= stub
            ) or (
                request.side is Side.LAY and best <= stub
            )
            if crosses:
                transient = order_exposure(
                    request.side, self.exchange_minimum, stub
                )
                return SmartExecutionPlan(
                    ExecutionKind.BLOCKED, economic, self.exchange_minimum,
                    stub, transient, "stub_price_would_cross_book",
                )

        transient = order_exposure(request.side, self.exchange_minimum, stub)
        if transient > cap:
            return SmartExecutionPlan(
                ExecutionKind.BLOCKED, economic, self.exchange_minimum, stub,
                transient, "transient_exposure_cap",
            )
        return SmartExecutionPlan(
            ExecutionKind.MICROSTAKE, economic, self.exchange_minimum, stub,
            transient,
        )


def order_exposure(side: Side, size: Decimal, price: Decimal) -> Decimal:
    if size < 0 or price < 1:
        raise ValueError("invalid order economics")
    return size if side is Side.BACK else size * (price - D("1"))


def market_commission(net_market_profit: Decimal, rate: Decimal = D("0.045")) -> Decimal:
    """Italian configured commission: positive net market profit only."""
    return max(D("0"), net_market_profit) * rate


class JsonSagaStore:
    """Small crash-recoverable store used only by this isolated prototype."""

    def __init__(self, path: Path | str):
        self.path = Path(path)

    def _all(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, ref: str, **values: Any) -> None:
        data = self._all()
        current = data.get(ref, {})
        for key, value in values.items():
            if isinstance(value, Enum):
                value = value.value
            if isinstance(value, Decimal):
                value = str(value)
            current[key] = value
        data[ref] = current
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, sort_keys=True, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)

    def load(self, ref: str) -> Optional[dict[str, Any]]:
        return self._all().get(ref)


class StatefulExchangeSimulator:
    """Deterministic exchange model with persistent orders and fault hooks."""

    def __init__(
        self,
        balance: Decimal = D("100"),
        *,
        hide_reconciliation: bool = False,
        _state: Optional[dict[str, Any]] = None,
    ):
        self._state = _state or {
            "balance": balance, "orders": {}, "next_id": 1,
            "faults": [], "log": [], "suspended": False, "books": {},
            "lock": threading.RLock(), "reservations": {},
        }
        self.hide_reconciliation = hide_reconciliation

    @property
    def operation_log(self):
        return self._state["log"]

    def restart(self) -> "StatefulExchangeSimulator":
        return StatefulExchangeSimulator(
            hide_reconciliation=self.hide_reconciliation, _state=self._state
        )

    def queue_fault(self, fault: Fault, value: Optional[Decimal] = None) -> None:
        self._state["faults"].append((fault, value))

    def set_liquidity(self, market_id: str, selection_id: int, side: Side,
                      price: Decimal, size: Decimal) -> None:
        """Set counterparty liquidity available to the submitted side."""
        self._state["books"][(market_id, selection_id, side)] = [price, size]

    def total_exposure(self) -> Decimal:
        return sum(
            (order_exposure(o.side, o.size, o.price) for o in self._state["orders"].values()),
            D("0"),
        )

    def reserve(self, customer_ref: str, exposure: Decimal, cap: Decimal) -> bool:
        """Atomically reserve transient risk across concurrent tables."""
        with self._state["lock"]:
            reservations = self._state["reservations"]
            if customer_ref in reservations:
                return True
            if sum(reservations.values(), D("0")) + exposure > cap:
                return False
            reservations[customer_ref] = exposure
            return True

    def release_reservation(self, customer_ref: str) -> None:
        with self._state["lock"]:
            self._state["reservations"].pop(customer_ref, None)

    def has_reservation(self, customer_ref: str) -> bool:
        with self._state["lock"]:
            return customer_ref in self._state["reservations"]

    def _take(self, *faults: Fault):
        for i, item in enumerate(self._state["faults"]):
            if item[0] in faults:
                return self._state["faults"].pop(i)
        return None

    def seed_order(self, **kwargs) -> str:
        return self.place(**kwargs).bet_id

    def order(self, bet_id: str) -> SimOrder:
        return self._state["orders"][bet_id]

    def find_by_ref(self, customer_ref: str) -> Optional[SimOrder]:
        if self.hide_reconciliation:
            return None
        matches = [o for o in self._state["orders"].values() if o.customer_ref == customer_ref]
        if len(matches) > 1:
            raise RuntimeError("duplicate customer reference")
        return matches[0] if matches else None

    def place(self, *, market_id: str, selection_id: int, side: Side,
              size: Decimal, price: Decimal, customer_ref: str) -> SimOrder:
        with self._state["lock"]:
            if self._state["suspended"] or self._take(Fault.SUSPEND):
                raise RuntimeError("MARKET_SUSPENDED")
            if self._take(Fault.TIMEOUT_PLACE):
                raise TimeoutError("place timeout before acceptance")
            if any(o.customer_ref == customer_ref for o in self._state["orders"].values()):
                raise RuntimeError("DUPLICATE_CUSTOMER_REF")
            bet_id = f"SIM-{self._state['next_id']}"
            self._state["next_id"] += 1
            match_fault = self._take(Fault.MATCH_ON_PLACE)
            matched = min(size, match_fault[1]) if match_fault else D("0")
            execution_price = price
            book = self._state["books"].get((market_id, selection_id, side))
            if book:
                book_price, liquidity = book
                crosses = (side is Side.BACK and book_price >= price) or (
                    side is Side.LAY and book_price <= price
                )
                if crosses:
                    book_match = min(size - matched, liquidity)
                    matched += book_match
                    book[1] -= book_match
                    execution_price = book_price
            order = SimOrder(bet_id, market_id, selection_id, side, execution_price,
                             size, matched, size - matched, customer_ref)
            exposure = order_exposure(side, size, execution_price)
            if self.total_exposure() + exposure > self._state["balance"]:
                raise RuntimeError("INSUFFICIENT_FUNDS")
            self._state["orders"][bet_id] = order
            self.operation_log.append(("PLACE", bet_id, str(size), str(price), customer_ref))
            if self._take(Fault.LOST_PLACE_RESPONSE):
                raise LostResponse("accepted but response lost")
            return order

    def cancel_reduction(self, bet_id: str, reduction: Decimal) -> SimOrder:
        order = self.order(bet_id)
        race = self._take(Fault.MATCH_BEFORE_CANCEL)
        if race:
            newly = min(order.remaining, race[1])
            order.matched += newly
            order.remaining -= newly
        if reduction < 0 or reduction > order.remaining:
            raise RuntimeError("INVALID_SIZE_REDUCTION")
        order.size -= reduction
        order.remaining -= reduction
        self.operation_log.append(("CANCEL", bet_id, str(reduction)))
        if self._take(Fault.LOST_CANCEL_RESPONSE):
            raise LostResponse("cancel accepted but response lost")
        return order

    def replace(self, bet_id: str, new_price: Decimal) -> SimOrder:
        if self._take(Fault.FAIL_REPLACE):
            raise RuntimeError("REPLACE_REJECTED")
        order = self.order(bet_id)
        if order.matched:
            raise RuntimeError("MATCHED_STUB_CANNOT_REPLACE")
        order.price = new_price
        self.operation_log.append(("REPLACE", bet_id, str(new_price)))
        if self._take(Fault.LOST_REPLACE_RESPONSE):
            raise LostResponse("replace accepted but response lost")
        return order

    def cancel_all_remaining(self, bet_id: str) -> SimOrder:
        order = self.order(bet_id)
        cancelled = order.remaining
        order.size -= cancelled
        order.remaining = D("0")
        self.operation_log.append(("CANCEL", bet_id, str(cancelled)))
        return order


class MicrostakeCoordinator:
    """Fail-closed isolated saga; never callable in live mode."""

    def __init__(self, exchange: StatefulExchangeSimulator, store: JsonSagaStore,
                 policy: MicrostakePolicy = MicrostakePolicy()):
        self.exchange = exchange
        self.store = store
        self.policy = policy

    def _result(self, phase: Phase, *, req: MicrostakeRequest,
                reason: str = "", order: Optional[SimOrder] = None,
                max_exposure: Decimal = D("0"), held: bool = False) -> MicrostakeResult:
        matched = order.matched if order else D("0")
        residual = order_exposure(req.side, matched, order.price) if order and matched else D("0")
        return MicrostakeResult(
            phase=phase, reason=reason, bet_id=order.bet_id if order else "",
            matched_size=matched,
            final_unmatched_size=order.remaining if order else D("0"),
            residual_exposure=residual, max_transient_exposure=max_exposure,
            reservation_held=held, commission_rate=self.policy.commission_rate,
        )

    def execute(self, req: MicrostakeRequest, *, stub_price: Decimal,
                transient_cap: Optional[Decimal] = None) -> MicrostakeResult:
        p = self.policy
        cap = p.max_transient_exposure if transient_cap is None else transient_cap
        if not self._mode_allowed():
            return self._result(Phase.BLOCKED, req=req, reason="adapter_mode_not_allowed")
        try:
            values = (req.economic_stake, req.target_price, stub_price,
                      p.exchange_stub_size, cap)
            if any(not v.is_finite() for v in values):
                raise ValueError
        except (AttributeError, InvalidOperation, ValueError):
            return self._result(Phase.BLOCKED, req=req, reason="invalid_numeric_input")
        if not (p.minimum_economic_stake <= req.economic_stake < p.exchange_stub_size):
            return self._result(Phase.BLOCKED, req=req, reason="outside_microstake_range")
        if req.economic_stake != req.economic_stake.quantize(D("0.01")):
            # Rilievo Codex #512: un importo non rappresentabile in centesimi verrebbe arrotondato
            # dall'exchange, cioe' eseguito diverso da quello richiesto. Rifiuto esplicito PRIMA dell'invio.
            return self._result(Phase.BLOCKED, req=req, reason="invalid_size_precision")
        transient = order_exposure(req.side, p.exchange_stub_size, stub_price)
        has_reservation = getattr(self.exchange, "has_reservation", None)
        # Adapter senza `has_reservation` => fail-closed: la riserva si considera PREESISTENTE e non
        # viene mai sciolta su store illeggibile (riesame indipendente ciclo 6: il default False
        # avrebbe fatto ereditare il difetto Sol a un futuro adapter che non implementa il metodo).
        pre_existing = bool(has_reservation(req.customer_ref)) if callable(has_reservation) else True
        if not self.exchange.reserve(req.customer_ref, transient, cap):
            return self._result(Phase.BLOCKED, req=req, reason="transient_exposure_cap",
                                max_exposure=transient)

        try:
            existing = self.store.load(req.customer_ref)
        except Exception:
            # Store illeggibile PRIMA di qualunque invio: la riserva transitoria appena presa
            # non deve restare appesa fino al riavvio (rilievo Codex #512). Nulla e' partito.
            # Una riserva PREESISTENTE (retry di un ref con saga gia' inviata) copre esposizione
            # reale e resta (rilievo Sol #512 su 5dd1b86).
            if not pre_existing:
                self.exchange.release_reservation(req.customer_ref)
            raise
        if existing and self._failed_without_order(existing):
            # Saga FAILED senza alcun ordine (rifiuto pre-invio o rifiuto esplicito dell'exchange): nulla e' sul
            # mercato, il nuovo tentativo dello stesso intento (l'engine ha rilasciato l'intento dedupe) riparte da
            # capo. recover() la salverebbe AMBIGUOUS con la riserva tenuta per sempre (review indipendente, P2).
            existing = None
        if existing:
            # Saga gia' avviata (invio precedente / AMBIGUOUS): la riserva copre esposizione REALE
            # e resta anche se recover() fallisce (rilievi Sol/Grok #512 su 464f347).
            return self.recover(req.customer_ref)
        try:
            # `stub_size` persistito: il recovery riduce lo stub che la saga ha DAVVERO piazzato, anche se il minimo
            # exchange cambia fra un avvio e l'altro (saghe pendenti con lo stub da 2 EUR, RECTIFICATION-01).
            self.store.save(req.customer_ref, phase=Phase.PREPARED, reservation_held=True,
                            transient_exposure=transient, request=self._request_json(req),
                            stub_price=stub_price, stub_size=p.exchange_stub_size, replace_sent=False,
                            bet_id="", stub_bet_id="", rollback_intent=False, foreign_ref=False)
        except Exception:
            self.exchange.release_reservation(req.customer_ref)   # nuova saga, nulla e' partito
            raise
        order: Optional[SimOrder] = None
        try:
            order = self.exchange.place(
                market_id=req.market_id, selection_id=req.selection_id, side=req.side,
                size=p.exchange_stub_size, price=stub_price,
                customer_ref=req.customer_ref,
            )
        except LostResponse as exc:
            if getattr(exc, "bet_id", ""):
                # Stub ACCETTATO con betId noto: identita' persistita prima di ogni altra lettura, cosi' il recovery
                # puo' leggerne l'esito anche se sparisce dagli ordini correnti (rilievo Codex P2 su f1594fb).
                self.store.save(req.customer_ref, bet_id=exc.bet_id, stub_bet_id=exc.bet_id)
            order = self.exchange.find_by_ref(req.customer_ref)
            if order is None:
                self.store.save(req.customer_ref, phase=Phase.AMBIGUOUS)
                return self._result(Phase.AMBIGUOUS, req=req, reason="place_outcome_unknown",
                                    max_exposure=transient, held=True)
        except Exception as exc:
            if str(exc) == "DUPLICATE_CUSTOMER_REF":
                # Rilievo Codex r4312719704 (#512): esiste gia' un ordine con questo customer_ref (stato del broker
                # ripristinato senza la saga). Non e' un fallimento: l'ordine puo' essere vivo. AMBIGUOUS con riserva
                # trattenuta, mai toccato (origine non dimostrabile), riconciliazione.
                self.store.save(req.customer_ref, phase=Phase.AMBIGUOUS, reason="existing_customer_ref",
                                foreign_ref=True)
                return self._result(Phase.AMBIGUOUS, req=req, reason="existing_customer_ref",
                                    max_exposure=transient, held=True)
            self.exchange.release_reservation(req.customer_ref)
            self.store.save(req.customer_ref, phase=Phase.FAILED, reservation_held=False,
                            reason=str(exc))
            return self._result(Phase.FAILED, req=req, reason=str(exc),
                                max_exposure=transient)

        assert order is not None
        # `stub_bet_id` resta l'identita' dello STUB anche quando `bet_id` passa al replacement (review F1).
        self.store.save(req.customer_ref, phase=Phase.STUB_ACCEPTED, bet_id=order.bet_id, stub_bet_id=order.bet_id)
        if order.matched > 0:
            self.store.save(req.customer_ref, phase=Phase.AMBIGUOUS, matched=order.matched)
            return self._result(Phase.AMBIGUOUS, req=req, reason="stub_matched",
                                order=order, max_exposure=transient, held=True)

        reduction = p.exchange_stub_size - req.economic_stake
        try:
            order = self.exchange.cancel_reduction(order.bet_id, reduction)
        except Exception as exc:
            self.store.save(req.customer_ref, phase=Phase.AMBIGUOUS, reason=str(exc))
            return self._result(Phase.AMBIGUOUS, req=req, reason="cancel_outcome_unknown",
                                order=order, max_exposure=transient, held=True)
        if order.matched > 0:
            self.store.save(req.customer_ref, phase=Phase.AMBIGUOUS, matched=order.matched)
            return self._result(Phase.AMBIGUOUS, req=req, reason="match_cancel_race",
                                order=order, max_exposure=transient, held=True)
        if order.remaining != req.economic_stake:
            # Importo eseguito != importo richiesto (rettifica owner 17/10): mai ACTIVE su un residuo
            # diverso; riconciliazione, come gia' fa recover() sullo stesso caso.
            self.store.save(req.customer_ref, phase=Phase.AMBIGUOUS)
            return self._result(Phase.AMBIGUOUS, req=req, reason="unexpected_residual_size",
                                order=order, max_exposure=transient, held=True)
        self.store.save(req.customer_ref, phase=Phase.REDUCED)
        if order.price == req.target_price:
            # Stub passivo gia' al prezzo target (nessuna controparte da evitare):
            # il replace sarebbe un no-op che Betfair rifiuta. Identita' invariata.
            return self._activate(req, order, transient)
        # Flag durevole PRIMA del replace: da qui un nuovo ordine puo' esistere anche se lo stub sparisce, quindi il
        # recovery non chiude mai la saga solo perche' lo stub non c'e' piu'.
        self.store.save(req.customer_ref, replace_sent=True)
        try:
            order = self.exchange.replace(order.bet_id, req.target_price)
        except LostResponse as exc:
            self.store.save(req.customer_ref, phase=Phase.AMBIGUOUS, reason=str(exc),
                            **self._replacement_identity(exc))
            return self._result(Phase.AMBIGUOUS, req=req, reason="replace_outcome_unknown",
                                order=order, max_exposure=transient, held=True)
        except Exception as exc:
            # `rollback_intent` e' DUREVOLE (riesame indipendente ciclo 9 #512): nessun salvataggio
            # intermedio successivo (AMBIGUOUS per ordine non trovato, snapshot non persistito) puo'
            # cancellarlo, quindi nessun recover futuro riprende il percorso riduzione/replace.
            self.store.save(req.customer_ref, phase=Phase.ROLLBACK_PENDING, rollback_intent=True,
                            reason=str(exc))
            # Identity comes only from the persisted bet id; never scan by price.
            try:
                order = self.exchange.cancel_all_remaining(order.bet_id)
            except Exception:
                # L'intento di rollback resta persistito (ROLLBACK_PENDING, rilievo Sol ciclo 8): il
                # prossimo recover ritenta SOLO la cancellazione, mai il percorso riduzione/replace.
                self.store.save(req.customer_ref, phase=Phase.ROLLBACK_PENDING, reason="rollback_unknown")
                return self._result(Phase.AMBIGUOUS, req=req, reason="rollback_unknown",
                                    order=order, max_exposure=transient, held=True)
            return self._terminal_or_ambiguous(req.customer_ref, req, Phase.ROLLED_BACK, order, transient,
                                               reason="replace_failed")
        return self._after_replace(req.customer_ref, req, order, transient)

    @staticmethod
    def _replacement_identity(exc: BaseException) -> dict[str, Any]:
        bet_id = str(getattr(exc, "bet_id", "") or "")
        return {"replacement_bet_id": bet_id} if bet_id else {}

    def _after_replace(self, customer_ref: str, req: MicrostakeRequest, order: SimOrder,
                       transient: Decimal) -> MicrostakeResult:
        """Replace accettato: identita' del nuovo ordine persistita PRIMA di qualunque altra decisione, poi verifica
        dell'importo. Betfair rimette solo il RESIDUO: se lo stub si e' abbinato in parte durante il replace, il nuovo
        ordine e' piu' piccolo dello stake richiesto e l'abbinato resta sul vecchio betId (rilievo Codex P1 su #561).
        Mai ACTIVE in quel caso: AMBIGUOUS con riserva tenuta, riconciliazione."""
        self.store.save(customer_ref, bet_id=order.bet_id, replacement_bet_id=order.bet_id)
        if order.matched + order.remaining != req.economic_stake:
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS, reservation_held=True,
                            reason="replacement_size_mismatch")
            return self._result(Phase.AMBIGUOUS, req=req, reason="replacement_size_mismatch",
                                order=order, max_exposure=transient, held=True)
        return self._activate(req, order, transient)

    @staticmethod
    def _failed_without_order(saved: dict[str, Any]) -> bool:
        return saved.get("phase") == Phase.FAILED.value and not saved.get("bet_id")

    @staticmethod
    def _saved_stub_size(saved: dict[str, Any]) -> Decimal:
        """Importo dello stub piazzato da QUESTA saga; assente (saga precedente a RECTIFICATION-01) => 2.00, il
        solo stub che quel codice piazzava."""
        try:
            value = D(str(saved.get("stub_size") or "2.00"))
        except (InvalidOperation, ValueError):
            value = D("2.00")
        return value if value.is_finite() and value > 0 else D("2.00")

    def _mode_allowed(self) -> bool:
        """SIM: policy `simulation_only` sopra una porta che NON si dichiara LIVE. LIVE: policy `live_adapter`
        sopra una porta che si dichiara LIVE. Mai una policy SIM sull'exchange reale ne' il contrario."""
        p = self.policy
        live_port = getattr(self.exchange, "is_live_port", False) is True
        if p.simulation_only:
            return not live_port and not p.live_adapter
        return p.live_adapter is True and live_port

    def _reload_source_stale(self) -> bool:
        """True se l'ULTIMO snapshot della fonte di reload dell'exchange (broker SIM) non e' stato
        persistito: un adapter senza il concetto (LIVE, simulatore di unita') non e' mai stantio."""
        return getattr(self.exchange, "last_snapshot_persisted", True) is False

    def _terminal_or_ambiguous(self, customer_ref: str, req: MicrostakeRequest, phase: Phase, order: SimOrder,
                               transient: Decimal, *, reason: str = "") -> MicrostakeResult:
        """Chiude la saga in una fase TERMINALE (ACTIVE / ROLLED_BACK) solo se la fonte di reload e'
        allineata all'exchange. Altrimenti la saga resta AMBIGUOUS con riserva tenuta e viene
        riconciliata al riavvio: una fase terminale persistita PRIMA della retrocessione lascerebbe,
        in caso di crash, una saga conclusa fuori da `pending_refs` sopra uno stato ricaricato senza
        l'ordine (riesame indipendente del ciclo 8 #512 sul rilievo r4306954173)."""
        if phase is Phase.ROLLED_BACK and order.remaining > 0:
            # Invariante del coordinatore (mandato PF-561-LIVE-MICROSTAKE-P1-HARDENING-01), indipendente dalla
            # porta: un residuo eseguibile ancora sull'exchange non e' mai un rollback. Riserva tenuta, rollback
            # durevole: il recover successivo rilegge e ritenta SOLO la cancellazione.
            self.store.save(customer_ref, phase=Phase.ROLLBACK_PENDING, rollback_intent=True, bet_id=order.bet_id,
                            reservation_held=True, reason="rollback_residual_open")
            return self._result(Phase.AMBIGUOUS, req=req, reason="rollback_residual_open",
                                order=order, max_exposure=transient, held=True)
        if phase is Phase.ACTIVE and order.matched + order.remaining != req.economic_stake:
            # ACTIVE solo sull'importo richiesto esatto (abbinato + residuo): altrimenti esposizione da riconciliare.
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS, bet_id=order.bet_id, reservation_held=True,
                            reason="active_size_mismatch")
            return self._result(Phase.AMBIGUOUS, req=req, reason="active_size_mismatch",
                                order=order, max_exposure=transient, held=True)
        if phase is Phase.ROLLED_BACK and self._stub_unaccounted(customer_ref, order):
            # Review F1 (diff completo #561): dopo un replace la chiusura non guarda solo il replacement. Lo stub
            # originale, riletto per id (correnti o listClearedOrders), deve essere a residuo e abbinato zero; un
            # abbinato dello stub (es. regolato e uscito dai correnti) o uno stub non rileggibile non sono un rollback.
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS, reservation_held=True, reason="stub_unaccounted")
            return self._result(Phase.AMBIGUOUS, req=req, reason="stub_unaccounted",
                                order=order, max_exposure=transient, held=True)
        if phase is Phase.ROLLED_BACK and order.matched > 0:
            # Lo stub si e' abbinato (anche in parte) prima della cancellazione di rollback: esiste uno stake
            # reale. ROLLED_BACK verrebbe registrato FAILED dall'engine, che libera tavolo e chiave: mai.
            # AMBIGUOUS con riserva tenuta, riconciliazione (riesame indipendente ciclo 10 #512).
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS, bet_id=order.bet_id, matched=order.matched,
                            reservation_held=True, reason="rollback_matched")
            return self._result(Phase.AMBIGUOUS, req=req, reason="rollback_matched",
                                order=order, max_exposure=transient, held=True)
        if self._reload_source_stale():
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS, bet_id=order.bet_id,
                            reservation_held=True, reason="snapshot_not_persisted")
            return self._result(Phase.AMBIGUOUS, req=req, reason="snapshot_not_persisted",
                                order=order, max_exposure=transient, held=True)
        self.store.save(customer_ref, phase=phase, bet_id=order.bet_id, reservation_held=False)
        self.exchange.release_reservation(customer_ref)
        return self._result(phase, req=req, reason=reason, order=order, max_exposure=transient, held=False)

    def _stub_unaccounted(self, customer_ref: str, order: SimOrder) -> bool:
        """True se lo stub di questa saga e' un ordine DIVERSO da `order` e non e' dimostrato a esposizione zero.

        Identita' dello stub ignota (saga persistita prima di `stub_bet_id`): si accetta solo se il replace non e' mai
        partito (`replace_sent is False`) o se e' un rollback dopo un replace RIFIUTATO sullo stesso betId persistito
        (nessun nuovo ordine). Altrimenti non dimostrato => True (rilievo GPT-6.1 Sol su 844ae41)."""
        try:
            saved = self.store.load(customer_ref) or {}
        except Exception:
            return True
        stub_id = str(saved.get("stub_bet_id") or "")
        if not stub_id:
            if saved.get("replace_sent") is False:
                return False
            return not (saved.get("rollback_intent") is True and str(order.bet_id) == str(saved.get("bet_id") or ""))
        if stub_id == str(order.bet_id):
            return False
        try:
            stub = self.exchange.order(stub_id)
        except Exception:
            return True
        return stub.matched > 0 or stub.remaining > 0

    def _activate(self, req: MicrostakeRequest, order: SimOrder, transient: Decimal) -> MicrostakeResult:
        """Saga conclusa: l'ordine economico e' sull'exchange e i suoi fondi li contabilizza
        l'exchange stesso. La riserva TRANSITORIA (stub al minimo exchange) va rilasciata, altrimenti ogni
        microstake riuscito consumerebbe per sempre l'importo dello stub di cap (rilievo Codex PR #512)."""
        return self._terminal_or_ambiguous(req.customer_ref, req, Phase.ACTIVE, order, transient)

    def recover(self, customer_ref: str, *, entry_allowed: bool = True) -> MicrostakeResult:
        """Riprende una saga non conclusa dalla sua identita' persistita.

        `entry_allowed=False` (LIVE non consentito o STOP attivo, rilievo Codex P1 su #561): nessuna riduzione ne'
        replace, cioe' nessun ordine portato al prezzo d'ingresso; restano la lettura e la cancellazione protettiva
        dello stub (rollback durevole).

        LIVE (decisione owner PF-561-LIVE-MICROSTAKE-P1-HARDENING-01 §4): il recovery NON completa mai un vecchio
        ingresso dalla sola saga persistita, a prescindere da `entry_allowed`. Ordine richiesto gia' presente
        (prezzo target, importo esatto) => riconciliato ACTIVE; stub (anche ridotto) => cancellazione verificata e
        ROLLED_BACK solo a residuo zero; quota abbinata => conservata (AMBIGUOUS `rollback_matched`); stato non
        dimostrabile => AMBIGUOUS. SIM invariata (#512): completa la saga."""
        if self.policy.live_adapter:
            entry_allowed = False
        saved = self.store.load(customer_ref)
        if not saved:
            raise KeyError(customer_ref)
        req = self._request_from_json(saved["request"])
        transient = D(saved.get("transient_exposure", "0"))
        if not self._mode_allowed():
            # Porta e policy di modalita' diverse: nessuna operazione exchange, la saga resta pendente.
            return self._result(Phase.AMBIGUOUS, req=req, reason="adapter_mode_not_allowed",
                                max_exposure=transient, held=True)
        if self._failed_without_order(saved):
            # Terminale e senza ordine: nulla da riconciliare, nessuna operazione exchange.
            self.exchange.release_reservation(customer_ref)
            return self._result(Phase.FAILED, req=req, reason=str(saved.get("reason") or "failed"),
                                max_exposure=transient)
        if saved.get("foreign_ref") is True:
            # Ordine preesistente con lo stesso customer_ref, non piazzato da questa saga (riesame indipendente
            # ciclo 14 #512): la saga non lo adotta MAI come proprio stub (niente riduzione/replace/cancel).
            # Resta AMBIGUOUS con riserva tenuta fino alla riconciliazione manuale/esterna.
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS, reservation_held=True)
            return self._result(Phase.AMBIGUOUS, req=req, reason="existing_customer_ref",
                                max_exposure=transient, held=True)
        rollback_intent = saved.get("rollback_intent") is True or saved.get("phase") == Phase.ROLLBACK_PENDING.value
        order = self.exchange.find_by_ref(customer_ref)
        if order is None and rollback_intent and saved.get("bet_id"):
            # Rollback gia' deciso: un ordine interamente cancellato puo' sparire dagli ordini correnti
            # (broker SIM reale). Si cerca per bet id persistito, ma si accetta SOLO una riga terminale
            # senza residuo: un ordine ancora vivo resta "non trovato" e la saga resta pendente
            # (secondo riesame indipendente ciclo 9 #512).
            try:
                by_id = self.exchange.order(str(saved["bet_id"]))
            except Exception:
                by_id = None
            if by_id is not None and by_id.remaining == 0:
                order = by_id
        replacement_gone = False
        if order is None and not rollback_intent and saved.get("replacement_bet_id"):
            # Replace accettato con identita' nota (persistita): il nuovo ordine si cerca per id, anche fra gli esiti
            # di listClearedOrders, invece di dipendere dal riferimento conservato o meno dal replace.
            try:
                by_id = self.exchange.order(str(saved["replacement_bet_id"]))
            except Exception:
                by_id = None
            if by_id is not None:
                order = by_id
                replacement_gone = by_id.remaining == 0 and by_id.matched == 0
        stub_gone = False
        # `replace_sent is False` ESPLICITO (scritto alla creazione della saga): una saga persistita prima di questo
        # campo non dice se il replace e' partito, quindi non si chiude per stub sparito (rilievo GPT-6.1 Sol).
        if order is None and not rollback_intent and saved.get("bet_id") and saved.get("replace_sent") is False:
            # Stub piazzato (betId noto) e MAI sostituito: se non e' piu' fra gli ordini correnti si legge il suo
            # esito. Cancellato/lapsato senza abbinato => nessuna esposizione, la saga si chiude (review indipendente,
            # P2: prima restava pendente per sempre). Con un replace inviato no: il nuovo ordine potrebbe esistere.
            try:
                by_id = self.exchange.order(str(saved["bet_id"]))
            except Exception:
                by_id = None
            if by_id is not None and by_id.remaining == 0:
                order, stub_gone = by_id, True
        if order is None:
            # Ordine non (ancora) visibile: riconciliazione incompleta. Un rollback gia' deciso resta
            # ROLLBACK_PENDING, mai declassato ad AMBIGUOUS generico (riesame indipendente ciclo 9 #512:
            # il recover successivo avrebbe ridotto e riprezzato l'ordine che si era deciso di cancellare).
            self.store.save(customer_ref,
                            phase=Phase.ROLLBACK_PENDING if rollback_intent else Phase.AMBIGUOUS,
                            reservation_held=True)
            return self._result(Phase.AMBIGUOUS, req=req, reason="reconciliation_incomplete",
                                max_exposure=transient, held=True)
        phase = Phase(saved["phase"])
        if phase is Phase.ACTIVE:
            self.exchange.release_reservation(customer_ref)
            return self._result(phase, req=req, order=order,
                                max_exposure=transient, held=False)
        if (stub_gone or replacement_gone) and order.matched == 0:
            return self._terminal_or_ambiguous(customer_ref, req, Phase.ROLLED_BACK, order, transient,
                                               reason="stub_gone_unmatched" if stub_gone else "replacement_gone_unmatched")
        if (not rollback_intent and order.price == req.target_price
                and order.matched + order.remaining == req.economic_stake):
            # E' l'ordine richiesto: prezzo target e importo esatto (dopo riduzione e, se serviva, replace), anche se
            # gia' abbinato. Mai "stub abbinato" pendente per sempre su un ordine che e' il risultato voluto
            # (review indipendente, P2: in LIVE il replace c'e' sempre e l'ordine finale si abbina subito).
            return self._activate(req, order, transient)
        if not entry_allowed and not rollback_intent:
            # Ingressi bloccati: la saga non viene completata ma annullata (durevole: anche a ingressi riaperti il
            # recover successivo completa solo la cancellazione).
            rollback_intent = True
            self.store.save(customer_ref, phase=Phase.ROLLBACK_PENDING, rollback_intent=True, reason="entry_blocked")
        # Resume only through execute's idempotent reconciliation path.  An
        # accepted stub is never placed again.
        # Con rollback deciso il residuo si cancella PRIMA di classificare l'abbinato: un abbinamento parziale
        # avvenuto prima del recover non deve lasciare il resto dello stub eseguibile (rilievo Codex r4309039350).
        # La parte abbinata resta poi AMBIGUOUS `rollback_matched` con riserva tenuta (_terminal_or_ambiguous).
        if order.matched > 0 and not rollback_intent:
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS)
            return self._result(Phase.AMBIGUOUS, req=req, reason="reconciled_matched_stub",
                                order=order, max_exposure=transient, held=True)
        if rollback_intent:
            # Rollback gia' deciso prima del crash (replace rifiutato): si completa SOLO la
            # cancellazione del residuo, mai un nuovo replace che riattiverebbe la saga
            # (rilievo Codex #512 su 034af59). Cancellazione fallita => risultato AMBIGUOUS con riserva
            # tenuta, ma la fase persistita RESTA ROLLBACK_PENDING (rilievo Sol ciclo 8): l'intento di
            # rollback non si perde e il recover successivo ritenta solo la cancellazione.
            # Residuo gia' cancellato (es. cancellazione riuscita ma snapshot non persistito al giro
            # precedente): nulla da cancellare, si chiude soltanto la saga, senza ripetere la chiamata.
            try:
                if order.remaining > 0:
                    order = self.exchange.cancel_all_remaining(order.bet_id)
            except Exception:
                self.store.save(customer_ref, phase=Phase.ROLLBACK_PENDING, reason="rollback_unknown")
                return self._result(Phase.AMBIGUOUS, req=req, reason="rollback_unknown",
                                    order=order, max_exposure=transient, held=True)
            return self._terminal_or_ambiguous(customer_ref, req, Phase.ROLLED_BACK, order, transient,
                                               reason="replace_failed")
        # Complete a known accepted, fully-unmatched stub (importo dello stub persistito dalla saga).
        stub_size = self._saved_stub_size(saved)
        reduction = stub_size - req.economic_stake
        if order.remaining == stub_size:
            order = self.exchange.cancel_reduction(order.bet_id, reduction)
        if order.remaining != req.economic_stake:
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS)
            return self._result(Phase.AMBIGUOUS, req=req, reason="unexpected_residual_size",
                                order=order, max_exposure=transient, held=True)
        if order.price == req.target_price:
            return self._activate(req, order, transient)
        self.store.save(customer_ref, replace_sent=True)
        try:
            order = self.exchange.replace(order.bet_id, req.target_price)
        except Exception as exc:
            self.store.save(customer_ref, phase=Phase.AMBIGUOUS, **self._replacement_identity(exc))
            return self._result(Phase.AMBIGUOUS, req=req, reason="recovery_replace_unknown",
                                order=order, max_exposure=transient, held=True)
        return self._after_replace(customer_ref, req, order, transient)

    @staticmethod
    def _request_json(req: MicrostakeRequest) -> dict[str, Any]:
        data = asdict(req)
        data["side"] = req.side.value
        data["economic_stake"] = str(req.economic_stake)
        data["target_price"] = str(req.target_price)
        return data

    @staticmethod
    def _request_from_json(data: dict[str, Any]) -> MicrostakeRequest:
        return MicrostakeRequest(
            market_id=data["market_id"], selection_id=int(data["selection_id"]),
            side=Side(data["side"]), economic_stake=D(data["economic_stake"]),
            target_price=D(data["target_price"]), customer_ref=data["customer_ref"],
        )


class NativeMicrostakeExecutor:
    """Translate the coordinator saga into the TradingEngine response contract.

    Selection is automatic.  Quote proof and transient cap are injected from
    authoritative engine/risk adapters; they are not user enable switches.
    """

    def __init__(
        self,
        coordinator: MicrostakeCoordinator,
        *,
        quote_provider: Any,
        transient_exposure_cap: Decimal,
        exchange_minimum: Decimal = EXCHANGE_MIN_ORDER,
    ) -> None:
        if not callable(quote_provider):
            raise TypeError("quote_provider must be callable")
        self.coordinator = coordinator
        self.quote_provider = quote_provider
        # Cap dell'esposizione transitoria: valore fisso oppure provider
        # callable(payload) -> Decimal | None, letto dai limiti owner a ogni
        # richiesta. None => BLOCKED: un cap che non si legge non e' un cap.
        self.transient_exposure_cap = (
            transient_exposure_cap if callable(transient_exposure_cap)
            else D(str(transient_exposure_cap))
        )
        self.planner = SmartExecutionEngine(exchange_minimum=exchange_minimum)

    def _resolve_cap(self, payload: dict[str, Any]) -> Optional[Decimal]:
        cap = self.transient_exposure_cap
        if callable(cap):
            cap = cap(dict(payload))
        if cap is None:
            return None
        value = D(str(cap))
        if not value.is_finite() or value < 0:
            return None
        return value

    @staticmethod
    def _request(payload: dict[str, Any]) -> MicrostakeRequest:
        side_raw = str(payload.get("bet_type") or payload.get("side") or "").upper()
        return MicrostakeRequest(
            market_id=str(payload["market_id"]),
            selection_id=int(payload["selection_id"]),
            side=Side(side_raw),
            economic_stake=D(str(payload.get("stake", payload.get("size")))),
            target_price=D(str(payload["price"])),
            customer_ref=str(payload["customer_ref"]),
        )

    @staticmethod
    def _blocked(reason: str, *, transient: Decimal = D("0")) -> dict[str, Any]:
        return {
            "ok": False,
            "status": "FAILURE",
            "error": reason,
            "reason_code": reason,
            "microstake": True,
            "transient_exposure": str(transient),
        }

    def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        log = getattr(self.coordinator.exchange, "operation_log", None)
        ref = str((payload or {}).get("customer_ref") or "")

        def blocked(reason: str, *, transient: Decimal = D("0")) -> dict[str, Any]:
            # pre_send=True SOLO se nessuna operazione exchange e' partita PER QUESTA saga: l'engine
            # puo' allora rilasciare l'intento dedupe (un nuovo tentativo dopo che la condizione
            # transitoria e' passata non e' un duplicato). Rilievo Codex #512; per customer_ref e non
            # dalla lunghezza globale del log (rilievo Grok: una saga concorrente non deve mascherarlo).
            out = self._blocked(reason, transient=transient)
            out["pre_send"] = (log is not None and bool(ref) and not any(
                isinstance(op, tuple) and len(op) >= 5 and op[0] == "PLACE" and op[-1] == ref for op in log))
            return out

        try:
            req = self._request(payload)
            quote = self.quote_provider(dict(payload))
            if not isinstance(quote, dict):
                return blocked("passive_stub_price_unverified")
            stub_raw = quote.get("passive_stub_price")
            best_raw = quote.get("best_counterparty_price")
            stub = None if stub_raw is None else D(str(stub_raw))
            best = None if best_raw is None else D(str(best_raw))
            cap = self._resolve_cap(payload)
            if cap is None:
                return blocked("transient_cap_unavailable")
            plan = self.planner.plan(
                req,
                passive_stub_price=stub,
                best_counterparty_price=best,
                transient_exposure_cap=cap,
            )
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            return blocked(f"invalid_microstake_request:{type(exc).__name__}")
        if plan.kind is ExecutionKind.BLOCKED:
            return blocked(plan.reason, transient=plan.transient_exposure)
        if plan.kind is not ExecutionKind.MICROSTAKE:
            return blocked("native_route_not_microstake")

        result = self.coordinator.execute(req, stub_price=plan.api_price, transient_cap=cap)
        common = {
            "microstake": True,
            "phase": result.phase.value,
            "matched": str(result.matched_size),
            "unmatched": str(result.final_unmatched_size),
            "residual_exposure": str(result.residual_exposure),
            "transient_exposure": str(result.max_transient_exposure),
            "reservation_held": result.reservation_held,
        }
        if result.phase is Phase.ACTIVE and result.bet_id:
            return {
                **common,
                "status": "SUCCESS",
                "instructionReports": [{
                    "status": "SUCCESS",
                    "betId": result.bet_id,
                    "sizeMatched": float(result.matched_size),
                }],
            }
        if result.phase is Phase.AMBIGUOUS:
            return {
                **common, "status": "AMBIGUOUS", "order_unknown": True,
                "error": result.reason,
            }
        return {**common, **blocked(result.reason or result.phase.value)}

