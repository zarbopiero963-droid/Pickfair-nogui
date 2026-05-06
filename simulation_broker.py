from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.pnl_engine import MarketNetRealizedSettlementAggregator
from core.position_ledger import PositionLedger

logger = logging.getLogger(__name__)


@dataclass
class SimOrder:
    bet_id: str
    market_id: str
    selection_id: int
    side: str
    price: float
    size: float
    matched_size: float = 0.0
    avg_price_matched: float = 0.0
    status: str = "EXECUTABLE"
    customer_ref: str = ""
    event_key: str = ""
    table_id: Optional[int] = None
    batch_id: str = ""
    event_name: str = ""
    market_name: str = ""
    runner_name: str = ""
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bet_id": self.bet_id,
            "market_id": self.market_id,
            "selection_id": self.selection_id,
            "side": self.side,
            "price": self.price,
            "size": self.size,
            "matched_size": self.matched_size,
            "avg_price_matched": self.avg_price_matched,
            "status": self.status,
            "customer_ref": self.customer_ref,
            "event_key": self.event_key,
            "table_id": self.table_id,
            "batch_id": self.batch_id,
            "event_name": self.event_name,
            "market_name": self.market_name,
            "runner_name": self.runner_name,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class SimulationState:
    def __init__(self, starting_balance: float, commission_pct: float):
        self.starting_balance = float(starting_balance)
        self.balance = float(starting_balance)
        self.exposure = 0.0
        self.unrealized_pnl = 0.0
        self.commission_pct = float(commission_pct)
        self.realized_pnl = 0.0
        self.realized_commission = 0.0
        self.market_commission_ledger: Dict[str, Dict[str, float]] = {}
        self.last_settlement: Dict[str, float | str] = {}
        self.orders: Dict[str, SimOrder] = {}
        self.market_books: Dict[str, Dict[str, Any]] = {}
        self.event_index: Dict[str, Dict[str, Any]] = {}
        self.position_ledgers: Dict[str, PositionLedger] = {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "starting_balance": self.starting_balance,
            "balance": self.balance,
            "exposure": self.exposure,
            "unrealized_pnl": self.unrealized_pnl,
            "commission_pct": self.commission_pct,
            "realized_pnl": self.realized_pnl,
            "realized_commission": self.realized_commission,
            "market_commission_ledger": self.market_commission_ledger,
            "last_settlement": dict(self.last_settlement),
            "orders": {k: v.to_dict() for k, v in self.orders.items()},
            "market_books": self.market_books,
            "event_index": self.event_index,
            "position_ledgers": {
                key: {
                    "market_id": ledger.market_id,
                    "runner_id": ledger.runner_id,
                    "snapshot": ledger.snapshot().__dict__,
                }
                for key, ledger in self.position_ledgers.items()
            },
        }

    def load_from_dict(self, data: Dict[str, Any]) -> None:
        def _to_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        def _to_int(value: Any, default: int = 0) -> int:
            try:
                return int(value)
            except (TypeError, ValueError):
                return default

        data = dict(data or {})
        self.starting_balance = _to_float(data.get("starting_balance"), self.starting_balance)
        self.balance = _to_float(data.get("balance"), self.starting_balance)
        self.exposure = _to_float(data.get("exposure"), 0.0)
        self.unrealized_pnl = _to_float(data.get("unrealized_pnl"), 0.0)
        self.commission_pct = _to_float(data.get("commission_pct"), self.commission_pct)
        self.realized_pnl = _to_float(data.get("realized_pnl"), 0.0)
        self.realized_commission = _to_float(data.get("realized_commission"), 0.0)
        raw_ledger = data.get("market_commission_ledger") or {}
        self.market_commission_ledger = {}
        for market_id, item in dict(raw_ledger).items():
            row = dict(item or {})
            self.market_commission_ledger[str(market_id)] = {
                "gross": _to_float(row.get("gross"), 0.0),
                "commission": _to_float(row.get("commission"), 0.0),
            }
        self.last_settlement = dict(data.get("last_settlement") or {})

        self.orders = {}
        for bet_id, payload in (data.get("orders") or {}).items():
            p = dict(payload or {})
            self.orders[bet_id] = SimOrder(
                bet_id=str(p.get("bet_id") or bet_id),
                market_id=str(p.get("market_id") or ""),
                selection_id=_to_int(p.get("selection_id"), 0),
                side=str(p.get("side") or "BACK"),
                price=_to_float(p.get("price"), 0.0),
                size=_to_float(p.get("size"), 0.0),
                matched_size=_to_float(p.get("matched_size"), 0.0),
                avg_price_matched=_to_float(p.get("avg_price_matched"), 0.0),
                status=str(p.get("status") or "EXECUTABLE"),
                customer_ref=str(p.get("customer_ref") or ""),
                event_key=str(p.get("event_key") or ""),
                table_id=p.get("table_id"),
                batch_id=str(p.get("batch_id") or ""),
                event_name=str(p.get("event_name") or ""),
                market_name=str(p.get("market_name") or ""),
                runner_name=str(p.get("runner_name") or ""),
                created_at=str(p.get("created_at") or datetime.utcnow().isoformat()),
                updated_at=str(p.get("updated_at") or datetime.utcnow().isoformat()),
            )

        self.market_books = dict(data.get("market_books") or {})
        self.event_index = dict(data.get("event_index") or {})
        self.position_ledgers = {}
        raw_ledgers = data.get("position_ledgers") or {}
        for key, payload in dict(raw_ledgers).items():
            item = dict(payload or {})
            market_id = str(item.get("market_id") or "")
            runner_id = _to_int(item.get("runner_id"), 0)
            if not market_id or runner_id <= 0:
                continue
            ledger = PositionLedger(market_id=market_id, runner_id=runner_id)
            snap = dict(item.get("snapshot") or {})
            open_side = str(snap.get("open_side") or "").strip().upper()
            open_size = _to_float(snap.get("open_size"), 0.0)
            avg_price = _to_float(snap.get("avg_entry_price"), 0.0)
            if open_side in {"BACK", "LAY"} and open_size > 0.0 and avg_price > 1.0:
                ledger.apply_fill(
                    fill_id=f"restore:{key}:open",
                    side=open_side,
                    price=avg_price,
                    size=open_size,
                )
            self.position_ledgers[str(key)] = ledger


class SimulationBroker:
    """
    Broker simulato compatibile con:
    - BetfairService
    - TelegramBetResolver
    - OrderManager

    Modello semplice ma coerente:
    - match aggressivo sul best lay per BACK
    - match aggressivo sul best back per LAY
    - nessun auto-close
    - stato persistibile
    """

    def __init__(
        self,
        *,
        starting_balance: float = 1000.0,
        commission_pct: float = 4.5,
        partial_fill_enabled: bool = True,
        consume_liquidity: bool = True,
        db=None,
    ):
        self.db = db
        self.partial_fill_enabled = bool(partial_fill_enabled)
        self.consume_liquidity = bool(consume_liquidity)
        self.connected = False
        self.session_token = ""
        self.state = SimulationState(
            starting_balance=float(starting_balance),
            commission_pct=float(commission_pct),
        )
        self._market_net_realized_aggregator = MarketNetRealizedSettlementAggregator(
            commission_pct=self.state.commission_pct,
            context="simulation_broker_settlement",
        )
        self._market_net_realized_aggregator.ledger = self.state.market_commission_ledger
        self._lock = threading.RLock()

    @staticmethod
    def _ledger_key(market_id: str, selection_id: int) -> str:
        return f"{str(market_id)}::{int(selection_id)}"

    def _get_or_create_ledger(self, *, market_id: str, selection_id: int) -> PositionLedger:
        key = self._ledger_key(market_id, selection_id)
        ledger = self.state.position_ledgers.get(key)
        if ledger is None:
            ledger = PositionLedger(market_id=str(market_id), runner_id=int(selection_id))
            self.state.position_ledgers[key] = ledger
        return ledger

    def _recompute_exposure_and_unrealized(self) -> None:
        total_exposure = 0.0
        total_unrealized = 0.0
        for ledger in self.state.position_ledgers.values():
            snap = ledger.snapshot()
            total_exposure += float(snap.exposure)
            total_unrealized += float(snap.unrealized_pnl)
        self.state.exposure = float(total_exposure)
        self.state.unrealized_pnl = float(total_unrealized)

    def _apply_fill_to_position_ledger(
        self,
        *,
        fill_id: str,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
    ) -> Dict[str, Any]:
        ledger = self._get_or_create_ledger(market_id=market_id, selection_id=selection_id)
        before = ledger.snapshot()
        applied = ledger.apply_fill(
            fill_id=fill_id,
            side=side,
            price=price,
            size=size,
        )
        after = applied["snapshot"]
        if applied.get("applied", False):
            before_exposure = float(before.exposure)
            after_exposure = float(after.exposure)
            realized_delta = float(applied.get("realized_delta") or 0.0)
            self.state.balance += (before_exposure - after_exposure) + realized_delta
        self._recompute_exposure_and_unrealized()
        return applied

    # =========================================================
    # SESSION
    # =========================================================
    def login(self, password: Optional[str] = None) -> Dict[str, Any]:
        _ = password
        self.connected = True
        self.session_token = "SIM-" + uuid.uuid4().hex[:16]
        return {
            "session_token": self.session_token,
            "expiry": "",
            "connected": True,
            "simulated": True,
        }

    def logout(self) -> None:
        self.connected = False
        self.session_token = ""

    # =========================================================
    # ACCOUNT
    # =========================================================
    def get_account_funds(self) -> Dict[str, float]:
        with self._lock:
            return {
                "available": float(self.state.balance),
                "exposure": float(self.state.exposure),
                "total": float(self.state.balance + self.state.exposure),
                "simulated": True,
            }

    # =========================================================
    # EVENT / MARKET DATA INDEX
    # =========================================================
    def update_market_book(self, *args) -> Dict[str, Any]:
        """
        Compatibile con:
        - update_market_book(market_book)
        - update_market_book(market_id, market_book)
        """
        market_id = ""
        market_book: Dict[str, Any] = {}

        if len(args) == 1 and isinstance(args[0], dict):
            market_book = dict(args[0] or {})
            market_id = str(
                market_book.get("marketId")
                or market_book.get("market_id")
                or ""
            ).strip()
        elif len(args) >= 2:
            market_id = str(args[0] or "").strip()
            market_book = dict(args[1] or {})
        else:
            return {"ok": False, "reason": "invalid_arguments", "simulated": True}

        if not market_id:
            return {"ok": False, "reason": "missing_market_id", "simulated": True}

        normalized = dict(market_book)
        normalized["marketId"] = market_id
        normalized["market_id"] = market_id

        with self._lock:
            self.state.market_books[market_id] = normalized
            self._refresh_unrealized_for_market(market_id)

        event_name = str(
            normalized.get("event_name")
            or normalized.get("eventName")
            or normalized.get("event")
            or ""
        ).strip()
        event_id = str(
            normalized.get("event_id")
            or normalized.get("eventId")
            or event_name
            or market_id
        ).strip()
        market_name = str(
            normalized.get("market_name")
            or normalized.get("marketName")
            or ""
        ).strip()

        if event_id:
            event_entry = self.state.event_index.setdefault(
                event_id,
                {
                    "event_id": event_id,
                    "event_name": event_name or event_id,
                    "markets": {},
                    "inplay": bool(normalized.get("inplay", True)),
                },
            )
            if event_name:
                event_entry["event_name"] = event_name
            event_entry["markets"][market_id] = {
                "market_id": market_id,
                "market_name": market_name,
            }

        return {"ok": True, "market_id": market_id, "simulated": True}

    def _refresh_unrealized_for_market(self, market_id: str) -> None:
        market = self.state.market_books.get(str(market_id))
        if not market:
            self._recompute_exposure_and_unrealized()
            return
        runners_by_selection: Dict[int, Dict[str, Any]] = {}
        for runner in market.get("runners") or []:
            try:
                sid = int(runner.get("selectionId") or runner.get("selection_id") or 0)
            except Exception:
                sid = 0
            if sid > 0:
                runners_by_selection[sid] = dict(runner or {})

        for key, ledger in self.state.position_ledgers.items():
            if not key.startswith(f"{str(market_id)}::"):
                continue
            snap = ledger.snapshot()
            if snap.open_side not in {"BACK", "LAY"} or snap.open_size <= 0.0:
                continue
            runner = runners_by_selection.get(int(snap.runner_id))
            if not runner:
                continue
            ex = runner.get("ex") or {}
            backs = ex.get("availableToBack") or []
            lays = ex.get("availableToLay") or []
            if snap.open_side == "BACK":
                mark = float((lays[0] if lays else {}).get("price") or 0.0)
            else:
                mark = float((backs[0] if backs else {}).get("price") or 0.0)
            if mark > 1.0:
                ledger.mark_to_market(mark_price=mark)
        self._recompute_exposure_and_unrealized()

    def get_market_book(self, market_id: str) -> Optional[Dict[str, Any]]:
        return self.state.market_books.get(str(market_id))

    def list_live_soccer_events(self) -> List[Dict[str, Any]]:
        out = []
        for _, event in self.state.event_index.items():
            if not event.get("inplay", True):
                continue
            out.append(
                {
                    "event_id": event.get("event_id"),
                    "event_name": event.get("event_name"),
                }
            )
        return out

    def list_event_markets(self, event_id: Any) -> List[Dict[str, Any]]:
        event = self.state.event_index.get(str(event_id))
        if not event:
            return []

        out = []
        for market_id, meta in (event.get("markets") or {}).items():
            market_book = self.state.market_books.get(market_id, {})
            out.append(
                {
                    "market_id": market_id,
                    "market_name": meta.get("market_name", ""),
                    "event_id": event.get("event_id"),
                    "event_name": event.get("event_name", ""),
                    "runners": market_book.get("runners") or [],
                }
            )
        return out

    # =========================================================
    # ORDERS
    # =========================================================
    def place_bet(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
        customer_ref: str = "",
        event_key: str = "",
        table_id: Any = None,
        batch_id: str = "",
        event_name: str = "",
        market_name: str = "",
        runner_name: str = "",
    ) -> Dict[str, Any]:
        order = SimOrder(
            bet_id="SIMBET-" + uuid.uuid4().hex[:14],
            market_id=str(market_id),
            selection_id=int(selection_id),
            side=str(side or "BACK").upper(),
            price=float(price),
            size=float(size),
            customer_ref=str(customer_ref or ""),
            event_key=str(event_key or ""),
            table_id=table_id,
            batch_id=str(batch_id or ""),
            event_name=str(event_name or ""),
            market_name=str(market_name or ""),
            runner_name=str(runner_name or ""),
        )

        with self._lock:
            self._match_order(order)
            self.state.orders[order.bet_id] = order
            self._persist_order(order)

        return {
            "status": "SUCCESS",
            "marketId": order.market_id,
            "instructionReports": [
                {
                    "status": "SUCCESS" if order.matched_size > 0 else "FAILURE",
                    "betId": order.bet_id,
                    "sizeMatched": order.matched_size,
                    "averagePriceMatched": order.avg_price_matched,
                }
            ],
            "simulated": True,
        }

    def place_orders(
        self,
        *,
        market_id: str,
        instructions: List[Dict[str, Any]],
        customer_ref: str = "",
        event_key: str = "",
        table_id: Any = None,
        batch_id: str = "",
        event_name: str = "",
        market_name: str = "",
    ) -> Dict[str, Any]:
        reports = []
        for item in instructions or []:
            try:
                selection_id = int(item.get("selection_id", item.get("selectionId")))
            except (TypeError, ValueError):
                selection_id = 0
            result = self.place_bet(
                market_id=str(market_id),
                selection_id=selection_id,
                side=str(item.get("side") or item.get("bet_type") or "BACK"),
                price=float(item.get("price") or 0.0),
                size=float(item.get("size", item.get("stake")) or 0.0),
                customer_ref=customer_ref,
                event_key=event_key,
                table_id=table_id,
                batch_id=batch_id,
                event_name=event_name,
                market_name=market_name,
                runner_name=str(item.get("runner_name") or item.get("runnerName") or ""),
            )
            reports.extend(result.get("instructionReports") or [])

        return {
            "status": "SUCCESS",
            "marketId": str(market_id),
            "instructionReports": reports,
            "simulated": True,
        }

    def list_current_orders(self, market_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        with self._lock:
            orders = []
            wanted = {str(m) for m in (market_ids or [])}

            for order in self.state.orders.values():
                if wanted and order.market_id not in wanted:
                    continue

                orders.append(
                    {
                        "betId": order.bet_id,
                        "marketId": order.market_id,
                        "selectionId": order.selection_id,
                        "side": order.side,
                        "priceSize": {
                            "price": order.price,
                            "size": order.size,
                        },
                        "sizeMatched": order.matched_size,
                        "sizeRemaining": max(0.0, order.size - order.matched_size),
                        "status": order.status,
                    }
                )

        return {
            "currentOrders": orders,
            "moreAvailable": False,
            "simulated": True,
        }

    def cancel_orders(
        self,
        *,
        market_id: Optional[str] = None,
        instructions: Optional[List[Dict[str, Any]]] = None,
        customer_ref: str = "",
    ) -> Dict[str, Any]:
        _ = customer_ref
        reports = []
        with self._lock:
            if not instructions:
                for order in self.state.orders.values():
                    if market_id and str(order.market_id) != str(market_id):
                        continue
                    if order.status == "EXECUTABLE":
                        order.status = "CANCELLED"
                        order.updated_at = datetime.utcnow().isoformat()
                        reports.append(
                            {
                                "status": "SUCCESS",
                                "sizeCancelled": max(0.0, order.size - order.matched_size),
                            }
                        )
                return {
                    "status": "SUCCESS",
                    "instructionReports": reports,
                    "simulated": True,
                }

            target_bet_ids = {
                str(item.get("betId") or item.get("bet_id") or "")
                for item in instructions
            }

            for bet_id in target_bet_ids:
                if not bet_id:
                    continue
                order = self.state.orders.get(bet_id)
                if not order:
                    reports.append({"status": "FAILURE", "sizeCancelled": 0.0})
                    continue
                if market_id and str(order.market_id) != str(market_id):
                    reports.append({"status": "FAILURE", "sizeCancelled": 0.0})
                    continue

                if order.status == "EXECUTABLE":
                    order.status = "CANCELLED"
                    order.updated_at = datetime.utcnow().isoformat()
                    reports.append(
                        {
                            "status": "SUCCESS",
                            "sizeCancelled": max(0.0, order.size - order.matched_size),
                        }
                    )
                else:
                    reports.append({"status": "FAILURE", "sizeCancelled": 0.0})

        return {
            "status": "SUCCESS",
            "instructionReports": reports,
            "simulated": True,
        }

    # =========================================================
    # MATCHING
    # =========================================================
    def _match_order(self, order: SimOrder) -> None:
        market = self.state.market_books.get(order.market_id)
        if not market:
            order.status = "EXECUTABLE"
            return

        runner = None
        for r in market.get("runners") or []:
            try:
                runner_selection_id = int(r.get("selectionId") or r.get("selection_id") or 0)
            except (TypeError, ValueError):
                runner_selection_id = 0
            if runner_selection_id == int(order.selection_id):
                runner = r
                break

        if not runner:
            order.status = "EXECUTABLE"
            return

        ex = runner.get("ex") or {}
        backs = ex.get("availableToBack") or []
        lays = ex.get("availableToLay") or []

        if order.side == "BACK":
            best_counter = lays[0] if lays else None
        else:
            best_counter = backs[0] if backs else None

        if not best_counter:
            order.status = "EXECUTABLE"
            return

        best_price = float(best_counter.get("price") or 0.0)
        available_size = float(best_counter.get("size") or 0.0)

        if best_price <= 1.0:
            order.status = "EXECUTABLE"
            return

        # ordine aggressivo: match se il prezzo è compatibile
        can_match = False
        if order.side == "BACK":
            can_match = order.price >= best_price
        else:
            can_match = order.price <= best_price

        if not can_match:
            order.status = "EXECUTABLE"
            return

        requested = float(order.size)
        matched = requested

        if self.partial_fill_enabled and available_size > 0:
            matched = min(requested, available_size)
        elif available_size > 0:
            if available_size < requested:
                order.status = "EXECUTABLE"
                return
            matched = requested

        if matched <= 0:
            order.status = "EXECUTABLE"
            return

        order.matched_size = matched
        order.avg_price_matched = best_price
        if matched >= requested:
            order.status = "EXECUTION_COMPLETE"
        else:
            order.status = "EXECUTABLE"

        order.updated_at = datetime.utcnow().isoformat()

        self._apply_balance_effect(order, matched, best_price)

        if self.consume_liquidity and available_size > 0:
            best_counter["size"] = max(0.0, available_size - matched)

    def _apply_balance_effect(self, order: SimOrder, matched: float, price: float) -> None:
        self._apply_fill_to_position_ledger(
            fill_id=str(order.bet_id),
            market_id=order.market_id,
            selection_id=int(order.selection_id),
            side=order.side,
            price=float(price),
            size=float(matched),
        )

    def _persist_order(self, order: SimOrder) -> None:
        if self.db and hasattr(self.db, "save_simulation_bet"):
            try:
                self.db.save_simulation_bet(order.to_dict())
            except Exception:
                logger.exception("Errore save_simulation_bet")

    def record_realized_settlement(self, gross_pnl: float, market_id: str) -> Dict[str, Any]:
        """
        Registra un risultato settlement realizzato e rende ispezionabile
        la commissione applicata:
        - commissione solo su pnl positivo
        - nessuna commissione su pnl <= 0
        - mercato: commissione su market-net realizzato (non per singolo leg positivo)
        """
        with self._lock:
            gross = float(gross_pnl or 0.0)
            market_key = str(market_id or "").strip()
            if not market_key:
                raise ValueError("market_id is required for record_realized_settlement")
            legacy_market_key = "__GLOBAL__"
            if market_key != legacy_market_key and market_key not in self.state.market_commission_ledger:
                legacy_row = self.state.market_commission_ledger.get(legacy_market_key)
                if isinstance(legacy_row, dict):
                    non_legacy_market_keys = [
                        str(k)
                        for k in self.state.market_commission_ledger.keys()
                        if str(k) and str(k) != legacy_market_key
                    ]
                    if not non_legacy_market_keys:
                        self.state.market_commission_ledger[market_key] = {
                            "gross": float(legacy_row.get("gross", 0.0) or 0.0),
                            "commission": float(legacy_row.get("commission", 0.0) or 0.0),
                        }
                        self.state.market_commission_ledger.pop(legacy_market_key, None)
            realized = self._market_net_realized_aggregator.apply(
                market_id=market_key,
                gross_pnl=gross,
            )
            commission = float(realized["commission_amount"])
            net = float(realized["net_pnl"])

            self.state.realized_pnl += net
            self.state.realized_commission += commission
            self.state.balance += net
            settlement = {
                "market_id": market_id,
                "gross_pnl": gross,
                "commission_amount": commission,
                "net_pnl": net,
                "commission_pct": float(realized["commission_pct"]),
                "market_net_gross": float(realized["market_net_gross"]),
                "market_commission_amount_total": float(realized["market_commission_amount_total"]),
                "settlement_basis": str(realized["settlement_basis"]),
                "settlement_source": "simulation_broker",
                "settlement_kind": "realized_settlement",
                "realized_pnl": self.state.realized_pnl,
                "realized_commission": self.state.realized_commission,
            }
            # legacy alias retained for downstream compatibility
            settlement["pnl"] = settlement["net_pnl"]
            self.state.last_settlement = {
                "gross_pnl": float(settlement["gross_pnl"]),
                "commission_amount": float(settlement["commission_amount"]),
                "net_pnl": float(settlement["net_pnl"]),
                "commission_pct": float(settlement["commission_pct"]),
                "settlement_source": str(settlement["settlement_source"]),
                "settlement_kind": str(settlement["settlement_kind"]),
            }
            return settlement

    # =========================================================
    # UTILS
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
            "connected": self.connected,
            "simulated": True,
            "balance": self.state.balance,
            "exposure": self.state.exposure,
            "starting_balance": self.state.starting_balance,
            "commission_pct": self.state.commission_pct,
            "realized_pnl": self.state.realized_pnl,
            "realized_commission": self.state.realized_commission,
            "unrealized_pnl": self.state.unrealized_pnl,
            "last_settlement": dict(self.state.last_settlement),
            "orders": [o.to_dict() for o in self.state.orders.values()],
            "tracked_markets": list(self.state.market_books.keys()),
            "tracked_events": list(self.state.event_index.keys()),
            "open_positions": [ledger.snapshot().__dict__ for ledger in self.state.position_ledgers.values()],
        }

    def reset(self, starting_balance: float | None = None) -> Dict[str, Any]:
        with self._lock:
            new_balance = float(
                starting_balance
                if starting_balance is not None
                else self.state.starting_balance
            )
            self.state = SimulationState(
                starting_balance=new_balance,
                commission_pct=self.state.commission_pct,
            )
            self._market_net_realized_aggregator = MarketNetRealizedSettlementAggregator(
                commission_pct=self.state.commission_pct,
                context="simulation_broker_settlement",
            )
            self._market_net_realized_aggregator.ledger = self.state.market_commission_ledger
            return {
                "ok": True,
                "starting_balance": new_balance,
                "simulated": True,
            }
