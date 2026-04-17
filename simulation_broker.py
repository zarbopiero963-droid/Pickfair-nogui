from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from trading_config import enforce_betfair_italy_commission_pct

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
        self.commission_pct = float(commission_pct)
        self.realized_pnl = 0.0
        self.realized_commission = 0.0
        self.last_settlement: Dict[str, float | str] = {}
        self.orders: Dict[str, SimOrder] = {}
        self.market_books: Dict[str, Dict[str, Any]] = {}
        self.event_index: Dict[str, Dict[str, Any]] = {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "starting_balance": self.starting_balance,
            "balance": self.balance,
            "exposure": self.exposure,
            "commission_pct": self.commission_pct,
            "realized_pnl": self.realized_pnl,
            "realized_commission": self.realized_commission,
            "last_settlement": dict(self.last_settlement),
            "orders": {k: v.to_dict() for k, v in self.orders.items()},
            "market_books": self.market_books,
            "event_index": self.event_index,
        }

    def load_from_dict(self, data: Dict[str, Any]) -> None:
        data = dict(data or {})
        self.starting_balance = float(data.get("starting_balance", self.starting_balance))
        self.balance = float(data.get("balance", self.starting_balance))
        self.exposure = float(data.get("exposure", 0.0))
        self.commission_pct = float(data.get("commission_pct", self.commission_pct))
        self.realized_pnl = float(data.get("realized_pnl", 0.0))
        self.realized_commission = float(data.get("realized_commission", 0.0))
        self.last_settlement = dict(data.get("last_settlement") or {})

        self.orders = {}
        for bet_id, payload in (data.get("orders") or {}).items():
            p = dict(payload or {})
            self.orders[bet_id] = SimOrder(
                bet_id=str(p.get("bet_id") or bet_id),
                market_id=str(p.get("market_id") or ""),
                selection_id=int(p.get("selection_id") or 0),
                side=str(p.get("side") or "BACK"),
                price=float(p.get("price") or 0.0),
                size=float(p.get("size") or 0.0),
                matched_size=float(p.get("matched_size") or 0.0),
                avg_price_matched=float(p.get("avg_price_matched") or 0.0),
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

    # =========================================================
    # SESSION
    # =========================================================
    def login(self, password: str = "SIMULATION") -> Dict[str, Any]:
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

        self.state.market_books[market_id] = normalized

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
            result = self.place_bet(
                market_id=str(market_id),
                selection_id=int(item.get("selection_id", item.get("selectionId"))),
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
            if int(r.get("selectionId") or r.get("selection_id") or 0) == int(order.selection_id):
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
        """
        Modellazione semplice:
        - BACK: blocca stake come esposizione
        - LAY: blocca liability
        """
        if order.side == "BACK":
            self.state.balance -= matched
            self.state.exposure += matched
        else:
            liability = matched * max(0.0, price - 1.0)
            self.state.balance -= liability
            self.state.exposure += liability

    def _persist_order(self, order: SimOrder) -> None:
        if self.db and hasattr(self.db, "save_simulation_bet"):
            try:
                self.db.save_simulation_bet(order.to_dict())
            except Exception:
                logger.exception("Errore save_simulation_bet")

    def record_realized_settlement(self, gross_pnl: float) -> Dict[str, Any]:
        """
        Registra un risultato settlement realizzato e rende ispezionabile
        la commissione applicata:
        - commissione solo su pnl positivo
        - nessuna commissione su pnl <= 0
        """
        gross = float(gross_pnl or 0.0)
        commission_pct = enforce_betfair_italy_commission_pct(
            self.state.commission_pct,
            context="simulation_broker_settlement",
        )
        commission = 0.0
        if gross > 0.0 and commission_pct > 0.0:
            commission = gross * (commission_pct / 100.0)
        net = gross - commission

        self.state.realized_pnl += net
        self.state.realized_commission += commission
        self.state.balance += net
        settlement = {
            "gross_pnl": gross,
            "commission_amount": commission,
            "net_pnl": net,
            "commission_pct": float(commission_pct),
            "settlement_source": "simulation_broker",
            "settlement_kind": "realized_settlement",
            "settlement_basis": "market_net_realized",
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
            "settlement_basis": str(settlement["settlement_basis"]),
        }
        return settlement

    # =========================================================
    # UTILS
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        return {
            "connected": self.connected,
            "simulated": True,
            "balance": self.state.balance,
            "exposure": self.state.exposure,
            "starting_balance": self.state.starting_balance,
            "commission_pct": self.state.commission_pct,
            "realized_pnl": self.state.realized_pnl,
            "realized_commission": self.state.realized_commission,
            "last_settlement": dict(self.state.last_settlement),
            "orders": [o.to_dict() for o in self.state.orders.values()],
            "tracked_markets": list(self.state.market_books.keys()),
            "tracked_events": list(self.state.event_index.keys()),
        }

    def reset(self, starting_balance: float | None = None) -> Dict[str, Any]:
        new_balance = float(
            starting_balance
            if starting_balance is not None
            else self.state.starting_balance
        )
        self.state = SimulationState(
            starting_balance=new_balance,
            commission_pct=self.state.commission_pct,
        )
        return {
            "ok": True,
            "starting_balance": new_balance,
            "simulated": True,
        }
