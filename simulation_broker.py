from __future__ import annotations

import uuid
from datetime import datetime
from threading import RLock
from typing import Any, Dict, List, Optional

from core.simulation_matching_engine import SimulationMatchingEngine
from core.simulation_order_book import SimulationOrderBook
from core.simulation_state import SimulationState


class SimulationBroker:
    """
    Broker simulato quasi-live.

    Espone un'interfaccia simile al client reale:
    - login / logout
    - get_account_funds
    - update_market_book
    - place_bet / place_orders
    - list_current_orders
    - cancel_orders
    - settle_bet

    Internamente usa:
    - SimulationOrderBook
    - SimulationMatchingEngine
    - SimulationState

    IMPORTANTE:
    questo broker NON usa denaro reale e NON chiama Betfair.
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
        self._lock = RLock()

        self.starting_balance = float(starting_balance or 0.0)
        self.commission_pct = float(commission_pct or 0.0)
        self.partial_fill_enabled = bool(partial_fill_enabled)
        self.consume_liquidity = bool(consume_liquidity)
        self.db = db

        self.connected = False
        self.session_token = f"SIM-{uuid.uuid4().hex}"

        self.order_book = SimulationOrderBook()
        self.state = SimulationState(starting_balance=self.starting_balance)
        self.matching_engine = SimulationMatchingEngine(
            order_book=self.order_book,
            state=self.state,
            partial_fill_enabled=self.partial_fill_enabled,
            consume_liquidity=self.consume_liquidity,
        )

    # =========================================================
    # INTERNAL DB HELPERS
    # =========================================================
    def _persist_state(self) -> None:
        if self.db and hasattr(self.db, "save_simulation_state"):
            try:
                self.db.save_simulation_state("default", self.state.to_dict())
            except Exception:
                pass

    def _persist_position(self, bet_id: str) -> None:
        if not self.db or not hasattr(self.db, "save_simulation_bet"):
            return

        pos = self.state.get_position(bet_id)
        if not pos:
            return

        payload = {
            "bet_id": pos.bet_id,
            "market_id": pos.market_id,
            "selection_id": pos.selection_id,
            "side": pos.side,
            "price": pos.price,
            "size": pos.size,
            "matched_size": pos.matched_size,
            "avg_price_matched": pos.avg_price_matched,
            "status": pos.status,
            "event_key": pos.event_key,
            "table_id": pos.table_id,
            "batch_id": pos.batch_id,
            "event_name": pos.event_name,
            "market_name": pos.market_name,
            "runner_name": pos.runner_name,
            "created_at": pos.created_at,
            "updated_at": pos.updated_at,
        }

        try:
            self.db.save_simulation_bet(payload)
        except Exception:
            pass

    def _persist_all_open_positions(self) -> None:
        if not self.db or not hasattr(self.db, "save_simulation_bet"):
            return

        for pos in self.state.list_positions():
            self._persist_position(pos.bet_id)

    # =========================================================
    # SESSION
    # =========================================================
    def login(self, password: str = "") -> Dict[str, Any]:
        with self._lock:
            self.connected = True
            self._persist_state()
            return {
                "session_token": self.session_token,
                "expiry": "",
                "simulated": True,
            }

    def logout(self) -> None:
        with self._lock:
            self._persist_all_open_positions()
            self._persist_state()
            self.connected = False

    # =========================================================
    # ACCOUNT / STATUS
    # =========================================================
    def get_account_funds(self) -> Dict[str, float]:
        snap = self.state.snapshot()
        return {
            "available": float(snap.bankroll_available),
            "exposure": float(snap.exposure_open),
            "total": float(snap.equity_current),
            "simulated": True,
        }

    def status(self) -> Dict[str, Any]:
        snap = self.state.snapshot()
        return {
            "connected": bool(self.connected),
            "simulated": True,
            "bankroll_available": float(snap.bankroll_available),
            "exposure_open": float(snap.exposure_open),
            "realized_pnl": float(snap.realized_pnl),
            "unrealized_pnl": float(snap.unrealized_pnl),
            "equity_current": float(snap.equity_current),
            "equity_peak": float(snap.equity_peak),
            "open_positions_count": int(snap.open_positions_count),
        }

    # =========================================================
    # MARKET BOOK
    # =========================================================
    def update_market_book(self, market_id: str, market_book: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            self.order_book.update_market_book(str(market_id), market_book or {})
            reprocessed = self.matching_engine.reprocess_open_orders(str(market_id))

            for item in reprocessed.get("results", []) or []:
                bet_id = str(item.get("bet_id") or "")
                if bet_id:
                    self._persist_position(bet_id)

            self._persist_state()

            return {
                "ok": True,
                "market_id": str(market_id),
                "reprocessed": reprocessed,
                "simulated": True,
            }

    def get_market_book(self, market_id: str) -> Dict[str, Any]:
        return self.order_book.get_market_book(str(market_id))

    # =========================================================
    # ORDER API
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
        table_id: Optional[int] = None,
        batch_id: str = "",
        event_name: str = "",
        market_name: str = "",
        runner_name: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            if not self.connected:
                raise RuntimeError("SimulationBroker non connesso")

            bet_id = f"SIMBET-{uuid.uuid4().hex[:16]}"

            result = self.matching_engine.submit_order(
                bet_id=bet_id,
                market_id=str(market_id),
                selection_id=int(selection_id),
                side=str(side).upper(),
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

            self._persist_position(bet_id)
            self._persist_state()

            instruction_status = "SUCCESS" if result.status != "FAILURE" else "FAILURE"

            return {
                "status": "SUCCESS" if instruction_status == "SUCCESS" else "FAILURE",
                "instructionReports": [
                    {
                        "status": instruction_status,
                        "betId": result.bet_id if instruction_status == "SUCCESS" else "",
                        "sizeMatched": float(result.matched_size),
                        "averagePriceMatched": float(result.average_matched_price),
                        "customerRef": str(customer_ref or ""),
                        "simulated": True,
                        "orderStatus": result.status,
                    }
                ],
                "simulated": True,
                "placed_at": datetime.utcnow().isoformat(),
            }

    def place_orders(
        self,
        *,
        market_id: str,
        instructions: List[Dict[str, Any]],
        customer_ref: str = "",
        event_key: str = "",
        table_id: Optional[int] = None,
        batch_id: str = "",
        event_name: str = "",
        market_name: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            if not self.connected:
                raise RuntimeError("SimulationBroker non connesso")

            reports: List[Dict[str, Any]] = []

            for instr in instructions or []:
                selection_id = int(
                    instr.get("selection_id", instr.get("selectionId"))
                )
                side = str(
                    instr.get("side")
                    or instr.get("bet_type")
                    or instr.get("action")
                    or "BACK"
                ).upper()
                price = float(instr.get("price"))
                size = float(instr.get("size", instr.get("stake")))
                runner_name = str(
                    instr.get("runner_name")
                    or instr.get("runnerName")
                    or instr.get("selection")
                    or ""
                )
                local_customer_ref = str(
                    instr.get("customer_ref")
                    or customer_ref
                    or ""
                )

                single = self.place_bet(
                    market_id=str(market_id),
                    selection_id=selection_id,
                    side=side,
                    price=price,
                    size=size,
                    customer_ref=local_customer_ref,
                    event_key=event_key,
                    table_id=table_id,
                    batch_id=batch_id,
                    event_name=event_name,
                    market_name=market_name,
                    runner_name=runner_name,
                )
                reports.extend(single.get("instructionReports", []))

            overall_ok = all(r.get("status") == "SUCCESS" for r in reports) if reports else False

            self._persist_state()

            return {
                "status": "SUCCESS" if overall_ok else "FAILURE",
                "instructionReports": reports,
                "simulated": True,
                "placed_at": datetime.utcnow().isoformat(),
            }

    def cancel_orders(
        self,
        *,
        market_id: Optional[str] = None,
        instructions: Optional[List[Dict[str, Any]]] = None,
        customer_ref: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            reports: List[Dict[str, Any]] = []

            if instructions:
                for instr in instructions:
                    bet_id = str(instr.get("betId") or instr.get("bet_id") or "")
                    if not bet_id:
                        reports.append(
                            {
                                "status": "FAILURE",
                                "betId": "",
                                "sizeCancelled": 0.0,
                                "customerRef": customer_ref,
                                "simulated": True,
                            }
                        )
                        continue

                    result = self.matching_engine.cancel_order(bet_id)
                    self._persist_position(bet_id)

                    reports.append(
                        {
                            "status": "SUCCESS" if result.get("ok") else "FAILURE",
                            "betId": bet_id,
                            "sizeCancelled": float(result.get("size_cancelled", 0.0) or 0.0),
                            "customerRef": customer_ref,
                            "simulated": True,
                        }
                    )
            else:
                target_market = str(market_id or "")
                for pos in self.state.list_open_positions():
                    if target_market and str(pos.market_id) != target_market:
                        continue

                    result = self.matching_engine.cancel_order(pos.bet_id)
                    self._persist_position(pos.bet_id)

                    reports.append(
                        {
                            "status": "SUCCESS" if result.get("ok") else "FAILURE",
                            "betId": pos.bet_id,
                            "sizeCancelled": float(result.get("size_cancelled", 0.0) or 0.0),
                            "customerRef": customer_ref,
                            "simulated": True,
                        }
                    )

            overall_ok = all(r.get("status") == "SUCCESS" for r in reports) if reports else False
            self._persist_state()

            return {
                "status": "SUCCESS" if overall_ok else "FAILURE",
                "instructionReports": reports,
                "simulated": True,
                "cancelled_at": datetime.utcnow().isoformat(),
            }

    def list_current_orders(
        self,
        market_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            wanted = {str(x) for x in (market_ids or [])}

            current_orders = []
            for pos in self.state.list_positions():
                if wanted and str(pos.market_id) not in wanted:
                    continue

                current_orders.append(
                    {
                        "betId": pos.bet_id,
                        "marketId": pos.market_id,
                        "selectionId": pos.selection_id,
                        "side": pos.side,
                        "priceSize": {
                            "price": float(pos.price),
                            "size": float(pos.size),
                        },
                        "sizeMatched": float(pos.matched_size),
                        "averagePriceMatched": float(pos.avg_price_matched),
                        "status": pos.status,
                        "customerRef": str(pos.notes.get("customer_ref", "") or ""),
                        "placedDate": pos.created_at,
                        "simulated": True,
                    }
                )

            return {
                "currentOrders": current_orders,
                "simulated": True,
            }

    # =========================================================
    # SETTLEMENT / PNL
    # =========================================================
    def settle_bet(self, *, bet_id: str, pnl: float) -> Dict[str, Any]:
        with self._lock:
            result = self.matching_engine.settle_position(str(bet_id), float(pnl or 0.0))
            self._persist_position(str(bet_id))
            self._persist_state()

            result["settled_at"] = datetime.utcnow().isoformat()
            result["simulated"] = True
            return result

    def settle_market(self, *, market_id: str, pnl_by_bet_id: Dict[str, float]) -> Dict[str, Any]:
        with self._lock:
            reports = []
            for pos in self.state.list_positions():
                if str(pos.market_id) != str(market_id):
                    continue

                pnl = float(pnl_by_bet_id.get(pos.bet_id, 0.0) or 0.0)
                result = self.matching_engine.settle_position(pos.bet_id, pnl)
                self._persist_position(pos.bet_id)
                reports.append(result)

            self._persist_state()

            return {
                "market_id": str(market_id),
                "reports": reports,
                "simulated": True,
                "settled_at": datetime.utcnow().isoformat(),
            }

    # =========================================================
    # SNAPSHOT / RESET
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "broker": "SimulationBroker",
                "connected": bool(self.connected),
                "session_token": self.session_token,
                "state": self.state.to_dict(),
                "simulated": True,
            }

    def reset(self, *, starting_balance: Optional[float] = None) -> Dict[str, Any]:
        with self._lock:
            if starting_balance is not None:
                self.starting_balance = float(starting_balance or 0.0)

            self.state.reset(starting_balance=self.starting_balance)
            self.order_book = SimulationOrderBook()
            self.matching_engine = SimulationMatchingEngine(
                order_book=self.order_book,
                state=self.state,
                partial_fill_enabled=self.partial_fill_enabled,
                consume_liquidity=self.consume_liquidity,
            )

            self._persist_state()

            return {
                "ok": True,
                "starting_balance": float(self.starting_balance),
                "simulated": True,
                "reset_at": datetime.utcnow().isoformat(),
            }