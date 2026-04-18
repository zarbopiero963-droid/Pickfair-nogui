from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional

try:
    from dutching import calculate_dutching
except ImportError:
    # compat legacy guardrail
    from dutching import calculate_dutching_stakes as _calculate_dutching_stakes

    def calculate_dutching(selections, total_stake, commission=4.5):
        odds = [float(s["price"]) for s in selections]
        commission_value = float(commission if commission is not None else 4.5)
        res = _calculate_dutching_stakes(
            odds,
            float(total_stake),
            commission=commission_value,
            commission_aware=True,
        )
        stakes = res.get("stakes", []) or []
        profits = res.get("profits", []) or []
        net_profits = res.get("net_profits", []) or []
        avg_profit = float(res.get("avg_profit", 0.0) or 0.0)
        avg_net_profit = float(res.get("avg_net_profit", avg_profit) or avg_profit)
        book_pct = float(res.get("book_pct", 0.0) or 0.0)

        results = []
        for idx, selection in enumerate(selections):
            side = str(selection.get("side") or selection.get("effectiveType") or "BACK").upper()
            item = {
                "selectionId": int(selection["selectionId"]),
                "price": float(selection["price"]),
                "stake": float(stakes[idx]) if idx < len(stakes) else 0.0,
                "side": side,
                "runnerName": selection.get("runnerName", ""),
                "profitIfWins": float(profits[idx]) if idx < len(profits) else 0.0,
                "profitIfWinsNet": (
                    float(net_profits[idx]) if idx < len(net_profits) else 0.0
                ),
            }
            if side == "LAY":
                item["liability"] = round(
                    float(item["stake"]) * max(0.0, float(item["price"]) - 1.0),
                    2,
                )
            results.append(item)

        return results, avg_profit, book_pct, avg_net_profit


logger = logging.getLogger(__name__)


class DutchingController:
    """
    Controller headless per dutching, con contract stabile.

    API pubbliche:
    - validate(payload)
    - preview(payload)
    - precheck(payload)
    - submit_dutching(payload, dry_run=False, preflight=False)
    - execute(payload)  # alias compatibile
    - manual_bet(payload)
    - check_duplicate(payload)
    """

    def __init__(self, bus, runtime_controller):
        self.bus = bus
        self.runtime = runtime_controller
        self._recent_batches: Dict[str, float] = {}
        self._batch_ttl_seconds = 6 * 60 * 60

    # =========================================================
    # HELPERS
    # =========================================================
    def _ok(self, **kwargs) -> Dict[str, Any]:
        out = {"ok": True}
        out.update(kwargs)
        return out

    def _fail(self, error: str, **kwargs) -> Dict[str, Any]:
        out = {"ok": False, "error": str(error)}
        out.update(kwargs)
        return out

    def _safe_publish(self, event_name: str, payload: Dict[str, Any]) -> None:
        if self.bus is None or not hasattr(self.bus, "publish"):
            return
        self.bus.publish(event_name, payload)

    def _publish_audit(self, event_name: str, payload: Dict[str, Any]) -> None:
        try:
            self._safe_publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish audit event %s", event_name)

    def _cleanup_batches(self) -> None:
        now = time.time()
        expired = [
            batch_id
            for batch_id, ts in self._recent_batches.items()
            if now - ts > self._batch_ttl_seconds
        ]
        for batch_id in expired:
            self._recent_batches.pop(batch_id, None)

    def _build_batch_id(self, payload: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
        normalized = {
            "market_id": str(payload.get("market_id") or ""),
            "event_name": str(payload.get("event_name") or ""),
            "market_name": str(payload.get("market_name") or ""),
            "simulation_mode": bool(payload.get("simulation_mode", False)),
            "legs": [
                {
                    "selectionId": int(item["selectionId"]),
                    "price": float(item["price"]),
                    "stake": float(item["stake"]),
                    "side": str(item.get("side", "BACK")).upper(),
                }
                for item in results
            ],
        }
        raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _build_event_key(self, payload: Dict[str, Any], results: List[Dict[str, Any]]) -> str:
        market_id = str(payload.get("market_id") or "")
        event_name = str(payload.get("event_name") or "")
        market_name = str(payload.get("market_name") or "")
        selection_part = ",".join(
            str(int(item["selectionId"]))
            for item in sorted(results, key=lambda x: int(x["selectionId"]))
        )
        base = f"dutching|{market_id}|{event_name}|{market_name}|{selection_part}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def _duplication_guard(self):
        return getattr(self.runtime, "duplication_guard", None)

    def _table_manager(self):
        return getattr(self.runtime, "table_manager", None)

    def _config(self):
        return getattr(self.runtime, "config", None)

    def _mode(self):
        return getattr(self.runtime, "mode", None)

    def _risk_desk(self):
        return getattr(self.runtime, "risk_desk", None)

    def _batch_manager(self):
        return getattr(self.runtime, "dutching_batch_manager", None)

    def _table_total_exposure(self) -> float:
        table_manager = self._table_manager()
        if table_manager and hasattr(table_manager, "total_exposure"):
            try:
                return float(table_manager.total_exposure() or 0.0)
            except Exception:
                return 0.0
        return 0.0

    def _event_current_exposure(self, event_key: str) -> float:
        table_manager = self._table_manager()
        if table_manager and hasattr(table_manager, "find_by_event_key"):
            try:
                table = table_manager.find_by_event_key(event_key)
                if table:
                    return float(getattr(table, "current_exposure", 0.0) or 0.0)
            except Exception:
                return 0.0
        return 0.0

    def _bankroll_current(self) -> float:
        risk_desk = self._risk_desk()
        if risk_desk:
            return float(getattr(risk_desk, "bankroll_current", 0.0) or 0.0)
        return 0.0

    def _compute_order_exposure(self, item: Dict[str, Any]) -> float:
        side = str(item.get("side", "BACK")).upper()
        stake = float(item.get("stake", 0.0) or 0.0)
        price = float(item.get("price", 0.0) or 0.0)

        if side == "LAY":
            if "liability" in item:
                return max(0.0, float(item.get("liability", 0.0) or 0.0))
            return max(0.0, stake * max(0.0, price - 1.0))

        return max(0.0, stake)

    def _compute_batch_exposure(self, results: List[Dict[str, Any]]) -> float:
        return sum(self._compute_order_exposure(item) for item in results)

    def _allocate_table(
        self,
        event_key: str,
        batch_exposure: float,
        meta: Dict[str, Any],
    ) -> Optional[int]:
        table_manager = self._table_manager()
        config = self._config()

        if table_manager is None:
            return None

        allow_recovery = bool(getattr(config, "allow_recovery", True)) if config else True

        table = None
        if hasattr(table_manager, "allocate"):
            table = table_manager.allocate(event_key=event_key, allow_recovery=allow_recovery)

        if table is None:
            return None

        if hasattr(table_manager, "activate"):
            table_manager.activate(
                table_id=table.table_id,
                event_key=event_key,
                exposure=float(batch_exposure),
                market_id=str(meta.get("market_id") or ""),
                selection_id=None,
                meta=meta,
            )

        return int(table.table_id)

    def _release_table_and_key(self, table_id: Optional[int], event_key: str) -> None:
        duplication_guard = self._duplication_guard()
        table_manager = self._table_manager()

        if duplication_guard and event_key:
            try:
                duplication_guard.release(event_key)
            except Exception:
                logger.exception("Errore release duplication key")

        if table_manager and table_id:
            try:
                if hasattr(table_manager, "force_unlock"):
                    table_manager.force_unlock(int(table_id))
            except Exception:
                logger.exception("Errore force_unlock table")

    def _runtime_active(self) -> bool:
        mode = self._mode()
        return bool(mode and str(getattr(mode, "value", mode)) == "ACTIVE")

    def _bus_available(self) -> bool:
        return self.bus is not None and hasattr(self.bus, "publish")

    def _normalize_side(self, value: Any) -> str:
        side = str(value or "BACK").upper().strip()
        return side if side in {"BACK", "LAY"} else "BACK"

    def _resolve_selection_side(self, selection: Dict[str, Any]) -> str:
        raw = (selection or {}).get("side") or (selection or {}).get("effectiveType") or "BACK"
        return self._normalize_side(raw)

    def _resolve_commission_pct(self, payload: Dict[str, Any]) -> float:
        if "commission" in payload:
            try:
                return max(0.0, float(payload.get("commission", 4.5) or 0.0))
            except Exception:
                return 4.5
        return 4.5

    def _calculate_dutching_with_commission(
        self, payload: Dict[str, Any]
    ) -> tuple[List[Dict[str, Any]], float, float, float]:
        normalized_selections: List[Dict[str, Any]] = []
        for selection in list(payload.get("selections") or []):
            item = dict(selection or {})
            item["side"] = self._resolve_selection_side(item)
            normalized_selections.append(item)

        commission_pct = self._resolve_commission_pct(payload)
        try:
            calc_out = calculate_dutching(
                normalized_selections,
                float(payload["total_stake"]),
                commission=commission_pct,
            )
        except TypeError:
            # Compat path for test doubles/legacy callables without commission argument.
            calc_out = calculate_dutching(
                normalized_selections,
                float(payload["total_stake"]),
            )

        if isinstance(calc_out, tuple) and len(calc_out) >= 4:
            results, avg_profit, book_pct, avg_net_profit = calc_out[:4]
        elif isinstance(calc_out, tuple) and len(calc_out) == 3:
            results, avg_profit, book_pct = calc_out
            avg_net_profit = float(avg_profit)
        else:
            raise ValueError("Formato output calculate_dutching non valido")

        return (
            results,
            float(avg_profit),
            float(book_pct),
            float(avg_net_profit),
        )

    def _dutching_model(self, results: List[Dict[str, Any]]) -> str:
        sides = {
            self._normalize_side(item.get("side", "BACK"))
            for item in (results or [])
            if isinstance(item, dict)
        }
        if len(sides) == 1:
            side = next(iter(sides))
            return f"{side}_EQUAL_PROFIT_FIXED_TOTAL_STAKE"
        return "UNSPECIFIED"

    def _lay_liability_metrics(self, results: List[Dict[str, Any]]) -> Dict[str, float]:
        liabilities: List[float] = []
        for item in results or []:
            side = self._normalize_side((item or {}).get("side", "BACK"))
            if side != "LAY":
                continue
            try:
                liabilities.append(max(0.0, float((item or {}).get("liability", 0.0) or 0.0)))
            except Exception:
                continue
        if not liabilities:
            return {
                "lay_total_liability": 0.0,
                "lay_worst_case_liability": 0.0,
            }
        return {
            "lay_total_liability": float(sum(liabilities)),
            "lay_worst_case_liability": float(max(liabilities)),
        }

    def _min_net_profit(self, results: List[Dict[str, Any]], avg_net_profit: float) -> float:
        net_values: List[float] = []
        for item in results:
            if "profitIfWinsNet" not in item:
                continue
            try:
                net_values.append(float(item.get("profitIfWinsNet", 0.0) or 0.0))
            except Exception:
                continue
        if net_values and len(net_values) == len(results):
            return min(net_values)
        return float(avg_net_profit)

    def _batch_manager_create(
        self,
        batch_id: str,
        event_key: str,
        payload: Dict[str, Any],
        orders: List[Dict[str, Any]],
    ) -> None:
        batch_manager = self._batch_manager()
        if batch_manager is None:
            return

        if hasattr(batch_manager, "create_batch"):
            batch_manager.create_batch(
                batch_id=batch_id,
                event_key=event_key,
                market_id=str(payload.get("market_id") or ""),
                legs=[
                    {
                        "selectionId": int(o["selection_id"]),
                        "price": float(o["price"]),
                        "stake": float(o["stake"]),
                        "side": str(o["bet_type"]).upper(),
                    }
                    for o in orders
                ],
            )

    def _batch_manager_mark_failed(self, batch_id: str, error: str) -> None:
        batch_manager = self._batch_manager()
        if batch_manager is None:
            return

        if hasattr(batch_manager, "mark_batch_failed"):
            batch_manager.mark_batch_failed(batch_id=batch_id, error=error)
            return

        if hasattr(batch_manager, "fail_batch"):
            batch_manager.fail_batch(batch_id=batch_id, error=error)

    # =========================================================
    # VALIDAZIONE
    # =========================================================
    def validate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not isinstance(payload, dict):
                return self._fail("Payload non valido")

            market_id = payload.get("market_id")
            selections = payload.get("selections", [])
            total_stake = float(payload.get("total_stake", 0) or 0)

            if not market_id:
                return self._fail("market_id mancante")

            if not isinstance(selections, list) or not selections:
                return self._fail("Nessuna selezione")

            seen_selection_ids = set()

            for idx, selection in enumerate(selections, start=1):
                if not isinstance(selection, dict):
                    return self._fail(f"Selezione #{idx} non valida")

                if "selectionId" not in selection:
                    return self._fail(f"selectionId mancante alla selezione #{idx}")

                if "price" not in selection:
                    return self._fail(f"price mancante alla selezione #{idx}")

                try:
                    selection_id = int(selection["selectionId"])
                except Exception:
                    return self._fail(f"selectionId non valido alla selezione #{idx}")

                if selection_id in seen_selection_ids:
                    return self._fail(f"selectionId duplicato: {selection_id}")
                seen_selection_ids.add(selection_id)

                try:
                    price = float(selection["price"])
                except Exception:
                    return self._fail(f"price non valido alla selezione #{idx}")

                if price <= 1.01:
                    return self._fail(f"Quota non valida alla selezione #{idx}: {price}")

                if "side" in selection:
                    side = self._resolve_selection_side(selection)
                    if side not in {"BACK", "LAY"}:
                        return self._fail(f"side non valido alla selezione #{idx}: {side}")

            if total_stake <= 0:
                return self._fail("total_stake non valido")

            return self._ok()
        except Exception as exc:
            return self._fail(str(exc))

    # =========================================================
    # PREVIEW / DRY RUN
    # =========================================================
    def preview(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            validation = self.validate(payload)
            if not validation["ok"]:
                return validation

            results, avg_profit, book_pct, avg_net_profit = (
                self._calculate_dutching_with_commission(payload)
            )

            if not isinstance(results, list):
                return self._fail("Risultato dutching non valido")

            event_key = self._build_event_key(payload, results)
            batch_id = self._build_batch_id(payload, results)
            batch_exposure = self._compute_batch_exposure(results)
            min_net_profit = self._min_net_profit(results, avg_net_profit)

            return self._ok(
                dry_run=True,
                preflight=False,
                results=results,
                avg_profit=float(avg_profit),
                avg_profit_net=float(avg_net_profit),
                avg_profit_semantics="gross",
                book_pct=float(book_pct),
                event_key=event_key,
                batch_id=batch_id,
                batch_exposure=round(batch_exposure, 2),
                commission_pct=self._resolve_commission_pct(payload),
                profitable_net=bool(float(min_net_profit) > 0.0),
                dutching_model=self._dutching_model(results),
                **self._lay_liability_metrics(results),
            )
        except Exception as exc:
            logger.exception("Errore preview dutching")
            return self._fail(str(exc))

    # =========================================================
    # PRECHECK RISCHIO / DUPLICATI
    # =========================================================
    def precheck(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        validation = self.validate(payload)
        if not validation["ok"]:
            return validation

        if not self._runtime_active():
            return self._fail("Runtime non attivo")

        try:
            results, avg_profit, book_pct, avg_net_profit = (
                self._calculate_dutching_with_commission(payload)
            )
        except Exception as exc:
            logger.exception("Errore calculate_dutching in precheck")
            return self._fail(str(exc))

        if not results:
            return self._fail("Dutching vuoto")

        event_key = self._build_event_key(payload, results)
        batch_id = self._build_batch_id(payload, results)
        batch_exposure = self._compute_batch_exposure(results)
        min_net_profit = self._min_net_profit(results, avg_net_profit)

        self._cleanup_batches()
        if batch_id in self._recent_batches:
            return self._fail(
                "Batch già inviato (idempotency guard)",
                batch_id=batch_id,
            )

        duplication_guard = self._duplication_guard()
        config = self._config()
        bankroll = self._bankroll_current()
        current_total_exposure = self._table_total_exposure()
        event_current_exposure = self._event_current_exposure(event_key)

        if duplication_guard and bool(getattr(config, "anti_duplication_enabled", True)):
            try:
                if not duplication_guard.acquire(event_key):
                    return self._fail("Duplicato bloccato", event_key=event_key)
            except Exception:
                logger.exception("Errore duplication_guard.acquire")

        if bankroll > 0 and config is not None:
            max_total_exposure = bankroll * (
                float(getattr(config, "max_total_exposure_pct", 35.0)) / 100.0
            )
            max_event_exposure = bankroll * (
                float(getattr(config, "max_event_exposure_pct", 18.0)) / 100.0
            )
            max_single_bet = bankroll * (
                float(getattr(config, "max_single_bet_pct", 18.0)) / 100.0
            )

            if current_total_exposure + batch_exposure > max_total_exposure + 1e-9:
                return self._fail(
                    "Esposizione globale oltre limite",
                    batch_exposure=round(batch_exposure, 2),
                    current_total_exposure=round(current_total_exposure, 2),
                    max_total_exposure=round(max_total_exposure, 2),
                )

            if event_current_exposure + batch_exposure > max_event_exposure + 1e-9:
                return self._fail(
                    "Esposizione evento oltre limite",
                    batch_exposure=round(batch_exposure, 2),
                    event_current_exposure=round(event_current_exposure, 2),
                    max_event_exposure=round(max_event_exposure, 2),
                )

            too_large = [
                {
                    "selectionId": int(item["selectionId"]),
                    "stake": round(float(item["stake"]), 2),
                    "limit": round(max_single_bet, 2),
                }
                for item in results
                if self._compute_order_exposure(item) > max_single_bet + 1e-9
            ]
            if too_large:
                return self._fail(
                    "Una o più gambe superano max_single_bet",
                    violations=too_large,
                )

        return self._ok(
            preflight=True,
            dry_run=False,
            results=results,
            avg_profit=float(avg_profit),
            avg_profit_net=float(avg_net_profit),
            avg_profit_semantics="gross",
            book_pct=float(book_pct),
            event_key=event_key,
            batch_id=batch_id,
            batch_exposure=float(batch_exposure),
            commission_pct=self._resolve_commission_pct(payload),
            profitable_net=bool(float(min_net_profit) > 0.0),
            dutching_model=self._dutching_model(results),
            **self._lay_liability_metrics(results),
        )

    # =========================================================
    # API FINALE
    # =========================================================
    def submit_dutching(
        self,
        payload: Dict[str, Any],
        dry_run: bool = False,
        preflight: bool = False,
    ) -> Dict[str, Any]:
        """
        API finale pubblica stabile.

        Path:
        - dry_run=True  -> preview
        - preflight=True -> precheck
        - default -> execute reale
        """
        if dry_run:
            out = self.preview(payload)
            out.setdefault("dry_run", True)
            out.setdefault("preflight", False)
            return out

        if preflight:
            out = self.precheck(payload)
            out.setdefault("dry_run", False)
            out.setdefault("preflight", True)
            return out

        return self._execute_impl(payload)

    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.submit_dutching(payload, dry_run=False, preflight=False)

    # =========================================================
    # EXECUTE
    # =========================================================
    def _execute_impl(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        pre = self.precheck(payload)
        if not pre["ok"]:
            self._publish_audit(
                "DUTCHING_BATCH_REJECTED",
                {
                    "payload": payload,
                    "reason": pre["error"],
                },
            )
            pre.setdefault("dry_run", False)
            pre.setdefault("preflight", False)
            return pre

        if not self._bus_available():
            return self._fail(
                "EventBus non disponibile",
                dry_run=False,
                preflight=False,
                batch_id=pre.get("batch_id"),
                event_key=pre.get("event_key"),
            )

        results: List[Dict[str, Any]] = pre["results"]
        avg_profit = pre["avg_profit"]
        book_pct = pre["book_pct"]
        event_key = pre["event_key"]
        batch_id = pre["batch_id"]
        batch_exposure = float(pre["batch_exposure"] or 0.0)

        duplication_guard = self._duplication_guard()
        table_id = payload.get("table_id")
        allocated_here = False

        if not table_id:
            table_id = self._allocate_table(
                event_key=event_key,
                batch_exposure=batch_exposure,
                meta={
                    "market_id": payload.get("market_id"),
                    "event_name": payload.get("event_name", ""),
                    "market_name": payload.get("market_name", ""),
                    "type": "dutching_batch",
                    "batch_id": batch_id,
                },
            )
            allocated_here = table_id is not None

        if table_id is None and self._table_manager() is not None:
            msg = "Nessun tavolo disponibile per batch dutching"
            self._publish_audit("DUTCHING_BATCH_REJECTED", {"payload": payload, "reason": msg})
            return self._fail(
                msg,
                dry_run=False,
                preflight=False,
                batch_id=batch_id,
                event_key=event_key,
            )

        orders = []
        published_orders = []
        batch_created = False

        try:
            for idx, item in enumerate(results, start=1):
                order = {
                    "market_id": str(payload["market_id"]),
                    "selection_id": int(item["selectionId"]),
                    "bet_type": str(item.get("side", "BACK")).upper(),
                    "price": float(item["price"]),
                    "stake": float(item["stake"]),
                    "event_name": payload.get("event_name", ""),
                    "market_name": payload.get("market_name", ""),
                    "runner_name": item.get("runnerName", ""),
                    "simulation_mode": bool(payload.get("simulation_mode", False)),
                    "table_id": table_id,
                    "event_key": event_key,
                    "batch_id": batch_id,
                    "batch_size": len(results),
                    "batch_leg_index": idx,
                    "batch_avg_profit": float(avg_profit),
                    "batch_book_pct": float(book_pct),
                    "batch_exposure": float(batch_exposure),
                }
                orders.append(order)

            self._batch_manager_create(batch_id, event_key, payload, orders)
            batch_created = True

            self._publish_audit(
                "DUTCHING_BATCH_APPROVED",
                {
                    "batch_id": batch_id,
                    "event_key": event_key,
                    "table_id": table_id,
                    "count": len(orders),
                    "avg_profit": avg_profit,
                    "book_pct": book_pct,
                    "batch_exposure": round(batch_exposure, 2),
                    "payload": payload,
                },
            )

            for order in orders:
                self.bus.publish("CMD_QUICK_BET", order)
                published_orders.append(order)

            self._recent_batches[batch_id] = time.time()

            return self._ok(
                dry_run=False,
                preflight=False,
                status="SUBMITTED",
                batch_id=batch_id,
                event_key=event_key,
                table_id=table_id,
                orders=orders,
                published_count=len(published_orders),
                count=len(orders),
                avg_profit=float(avg_profit),
                book_pct=float(book_pct),
                batch_exposure=round(batch_exposure, 2),
            )

        except Exception as exc:
            logger.exception("Errore execute dutching batch")

            self._publish_audit(
                "DUTCHING_BATCH_PARTIAL_FAILURE",
                {
                    "batch_id": batch_id,
                    "event_key": event_key,
                    "table_id": table_id,
                    "published_count": len(published_orders),
                    "total_count": len(orders),
                    "error": str(exc),
                },
            )

            if batch_created:
                self._batch_manager_mark_failed(batch_id=batch_id, error=str(exc))

            if allocated_here:
                self._release_table_and_key(table_id, event_key)
            elif duplication_guard:
                try:
                    duplication_guard.release(event_key)
                except Exception:
                    logger.exception("Errore release duplication key after failure")

            return self._fail(
                str(exc),
                dry_run=False,
                preflight=False,
                batch_id=batch_id,
                event_key=event_key,
                table_id=table_id,
                published_count=len(published_orders),
                total_count=len(orders),
            )

    # =========================================================
    # MANUAL BET
    # =========================================================
    def manual_bet(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            required = ["market_id", "selection_id", "price", "stake"]
            for key in required:
                if key not in payload:
                    return self._fail(f"{key} mancante")

            if not self._runtime_active():
                return self._fail("Runtime non attivo")

            if not self._bus_available():
                return self._fail("EventBus non disponibile")

            market_id = str(payload["market_id"])
            selection_id = int(payload["selection_id"])
            price = float(payload["price"])
            stake = float(payload["stake"])

            if price <= 1.01:
                return self._fail("Quota non valida")
            if stake <= 0:
                return self._fail("Stake non valido")

            event_key = str(payload.get("event_key") or f"manual_{market_id}_{selection_id}")
            duplication_guard = self._duplication_guard()
            config = self._config()

            if duplication_guard and bool(getattr(config, "anti_duplication_enabled", True)):
                if not duplication_guard.acquire(event_key):
                    return self._fail("Duplicato bloccato")

            bankroll = self._bankroll_current()
            if bankroll > 0 and config is not None:
                exposure = stake
                current_total_exposure = self._table_total_exposure()
                max_total_exposure = bankroll * (
                    float(getattr(config, "max_total_exposure_pct", 35.0)) / 100.0
                )
                max_single_bet = bankroll * (
                    float(getattr(config, "max_single_bet_pct", 18.0)) / 100.0
                )

                if exposure > max_single_bet + 1e-9:
                    return self._fail("Stake oltre max_single_bet")

                if current_total_exposure + exposure > max_total_exposure + 1e-9:
                    return self._fail("Esposizione globale oltre limite")

            order = {
                "market_id": market_id,
                "selection_id": selection_id,
                "bet_type": self._normalize_side(payload.get("bet_type", "BACK")),
                "price": price,
                "stake": stake,
                "event_name": payload.get("event_name", ""),
                "market_name": payload.get("market_name", ""),
                "runner_name": payload.get("runner_name", ""),
                "simulation_mode": bool(payload.get("simulation_mode", False)),
                "table_id": payload.get("table_id"),
                "event_key": event_key,
            }

            try:
                self.bus.publish("CMD_QUICK_BET", order)
            except Exception:
                if duplication_guard:
                    duplication_guard.release(event_key)
                raise

            self._publish_audit("MANUAL_BET_APPROVED", {"order": order})
            return self._ok(order=order)

        except Exception as exc:
            logger.exception("Errore manual_bet")
            return self._fail(str(exc))

    # =========================================================
    # SOFT CHECK
    # =========================================================
    def check_duplicate(self, payload: Dict[str, Any]) -> bool:
        try:
            pre = self.preview(payload)
            if not pre.get("ok"):
                return False
            event_key = pre.get("event_key", "")
            duplication_guard = self._duplication_guard()
            if duplication_guard and event_key:
                return bool(duplication_guard.is_duplicate(event_key))
            return False
        except Exception:
            return False
