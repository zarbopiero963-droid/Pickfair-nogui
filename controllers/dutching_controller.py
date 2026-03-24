from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from dutching import calculate_dutching

logger = logging.getLogger(__name__)


class DutchingController:
    """
    Controller headless per dutching.

    Obiettivi:
    - compatibile con dutching.py reale
    - niente dipendenze GUI
    - publish ordini su EventBus
    - anti-duplicazione hard
    - precheck esposizione batch
    - integrazione con RuntimeController / Roserpina
    """

    def __init__(self, bus, runtime_controller):
        self.bus = bus
        self.runtime = runtime_controller
        self._recent_batches: Dict[str, float] = {}
        self._batch_ttl_seconds = 6 * 60 * 60

    # =========================================================
    # HELPERS
    # =========================================================
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
            str(int(item["selectionId"])) for item in sorted(results, key=lambda x: int(x["selectionId"]))
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
        """
        Conservativo:
        - BACK: usa stake
        - LAY: se presente liability usa quella, altrimenti stake * (price - 1)
        """
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

    def _allocate_table(self, event_key: str, batch_exposure: float, meta: Dict[str, Any]) -> Optional[int]:
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
                table_manager.force_unlock(int(table_id))
            except Exception:
                logger.exception("Errore force_unlock table")

    def _runtime_active(self) -> bool:
        mode = self._mode()
        return bool(mode and str(getattr(mode, "value", mode)) == "ACTIVE")

    def _publish_audit(self, event_name: str, payload: Dict[str, Any]) -> None:
        try:
            self.bus.publish(event_name, payload)
        except Exception:
            logger.exception("Errore publish audit event %s", event_name)

    # =========================================================
    # VALIDAZIONE
    # =========================================================
    def validate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not isinstance(payload, dict):
                return {"ok": False, "error": "Payload non valido"}

            market_id = payload.get("market_id")
            selections = payload.get("selections", [])
            total_stake = float(payload.get("total_stake", 0) or 0)

            if not market_id:
                return {"ok": False, "error": "market_id mancante"}

            if not isinstance(selections, list) or not selections:
                return {"ok": False, "error": "Nessuna selezione"}

            seen_selection_ids = set()

            for idx, selection in enumerate(selections, start=1):
                if not isinstance(selection, dict):
                    return {"ok": False, "error": f"Selezione #{idx} non valida"}

                if "selectionId" not in selection:
                    return {"ok": False, "error": f"selectionId mancante alla selezione #{idx}"}

                if "price" not in selection:
                    return {"ok": False, "error": f"price mancante alla selezione #{idx}"}

                try:
                    selection_id = int(selection["selectionId"])
                except Exception:
                    return {"ok": False, "error": f"selectionId non valido alla selezione #{idx}"}

                if selection_id in seen_selection_ids:
                    return {"ok": False, "error": f"selectionId duplicato: {selection_id}"}
                seen_selection_ids.add(selection_id)

                try:
                    price = float(selection["price"])
                except Exception:
                    return {"ok": False, "error": f"price non valido alla selezione #{idx}"}

                if price <= 1.01:
                    return {"ok": False, "error": f"Quota non valida alla selezione #{idx}: {price}"}

                if "side" in selection:
                    side = str(selection.get("side", "BACK")).upper()
                    if side not in {"BACK", "LAY"}:
                        return {"ok": False, "error": f"side non valido alla selezione #{idx}: {side}"}

            if total_stake <= 0:
                return {"ok": False, "error": "total_stake non valido"}

            return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    # =========================================================
    # PREVIEW
    # =========================================================
    def preview(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            validation = self.validate(payload)
            if not validation["ok"]:
                return validation

            results, avg_profit, book_pct = calculate_dutching(
                payload["selections"],
                float(payload["total_stake"]),
            )

            event_key = self._build_event_key(payload, results)
            batch_exposure = self._compute_batch_exposure(results)

            return {
                "ok": True,
                "results": results,
                "avg_profit": avg_profit,
                "book_pct": book_pct,
                "event_key": event_key,
                "batch_exposure": round(batch_exposure, 2),
            }
        except Exception as exc:
            logger.exception("Errore preview dutching")
            return {"ok": False, "error": str(exc)}

    # =========================================================
    # PRECHECK RISCHIO / DUPLICATI
    # =========================================================
    def precheck(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        validation = self.validate(payload)
        if not validation["ok"]:
            return validation

        if not self._runtime_active():
            return {"ok": False, "error": "Runtime non attivo"}

        try:
            results, avg_profit, book_pct = calculate_dutching(
                payload["selections"],
                float(payload["total_stake"]),
            )
        except Exception as exc:
            logger.exception("Errore calculate_dutching in precheck")
            return {"ok": False, "error": str(exc)}

        if not results:
            return {"ok": False, "error": "Dutching vuoto"}

        event_key = self._build_event_key(payload, results)
        batch_id = self._build_batch_id(payload, results)
        batch_exposure = self._compute_batch_exposure(results)

        self._cleanup_batches()
        if batch_id in self._recent_batches:
            return {"ok": False, "error": "Batch già inviato (idempotency guard)", "batch_id": batch_id}

        duplication_guard = self._duplication_guard()
        config = self._config()
        bankroll = self._bankroll_current()
        current_total_exposure = self._table_total_exposure()
        event_current_exposure = self._event_current_exposure(event_key)

        if duplication_guard and bool(getattr(config, "anti_duplication_enabled", True)):
            try:
                if duplication_guard.is_duplicate(event_key):
                    return {"ok": False, "error": "Duplicato bloccato", "event_key": event_key}
            except Exception:
                logger.exception("Errore duplication_guard.is_duplicate")

        if bankroll > 0 and config is not None:
            max_total_exposure = bankroll * (float(getattr(config, "max_total_exposure_pct", 35.0)) / 100.0)
            max_event_exposure = bankroll * (float(getattr(config, "max_event_exposure_pct", 18.0)) / 100.0)
            max_single_bet = bankroll * (float(getattr(config, "max_single_bet_pct", 18.0)) / 100.0)

            if current_total_exposure + batch_exposure > max_total_exposure + 1e-9:
                return {
                    "ok": False,
                    "error": "Esposizione globale oltre limite",
                    "batch_exposure": round(batch_exposure, 2),
                    "current_total_exposure": round(current_total_exposure, 2),
                    "max_total_exposure": round(max_total_exposure, 2),
                }

            if event_current_exposure + batch_exposure > max_event_exposure + 1e-9:
                return {
                    "ok": False,
                    "error": "Esposizione evento oltre limite",
                    "batch_exposure": round(batch_exposure, 2),
                    "event_current_exposure": round(event_current_exposure, 2),
                    "max_event_exposure": round(max_event_exposure, 2),
                }

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
                return {
                    "ok": False,
                    "error": "Una o più gambe superano max_single_bet",
                    "violations": too_large,
                }

        return {
            "ok": True,
            "results": results,
            "avg_profit": avg_profit,
            "book_pct": book_pct,
            "event_key": event_key,
            "batch_id": batch_id,
            "batch_exposure": batch_exposure,
        }

    # =========================================================
    # EXECUTE
    # =========================================================
    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flusso blindato:
        1. validate
        2. precheck rischio / duplicazione
        3. allocate table (se disponibile)
        4. register duplication key
        5. publish ordini
        6. rollback lock/table se publish fallisce
        """
        pre = self.precheck(payload)
        if not pre["ok"]:
            self._publish_audit(
                "DUTCHING_BATCH_REJECTED",
                {
                    "payload": payload,
                    "reason": pre["error"],
                },
            )
            return pre

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
            return {"ok": False, "error": msg}

        if duplication_guard:
            try:
                duplication_guard.register(event_key)
            except Exception:
                logger.exception("Errore duplication_guard.register")
                if allocated_here:
                    self._release_table_and_key(table_id, event_key)
                return {"ok": False, "error": "Errore registrazione anti-duplicazione"}

        orders = []
        published_orders = []

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

            return {
                "ok": True,
                "batch_id": batch_id,
                "event_key": event_key,
                "table_id": table_id,
                "orders": orders,
                "published_count": len(published_orders),
                "count": len(orders),
                "avg_profit": avg_profit,
                "book_pct": book_pct,
                "batch_exposure": round(batch_exposure, 2),
            }

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

            if allocated_here:
                self._release_table_and_key(table_id, event_key)
            elif duplication_guard:
                try:
                    duplication_guard.release(event_key)
                except Exception:
                    logger.exception("Errore release duplication key after failure")

            return {
                "ok": False,
                "error": str(exc),
                "batch_id": batch_id,
                "event_key": event_key,
                "table_id": table_id,
                "published_count": len(published_orders),
                "total_count": len(orders),
            }

    # =========================================================
    # MANUAL BET
    # =========================================================
    def manual_bet(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            required = ["market_id", "selection_id", "price", "stake"]
            for key in required:
                if key not in payload:
                    return {"ok": False, "error": f"{key} mancante"}

            if not self._runtime_active():
                return {"ok": False, "error": "Runtime non attivo"}

            market_id = str(payload["market_id"])
            selection_id = int(payload["selection_id"])
            price = float(payload["price"])
            stake = float(payload["stake"])

            if price <= 1.01:
                return {"ok": False, "error": "Quota non valida"}
            if stake <= 0:
                return {"ok": False, "error": "Stake non valido"}

            event_key = str(payload.get("event_key") or f"manual_{market_id}_{selection_id}")
            duplication_guard = self._duplication_guard()
            config = self._config()

            if duplication_guard and bool(getattr(config, "anti_duplication_enabled", True)):
                if duplication_guard.is_duplicate(event_key):
                    return {"ok": False, "error": "Duplicato bloccato"}

            bankroll = self._bankroll_current()
            if bankroll > 0 and config is not None:
                exposure = stake
                current_total_exposure = self._table_total_exposure()
                max_total_exposure = bankroll * (float(getattr(config, "max_total_exposure_pct", 35.0)) / 100.0)
                max_single_bet = bankroll * (float(getattr(config, "max_single_bet_pct", 18.0)) / 100.0)

                if exposure > max_single_bet + 1e-9:
                    return {"ok": False, "error": "Stake oltre max_single_bet"}

                if current_total_exposure + exposure > max_total_exposure + 1e-9:
                    return {"ok": False, "error": "Esposizione globale oltre limite"}

            if duplication_guard:
                duplication_guard.register(event_key)

            order = {
                "market_id": market_id,
                "selection_id": selection_id,
                "bet_type": str(payload.get("bet_type", "BACK")).upper(),
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

            self._publish_audit(
                "MANUAL_BET_APPROVED",
                {
                    "order": order,
                },
            )

            return {"ok": True, "order": order}

        except Exception as exc:
            logger.exception("Errore manual_bet")
            return {"ok": False, "error": str(exc)}

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