from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Dict, List, Optional


logger = logging.getLogger(__name__)


class ShutdownManager:
    """
    Gestore centralizzato shutdown.

    Obiettivi:
    - registrare hook con priorità
    - eseguire hook in ordine
    - evitare doppia esecuzione
    - continuare anche se un hook fallisce
    - compatibile con firme diverse usate nel repo
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._hooks: List[Dict[str, Any]] = []
        self._has_run = False

    # =========================================================
    # INTERNAL
    # =========================================================
    def _normalize_name(self, name: Any) -> str:
        return str(name or "shutdown_hook").strip() or "shutdown_hook"

    def _normalize_priority(self, priority: Any) -> int:
        try:
            return int(priority)
        except Exception:
            return 100

    def _sort_hooks(self) -> None:
        self._hooks.sort(key=lambda x: (int(x["priority"]), str(x["name"])))

    def _register(self, name: str, fn: Callable, priority: int = 100) -> None:
        if not callable(fn):
            raise TypeError("fn deve essere callable")

        item = {
            "name": self._normalize_name(name),
            "fn": fn,
            "priority": self._normalize_priority(priority),
        }

        with self._lock:
            # se stesso nome già esiste, lo sostituiamo
            self._hooks = [h for h in self._hooks if h["name"] != item["name"]]
            self._hooks.append(item)
            self._sort_hooks()

    # =========================================================
    # PUBLIC REGISTER API
    # =========================================================
    def register(self, name: str, fn: Callable, priority: int = 100) -> None:
        self._register(name, fn, priority)

    def register_shutdown_hook(self, *args, **kwargs) -> None:
        """
        Compatibile con vari stili:
        - register_shutdown_hook("name", fn, priority=10)
        - register_shutdown_hook(fn)
        - register_shutdown_hook(fn, priority=10)
        """
        if not args:
            raise TypeError("register_shutdown_hook richiede almeno un argomento")

        priority = kwargs.get("priority", 100)

        if callable(args[0]):
            fn = args[0]
            name = getattr(fn, "__name__", "shutdown_hook")
            self._register(name, fn, priority)
            return

        if len(args) >= 2 and callable(args[1]):
            name = args[0]
            fn = args[1]
            self._register(str(name), fn, priority)
            return

        raise TypeError("Firma non valida per register_shutdown_hook")

    # =========================================================
    # EXECUTION
    # =========================================================
    def shutdown(self) -> List[Dict[str, Any]]:
        with self._lock:
            if self._has_run:
                return []
            self._has_run = True
            hooks = list(self._hooks)

        results: List[Dict[str, Any]] = []

        for item in hooks:
            name = str(item["name"])
            fn = item["fn"]

            try:
                logger.info("Shutdown hook start: %s", name)
                fn()
                logger.info("Shutdown hook done: %s", name)
                results.append(
                    {
                        "name": name,
                        "ok": True,
                        "error": "",
                    }
                )
            except Exception as exc:
                logger.exception("Shutdown hook failed: %s", name)
                results.append(
                    {
                        "name": name,
                        "ok": False,
                        "error": str(exc),
                    }
                )

        return results

    def run(self) -> List[Dict[str, Any]]:
        """
        Alias compatibilità.
        """
        return self.shutdown()

    # =========================================================
    # MANAGEMENT
    # =========================================================
    def remove(self, name: str) -> bool:
        name = self._normalize_name(name)
        with self._lock:
            before = len(self._hooks)
            self._hooks = [h for h in self._hooks if h["name"] != name]
            return len(self._hooks) != before

    def clear(self) -> None:
        with self._lock:
            self._hooks.clear()
            self._has_run = False

    # =========================================================
    # STATUS
    # =========================================================
    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "name": str(h["name"]),
                    "priority": int(h["priority"]),
                }
                for h in self._hooks
            ]

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "registered_hooks": len(self._hooks),
                "has_run": bool(self._has_run),
                "hooks": self.snapshot(),
            }