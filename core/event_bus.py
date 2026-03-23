"""
EventBus (Pub/Sub)
Il sistema nervoso centrale dell'applicazione.
Permette ai moduli di comunicare senza conoscersi (Decoupling totale).
"""

__all__ = ["EventBus"]

import logging
import threading

logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self):
        self._subscribers = {}
        self._lock = threading.Lock()

    def subscribe(self, event_type: str, callback: callable):
        """Iscrive una funzione a un determinato tipo di evento."""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            if callback not in self._subscribers[event_type]:
                self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: callable):
        """Rimuove l'iscrizione di una funzione a un evento."""
        with self._lock:
            if event_type in self._subscribers:
                if callback in self._subscribers[event_type]:
                    self._subscribers[event_type].remove(callback)

    def publish(self, event_type: str, data=None):
        """Pubblica un evento. Tutti gli iscritti riceveranno i dati."""
        with self._lock:
            # Creiamo una copia della lista per evitare blocchi o modifiche durante l'iterazione
            callbacks = self._subscribers.get(event_type, []).copy()

        for callback in callbacks:
            try:
                callback(data)
            except Exception as e:
                logger.error(
                    f"[EventBus] Errore nell'esecuzione del subscriber {callback.__name__} per l'evento '{event_type}': {e}"
                )

