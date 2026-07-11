"""Token di generazione per l'auto-clear del CSV (chiude la race #2).

Problema: l'auto-clear gira su un `threading.Timer`; se un nuovo segnale arriva
mentre il timer del segnale precedente è già partito, `Timer.cancel()` non può
fermare un callback già avviato e il vecchio clear cancellerebbe il segnale
appena scritto.

Soluzione: ogni nuovo segnale chiama `begin()` e ottiene una "generazione".
Il clear ricorda la generazione per cui è stato programmato ed esegue lo
svuotamento solo se è ancora quella corrente (`clear_if_current`), sotto lock,
così non può mai cancellare un segnale più recente.
"""

import threading


class SignalGate:
    def __init__(self):
        self._lock = threading.Lock()
        self._gen = 0

    def begin(self) -> int:
        """Registra un nuovo segnale e restituisce la sua generazione."""
        with self._lock:
            self._gen += 1
            return self._gen

    def is_current(self, gen: int) -> bool:
        with self._lock:
            return gen == self._gen

    def clear_if_current(self, gen: int, action) -> bool:
        """Esegue `action()` (lo svuotamento) solo se `gen` è ancora la
        generazione corrente. Tutto sotto lock: atomico rispetto a `begin()`.
        Ritorna True se ha eseguito l'azione, False se la generazione era
        obsoleta (è arrivato un segnale più recente)."""
        with self._lock:
            if gen == self._gen:
                action()
                return True
            return False
