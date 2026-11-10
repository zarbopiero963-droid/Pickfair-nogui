"""Test di SICUREZZA EFFETTIVA del gate di rilascio PF-RELEASE-GATE (#625, #626).

NON sono raccolti dalla CI ordinaria (il nome non segue `test_*.py`): li esegue solo `scripts/release_gate.py`, che
pretende che OGNI test di `REQUIRED_TESTS` sia raccolto, eseguito e superato. Nessun xfail, skip o filtro: un test
mancante, saltato o deselezionato rende il gate rosso.

Criterio (OWNER GO PF-OWNER-626-SECURITY-HARDENING-20261110, D04): con la scatola nera NON disponibile il launcher non
deve raggiungere NESSUN componente capace di operare. Non basta che la finestra non si apra o che il processo esca con
errore: una finta BB-1G che costruisce il motore per un'altra strada e poi esce con 1 deve essere rifiutata.
`assert_launcher_is_safe` sostituisce, A LIVELLO DI CLASSE (quindi qualunque sia il percorso d'import), costruttori e
avvio dei componenti operativi (`OPERATIVE_ENTRY_POINTS`) con sentinelle che registrano l'accesso e lo bloccano.

Oggi la GUI costruisce il motore senza scatola nera, quindi il gate e' rosso (nessun eseguibile); dopo BB-1G i test
passano solo se il rifiuto sicuro e' vero. Nessun test apre finestre, avvia il runtime o contatta Betfair.
"""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager

# (modulo, classe, metodo): raggiungere uno di questi punti senza scatola nera = percorso operativo non protetto.
OPERATIVE_ENTRY_POINTS = (
    ("core.runtime_controller", "RuntimeController", "__init__"),
    ("core.runtime_controller", "RuntimeController", "start"),
    ("core.trading_engine", "TradingEngine", "__init__"),
    ("headless_main", "HeadlessApp", "__init__"),
    ("mini_gui", "MiniPickfairGUI", "__init__"),
    ("betfair_client", "BetfairClient", "__init__"),
)


class OperativePathReached(BaseException):
    """BaseException: un `except Exception` del launcher non puo' nasconderla e trasformarla in un rc != 0 pulito."""


@contextmanager
def operative_paths_guarded(monkeypatch):
    reached = []
    for module_name, class_name, method in OPERATIVE_ENTRY_POINTS:
        cls = getattr(importlib.import_module(module_name), class_name)

        def sentinel(self, *args, _where=f"{module_name}.{class_name}.{method}", **kwargs):
            reached.append(_where)
            raise OperativePathReached(_where)

        monkeypatch.setattr(cls, method, sentinel)
    yield reached


def blackbox_unavailable(monkeypatch):
    from core import blackbox as bb
    from core import blackbox_runtime

    def refuse(*args, **kwargs):
        raise bb.BlackBoxUnavailable("BLACKBOX_DIR_INSECURE: simulazione del gate di rilascio")

    monkeypatch.setattr(blackbox_runtime, "install", refuse)


def assert_launcher_is_safe(monkeypatch, launcher, argv):
    """Con la scatola nera non disponibile `launcher()` non raggiunge alcun punto operativo e termina con errore."""
    blackbox_unavailable(monkeypatch)
    monkeypatch.setattr(sys, "argv", list(argv))
    with operative_paths_guarded(monkeypatch) as reached:
        try:
            rc = launcher()
        except OperativePathReached:
            rc = None
    assert reached == [], f"percorso operativo raggiunto senza scatola nera: {reached}"
    assert rc not in (0, None), f"il launcher deve terminare con errore senza scatola nera, rc={rc!r}"


def test_headless_entrypoint_never_builds_the_runtime_without_the_blackbox(monkeypatch):
    import headless_main

    assert_launcher_is_safe(monkeypatch, headless_main.main, ["pickfair-headless", "--headless", "--simulation"])


def test_gui_entrypoint_never_builds_the_runtime_without_the_blackbox(monkeypatch):
    import mini_gui

    assert_launcher_is_safe(monkeypatch, mini_gui.main, ["pickfair"])


def test_default_launcher_main_py_never_builds_the_runtime_without_the_blackbox(monkeypatch):
    import main

    assert_launcher_is_safe(monkeypatch, main.main, ["pickfair"])
