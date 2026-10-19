"""Corsie Dutching e ``manual_bet`` dormienti finche' non e' chiusa la #523 (verifica owner pre-merge #522).

La #522 ha rinviato due P1 preesistenti (lato obbligatorio sulle selezioni Dutching, barriera cashout e fence
d'ammissione su Dutching e ``manual_bet``) perche' oggi nessun percorso operativo li raggiunge. Questo test
e' cio' che lo rende vero nel tempo: se il codice di produzione inizia a pubblicare ``REQ_PLACE_DUTCHING``
o a chiamare ``manual_bet``/``submit_dutching``/``precheck``, fallisce e rimanda alla #523, che chiede la
correzione e i test PRIMA dell'attivazione. Analisi ``ast``, non grep: commenti e docstring non contano.
"""
from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = [pytest.mark.guardrail]

RADICE = pathlib.Path(__file__).resolve().parents[2]
FUORI = {".git", ".venv", "venv", "build", "dist", "tests", "__pycache__", "node_modules"}
CONTROLLER = pathlib.Path("controllers/dutching_controller.py")
METODI_DORMIENTI = {"manual_bet", "submit_dutching", "precheck"}
# L'unico inoltro ammesso: il RiskMiddleware gira REQ_PLACE_DUTCHING (che nessuno pubblica) in CMD_PLACE_DUTCHING.
INOLTRI_AMMESSI = {("CMD_PLACE_DUTCHING", pathlib.Path("core/risk_middleware.py"))}


def _sorgenti():
    for percorso in sorted(RADICE.rglob("*.py")):
        relativo = percorso.relative_to(RADICE)
        if FUORI.intersection(relativo.parts):
            continue
        yield relativo, ast.parse(percorso.read_text(encoding="utf-8"), filename=str(relativo))


def _testo(nodo):
    return nodo.value if isinstance(nodo, ast.Constant) and isinstance(nodo.value, str) else None


def test_nessun_codice_di_produzione_pubblica_la_corsia_dutching():
    violazioni = []
    for relativo, albero in _sorgenti():
        for nodo in ast.walk(albero):
            if not (isinstance(nodo, ast.Call) and isinstance(nodo.func, ast.Attribute)
                    and nodo.func.attr == "publish" and nodo.args):
                continue
            topic = _testo(nodo.args[0])
            if topic in {"REQ_PLACE_DUTCHING", "CMD_PLACE_DUTCHING"} and (topic, relativo) not in INOLTRI_AMMESSI:
                violazioni.append(f"{relativo}:{nodo.lineno} publish({topic!r})")
    assert violazioni == [], (
        "Corsia Dutching attivata: prima di attivarla chiudere la #523 (lato obbligatorio, barriera cashout, "
        f"fence d'ammissione, test RED/PASS+BLOCK): {violazioni}")


def test_nessun_codice_di_produzione_chiama_manual_bet_o_il_submit_dutching():
    violazioni = []
    for relativo, albero in _sorgenti():
        if relativo == CONTROLLER:
            continue                     # le chiamate interne del controller restano dentro la corsia dormiente
        for nodo in ast.walk(albero):
            if (isinstance(nodo, ast.Call) and isinstance(nodo.func, ast.Attribute)
                    and nodo.func.attr in METODI_DORMIENTI):
                violazioni.append(f"{relativo}:{nodo.lineno} .{nodo.func.attr}()")
    assert violazioni == [], (
        "manual_bet/submit_dutching/precheck chiamati da codice di produzione: prima chiudere la #523 "
        f"(barriera cashout, fence d'ammissione, lato obbligatorio, esposizione del manuale): {violazioni}")


def test_il_controllo_rileva_davvero_un_publisher_e_un_chiamante():
    """Il test non e' verde per costruzione: un sorgente con le due violazioni viene riconosciuto."""
    albero = ast.parse("bus.publish('REQ_PLACE_DUTCHING', {})\ncontroller.manual_bet({})\n")
    chiamate = [n for n in ast.walk(albero) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
    assert any(c.func.attr == "publish" and _testo(c.args[0]) == "REQ_PLACE_DUTCHING" for c in chiamate)
    assert any(c.func.attr in METODI_DORMIENTI for c in chiamate)
