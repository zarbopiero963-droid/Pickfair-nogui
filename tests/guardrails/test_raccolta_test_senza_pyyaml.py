"""BLOCK — nessun test importa PyYAML a livello di modulo.

Perche' esiste questo file
--------------------------
PyYAML NON e' installato nel job CI `tests` (installa pytest, pytest-asyncio,
pytest-cov, pytest-xdist e le loro dipendenze — nient'altro). Un `import yaml`
in testa a un modulo di test non fa fallire QUEL test: fa fallire la
**raccolta**, e pytest interrompe l'intero job.

    ERROR collecting tests/guardrails/test_gpt56sol_su_openrouter.py
    E   ModuleNotFoundError: No module named 'yaml'
    !!!! Interrupted: 1 error during collection !!!!
    1 error in 8.31s

Un test in piu' che spegne gli altri 5405 e' peggio del test che non c'era: il
job diventa rosso senza aver eseguito NIENTE, e il rosso non dice quale
invariante e' saltata — dice solo che manca un pacchetto.

E' successo davvero sulla PR #463, in un file scritto nella stessa PR che
modificava `test_ai_review_effort.py`, dove il commento di `_env()` avverte
per iscritto proprio di questo:

    «PyYAML non e' garantito negli ambienti CI di questo repo [...]
    Importarlo in testa faceva fallire la RACCOLTA dell'intero job, non solo
    questo test.»

Il commento c'era e non e' bastato: per questo ora c'e' un test.

Limite dichiarato: questo NON e' un controllo generale sulle dipendenze. Vieta
il solo PyYAML, l'unico modulo per cui esiste la prova che il job CI ne sia
privo. Altri import esterni presenti nell'albero dei test (es. `requests`) in
CI si risolvono — l'errore di raccolta della #463 era UNO, il mio file.

Come leggere un workflow senza PyYAML: `_env()` in `test_ai_review_effort.py`
(regex sull'indentazione, piu' un assert che il parser abbia letto qualcosa).
Se PyYAML serve davvero, lo si importa DENTRO la funzione dentro un
try/except — convenzione gia' usata da `tests/testsuite/test_false_green_semantics.py`.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ALBERO_TEST = ROOT / "tests"


def _import_di_modulo(albero: ast.Module) -> set[str]:
    """Solo gli import a livello di modulo: sono quelli che rompono la raccolta.

    Un import dentro una funzione viene eseguito quando quel test gira, quindi
    al massimo fa fallire il singolo test — non la raccolta dell'intero job.
    """
    nomi: set[str] = set()
    for nodo in albero.body:
        if isinstance(nodo, ast.Import):
            nomi.update(alias.name.split(".")[0] for alias in nodo.names)
        elif isinstance(nodo, ast.ImportFrom) and nodo.level == 0 and nodo.module:
            nomi.add(nodo.module.split(".")[0])
    return nomi


def test_block_nessun_import_pyyaml_a_livello_di_modulo() -> None:
    moduli = sorted(ALBERO_TEST.rglob("*.py"))

    # Zero file scansionati vuol dire che il glob non ha letto NIENTE, non che
    # l'albero sia pulito: senza questa riga il test resterebbe verde per
    # sempre anche dopo uno spostamento della cartella, cioe' sparirebbe
    # restando verde — lo stesso modo di rompersi che sta qui a vietare.
    assert len(moduli) > 100, (
        f"scansionati solo {len(moduli)} file sotto {ALBERO_TEST}: il glob non "
        f"sta piu' leggendo l'albero dei test e questo guardrail non verifica "
        f"piu' niente."
    )

    colpevoli = []
    for modulo in moduli:
        try:
            albero = ast.parse(modulo.read_text(encoding="utf-8"))
        except SyntaxError:
            continue  # non e' questo il test che giudica la sintassi
        if "yaml" in _import_di_modulo(albero):
            colpevoli.append(str(modulo.relative_to(ROOT)))

    assert not colpevoli, (
        "questi moduli di test importano PyYAML a livello di modulo:\n  "
        + "\n  ".join(colpevoli)
        + "\n\nPyYAML non e' installato nel job CI `tests`: l'import in testa "
        "non fa fallire il singolo test, fa fallire la RACCOLTA e pytest "
        "interrompe tutto il job (`Interrupted: 1 error during collection`). "
        "Leggi il workflow senza PyYAML come fa `_env()` in "
        "test_ai_review_effort.py, oppure importa yaml DENTRO la funzione in "
        "un try/except come test_false_green_semantics.py."
    )
