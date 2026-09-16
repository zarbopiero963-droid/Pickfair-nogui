"""BLOCK — PyYAML deve restare una dipendenza di test DICHIARATA.

Questo file sostituisce `test_raccolta_test_senza_pyyaml.py`, che vietava
l'import di PyYAML a livello di modulo. Quel divieto aveva una ragione vera:
PyYAML non era installato nel job CI `tests`, e un `import yaml` in testa non
faceva fallire QUEL test — faceva fallire la **raccolta**, spegnendo l'intero
job::

    ERROR collecting tests/guardrails/test_gpt56sol_su_openrouter.py
    E   ModuleNotFoundError: No module named 'yaml'
    !!!! Interrupted: 1 error during collection !!!!

E' successo davvero sulla #463.

PERCHE' IL DIVIETO E' CADUTO

Perche' il suo presupposto non vale piu': PyYAML e' ora in
`requirements-test.txt`. Non e' stata una comodita' — e' stata la conseguenza
di un difetto misurato.

`tests/testsuite/test_false_green_semantics.py` ispeziona i workflow PARSATI.
Senza PyYAML restituiva un sentinel `{"_text": testo}` e ogni consumatore
scivolava su un ramo testuale: le asserzioni continuavano a girare, ma su un
bersaglio 3,1 volte piu' grande (46.329 caratteri contro 14.770 sui 26 workflow
ispezionati), fatto di commenti, nomi di step, trigger ed env.

La prova che non fosse teoria, per mutazione di `merge-simulation-hard.yml` —
il workflow che deve garantire che la simulazione di merge esegua la suite
completa. Tolto pytest dai blocchi `run:` (`pip install nulla`,
`echo NESSUN_TEST`), lasciando la parola solo nei NOMI degli step, che non
eseguono nulla::

    con PyYAML    ->  assert "pytest" in hard_run   FALLISCE
    senza PyYAML  ->  1 passed

E PyYAML in CI non c'era. Quindi nell'unico ambiente che conta, quel workflow
poteva smettere del tutto di eseguire i test e il guard restava verde.

Ora `_workflow()` e' fail-closed: senza parser solleva. Un controllo che non
puo' verificare deve fermarsi, non accontentarsi di un bersaglio piu' facile.

COSA GUARDA QUESTO FILE

Che la dipendenza resti dichiarata. Toglierla da `requirements-test.txt` non
riporterebbe il vecchio falso verde — `_workflow()` ora solleva — ma
spegnerebbe in CI i test che leggono i workflow, e lo farebbe con un errore che
parla di un pacchetto mancante invece che dell'invariante saltata.

Limite dichiarato: questo NON e' un controllo generale sulle dipendenze.
Verifica il solo PyYAML, l'unico pacchetto per cui esiste la prova — misurata,
sopra — che la sua assenza indebolisca una verifica invece di romperla.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path
from unittest import TestCase

ASSERTIONS = TestCase()

ROOT = Path(__file__).resolve().parents[2]
REQUISITI_TEST = ROOT / "requirements-test.txt"
CHI_LO_USA = ROOT / "tests" / "testsuite" / "test_false_green_semantics.py"


def test_block_pyyaml_resta_dichiarato_fra_le_dipendenze_di_test() -> None:
    ASSERTIONS.assertTrue(
        REQUISITI_TEST.is_file(), f"{REQUISITI_TEST} non esiste: percorso sbagliato?"
    )
    testo = REQUISITI_TEST.read_text(encoding="utf-8")
    righe = [r.strip() for r in testo.splitlines() if r.strip() and not r.strip().startswith("#")]
    ASSERTIONS.assertTrue(righe, "requirements-test.txt non dichiara nessun pacchetto")

    dichiarato = any(re.match(r"(?i)^pyyaml\b", r) for r in righe)
    ASSERTIONS.assertTrue(
        dichiarato,
        "PyYAML non e' piu' dichiarato in requirements-test.txt. "
        f"{CHI_LO_USA.relative_to(ROOT)} legge i workflow PARSATI e senza parser "
        "si ferma (fail-closed): togliendo la dipendenza quei test non "
        "verificherebbero piu' nulla in CI, e il rosso parlerebbe di un pacchetto "
        "mancante invece dell'invariante saltata.",
    )


def test_block_chi_dipende_da_pyyaml_si_ferma_invece_di_degradare() -> None:
    """Il fail-closed va inchiodato qui, non solo nel file che lo implementa.

    Se un domani qualcuno rimettesse un fallback testuale in `_workflow()`, la
    suite resterebbe verde e nessuno lo saprebbe: e' precisamente il modo in cui
    il difetto era nato.
    """
    ASSERTIONS.assertTrue(CHI_LO_USA.is_file(), f"{CHI_LO_USA} non esiste")
    albero = ast.parse(CHI_LO_USA.read_text(encoding="utf-8"))
    funzione = next(
        (n for n in ast.walk(albero) if isinstance(n, ast.FunctionDef) and n.name == "_workflow"),
        None,
    )
    ASSERTIONS.assertIsNotNone(funzione, "_workflow non trovata: il file e' stato riscritto?")

    # Il CODICE, non la prosa: il docstring nomina il sentinel per spiegarlo, e
    # un controllo sul testo grezzo ci cascherebbe — sarebbe un guard che legge
    # i commenti invece di cio' che viene eseguito.
    corpo = list(funzione.body)
    if corpo and isinstance(corpo[0], ast.Expr) and isinstance(corpo[0].value, ast.Constant):
        corpo = corpo[1:]
    codice = "\n".join(ast.dump(n) for n in corpo)

    ASSERTIONS.assertIn(
        "RuntimeError",
        codice,
        "_workflow non solleva piu' quando PyYAML manca: e' tornata a degradare "
        "in silenzio, ed e' il difetto che questo guard esiste per bloccare.",
    )
    ASSERTIONS.assertNotIn(
        "_text",
        codice,
        "il sentinel `{\"_text\": ...}` e' tornato in _workflow: i consumatori "
        "riscivolerebbero sul confronto testuale col file intero, cioe' su un "
        "bersaglio 3,1 volte piu' grande.",
    )
