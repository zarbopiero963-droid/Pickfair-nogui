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


def _funzione_workflow() -> ast.FunctionDef:
    ASSERTIONS.assertTrue(CHI_LO_USA.is_file(), f"{CHI_LO_USA} non esiste")
    albero = ast.parse(CHI_LO_USA.read_text(encoding="utf-8"))
    funzione = next(
        (n for n in ast.walk(albero) if isinstance(n, ast.FunctionDef) and n.name == "_workflow"),
        None,
    )
    ASSERTIONS.assertIsNotNone(funzione, "_workflow non trovata: il file e' stato riscritto?")
    return funzione


def _stringhe_usate_come_dato(funzione: ast.FunctionDef) -> list[str]:
    """Le stringhe che il codice USA, non quelle che racconta.

    I messaggi d'errore vivono dentro `raise`, e nominare li' il sentinel e'
    legittimo — serve a dire cosa non si fa piu'. Quei sottoalberi restano
    quindi fuori, insieme al docstring: cosi' il guard misura il comportamento
    e non la prosa, che e' la distinzione che questa PR difende. Guardare il
    dump dell'AST per intero non bastava: ci finiva dentro anche il testo del
    messaggio, e un messaggio onesto avrebbe fatto fallire il guard.
    """
    trovate: list[str] = []

    class _Visita(ast.NodeVisitor):
        def visit_Raise(self, node: ast.Raise) -> None:  # noqa: N802
            return  # messaggio d'errore: prosa, non dato

        def visit_Constant(self, node: ast.Constant) -> None:  # noqa: N802
            if isinstance(node.value, str):
                trovate.append(node.value)

    corpo = list(funzione.body)
    if corpo and isinstance(corpo[0], ast.Expr) and isinstance(corpo[0].value, ast.Constant):
        corpo = corpo[1:]
    for nodo in corpo:
        _Visita().visit(nodo)
    return trovate


def test_block_nessun_handler_di_workflow_restituisce_un_ripiego() -> None:
    """Il fail-closed va inchiodato qui, non solo nel file che lo implementa.

    Se un domani qualcuno rimettesse un fallback in `_workflow()`, la suite
    resterebbe verde e nessuno lo saprebbe: e' precisamente il modo in cui il
    difetto era nato — `except Exception: return {"_text": text}`.

    L'invariante e' strutturale, non lessicale: nessun gestore di eccezione di
    `_workflow` puo' RESTITUIRE un valore, e almeno uno deve rilanciare.
    """
    funzione = _funzione_workflow()
    handler = [n for n in ast.walk(funzione) if isinstance(n, ast.ExceptHandler)]
    ASSERTIONS.assertTrue(
        handler,
        "_workflow non intercetta piu' l'import di PyYAML: se il file e' stato "
        "riscritto, riscrivi anche questo guard invece di cancellarlo.",
    )

    degradano = [
        h
        for h in handler
        if any(isinstance(n, ast.Return) for n in ast.walk(h))
    ]
    ASSERTIONS.assertEqual(
        [],
        [ast.unparse(h).splitlines()[0] for h in degradano],
        "un gestore di eccezione di _workflow torna a RESTITUIRE un valore "
        "invece di fermarsi: e' il ripiego silenzioso che questo guard esiste "
        "per bloccare. Un controllo che non puo' verificare deve sollevare.",
    )

    rilancia = any(
        isinstance(n, ast.Raise)
        and isinstance(n.exc, ast.Call)
        and isinstance(n.exc.func, ast.Name)
        and n.exc.func.id == "RuntimeError"
        for h in handler
        for n in ast.walk(h)
    )
    ASSERTIONS.assertTrue(
        rilancia,
        "nessun gestore di _workflow solleva piu' RuntimeError quando PyYAML "
        "manca: senza parser il file non verifica nulla, e deve dirlo.",
    )


def test_block_il_sentinel_text_non_torna_come_dato_in_workflow() -> None:
    """La chiave `_text` non deve tornare a esistere come VALORE.

    Non basta che `_workflow` sollevi: se qualcuno reintroducesse il sentinel
    su un altro ramo, i consumatori riscivolerebbero sul confronto testuale col
    file intero — un bersaglio 3,1 volte piu' grande, fatto di commenti, nomi
    di step, trigger ed env.
    """
    usate = _stringhe_usate_come_dato(_funzione_workflow())
    ASSERTIONS.assertNotIn(
        "_text",
        usate,
        'il sentinel `{"_text": ...}` e\' tornato a essere un dato dentro '
        "_workflow: i consumatori tornerebbero a confrontare il testo grezzo "
        "del workflow invece della sua struttura.",
    )
