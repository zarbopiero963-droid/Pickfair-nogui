"""Nei test non si assegna a mano un attributo di modulo: si usa `monkeypatch`.

FOLLOW-UP #462. `tests/scripts/test_pr_flow_automation.py` installava lo stub di
`flow.pr_view` a mano::

    original = flow.pr_view
    try:
        flow.pr_view = lambda _repo, _pr: pr
        ...
    finally:
        flow.pr_view = original

Era l'unico punto di quel file, su una quindicina, a non usare `monkeypatch`.
Il `try/finally` protegge dalle eccezioni, non dall'interleaving: se un altro
test dello stesso processo legge `flow.pr_view` MENTRE lo stub e' installato, lo
salva come "originale" e poi lo ripristina — lo stub diventa permanente e ogni
test successivo del worker gira su una `pr_view` finta senza che nulla lo dica.
Un test che passa su un mock sbagliato e' indistinguibile da un test che passa.

`monkeypatch` non rende sicura l'esecuzione concorrente in-process (muta
comunque un globale di modulo): toglie la gestione a mano, garantisce il
ripristino anche se il test esplode, e in ordine LIFO, quindi l'avvelenamento
permanente non puo' piu' accadere. Per l'isolamento vero servirebbe un seam di
iniezione nel codice di produzione — scelta dell'owner, non di un test.

Questo guard blocca la comparsa di NUOVI punti. I punti preesistenti sono
elencati qui sotto, non nascosti: e' debito visibile, non una deroga silenziosa.
"""

import ast
from pathlib import Path
from unittest import TestCase

ASSERTIONS = TestCase()

RADICE_TEST = Path(__file__).resolve().parents[1]

# Punti preesistenti al follow-up #462, fuori dal suo scope. Ogni voce e'
# (percorso relativo a tests/, "modulo.attributo"). Un attributo NUOVO, o lo
# stesso attributo in un file NUOVO, resta bloccato.
PREESISTENTI: frozenset[tuple[str, str]] = frozenset(
    {
        ("chaos/test_runtime_reconcile_under_stress.py", "time.sleep"),
        ("chaos/test_runtime_reconcile_under_stress.py", "time.time"),
        ("failure/test_reconcile_retry_policy.py", "time.sleep"),
        ("parsers/test_parser_personalizzati.py", "cpe.matches_message"),
        ("unit/test_tick_throttle_executor_determinism.py", "tick_module._dispatcher"),
    }
)


def _moduli_importati(albero: ast.Module) -> set[str]:
    """I nomi legati da un `import x` / `import x as y` / `import x.y`."""
    nomi: set[str] = set()
    for nodo in ast.walk(albero):
        if isinstance(nodo, ast.Import):
            for alias in nodo.names:
                nomi.add(alias.asname or alias.name.split(".")[0])
    return nomi


def _assegnazioni_a_moduli(percorso: Path) -> list[tuple[int, str]]:
    """(riga, "modulo.attributo") per ogni assegnazione diretta nel file."""
    try:
        albero = ast.parse(percorso.read_text(encoding="utf-8"))
    except SyntaxError:  # pragma: no cover - un file rotto lo dice la raccolta
        return []
    moduli = _moduli_importati(albero)
    trovate: list[tuple[int, str]] = []
    for nodo in ast.walk(albero):
        if not isinstance(nodo, ast.Assign):
            continue
        for bersaglio in nodo.targets:
            if (
                isinstance(bersaglio, ast.Attribute)
                and isinstance(bersaglio.value, ast.Name)
                and bersaglio.value.id in moduli
            ):
                trovate.append((nodo.lineno, f"{bersaglio.value.id}.{bersaglio.attr}"))
    return trovate


def test_block_nessuna_nuova_assegnazione_diretta_a_un_modulo() -> None:
    moduli_test = sorted(RADICE_TEST.rglob("test_*.py"))
    # Anti-vacuo: se la scansione non trova la suite, il verde non prova nulla.
    ASSERTIONS.assertGreater(
        len(moduli_test), 100,
        f"solo {len(moduli_test)} file di test trovati sotto {RADICE_TEST}: "
        "la scansione e' rotta, non la suite",
    )

    nuove: list[str] = []
    for percorso in moduli_test:
        relativo = percorso.relative_to(RADICE_TEST).as_posix()
        for riga, bersaglio in _assegnazioni_a_moduli(percorso):
            if (relativo, bersaglio) not in PREESISTENTI:
                nuove.append(f"tests/{relativo}:{riga}  {bersaglio} = ...")

    ASSERTIONS.assertEqual(
        nuove, [],
        "assegnazione diretta a un attributo di modulo in un test:\n  "
        + "\n  ".join(nuove)
        + "\nUsa `monkeypatch.setattr(modulo, \"attributo\", ...)`: il ripristino "
        "a mano non regge l'interleaving e puo' rendere lo stub permanente.",
    )


def test_block_l_elenco_dei_preesistenti_non_invecchia() -> None:
    """Una voce che non esiste piu' va tolta, non lasciata a coprire il nulla.

    Un allow-list che sopravvive al codice che scusava e' una deroga aperta su
    un punto che nessuno controlla piu'.
    """
    ancora_presenti = set()
    for relativo, bersaglio in PREESISTENTI:
        percorso = RADICE_TEST / relativo
        if not percorso.exists():
            # File sparito: la voce NON finisce fra le presenti, quindi viene
            # segnalata come obsoleta qui sotto. Il `continue` evita solo la
            # lettura di un file che non c'e'.
            continue
        if any(b == bersaglio for _riga, b in _assegnazioni_a_moduli(percorso)):
            ancora_presenti.add((relativo, bersaglio))

    obsolete = sorted(f"{f} -> {b}" for f, b in PREESISTENTI - ancora_presenti)
    ASSERTIONS.assertEqual(
        obsolete, [],
        "voci dell'allow-list che non corrispondono piu' a nulla nel codice: "
        f"{obsolete}. Toglile: una deroga senza oggetto copre solo il prossimo caso.",
    )
