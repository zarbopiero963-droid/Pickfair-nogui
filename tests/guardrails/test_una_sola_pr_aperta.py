"""Piu' di una PR aperta deve diventare rosso.

"UNA SOLA PR aperta / UN SOLO task attivo alla volta" sta fra le REGOLE NON
NEGOZIABILI di CLAUDE.md. E' stata violata due volte — #421 aperta mentre #420
era aperta, #422 con entrambe — senza che nulla in CI se ne accorgesse.

Il perche' e' verificabile nel log di `overlap-check` su #422:

    ⚠️ Non-critical overlap detected (allowed, but be careful)

...seguito da `exit 0`. Quel job esce 1 solo se la sovrapposizione tocca cinque
file critici scritti a mano; `.guardrails/allowed_scope.json` e
`.github/workflows/*` non sono in quella lista. Un controllo sul NUMERO di PR
aperte non esisteva affatto.

La funzione qui sotto NON e' ricopiata: viene estratta dal workflow vero e
compilata, altrimenti il test verificherebbe una copia e non cio' che gira.
"""
from __future__ import annotations

import ast
import textwrap
from pathlib import Path
from typing import Any, Callable, Dict, List

ROOT = Path(__file__).resolve().parents[2]

WORKFLOW = ".github/workflows/pr-overlap-guard.yml"
FUNZIONE = "pr_di_troppo"


def _carica() -> Callable:
    righe = (ROOT / WORKFLOW).read_text(encoding="utf-8").splitlines()
    try:
        inizio = next(i for i, r in enumerate(righe) if r.strip() == "python3 <<'PY'")
        fine = next(i for i in range(inizio + 1, len(righe)) if righe[i].strip() == "PY")
    except StopIteration:
        raise AssertionError(
            f"{WORKFLOW}: blocco `python3 <<'PY' ... PY` non trovato. Il test "
            f"estrae la funzione da li', quindi va aggiornato insieme al workflow"
        ) from None

    albero = ast.parse(textwrap.dedent("\n".join(righe[inizio + 1:fine])))
    nodo = next((n for n in ast.walk(albero)
                 if isinstance(n, ast.FunctionDef) and n.name == FUNZIONE), None)
    assert nodo is not None, f"{FUNZIONE} non trovata in {WORKFLOW}"

    spazio: Dict[str, Any] = {}
    exec(  # skipcq: PY-W0122, PYL-W0122 - codice versionato del repo, gia passato da ast
        compile(ast.Module(body=[nodo], type_ignores=[]), WORKFLOW, "exec"), spazio)
    return spazio[FUNZIONE]


def _prs(*numeri: int, draft: List[int] | None = None) -> List[Dict[str, Any]]:
    draft = draft or []
    return [{"number": n, "title": f"PR {n}", "draft": n in draft} for n in numeri]


def test_pass_una_sola_pr_aperta_va_bene() -> None:
    assert _carica()(_prs(420), 420) == []


def test_pass_nessuna_pr_aperta_non_esplode() -> None:
    """Caso di confine: il workflow gira su `pull_request`, quindi in pratica
    almeno una c'e'. Ma una lista vuota non deve far morire il job."""
    assert _carica()(_prs(), 0) == []


def test_block_due_pr_aperte_sono_una_di_troppo() -> None:
    assert _carica()(_prs(420, 421), 421) == [421]


def test_block_il_caso_reale_tre_pr_aperte() -> None:
    """Esattamente la situazione del 2026-08-19, passata verde."""
    assert _carica()(_prs(420, 421, 422), 422) == [421, 422]


def test_block_di_troppo_sono_le_successive_alla_piu_vecchia() -> None:
    """La piu' vecchia e' quella legittima, a prescindere da quale sta girando:
    il check e' rosso su TUTTE finche' non si rientra, ed e' voluto."""
    fn = _carica()
    assert fn(_prs(420, 421, 422), 420) == [421, 422]
    assert fn(_prs(420, 421, 422), 421) == [421, 422]


def test_block_l_ordine_della_lista_non_conta() -> None:
    """La API non garantisce un ordinamento: se lo si desse per scontato, la
    "piu' vecchia" sarebbe la prima arrivata e il verdetto cambierebbe."""
    assert _carica()(_prs(422, 420, 421), 422) == [421, 422]


def test_block_una_draft_conta_come_pr_aperta() -> None:
    """Una bozza e' comunque una PR aperta e un task in volo. Se un domani
    dovessero non contare, va deciso qui e non aggirato a valle."""
    assert _carica()(_prs(420, 421, draft=[421]), 421) == [421]
