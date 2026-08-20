"""Il gate di costo dei reviewer AI forti deve dire il vero su cosa e' CORE.

Fable 5 e Fugu Ultra costano. Per questo i due workflow chiamano il modello solo
quando la push tocca file CORE o CRITICI, e lo dichiarano per iscritto:

    "Su push che toccano solo workflow/docs/test il job parte ma NON spende"

Quella frase era falsa. I pattern erano scritti `(^|/)core/`, e quel `(^|/)`
prende anche `tests/core/`, `tests/services/`, `tests/controllers/` — cioe' le
copie di test dei tre moduli. Misurato sulla PR #419: tre push che toccavano
SOLO `tests/core/test_risk_gate.py` hanno fatto girare entrambi i reviewer
forti, ~0,56$ buttati, e si sarebbe ripetuto a ogni PR che tocca quelle
cartelle.

I pattern qui non sono ricopiati: vengono ESTRATTI dal workflow vero e
compilati, altrimenti il test verificherebbe una copia e non cio' che gira.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List

import pytest

# Radice calcolata dal file, non dalla directory di lancio: gli altri test sui
# workflow usano Path(".github/...") e falliscono se pytest parte da altrove.
ROOT = Path(__file__).resolve().parents[2]

WORKFLOWS = [
    ".github/workflows/pr-review-claude-fable5.yml",
    ".github/workflows/pr-review-openrouter-fugu-ultra.yml",
]

# I tre moduli reali stanno SOLO alla radice: se un giorno comparissero
# annidati, questo test andrebbe rivisto insieme ai pattern.
SOLO_ALLA_RADICE = ("core", "services", "controllers")


def _blocco(nome: str, testo: str) -> str:
    """Ritaglia la lista `nome = [ ... ]` dal sorgente del workflow.

    La chiusura si cerca su una RIGA fatta solo di `]`, non col primo `]`
    incontrato: dentro i pattern ce ne sono (`requirements[^/]*`), e cercare il
    primo troncava la lista a meta' — il test sarebbe passato guardando solo
    una parte delle regole.
    """
    righe = testo.splitlines()
    for i, riga in enumerate(righe):
        if riga.strip().startswith(f"{nome} = ["):
            for j in range(i + 1, len(righe)):
                if righe[j].strip() == "]":
                    return "\n".join(righe[i:j])
            break
    raise AssertionError(f"lista {nome} non trovata o non chiusa nel workflow")


def _core_patterns(testo: str) -> List[re.Pattern]:
    blocco = _blocco("CORE_TRIGGER_PATTERNS", testo)
    grezzi = re.findall(r're\.compile\(r"([^"]+)"\)', blocco)
    assert grezzi, "nessun pattern CORE estratto: il formato del workflow e' cambiato"
    return [re.compile(g) for g in grezzi]


def _critical_patterns(testo: str) -> List[re.Pattern]:
    blocco = _blocco("CRITICAL_PATTERNS", testo)
    grezzi = re.findall(r'^\s+r"([^"]+)",\s*$', blocco, re.MULTILINE)
    assert grezzi, "nessun pattern CRITICO estratto: il formato del workflow e' cambiato"
    return [re.compile(g) for g in grezzi]


def _spenderebbe(testo: str, filename: str) -> bool:
    """Replica la decisione di `touches_core` su un singolo file."""
    return (any(p.search(filename) for p in _core_patterns(testo))
            or any(p.search(filename) for p in _critical_patterns(testo)))


@pytest.mark.parametrize("workflow", WORKFLOWS)
@pytest.mark.parametrize(
    "file_di_test",
    ["tests/core/test_risk_gate.py",
     "tests/services/test_qualcosa.py",
     "tests/controllers/test_qualcosa.py",
     "tests/unit/test_qualcosa.py"],
)
def test_block_una_push_di_soli_test_non_fa_spendere(workflow: str, file_di_test: str) -> None:
    """E' il difetto che questo test blocca: erano CORE per via di `(^|/)`."""
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    assert not _spenderebbe(testo, file_di_test), (
        f"{file_di_test} fa scattare il reviewer forte in {workflow}: "
        f"una push di soli test spenderebbe, contro la regola dichiarata"
    )


@pytest.mark.parametrize("workflow", WORKFLOWS)
@pytest.mark.parametrize("modulo", SOLO_ALLA_RADICE)
def test_pass_il_modulo_vero_continua_a_far_spendere(workflow: str, modulo: str) -> None:
    """La contropartita: restringere i pattern non deve spegnere il trigger.

    Se questo diventasse rosso, il risparmio sarebbe stato ottenuto togliendo la
    review forte dove serve davvero — molto peggio del costo che risolve.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    assert _spenderebbe(testo, f"{modulo}/qualsiasi_modulo.py"), (
        f"{modulo}/ non fa piu' scattare il reviewer forte in {workflow}"
    )


@pytest.mark.parametrize("workflow", WORKFLOWS)
@pytest.mark.parametrize(
    "critico",
    [".github/workflows/pr-review-claude-fable5.yml",
     "requirements.txt",
     "betfair_client.py",
     "trading_config.py",
     "headless_main.py"],
)
def test_pass_le_aree_critiche_restano_coperte(workflow: str, critico: str) -> None:
    """Il fix tocca solo le tre cartelle: sicurezza e moduli critici non si toccano."""
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    assert _spenderebbe(testo, critico), f"{critico} non e' piu' coperto in {workflow}"


def test_block_i_tre_moduli_restano_solo_alla_radice() -> None:
    """Il presupposto su cui poggia l'ancoraggio, verificato invece che assunto.

    `^core/` e' corretto solo finche' i tre moduli stanno SOLO alla radice. Se
    un domani nascesse del codice vero annidato — `pkg/core/x.py`, `app/services/
    y.py` — la review forte verrebbe saltata su quel codice IN SILENZIO: nessun
    errore, nessun avviso, solo un modulo di produzione che smette di essere
    revisionato. E' lo stesso difetto che questa PR sta chiudendo, rovesciato.
    Qui l'assunto diventa una condizione che la CI ricontrolla a ogni commit: se
    diventa rosso, vanno riviste le regole nei due workflow, non questo test.

    Si guardano i file TRACCIATI da git, non il filesystem: cosi' un .venv o una
    cache locale non producono falsi allarmi, e si guarda esattamente cio' che
    la Compare API vede.
    """
    import subprocess

    esito = subprocess.run(
        ["git", "ls-files", "-z"], check=False, capture_output=True, cwd=ROOT)
    assert esito.returncode == 0, (
        f"git ls-files non eseguibile: {esito.stderr.decode('utf-8', 'replace')}"
    )
    tracciati = esito.stdout.decode("utf-8").split("\0")

    annidati = sorted({
        "/".join(parti[:i + 1])
        for percorso in tracciati if percorso
        for parti in [percorso.split("/")]
        for i, pezzo in enumerate(parti[:-1])
        # i > 0  -> non e' la radice;  non sotto tests/ -> non e' una copia di test
        if pezzo in SOLO_ALLA_RADICE and i > 0 and parti[0] != "tests"
    })
    assert not annidati, (
        f"moduli annidati trovati: {annidati}. I pattern nei due workflow sono "
        f"ancorati a ^, quindi questo codice NON riceverebbe la review forte, e "
        f"lo farebbe in silenzio. Rivedere CORE_TRIGGER_PATTERNS."
    )


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_niente_pattern_non_ancorati_sulle_tre_cartelle(workflow: str) -> None:
    """Blocca il ritorno esatto della forma difettosa, non solo il suo effetto.

    Un `(^|/)` reintrodotto su queste tre cartelle rimetterebbe i test dentro il
    perimetro CORE anche se qualcuno aggiungesse altrove un pattern che maschera
    il sintomo.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    blocco = _blocco("CORE_TRIGGER_PATTERNS", testo)
    for modulo in SOLO_ALLA_RADICE:
        assert f'(^|/){modulo}/' not in blocco, (
            f"pattern non ancorato per {modulo}/ in {workflow}: prenderebbe di "
            f"nuovo tests/{modulo}/"
        )
