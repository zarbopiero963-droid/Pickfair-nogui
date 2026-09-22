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
    ".github/workflows/pr-review-openrouter-gpt-astra.yml",
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


# I dodici file che la sezione AUTO-MERGE riserva al merge MANUALE dell'owner:
# i quattro file-policy, i sette workflow-gate e `scripts/guardrail_check.py`.
# I sette workflow erano gia' coperti da `(^|/)\.github/workflows/`; gli altri
# cinque NO — e questo e' il buco che il test chiude.
FILE_DI_GOVERNANCE = [
    "CLAUDE.md",
    "AGENTS.md",
    "docs/auto_pr_flow_spec.md",
    "docs/hard_verify_spec.md",
    "scripts/guardrail_check.py",
]


@pytest.mark.parametrize("workflow", WORKFLOWS)
@pytest.mark.parametrize("governance", FILE_DI_GOVERNANCE)
def test_pass_i_file_che_definiscono_i_gate_fanno_spendere(workflow: str, governance: str) -> None:
    """Chi decide cosa l'agente puo' mergiare dev'essere revisionato, sempre.

    Il difetto, osservato dal vivo sulla PR #473 e non dedotto: quella PR
    toccava `scripts/guardrail_check.py` — uno dei dodici file a merge manuale
    — e prima che l'agente applicasse le label a mano era **muta**. Nessuna
    `manual-review-required`, e i due reviewer forti partiti e chiusi senza
    pubblicare nulla: zero review a testa.

    Il gate restava soddisfatto solo perche' l'agente si ricordava di mettere le
    label, cioe' per una regola che l'agente applica a se stesso. Una PR che
    toccasse SOLO questi cinque file passerebbe senza alcun segnale automatico
    che il merge spetta all'owner.

    Il seguito e' la contropartita di `test_pass_il_modulo_vero_continua_a_far
    _spendere`: qui il costo si vuole, perche' cade esattamente sui file dove la
    review forte conta di piu'.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    assert _spenderebbe(testo, governance), (
        f"{governance} NON fa scattare il reviewer forte in {workflow}: e' uno "
        "dei dodici file a merge manuale, e una PR che tocca solo file come "
        "questo resterebbe senza etichetta e senza review dei gate forti"
    )


@pytest.mark.parametrize("workflow", WORKFLOWS)
@pytest.mark.parametrize(
    "innocuo",
    ["docs/design/design_handoff.md",
     "ops/roadmap_go_live.md",
     "README.md",
     "scripts/pr_costo_review.py"],
)
def test_block_gli_altri_file_di_docs_e_scripts_non_fanno_spendere(workflow: str, innocuo: str) -> None:
    """Il confine dall'altro lato: coprire i cinque non deve coprire tutto.

    Senza questo, il modo piu' semplice di far passare il test sopra sarebbe un
    pattern largo su `docs/` o `scripts/` — che farebbe spendere i due reviewer
    forti su ogni ritocco alla roadmap o al README. Sarebbe un costo continuo
    ottenuto per pigrizia, non una rete di sicurezza.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    assert not _spenderebbe(testo, innocuo), (
        f"{innocuo} fa scattare il reviewer forte in {workflow}: il pattern "
        "sui file di governance e' troppo largo e fa spendere su file comuni"
    )


# I quattro workflow di review, non solo i due col gate di costo: la lista
# `CRITICAL_PATTERNS` e' duplicata in tutti e quattro, ed e' la stessa in tutti
# e quattro. E' un invariante vero oggi, quindi va inchiodato prima che smetta
# di esserlo in silenzio.
TUTTI_I_REVIEWER = [
    ".github/workflows/pr-review-openrouter-gpt56-sol.yml",
    ".github/workflows/pr-review-xai-grok46.yml",
    ".github/workflows/pr-review-openrouter-fugu-ultra.yml",
    ".github/workflows/pr-review-claude-fable5.yml",
    ".github/workflows/pr-review-openrouter-gpt-astra.yml",
]


def test_block_i_quattro_reviewer_condividono_gli_stessi_pattern_critici() -> None:
    """Quattro copie della stessa lista divergono appena qualcuno ne tocca una.

    La conseguenza non sarebbe un errore rumoroso ma un disallineamento muto:
    un file considerato sensibile da due reviewer e ignorato dagli altri due,
    con l'etichetta `manual-review-required` che compare o no a seconda di
    quale workflow gira per primo. Confrontare le liste e' l'unico modo per
    accorgersene senza leggerle a mano ogni volta.
    """
    liste = {}
    for wf in TUTTI_I_REVIEWER:
        testo = (ROOT / wf).read_text(encoding="utf-8")
        liste[wf] = [p.pattern for p in _critical_patterns(testo)]

    riferimento = liste[TUTTI_I_REVIEWER[0]]
    for wf, pattern in liste.items():
        assert pattern == riferimento, (
            f"{wf} ha CRITICAL_PATTERNS diversi dagli altri reviewer.\n"
            f"  solo qui : {[p for p in pattern if p not in riferimento]}\n"
            f"  mancanti : {[p for p in riferimento if p not in pattern]}\n"
            "Le quattro liste devono restare allineate: un file sensibile per "
            "un reviewer e non per gli altri produce un gate a macchia di "
            "leopardo."
        )


@pytest.mark.parametrize("workflow", TUTTI_I_REVIEWER)
@pytest.mark.parametrize("governance", FILE_DI_GOVERNANCE)
def test_pass_i_file_di_governance_sono_critici_per_tutti_e_quattro(
    workflow: str, governance: str
) -> None:
    """`manual-review-required` la applicano tutti e quattro, non solo i forti.

    Il test sopra sul gate di costo riguarda i due reviewer che possono NON
    spendere; questo riguarda l'etichetta, che dipende da `CRITICAL_PATTERNS`
    in ciascuno dei quattro workflow.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    assert any(p.search(governance) for p in _critical_patterns(testo)), (
        f"{governance} non e' critico per {workflow}: una PR che lo tocca "
        "potrebbe non ricevere l'etichetta di controllo manuale"
    )


# ---------------------------------------------------------------------------
# Le liste di reviewer qui sopra sono scritte a mano. Finche' restano tali,
# aggiungere un reviewer significa ricordarsi di QUATTRO moduli diversi
# (cost_gate, range_solo_pr, copertura_diff, effort): dimenticarne uno fa
# sfuggire quel reviewer a un invariante IN SILENZIO, che e' il modo peggiore.
#
# Osservato aggiungendo GPT-6 Astra come terzo reviewer forte: quattro liste
# da toccare, nessuna che si lamenti se te ne scordi. Questo guard confronta
# l'elenco scritto con i file che esistono davvero, cosi' la dimenticanza
# diventa un test rosso invece di un buco.
# ---------------------------------------------------------------------------

def _reviewer_su_disco() -> set:
    return {
        f"{'.github/workflows'}/{p.name}"
        for p in (ROOT / ".github" / "workflows").glob("pr-review-*.yml")
    }


def test_block_la_lista_di_tutti_i_reviewer_e_completa() -> None:
    """`TUTTI_I_REVIEWER` deve elencare OGNI workflow di review che esiste."""
    scritti, su_disco = set(TUTTI_I_REVIEWER), _reviewer_su_disco()
    assert scritti == su_disco, (
        f"TUTTI_I_REVIEWER non coincide coi file reali.\n"
        f"  mancanti nella lista : {sorted(su_disco - scritti)}\n"
        f"  elencati ma assenti  : {sorted(scritti - su_disco)}"
    )


def test_block_la_lista_col_gate_di_costo_e_completa() -> None:
    """`WORKFLOWS` deve elencare TUTTI e SOLI i reviewer che hanno un gate di costo.

    Il criterio non e' "i forti" per convenzione: e' la presenza di
    `CORE_TRIGGER_PATTERNS`, cioe' del meccanismo che puo' decidere di NON
    chiamare il modello. Derivarlo dal file invece che dalla memoria e' il
    punto: un reviewer nuovo col gate di costo entra da solo, e se non entra
    questo test lo dice.
    """
    con_gate = {
        w for w in _reviewer_su_disco()
        if "CORE_TRIGGER_PATTERNS = [" in (ROOT / w).read_text(encoding="utf-8")
    }
    assert set(WORKFLOWS) == con_gate, (
        f"WORKFLOWS non coincide coi reviewer che hanno il gate di costo.\n"
        f"  col gate ma non elencati: {sorted(con_gate - set(WORKFLOWS))}\n"
        f"  elencati ma senza gate  : {sorted(set(WORKFLOWS) - con_gate)}"
    )
