"""Il range revisionato non deve contenere codice che arriva dal branch base.

Il push-range di GitHub e' `merge-base(base, head)...head`. Quando una push e' un
merge del branch base dentro la PR, quel range si tira dentro tutto cio' che
arriva da main: codice gia' revisionato, gia' mergiato e gia' pagato.

Misurato sulla PR #420, push `49dc486...3488672`:

    nel range           : allowed_scope.json, core/risk_gate.py, headless_main.py,
                          mini_gui.py, tests/core/test_risk_gate.py
    di questi, dalla PR : allowed_scope.json

Quattro file su cinque venivano da main. Il prompt di un reviewer e' passato da
~13k a 48k token — ~0,57$ in una sola push fra i due reviewer forti — e le review
hanno segnalato come "non verificabile perche' troncato" un file che non era
nemmeno di quella PR.

La funzione qui sotto NON e' ricopiata: viene estratta dai workflow veri e
compilata, altrimenti il test verificherebbe una copia e non cio' che gira.
"""
from __future__ import annotations

import ast
import textwrap
from pathlib import Path
from typing import Any, Callable, Dict, List

import pytest

ROOT = Path(__file__).resolve().parents[2]

WORKFLOWS = [
    ".github/workflows/pr-review-claude-fable5.yml",
    ".github/workflows/pr-review-openrouter-gpt56-sol.yml",
    ".github/workflows/pr-review-xai-grok46.yml",
    ".github/workflows/pr-review-openrouter-fugu-ultra.yml",
]

FUNZIONE = "solo_file_della_pr"

# Il caso reale, dalla PR #420 push 49dc486...3488672 (verificato con git).
RANGE_420 = [".guardrails/allowed_scope.json", "core/risk_gate.py",
             "headless_main.py", "mini_gui.py", "tests/core/test_risk_gate.py"]
FILE_PROPRI_420 = [".github/workflows/pr-review-claude-fable5.yml",
                   ".github/workflows/pr-review-openrouter-fugu-ultra.yml",
                   ".guardrails/allowed_scope.json",
                   "tests/guardrails/test_ai_review_cost_gate.py"]


def _script_python(workflow: str) -> str:
    """Ritaglia lo script python3 incorporato nel workflow."""
    righe = (ROOT / workflow).read_text(encoding="utf-8").splitlines()
    try:
        inizio = next(i for i, r in enumerate(righe) if r.strip() == "python3 <<'PY'")
        fine = next(i for i in range(inizio + 1, len(righe)) if righe[i].strip() == "PY")
    except StopIteration:
        # Se il workflow cambia forma, meglio dirlo che morire con uno
        # StopIteration nudo a chi legge il rosso in CI.
        raise AssertionError(
            f"{workflow}: blocco `python3 <<'PY' ... PY` non trovato; il test "
            f"estrae la funzione da li', quindi va aggiornato insieme al workflow"
        ) from None
    return textwrap.dedent("\n".join(righe[inizio + 1:fine]))


def carica_funzione(workflow: str, *, base_sha: str, pr_files: Any,
                    chiamate: List[str] | None = None) -> Callable:
    """Estrae SOLO la funzione dal workflow e la esegue con dipendenze finte.

    `pr_files` puo' essere una lista (risposta della Compare API) oppure
    un'eccezione da sollevare, per esercitare il ramo di errore.
    `chiamate`, se passata, registra ogni chiamata a compare_range: serve ad
    affermare che in certi rami GitHub non viene contattato affatto.
    """
    albero = ast.parse(_script_python(workflow))
    nodo = next((n for n in ast.walk(albero)
                 if isinstance(n, ast.FunctionDef) and n.name == FUNZIONE), None)
    assert nodo is not None, f"{FUNZIONE} non trovata in {workflow}"

    def compare_range(base: str, head: str) -> Dict[str, Any]:
        if chiamate is not None:
            chiamate.append(f"{base}...{head}")
        if isinstance(pr_files, Exception):
            raise pr_files
        return {"files": pr_files}

    spazio: Dict[str, Any] = {
        "BASE_SHA": base_sha,
        "REVIEW_ID": "test",
        "good_sha": lambda s: bool(s) and s != "0" * 40,
        "compare_range": compare_range,
        "redact": lambda s: s,
    }
    # `exec` e' deliberato ed e' il punto di tutto il test: eseguire la funzione
    # COME STA NEL WORKFLOW invece di una copia. L'input non e' arbitrario — e'
    # un file versionato di questo repo, gia' passato dal parser `ast`, e si
    # compila il SOLO nodo della funzione cercata, non lo script intero. Il test
    # gira in CI, non in produzione. L'alternativa (ricopiare la funzione) e'
    # esattamente il difetto che questo test esiste per impedire.
    exec(  # skipcq: PY-W0122 - vedi commento sopra: si esegue codice versionato del repo
        compile(ast.Module(body=[nodo], type_ignores=[]), workflow, "exec"), spazio)
    return spazio[FUNZIONE]


def _f(nomi: List[str]) -> List[Dict[str, str]]:
    return [{"filename": n} for n in nomi]


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_il_codice_che_arriva_da_main_esce_dal_range(workflow: str) -> None:
    """Il caso misurato: 5 file nel range, 4 provenienti da main."""
    fn = carica_funzione(workflow, base_sha="b" * 40, pr_files=_f(FILE_PROPRI_420))
    tenuti, esclusi = fn(_f(RANGE_420), "a" * 40, "c" * 40)

    assert [t["filename"] for t in tenuti] == [".guardrails/allowed_scope.json"], (
        "nel range deve restare solo cio' che la PR cambia davvero"
    )
    assert esclusi == ["core/risk_gate.py", "headless_main.py", "mini_gui.py",
                       "tests/core/test_risk_gate.py"]


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_pass_una_push_normale_non_viene_toccata(workflow: str) -> None:
    """La contropartita: dove non c'e' contaminazione non deve cambiare nulla.

    Su una push normale tutto cio' che pushi fa parte della tua PR, quindi
    l'intersezione e' l'insieme di partenza. Se questo diventasse rosso, il
    risparmio sarebbe stato ottenuto togliendo file dalla review — molto peggio
    del costo che risolve.
    """
    spinti = ["core/risk_gate.py", "tests/core/test_risk_gate.py"]
    propri = spinti + ["headless_main.py", "mini_gui.py"]
    fn = carica_funzione(workflow, base_sha="b" * 40, pr_files=_f(propri))
    tenuti, esclusi = fn(_f(spinti), "a" * 40, "c" * 40)

    assert [t["filename"] for t in tenuti] == spinti
    assert esclusi == []


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_pass_sul_gate_a_label_e_un_no_op(workflow: str) -> None:
    """Con range gia' PR-vs-base non c'e' niente da togliere, e non si chiama GitHub."""
    chiamate: List[str] = []
    fn = carica_funzione(workflow, base_sha="a" * 40,
                         pr_files=_f(["src/tutt_altro.py"]), chiamate=chiamate)
    tenuti, esclusi = fn(_f(RANGE_420), "a" * 40, "c" * 40)

    assert [t["filename"] for t in tenuti] == RANGE_420
    assert esclusi == []
    assert chiamate == [], "range gia' PR-vs-base: GitHub non va interrogato"


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_errore_github_non_restringe_il_range(workflow: str) -> None:
    """Nel dubbio si rivede DI PIU', non di meno.

    Se la chiamata a GitHub fallisce non si sa quali file siano della PR. Il
    verso prudente e' lasciare il range intero: un errore d'infrastruttura non
    deve poter ridurre in silenzio cio' che viene revisionato.
    """
    fn = carica_funzione(workflow, base_sha="b" * 40,
                         pr_files=RuntimeError("502 Bad Gateway"))
    tenuti, esclusi = fn(_f(RANGE_420), "a" * 40, "c" * 40)

    assert [t["filename"] for t in tenuti] == RANGE_420, (
        "un errore GitHub ha ristretto il range: review silenziosamente ridotta"
    )
    assert esclusi == []


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_lista_file_della_pr_troncata_non_restringe(workflow: str) -> None:
    """Rilievo di GPT-5.6 Sol, Grok, Fable e Fugu — tutti e quattro, ed era vero.

    La Compare API tronca a 300 file. Con la lista dei file PROPRI troncata,
    `nomi_propri` e' incompleto e l'intersezione butterebbe fuori dalla review
    file veri della PR, anche critici, in silenzio: e' il fail-close che il ramo
    d'errore evita di proposito, quindi va evitato anche qui.
    """
    propri_troncati = _f([f"src/modulo_{i}.py" for i in range(300)])
    fn = carica_funzione(workflow, base_sha="b" * 40, pr_files=propri_troncati)
    tenuti, esclusi = fn(_f(RANGE_420), "a" * 40, "c" * 40)

    assert [t["filename"] for t in tenuti] == RANGE_420, (
        "lista dei file della PR troncata: il range e' stato ristretto lo stesso, "
        "review silenziosamente ridotta"
    )
    assert esclusi == []


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_lista_file_della_push_al_cap_non_restringe(workflow: str) -> None:
    """Secondo verso dello stesso difetto, e ha una conseguenza in piu'.

    Se la lista della PUSH e' gia' al cap non si sa cosa manchi; ma soprattutto
    ridurre `files` spegnerebbe il fail-safe a valle, che riconosce il
    troncamento proprio da `len(files) >= 300` e per quello forza la review
    forte. Restringere qui lo disinnescherebbe senza che nessuno lo veda.
    E non si chiama nemmeno GitHub: non serve.
    """
    push_al_cap = _f([f"src/spinto_{i}.py" for i in range(300)])
    # File propri che NON intersecano: senza la guardia la restrizione
    # svuoterebbe il range, quindi il test distingue davvero i due casi.
    chiamate: List[str] = []
    fn = carica_funzione(workflow, base_sha="b" * 40,
                         pr_files=_f(["src/tutt_altro.py"]), chiamate=chiamate)
    tenuti, esclusi = fn(push_al_cap, "a" * 40, "c" * 40)

    assert len(tenuti) == 300, "il fail-safe a valle sul troncamento e' stato spento"
    assert esclusi == []
    assert chiamate == [], "al cap non serve chiedere nulla a GitHub"


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_pass_un_file_rinominato_resta_nel_range(workflow: str) -> None:
    """Un rename ha nome nuovo nel range e nome vecchio fra i file della PR (o
    viceversa): guardare solo `filename` lo butterebbe fuori per sbaglio."""
    fn = carica_funzione(
        workflow, base_sha="b" * 40,
        pr_files=[{"filename": "core/nuovo.py", "previous_filename": "core/vecchio.py"}])
    tenuti, esclusi = fn(
        [{"filename": "core/nuovo.py", "previous_filename": "core/vecchio.py"}],
        "a" * 40, "c" * 40)

    assert [t["filename"] for t in tenuti] == ["core/nuovo.py"]
    assert esclusi == []


# ---------------------------------------------------------------------------
# I test qui sopra eseguono la funzione. Restano verdi anche se il workflow
# smette di CHIAMARLA: la funzione esisterebbe, inerte, e il range tornerebbe
# in silenzio a contenere codice di main. Il cablaggio va affermato a parte.
# ---------------------------------------------------------------------------

RIGA_SORGENTE = 'files = compared.get("files")'
RIGA_CHIAMATA = ("files, files_dal_base = solo_file_della_pr("
                 "files, range_base, range_head)")


def _riga_attiva(workflow: str, frammento: str) -> int:
    """Indice dell'unica riga NON commentata che contiene `frammento`."""
    righe = (ROOT / workflow).read_text(encoding="utf-8").splitlines()
    trovate = [i for i, r in enumerate(righe)
               if frammento in r and not r.lstrip().startswith("#")]
    assert trovate, (
        f"{workflow}: riga attiva `{frammento}` assente. La restrizione del "
        f"range non e' piu' cablata: il reviewer tornerebbe a leggere (e a far "
        f"pagare) codice che arriva dal branch base."
    )
    assert len(trovate) == 1, (
        f"{workflow}: `{frammento}` compare {len(trovate)} volte "
        f"(righe {[i + 1 for i in trovate]}); l'ordine non e' piu' decidibile."
    )
    return trovate[0]


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_il_workflow_chiama_davvero_la_restrizione(workflow: str) -> None:
    """La restrizione deve essere applicata alla lista che si usa davvero."""
    i_sorgente = _riga_attiva(workflow, RIGA_SORGENTE)
    i_chiamata = _riga_attiva(workflow, RIGA_CHIAMATA)

    assert i_sorgente < i_chiamata, (
        f"{workflow}: `solo_file_della_pr` viene chiamata a riga "
        f"{i_chiamata + 1}, prima che `files` sia popolata a riga "
        f"{i_sorgente + 1}."
    )


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_la_restrizione_precede_il_gate_di_costo(workflow: str) -> None:
    """Dove esiste un gate di costo, il gate deve DECIDERE sui file gia'
    ristretti: altrimenti una push che dal punto di vista della PR non cambia
    niente supererebbe comunque il gate e pagherebbe una review intera.

    L'ancora e' l'INVOCAZIONE di `touches_core`, non la sua definizione. In
    questi script le funzioni stanno tutte in cima (`solo_file_della_pr` e'
    definita centinaia di righe prima del punto in cui la si usa): ancorarsi
    al `def` farebbe fallire il test su un raggruppamento delle definizioni —
    un refactor corretto — e soprattutto non affermerebbe cio' che conta, cioe'
    l'ordine in cui le due cose vengono ESEGUITE.

    I due reviewer economici non hanno gate di costo, e li' non c'e' ordine da
    verificare: il caso viene riconosciuto esplicitamente, non dato per
    scontato, cosi' se un domani il gate comparisse il test non tace.
    """
    righe = (ROOT / workflow).read_text(encoding="utf-8").splitlines()
    invocazioni = [i for i, r in enumerate(righe)
                   if "touches_core(" in r
                   and not r.lstrip().startswith("#")
                   and not r.lstrip().startswith("def ")]

    if not invocazioni:
        assert not any("CORE_TRIGGER_PATTERNS" in r for r in righe), (
            f"{workflow}: ci sono i pattern del gate di costo ma `touches_core` "
            f"non viene mai invocata; il gate non decide piu' niente, oppure "
            f"questo test non sa piu' dove guardare."
        )
        return

    i_chiamata = _riga_attiva(workflow, RIGA_CHIAMATA)
    assert i_chiamata < min(invocazioni), (
        f"{workflow}: il gate di costo decide a riga {min(invocazioni) + 1}, "
        f"prima che la restrizione del range sia applicata a riga "
        f"{i_chiamata + 1}: valuterebbe anche i file che arrivano da main, e "
        f"una push che per la PR non cambia niente pagherebbe una review."
    )


# ---------------------------------------------------------------------------
# CI-F1: `esclusi` non deve poter contenere None.
# La Compare API documenta `filename` come sempre presente. Se non lo fosse,
# il job morirebbe invece di degradare — e in un punto lontano dalla causa.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_una_entry_senza_filename_non_uccide_il_job(workflow: str) -> None:
    """Con DUE esclusi e uno privo di `filename`, `sorted` confronterebbe None
    con str: TypeError, e la review non gira affatto."""
    fn = carica_funzione(workflow, base_sha="b" * 40,
                         pr_files=[{"filename": "core/mio.py"}])
    tenuti, esclusi = fn(
        [{"previous_filename": "senza_nome.py"},   # entry senza `filename`
         {"filename": "da_main.py"},
         {"filename": "core/mio.py"}],
        "a" * 40, "c" * 40)

    assert [t["filename"] for t in tenuti] == ["core/mio.py"]
    assert esclusi == ["da_main.py"], (
        f"{workflow}: esclusi={esclusi!r}. Deve contenere solo nomi veri: "
        f"None qui dentro fa esplodere sorted() o il join del chiamante."
    )
    assert None not in esclusi


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_un_solo_escluso_senza_filename_non_passa_none_al_chiamante(
        workflow: str) -> None:
    """Il caso insidioso: con UN SOLO escluso `sorted` non solleva niente e
    restituisce [None]. A morire e' il `", ".join(...)` del chiamante, che
    stampa la notice — cioe' il codice muore lontano dalla causa."""
    fn = carica_funzione(workflow, base_sha="b" * 40,
                         pr_files=[{"filename": "core/mio.py"}])
    _, esclusi = fn([{"previous_filename": "senza_nome.py"},
                     {"filename": "core/mio.py"}],
                    "a" * 40, "c" * 40)

    assert esclusi == []
    # La riga che moriva davvero: si esercita, non si assume.
    ", ".join(esclusi[:10])


# ---------------------------------------------------------------------------
# CI-F2: il patch deve venire dalla BASE della PR, non dal range della push.
# Restringere la lista dei file non basta — vedi PR #422, 80 righe viste
# contro 37 reali e due falsi positivi in una sola tornata.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_su_push_contaminata_il_patch_arriva_dalla_base_della_pr(
        workflow: str) -> None:
    fn = carica_funzione(
        workflow, base_sha="b" * 40,
        pr_files=[{"filename": "core/mio.py", "patch": "PATCH_VS_BASE"}])
    tenuti, esclusi = fn(
        [{"filename": "core/mio.py", "patch": "PATCH_DELLA_PUSH_CONTAMINATO"},
         {"filename": "arriva_da_main.py", "patch": "roba di main"}],
        "a" * 40, "c" * 40)

    assert esclusi == ["arriva_da_main.py"]
    assert [t["filename"] for t in tenuti] == ["core/mio.py"]
    assert tenuti[0]["patch"] == "PATCH_VS_BASE", (
        f"{workflow}: il patch e' ancora quello del range della push. La lista "
        f"dei file e' ristretta ma il modello legge comunque righe che "
        f"arrivano dal branch base, e le attribuisce alla PR."
    )


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_pass_su_push_normale_il_patch_incrementale_non_viene_sostituito(
        workflow: str) -> None:
    """Senza contaminazione il patch della push e' esattamente cio' che si
    vuole rivedere. Sostituirlo mostrerebbe di nuovo tutto il file gia'
    revisionato nelle push precedenti: piu' caro, e contro lo scopo della
    review incrementale."""
    fn = carica_funzione(
        workflow, base_sha="b" * 40,
        pr_files=[{"filename": "core/mio.py", "patch": "TUTTA_LA_PR"},
                  {"filename": "toccato_prima.py", "patch": "..."}])
    tenuti, esclusi = fn(
        [{"filename": "core/mio.py", "patch": "SOLO_QUESTA_PUSH"}],
        "a" * 40, "c" * 40)

    assert esclusi == [], "nessun file estraneo: la push non e' contaminata"
    assert tenuti[0]["patch"] == "SOLO_QUESTA_PUSH", (
        f"{workflow}: su una push incrementale il patch e' stato sostituito "
        f"con il diff completo della PR. Ogni push farebbe rileggere (e "
        f"ripagare) codice gia' revisionato."
    )


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_block_non_si_scambia_un_patch_presente_con_uno_assente(
        workflow: str) -> None:
    """Il diff vs base e' CUMULATIVO, quindi piu' grande dell'incrementale, e
    GitHub omette `patch` oltre una certa dimensione.

    Scambiare un'entry CON patch per una SENZA farebbe saltare il file da
    `build_patch_payload` (`if not patch: skipped; continue`), e quel salto
    non fa scattare il tier-2 perche' non e' un troncamento da budget: la
    modifica sparirebbe dalla review in silenzio. Rilievo di GPT-5.6 Sol su
    #423, accolto — e su una PR lunga e' il caso normale, non un limite.
    """
    fn = carica_funzione(
        workflow, base_sha="b" * 40,
        pr_files=[{"filename": "core/enorme.py"},          # <- niente `patch`
                  {"filename": "core/piccolo.py", "patch": "PATCH_VS_BASE"}])
    tenuti, esclusi = fn(
        [{"filename": "core/enorme.py", "patch": "PATCH_INCREMENTALE"},
         {"filename": "core/piccolo.py", "patch": "patch della push"},
         {"filename": "arriva_da_main.py", "patch": "roba di main"}],
        "a" * 40, "c" * 40)

    assert esclusi == ["arriva_da_main.py"]
    per_nome = {t["filename"]: t for t in tenuti}

    assert per_nome["core/enorme.py"].get("patch") == "PATCH_INCREMENTALE", (
        f"{workflow}: il file ha perso il patch. A valle verrebbe saltato "
        f"senza nemmeno alzare il tier-2: modifiche vere invisibili al modello."
    )
    # Dove il patch vs base c'e', si scambia comunque: la protezione non deve
    # spegnere la correzione.
    assert per_nome["core/piccolo.py"]["patch"] == "PATCH_VS_BASE"
