"""GPT-5.6 Sol gira su OpenRouter, e NON deve mescolare le due forme di API.

Dal 2026-09-14 il reviewer GPT-5.6 Sol non passa piu' dall'API OpenAI diretta —
quella chiave e' esaurita e rispondeva `credit_balance_exhausted` (HTTP 429) su
OGNI push — ma da OpenRouter, con il secret che gia' esisteva per Fugu Ultra.
Cambia il FORNITORE, non il reviewer: il modello resta `gpt-5.6-sol` e il ruolo
resta per-push.

**Il difetto che questo test esiste per impedire.** Le due API hanno forme
diverse e i campi si somigliano abbastanza da scambiarli:

    OpenAI Responses      OpenRouter chat/completions
    ------------------    ---------------------------
    "input"               "messages"
    "max_output_tokens"   "max_tokens"
    status + incomplete_details   finish_reason == "length"

Il terzo e' quello che fa male davvero. Se il workflow legge il campo di
troncamento dell'ALTRA API, `truncated` e' SEMPRE falso: una review tagliata a
meta' viene pubblicata senza il banner "Output troncato", il done_marker la
registra come completata, e nessuno la rilancia. Cioe' un FALSO VERDE prodotto
da un gate di review — la cosa peggiore, perche' e' esattamente quello a cui si
crede. Questo repository ha gia' pagato l'errore in entrambe le direzioni.

Il test guarda il codice REALE del workflow, non una copia.
"""
from __future__ import annotations

import re
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

WORKFLOW = ".github/workflows/pr-review-openrouter-gpt56-sol.yml"
WORKFLOW_RITIRATO = ".github/workflows/pr-review-openai-gpt56-sol.yml"


def _testo() -> str:
    percorso = ROOT / WORKFLOW
    assert percorso.is_file(), (
        f"{WORKFLOW} non esiste: se il workflow e' stato rinominato, questo test "
        f"va aggiornato nella stessa PR"
    )
    return percorso.read_text(encoding="utf-8")


def _script_python() -> str:
    """Ritaglia lo script python3 incorporato nel workflow."""
    righe = _testo().splitlines()
    try:
        inizio = next(i for i, r in enumerate(righe) if r.strip().startswith("python3 ")
                      and "<<" in r)
        fine = next(i for i in range(inizio + 1, len(righe))
                    if righe[i].strip() in {"PY", "PYEOF", "EOF"})
    except StopIteration:
        raise AssertionError(
            f"{WORKFLOW}: blocco python incorporato non trovato; il test lo "
            f"analizza da li', quindi va aggiornato insieme al workflow"
        ) from None
    return textwrap.dedent("\n".join(righe[inizio + 1:fine]))


def _senza_commenti(testo: str) -> str:
    """Righe di solo commento via.

    Stessa convenzione di test_ai_review_effort.py: i divieti qui sotto
    riguardano il CODICE. Il workflow spiega di proposito, nei commenti, quali
    campi appartengono all'altra API — e' documentazione utile, non un bug, e
    un test che la vietasse costringerebbe a cancellare la spiegazione per far
    passare il controllo.
    """
    return "\n".join(r for r in testo.splitlines() if not r.lstrip().startswith("#"))


# ---------------------------------------------------------------------------
# BLOCK — il workflow OpenAI e' ritirato e non deve tornare
# ---------------------------------------------------------------------------
def _env() -> dict[str, str]:
    """Il blocco `env:` del job, letto dal file SENZA PyYAML.

    PyYAML non e' installato nel job CI `tests`: importarlo in testa non fa
    fallire questo test, fa fallire la RACCOLTA e pytest interrompe l'intero
    job. E' la convenzione gia' usata da `_env()` in test_ai_review_effort.py,
    ed e' vincolata da tests/guardrails/test_raccolta_test_senza_pyyaml.py.
    Le chiavi del job-env stanno a sei spazi: `      CHIAVE: valore`.
    """
    voci: dict[str, str] = {}
    for riga in _testo().splitlines():
        m = re.match(r"^      ([A-Z][A-Z0-9_]*): +(.*?)\s*$", riga)
        if m:
            voci[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    # Zero chiavi vuol dire che il parser non ha letto NIENTE, non che l'env
    # sia vuoto: questo workflow un job-env ce l'ha sempre. Senza questa riga
    # il fallimento del parser sarebbe silenzioso e passerebbe per successo.
    assert voci, (
        f"{WORKFLOW}: nessuna chiave letta dal blocco `env:` del job. Il parser "
        f"cerca `      CHIAVE: valore` a sei spazi d'indentazione: se il "
        f"workflow ha cambiato forma, questi test non verificano piu' niente."
    )
    return voci


def _tipi_del_trigger(evento: str) -> list[str]:
    """La lista `types:` di un evento sotto `on:`, letta dal file senza PyYAML."""
    testo = _senza_commenti(_testo())
    m = re.search(
        rf"^on:\n(?:.*\n)*?  {re.escape(evento)}:\n    types: \[([^\]]*)\]",
        testo,
        re.MULTILINE,
    )
    # Nessun match = il trigger non c'e' PIU' nella forma attesa. Non si
    # restituisce una lista vuota (che farebbe passare i divieti "x not in
    # tipi" a vuoto): si fallisce, perche' un guardrail che non trova quel che
    # controlla e' rotto, non soddisfatto.
    assert m, (
        f"{WORKFLOW}: non trovo `on: -> {evento}: -> types: [...]`. O il "
        f"trigger e' stato tolto, o il workflow ha cambiato forma e questo "
        f"guardrail va aggiornato nella stessa PR."
    )
    return [t.strip() for t in m.group(1).split(",") if t.strip()]


def test_block_il_workflow_openai_e_ritirato() -> None:
    assert not (ROOT / WORKFLOW_RITIRATO).is_file(), (
        f"{WORKFLOW_RITIRATO} e' tornato. La chiave OpenAI e' esaurita: quel "
        f"workflow produce solo un 429 rosso su ogni push, che non e' un difetto "
        f"del codice ma rumore sul gate. Il ruolo e' passato a OpenRouter."
    )


def test_block_nessun_riferimento_al_secret_openai_dismesso() -> None:
    testo = _testo()
    assert "PICKFAIR_OPENAI" not in testo, (
        "il workflow usa ancora il secret OpenAI dismesso invece di "
        "OPENROUTER_PICKFAIR"
    )
    assert "secrets.OPENROUTER_PICKFAIR" in testo, (
        "il workflow non legge OPENROUTER_PICKFAIR: e' la chiave che gia' "
        "esisteva per Fugu Ultra, riusata apposta per non crearne una nuova"
    )


# ---------------------------------------------------------------------------
# BLOCK — endpoint e id modello: i due punti dove si prende un 404
# ---------------------------------------------------------------------------
def test_block_endpoint_openrouter_col_segmento_api_v1() -> None:
    script = _script_python()
    assert "https://openrouter.ai/api/v1/chat/completions" in script, (
        "endpoint sbagliato. Il segmento /api/v1 e' OBBLIGATORIO: senza, "
        "OpenRouter risponde 404 e il reviewer sembra rotto invece che "
        "mal configurato"
    )
    assert "api.openai.com" not in _senza_commenti(script), (
        "il workflow punta ancora all'API OpenAI diretta"
    )


def test_block_model_id_col_prefisso_del_produttore() -> None:
    """Il VALORE reale della env, non la stringa da qualche parte nel file.

    Prima versione di questo test: `assert "openai/gpt-5.6-sol" in testo`.
    Passava anche togliendo il prefisso dalla env, perche' la stringa resta nei
    commenti che lo spiegano — cioe' controllava la prosa invece del codice.
    Trovato col sabotaggio, che e' il motivo per cui si sabotano i test.
    """
    env = _env()
    modello = env.get("OPENROUTER_MODEL")
    assert modello == "openai/gpt-5.6-sol", (
        f"OPENROUTER_MODEL = {modello!r}. Su OpenRouter l'id porta il prefisso "
        f"del produttore: senza `openai/` la richiesta viene rifiutata come "
        f"modello sconosciuto, e il reviewer sembra rotto invece che mal "
        f"configurato."
    )
    assert "OPENAI_MODEL" not in env, (
        "resta la env del vecchio trasporto OpenAI"
    )


# ---------------------------------------------------------------------------
# BLOCK — il cuore: NON mescolare le due forme di API
# ---------------------------------------------------------------------------
def test_block_payload_e_nella_forma_chat_completions() -> None:
    script = _script_python()
    assert '"messages"' in script, (
        "il prompt non e' in `messages`: e' la forma di chat/completions. "
        "`input` appartiene alla Responses API di OpenAI"
    )
    assert '"input": [' not in _senza_commenti(script), (
        "il payload usa ancora `input`, che e' la forma dell'ALTRA API"
    )
    assert '"max_tokens"' in script, (
        "il tetto di output su chat/completions si chiama `max_tokens`"
    )
    assert '"max_output_tokens":' not in _senza_commenti(script), (
        "il payload usa ancora `max_output_tokens`, che e' dell'ALTRA API"
    )


def test_block_il_troncamento_si_legge_da_finish_reason() -> None:
    """Il difetto peggiore: leggere il campo dell'altra API = mai accorgersene."""
    script = _script_python()
    assert 'finish_reason") == "length"' in script, (
        "il troncamento non viene letto da finish_reason=='length'. Su "
        "chat/completions e' l'UNICO campo che lo segnala: senza, `truncated` e' "
        "sempre falso e una review tagliata viene pubblicata come completa, con "
        "tanto di done_marker che impedisce di rifarla. Falso verde su un gate "
        "di review."
    )
    assert "incomplete_details" not in _senza_commenti(script), (
        "il workflow cerca ancora `incomplete_details`, che su chat/completions "
        "non esiste: il troncamento non verrebbe MAI rilevato"
    )
    # E il banner deve restare: e' cio' che fa scattare il guard del chiamante.
    assert "Output troncato" in script, (
        "sparito il banner di troncamento: senza, il chiamante non puo' "
        "distinguere una review parziale da una completa"
    )


def test_block_chiede_i_token_reali_invece_di_stimarli() -> None:
    script = _script_python()
    assert '"usage": {"include": True}' in script, (
        "senza usage.include OpenRouter non restituisce i token reali e il "
        "costo riportato sarebbe una stima locale spacciata per misura"
    )
    assert 'source = "OpenRouter usage"' in script, (
        "l'etichetta della fonte dei token dice ancora OpenAI: il rapporto "
        "costi direbbe il falso sulla provenienza del dato"
    )


# ---------------------------------------------------------------------------
# BLOCK — il RUOLO non cambia: per-push, nessun gate a label
# ---------------------------------------------------------------------------
def test_block_resta_un_reviewer_per_push_senza_label_gate() -> None:
    # _tipi_del_trigger fallisce se `on: -> pull_request_target: -> types:` non
    # c'e' piu': e' la forma che esegue il file dal branch BASE, cosi' una PR
    # non puo' esfiltrare i secret modificando il proprio .yml.
    tipi = _tipi_del_trigger("pull_request_target")
    assert set(tipi) == {"opened", "synchronize", "reopened", "ready_for_review"}, (
        f"il gating per-push e' cambiato: {tipi}. GPT-5.6 Sol e' uno dei due "
        f"reviewer che girano su OGNI push (con Grok 4.6); i due forti a label "
        f"sono Fugu Ultra e Fable 5."
    )
    assert "labeled" not in tipi, (
        "aggiunto un gate a label: cosi' Sol smetterebbe di coprire i push "
        "intermedi e resterebbe solo Grok"
    )


# ---------------------------------------------------------------------------
# BLOCK — ogni messaggio di NON-review deve far scattare il guard del chiamante
#
# Rilievo di Claude Fable 5 sulla PR #463, ed era un bug REALE introdotto
# mescolando i due file: il ramo `choices` vuoto viene da Fugu, il guard viene
# dal file OpenAI — e quel guard non elenca "choices", perche' la Responses API
# non ha quel campo. Risultato:
#
#   "Il modello non ha restituito choices."  ->  review_complete = True
#
# cioe' il done_marker veniva scritto, il range registrato come RECENSITO e i
# re-run deduplicati: una non-review pubblicata come review completa. Lo stesso
# falso verde che questa PR dichiara di voler togliere. (Fugu Ultra la voce ce
# l'ha: il difetto e' nato nel trapianto, non era in nessuno dei due originali.)
#
# Il test non pinna la singola stringa mancante: estrae TUTTI i messaggi di
# non-review che `call_model` puo' restituire e verifica che il guard li prenda
# tutti. Cosi' copre anche il prossimo messaggio aggiunto e dimenticato.
# ---------------------------------------------------------------------------
import ast  # noqa: E402  (vicino al test che lo usa, per leggibilita')

#: Un messaggio che comincia con uno di questi NON e' una review vera.
_PREFISSI_NON_REVIEW = ("Il modello non ha restituito", "Output troncato")


def _messaggi_di_non_review(albero: ast.Module) -> set[str]:
    fn = next((n for n in ast.walk(albero)
               if isinstance(n, ast.FunctionDef) and n.name == "call_model"), None)
    assert fn is not None, "call_model non trovata nello script del workflow"
    return {
        n.value for n in ast.walk(fn)
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
        and n.value.startswith(_PREFISSI_NON_REVIEW)
    }


def _prefissi_del_guard(albero: ast.Module) -> tuple[str, ...]:
    for nodo in ast.walk(albero):
        if (isinstance(nodo, ast.Assign) and len(nodo.targets) == 1
                and isinstance(nodo.targets[0], ast.Name)
                and nodo.targets[0].id == "review_complete"):
            # review_complete = not review.lstrip().startswith((...))
            chiamata = next((n for n in ast.walk(nodo)
                             if isinstance(n, ast.Call)
                             and getattr(n.func, "attr", "") == "startswith"), None)
            assert chiamata and chiamata.args, "guard startswith senza argomenti"
            arg = chiamata.args[0]
            elementi = arg.elts if isinstance(arg, (ast.Tuple, ast.List)) else [arg]
            return tuple(e.value for e in elementi if isinstance(e, ast.Constant))
    raise AssertionError(
        "assegnazione `review_complete` non trovata: il guard che decide il "
        "done_marker e' cambiato forma, questo test va aggiornato con esso"
    )


def test_block_ogni_non_review_fa_scattare_il_guard() -> None:
    albero = ast.parse(_script_python())
    guard = _prefissi_del_guard(albero)
    messaggi = _messaggi_di_non_review(albero)
    assert messaggi, "nessun messaggio di non-review trovato: regex o codice cambiati?"

    sfuggiti = sorted(m for m in messaggi if not m.lstrip().startswith(guard))
    assert not sfuggiti, (
        f"questi messaggi di NON-review non fanno scattare il guard: {sfuggiti}.\n"
        f"Prefissi riconosciuti: {list(guard)}.\n"
        f"Conseguenza: review_complete=True, done_marker scritto, il range "
        f"registrato come recensito e i re-run deduplicati — una non-review "
        f"pubblicata come completa."
    )
