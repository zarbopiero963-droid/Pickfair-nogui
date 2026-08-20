"""Un effort alto senza tetto di output adeguato falsa la review, non la migliora.

I tre reviewer non-Anthropic giravano a `reasoning effort: low`, cioe' sotto il
default del loro stesso modello (per grok-4.6 il default e' `high`). L'unico a
piena profondita', Fable, era anche l'unico che trovava difetti veri. Da qui
l'esperimento: alzarli a `high` e confrontare costo e reperti.

C'e' pero' una trappola documentata dai provider stessi:

    OpenAI      "i reasoning token contano dentro max_output_tokens";
                raccomandano di riservarne almeno 25.000
    OpenRouter  a "high" il reasoning prende ~80% di max_tokens (a "low" ~20%)

Con il tetto a 3000 il ragionamento se lo mangerebbe quasi tutto e la review
verrebbe TRONCATA: sembrerebbe che "high trova meno", cioe' la conclusione
opposta a quella vera, e l'esperimento direbbe il falso.

Il tetto e' un massimale, non un addebito: si paga cio' che viene generato.
Alzarlo non costa nulla di per se'; non alzarlo costa la validita' della misura.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict

import pytest

ROOT = Path(__file__).resolve().parents[2]

# I tre che l'esperimento tocca. Fable e' escluso di proposito: vedi in fondo.
WORKFLOWS_ESPERIMENTO = [
    ".github/workflows/pr-review-openai-gpt56-sol.yml",
    ".github/workflows/pr-review-xai-grok46.yml",
    ".github/workflows/pr-review-openrouter-fugu-ultra.yml",
]
WORKFLOW_FABLE = ".github/workflows/pr-review-claude-fable5.yml"

EFFORT_PROFONDI = {"high", "xhigh", "max"}
# Soglia dal consiglio OpenAI (>= 25.000 riservati) con un margine: sotto questo
# valore un effort profondo non ha spazio per ragionare E rispondere.
TETTO_MINIMO_PER_EFFORT_PROFONDO = 20000


def _testo(workflow: str) -> str:
    percorso = ROOT / workflow
    assert percorso.is_file(), (
        f"{workflow} non esiste: se il workflow e' stato rinominato o rimosso, "
        f"questo test va aggiornato nella stessa PR"
    )
    return percorso.read_text(encoding="utf-8")


def _senza_commenti(testo: str) -> str:
    """Righe di solo commento via: un rilievo su una parola dentro un commento
    sarebbe un falso positivo, e un test che grida al lupo si smette di leggerlo."""
    return "\n".join(r for r in testo.splitlines() if not r.strip().startswith("#"))


def _env(workflow: str) -> Dict[str, str]:
    """Legge il blocco `env:` del job leggendo il file, senza PyYAML.

    PyYAML non e' garantito negli ambienti CI di questo repo — infatti gli altri
    test sui workflow lo importano dentro un try/except. Importarlo in testa
    faceva fallire la RACCOLTA dell'intero job, non solo questo test.
    Le chiavi del job-env stanno a sei spazi d'indentazione: `      CHIAVE: valore`.
    """
    voci: Dict[str, str] = {}
    for riga in _testo(workflow).splitlines():
        m = re.match(r'^      ([A-Z][A-Z0-9_]*): +(.*?)\s*$', riga)
        if m:
            voci[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    # Zero chiavi vuol dire che il parser non ha letto NIENTE, non che l'env sia
    # vuoto: questi workflow un job-env ce l'hanno sempre. Senza questa riga il
    # fallimento del parser sarebbe silenzioso e passerebbe per un successo —
    # il test del tetto salterebbe (`effort` vuoto, soglia "non applicabile") e
    # quello su Fable passerebbe (`REVIEW_EFFORT` non e' in un dict vuoto).
    # Il guardrail sparirebbe restando verde, che e' il modo peggiore di
    # rompersi. Se cambia la forma del workflow va aggiornato il parser.
    assert voci, (
        f"{workflow}: nessuna chiave letta dal blocco `env:` del job. Il parser "
        f"cerca `      CHIAVE: valore` a sei spazi di indentazione: se il "
        f"workflow ha cambiato forma, i test dell'effort non stanno piu' "
        f"verificando niente."
    )
    return voci


@pytest.mark.parametrize("workflow", WORKFLOWS_ESPERIMENTO)
def test_block_effort_profondo_richiede_un_tetto_adeguato(workflow: str) -> None:
    """E' l'invariante che tiene in piedi la misura, non un dettaglio."""
    env = _env(workflow)
    assert "REVIEW_EFFORT" in env, (
        f"{workflow} e' fra i workflow dell'esperimento ma non dichiara "
        f"REVIEW_EFFORT nell'env del job: senza, il test salterebbe la soglia "
        f"invece di verificarla."
    )
    effort = env["REVIEW_EFFORT"].strip().lower()
    tetto = int(env.get("MAX_OUTPUT_TOKENS", "0"))

    if effort not in EFFORT_PROFONDI:
        # Non e' un buco: a effort basso la soglia non ha ragione di esistere.
        # Qui `effort` e' un valore letto davvero, non il vuoto di un parser
        # rotto — quel caso muore prima, dentro `_env`.
        pytest.skip(f"{workflow}: effort '{effort}', la soglia non si applica")

    assert tetto >= TETTO_MINIMO_PER_EFFORT_PROFONDO, (
        f"{workflow}: REVIEW_EFFORT='{effort}' con MAX_OUTPUT_TOKENS={tetto}. "
        f"A effort profondo il ragionamento mangia questo tetto (OpenAI: i "
        f"reasoning token contano dentro max_output_tokens; OpenRouter: ~80% a "
        f"'high'), quindi la review verrebbe troncata e sembrerebbe che l'effort "
        f"alto trovi meno. Alzare il tetto ad almeno "
        f"{TETTO_MINIMO_PER_EFFORT_PROFONDO}, oppure riabbassare l'effort."
    )


@pytest.mark.parametrize("workflow", WORKFLOWS_ESPERIMENTO)
def test_block_effort_arriva_da_env_non_scritto_nel_payload(workflow: str) -> None:
    """L'esperimento deve potersi fermare senza toccare il codice.

    Se il valore tornasse scritto dentro la richiesta, riabbassarlo vorrebbe dire
    un'altra PR e un altro giro di review a pagamento — cioe' il contrario del
    motivo per cui l'esperimento esiste.
    """
    testo = _testo(workflow)
    assert "REVIEW_EFFORT" in _env(workflow), f"{workflow}: REVIEW_EFFORT non nell'env"
    assert 'REVIEW_EFFORT = os.environ.get("REVIEW_EFFORT"' in testo, (
        f"{workflow}: l'effort non viene letto dall'ambiente"
    )
    for scritto in ('"reasoning": {"effort": "low"}', '"reasoning_effort": "low"',
                    '"reasoning": {"effort": "high"}', '"reasoning_effort": "high"'):
        assert scritto not in testo, (
            f"{workflow}: effort scritto nel payload ({scritto}): non si potrebbe "
            f"piu' fermare l'esperimento dall'ambiente"
        )


@pytest.mark.parametrize("workflow", WORKFLOWS_ESPERIMENTO)
def test_block_whitelist_solo_intersezione_sicura(workflow: str) -> None:
    """La whitelist non deve ammettere livelli che un provider potrebbe rifiutare.

    Rilievo di GPT-5.6 Sol e Fable: la validazione era la stessa per tutti e tre
    ma i provider non accettano gli stessi livelli. Un valore rifiutato produce
    400 e uccide la review — proprio il guasto che la guardia dovrebbe evitare —
    e lo fa solo in CI, dove nessuno lo prova prima.
    Si ammette percio' la sola intersezione documentata per tutti e tre. Allargarla
    richiede di verificare il provider, non di procedere per analogia.
    """
    codice = _senza_commenti(_testo(workflow))
    riga = next((r for r in codice.splitlines() if "_EFFORT_AMMESSI" in r and "=" in r), "")
    assert riga, f"{workflow}: _EFFORT_AMMESSI non trovata"
    ammessi = {v.strip().strip('"').strip("'") for v in
               riga.split("{", 1)[1].rsplit("}", 1)[0].split(",") if v.strip()}
    assert ammessi <= {"low", "medium", "high"}, (
        f"{workflow}: la whitelist ammette {sorted(ammessi - {'low','medium','high'})}, "
        f"livelli non documentati per tutti e tre i provider: un valore rifiutato "
        f"darebbe 400 e la review andrebbe persa"
    )


def test_block_fable_resta_a_piena_profondita() -> None:
    """Fable non entra nell'esperimento, ed e' voluto.

    E' l'unico reviewer che gira al default del suo modello, ed e' l'unico che ha
    trovato difetti veri: default silenziosi sui flag, `assert` che sparisce sotto
    `python -O`, test legato alla directory di lancio, config di produzione dentro
    il sottoprocesso. Abbassarlo lo trasformerebbe nel quarto che scrive "nessun
    bloccante". Questo test impedisce che ci finisca dentro per inerzia.
    """
    env = _env(WORKFLOW_FABLE)
    assert "REVIEW_EFFORT" not in env, (
        "Fable ha guadagnato un REVIEW_EFFORT: se e' voluto, va deciso "
        "esplicitamente e questo test aggiornato nella stessa PR"
    )
    # Si guarda il codice, non i commenti: la parola "effort" in una spiegazione
    # non e' un parametro, e un test che la scambiasse per tale sarebbe rumore.
    codice = _senza_commenti(_testo(WORKFLOW_FABLE))
    assert '"effort"' not in codice, (
        "Fable ha guadagnato un parametro effort: senza, gira al default del modello"
    )
