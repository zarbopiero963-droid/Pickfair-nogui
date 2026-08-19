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

from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

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


def _env(workflow: str) -> Dict[str, Any]:
    dati = yaml.safe_load((ROOT / workflow).read_text(encoding="utf-8"))
    job = (dati.get("jobs") or {}).get("review") or {}
    return job.get("env") or {}


@pytest.mark.parametrize("workflow", WORKFLOWS_ESPERIMENTO)
def test_block_effort_profondo_richiede_un_tetto_adeguato(workflow: str) -> None:
    """E' l'invariante che tiene in piedi la misura, non un dettaglio."""
    env = _env(workflow)
    effort = str(env.get("REVIEW_EFFORT", "")).strip().lower()
    tetto = int(str(env.get("MAX_OUTPUT_TOKENS", "0")))

    if effort not in EFFORT_PROFONDI:
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
    testo = (ROOT / workflow).read_text(encoding="utf-8")
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
    testo = (ROOT / WORKFLOW_FABLE).read_text(encoding="utf-8")
    assert '"effort"' not in testo, (
        "Fable ha guadagnato un parametro effort: senza, gira al default 'high'"
    )
