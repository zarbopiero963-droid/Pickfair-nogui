"""Cio' che i reviewer marcano come critico, devono anche redigerlo.

Rilievo di Fugu Ultra e Claude Fable 5.1 sulla full-range della #477, e la
prima risposta dell'agente ("`app_?key` non compare affatto, premessa
inventata") era SBAGLIATA: il grep cercava `app_?key` come espressione, non
come testo letterale, e non lo trovava. Il pattern c'e', in tutti e cinque i
workflow:

    CRITICAL_PATTERNS: ... |certlogin|app_?key|config|settings
    REDACTIONS       : ... (api[_-]?key|token|secret|password|...)

Le due liste dicono cose diverse sullo stesso segreto. La prima dichiara che
una `app_key` Betfair nel diff e' materia da controllo manuale; la seconda non
la redige, quindi quel valore arriva in chiaro al modello e dentro il commento
pubblicato. Una asimmetria fra "lo considero critico" e "lo nascondo" e'
proprio il caso in cui un segreto esce.

Il test non legge la lista: ESEGUE la regex estratta dal workflow vero e
verifica che il valore sparisca. Una lista che sembra giusta ma non matcha
sarebbe un verde falso.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Set

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Segreti che i workflow dichiarano critici e che quindi devono sparire.
# `app_key` e' la Betfair application key: money-critical su questo repo.
SEGRETI_DA_REDIGERE = (
    "api_key=SEGRETISSIMO",
    "app_key=SEGRETISSIMO",
    "appKey: SEGRETISSIMO",
    "token=SEGRETISSIMO",
    "secret=SEGRETISSIMO",
    "password=SEGRETISSIMO",
)
VALORE = "SEGRETISSIMO"


def _reviewer_su_disco() -> Set[str]:
    return {
        f".github/workflows/{p.name}"
        for p in (ROOT / ".github" / "workflows").glob("pr-review-*.yml")
    }


def _regex_generica(workflow: str) -> re.Pattern:
    """La regola `(chiave)\\s*[:=]\\s*valore` di REDACTIONS, compilata.

    Si cerca la riga che sostituisce con `\\1=[REDACTED]`: e' l'unica regola
    generica per coppie chiave/valore. Se non si trova, si FERMA invece di
    saltare il controllo: un workflow che perde la redazione generica senza
    farlo notare e' esattamente lo scenario da impedire.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    m = re.search(r're\.compile\(r"((?:[^"\\]|\\.)*)"\),\s*r"\\1=\[REDACTED\]"',
                  testo)
    assert m, (
        f"{workflow}: regola generica di REDACTIONS non trovata. Se la forma "
        f"e' cambiata, questo test va aggiornato nella stessa PR invece di "
        f"lasciare la redazione non verificata"
    )
    return re.compile(m.group(1))


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("campione", SEGRETI_DA_REDIGERE)
def test_block_il_valore_del_segreto_non_sopravvive_alla_redazione(
    workflow: str, campione: str
) -> None:
    redatto = _regex_generica(workflow).sub(r"\1=[REDACTED]", campione)
    assert VALORE not in redatto, (
        f"{workflow}: {campione!r} resta in chiaro dopo la redazione "
        f"({redatto!r}). Il diff viene inviato al modello e ripubblicato nel "
        f"commento: un valore non redatto esce da entrambe le parti"
    )


def test_block_ogni_chiave_critica_dichiarata_e_anche_redatta() -> None:
    """`CRITICAL_PATTERNS` e `REDACTIONS` non devono dire cose diverse.

    Se un workflow dichiara critica una chiave-segreto, deve anche redigerla:
    marcare `manual-review-required` e poi spedire il valore in chiaro e' la
    combinazione peggiore, perche' sembra che il caso sia gestito.
    """
    mancanti = []
    for workflow in sorted(_reviewer_su_disco()):
        testo = (ROOT / workflow).read_text(encoding="utf-8")
        critiche = set(re.findall(r"\b([a-z]+[_-]?\??key)\b", testo.lower()))
        generica = _regex_generica(workflow).pattern
        for chiave in sorted(critiche):
            # `app_?key` nel sorgente e' una regex: il caso concreto e' `app_key`
            concreta = chiave.replace("?", "")
            if not _regex_generica(workflow).search(f"{concreta}=X"):
                mancanti.append(f"  {workflow}: '{chiave}' critica ma non redatta "
                                f"(generica: {generica[:60]}...)")
    assert not mancanti, (
        "chiavi dichiarate critiche che la redazione non copre —\n"
        + "\n".join(mancanti)
    )
