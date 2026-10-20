"""#525 — gli orchestratori CI devono potersi avviare.

`ci-master-gate.yml` e `ci-dynamic-intelligent.yml` richiamano altri workflow
del repository con `uses: ./.github/workflows/<file>.yml`. GitHub accetta la
chiamata solo se il workflow richiamato dichiara `on: workflow_call`; se ne
manca anche uno, rifiuta l'INTERO workflow chiamante all'avvio (0 job, nessun
log), e l'orchestrazione non esiste: era il caso di `dutching.yml`,
`pnl-engine.yml` e `live-gate.yml`, 100 run su 100 falliti.

Il test segue il grafo delle chiamate in profondità a partire dagli
orchestratori, quindi copre anche i livelli annidati
(master -> dynamic -> modulo -> _module-ultra-check).
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
ORCHESTRATORI = ("ci-master-gate.yml", "ci-dynamic-intelligent.yml")
PREFISSO_LOCALE = "./.github/workflows/"


def _carica(nome: str) -> dict:
    dati = yaml.safe_load((WORKFLOWS / nome).read_text(encoding="utf-8"))
    assert isinstance(dati, dict), f"{nome}: YAML non e' una mappa"
    return dati


def _trigger(dati: dict) -> dict:
    # PyYAML legge la chiave `on` come booleano True (YAML 1.1).
    blocco = dati.get("on", dati.get(True))
    if isinstance(blocco, str):
        return {blocco: None}
    if isinstance(blocco, list):
        return {voce: None for voce in blocco}
    assert isinstance(blocco, dict), "blocco `on:` assente o malformato"
    return blocco


def _chiamate_locali(dati: dict) -> list[tuple[str, str]]:
    chiamate = []
    for job_id, job in (dati.get("jobs") or {}).items():
        uses = (job or {}).get("uses")
        if isinstance(uses, str) and uses.startswith(PREFISSO_LOCALE):
            chiamate.append((job_id, uses[len(PREFISSO_LOCALE):]))
    return chiamate


def _grafo(radice: str) -> list[tuple[str, str, str]]:
    """(chiamante, job, chiamato) per ogni chiamata raggiungibile da `radice`."""
    archi, visti, coda = [], set(), [radice]
    while coda:
        nome = coda.pop()
        if nome in visti:
            continue
        visti.add(nome)
        for job_id, chiamato in _chiamate_locali(_carica(nome)):
            archi.append((nome, job_id, chiamato))
            coda.append(chiamato)
    return archi


@pytest.mark.parametrize("orchestratore", ORCHESTRATORI)
def test_ogni_workflow_richiamato_dichiara_workflow_call(orchestratore):
    archi = _grafo(orchestratore)
    assert archi, f"{orchestratore} non richiama alcun workflow: grafo vuoto"
    mancanti = [
        f"{chiamante}:{job} -> {chiamato}"
        for chiamante, job, chiamato in archi
        if "workflow_call" not in _trigger(_carica(chiamato))
    ]
    assert not mancanti, (
        "workflow richiamati senza `on: workflow_call` (GitHub rifiuta il "
        "chiamante all'avvio): " + ", ".join(sorted(set(mancanti)))
    )


@pytest.mark.parametrize("orchestratore", ORCHESTRATORI)
def test_ogni_workflow_richiamato_esiste(orchestratore):
    assenti = sorted(
        {chiamato for _, _, chiamato in _grafo(orchestratore) if not (WORKFLOWS / chiamato).is_file()}
    )
    assert not assenti, f"workflow richiamati inesistenti: {assenti}"


@pytest.mark.parametrize("nome", ["dutching.yml", "pnl-engine.yml", "live-gate.yml"])
def test_i_tre_workflow_restano_avviabili_anche_da_soli(nome):
    # La correzione aggiunge `workflow_call` senza togliere i trigger diretti:
    # le lane continuano a girare da sole sulle PR che toccano i loro path.
    trigger = _trigger(_carica(nome))
    assert "workflow_call" in trigger
    assert "pull_request" in trigger and "push" in trigger
    assert trigger["pull_request"] and trigger["pull_request"].get("paths"), (
        f"{nome}: filtro paths della PR perso"
    )
