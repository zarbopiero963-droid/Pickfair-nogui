"""Le automazioni ritirate non devono tornare, e non deve tornare la loro FORMA.

Il 2026-08-20 e' stata rimossa la catena Telegram -> CloudCLI -> GitHub, insieme
alle automazioni di notifica e watchdog non piu' usate: 15 workflow, ~6.500
righe. Gli schedule che si svegliano da soli sono passati da 7 a 1.

`.github/quarantined-workflows/` resta invece dov'e', ed e' voluto: la copia
`.disabled` e' la PROVA che quei workflow sono parcheggiati di proposito e non
spariti per sbaglio, ed e' verificata da `scripts/ci/check_ci_quarantine.py`
in modo fail-closed. Toglierla vorrebbe dire riscrivere quella guardia, che e'
un lavoro a se'.

Il pezzo pericoloso non era "vecchio codice fermo". `telegram-command-bridge.yml`
aveva:

    "on":
      schedule:
        - cron: "*/5 * * * *"     <- si svegliava da solo, ogni 5 minuti
    permissions:
      actions: write              <- e poteva lanciare altri workflow

e come unico controllo `chatId !== allowedChatId`, senza mai guardare
`message.from.id`. Se la chat autorizzata era un gruppo, chiunque nel gruppo
poteva far partire `cloudcli-controller`, che consegnava PICKFAIR_ACTIONS_TOKEN
a un servizio esterno con facolta' di commit, push e PR.

Un elenco di nomi da solo non basterebbe: si aggira rinominando il file. Per
questo il test afferma anche l'INVARIANTE DI FORMA — nessun workflow che parte
da solo puo' anche lanciare altri workflow — che e' la combinazione che rendeva
quella catena raggiungibile senza alcun accesso a GitHub.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"

RITIRATI = [
    # la catena Telegram -> CloudCLI -> GitHub
    "telegram-command-bridge.yml",
    "cloudcli-controller.yml",
    "cloudcli-new-task.yml",
    "cloudcli-pr-fix.yml",
    "cloudcli-auto-pr-loop.yml",
    "cloudcli-health.yml",
    # notifiche e monitoraggio non piu' usati
    "pr-telegram-notify.yml",
    "pr-flow-guardrails.yml",
    "pr-health-monitor.yml",
    "audit-result-telegram.yml",
    "pr-self-check-refresh.yml",
    "pr-safe-autofix-canary.yml",
    # infrastruttura del runner self-hosted dismesso
    "vps-runner-health.yml",
    "runner-watchdog-dispatch.yml",
    "selfhosted-smoke.yml",
]


def _workflow_files() -> List[Path]:
    file = sorted(WORKFLOWS.glob("*.yml"))
    assert file, f"{WORKFLOWS} non contiene workflow: percorso sbagliato?"
    return file


@pytest.mark.parametrize("nome", RITIRATI)
def test_block_un_workflow_ritirato_non_deve_tornare(nome: str) -> None:
    assert not (WORKFLOWS / nome).exists(), (
        f"{nome} e' stato ritirato il 2026-08-20 e non deve rientrare. Se serve "
        f"davvero, va rivisto prima: la versione rimossa aveva i difetti "
        f"documentati nel docstring di questo test."
    )


def test_block_chi_parte_da_solo_non_puo_lanciare_altri_workflow() -> None:
    """L'invariante di forma, quella che un rinomino non aggira.

    `schedule` significa "parte senza che nessuno lo chieda"; `actions: write`
    significa "puo' far partire altri workflow". Insieme fanno un ponte che si
    apre da solo verso tutto il resto della CI: e' esattamente cio' che rendeva
    la vecchia catena raggiungibile da chi non aveva alcun accesso a GitHub.

    Se un domani servisse davvero, va spezzato in due: il workflow schedulato
    raccoglie e basta, un altro — con un innesco esplicito e un'autorizzazione
    per UTENTE, non solo per canale — decide se agire.
    """
    colpevoli = []
    for f in _workflow_files():
        testo = f.read_text(encoding="utf-8")
        parte_da_solo = re.search(r'^\s*-\s*cron:', testo, re.M)
        puo_lanciare = re.search(r'^\s+actions:\s*write\s*$', testo, re.M)
        if parte_da_solo and puo_lanciare:
            colpevoli.append(f.name)

    assert not colpevoli, (
        f"Questi workflow partono da soli (`schedule`) E possono lanciarne altri "
        f"(`actions: write`): {', '.join(colpevoli)}. E' la forma della catena "
        f"rimossa il 2026-08-20. Vanno spezzati, non ammessi."
    )


def test_block_nessun_riferimento_pendente_ai_workflow_ritirati() -> None:
    """Un `workflow_run` che aspetta un workflow inesistente non da' errore:
    semplicemente non parte mai. Un innesco morto in silenzio e' peggio di uno
    rotto, perche' sembra che ci sia."""
    nomi_file = set(RITIRATI)
    pendenti = []
    for f in _workflow_files():
        for riga in f.read_text(encoding="utf-8").splitlines():
            for morto in nomi_file:
                if morto in riga:
                    pendenti.append(f"{f.name}: {riga.strip()}")

    assert not pendenti, (
        "Riferimenti a workflow ritirati:\n  " + "\n  ".join(pendenti)
    )
