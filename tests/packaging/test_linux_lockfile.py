"""Guardrail sull'hash-lock della toolchain di build Linux (PR-B).

Blinda il contratto usato da ``requirements-build-linux.in`` +
``.github/workflows/generate-linux-lockfile.yaml`` +
``.github/workflows/build-linux.yml`` senza dipendere da rete/PyPI:

- il ``.in`` copre l'intera toolchain di build (runtime + pytest + GUI +
  PyInstaller);
- il workflow di generazione pinna il resolver, genera con ``--generate-hashes``,
  pubblica il lock nella Job Summary e ha i gate anti-stale / anti-deletion;
- ``build-linux.yml`` consuma il lock con ``--require-hashes`` gated sulla sua
  presenza (fallback legacy finche' il lock non e' committato);
- SE il lock e' committato: e' in forma ``--require-hashes`` (ogni pin ha un
  hash) e non contiene path assoluti del runner (riproducibilita').

Controlli statici (nessun build/install reale). ``unit`` per la suite veloce.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
IN = ROOT / "requirements-build-linux.in"
LOCK = ROOT / "requirements-build-linux.lock"
GEN_WF = ROOT / ".github" / "workflows" / "generate-linux-lockfile.yaml"
BUILD_WF = ROOT / ".github" / "workflows" / "build-linux.yml"

pytestmark = pytest.mark.unit


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def test_in_covers_full_build_toolchain():
    text = _read(IN)
    # runtime + test runner (via requirements-dev.txt -> requirements-test.txt)
    assert "-r requirements-dev.txt" in text, "il .in deve includere requirements-dev.txt"
    # extra di build non presenti nei requirements applicativi
    assert "customtkinter" in text, "manca customtkinter (GUI)"
    assert "pyinstaller" in text, "manca pyinstaller (packaging onefile)"


def test_generate_workflow_is_deterministic_and_reproducible():
    wf = _read(GEN_WF)
    # resolver deterministico pinnato
    assert 'pip==24.3.1' in wf, "pip non pinnato (resolver non deterministico)"
    assert 'pip-tools==7.4.1' in wf, "pip-tools non pinnato (resolver non deterministico)"
    # generazione con hash
    assert "--generate-hashes" in wf, "il lock va generato con --generate-hashes"
    # riproducibilita' cross-machine: header senza path (CUSTOM_COMPILE_COMMAND)
    # + niente annotazioni "# via -r <path assoluto>" (--no-annotate)
    assert "CUSTOM_COMPILE_COMMAND" in wf, "manca CUSTOM_COMPILE_COMMAND (header con path assoluto)"
    assert "--no-annotate" in wf, "manca --no-annotate (i commenti # via embeddano il path assoluto del checkout)"
    # consegna via Job Summary (non commit dal workflow)
    assert "GITHUB_STEP_SUMMARY" in wf, "il lock va pubblicato nella Job Summary"
    # anti-deletion funzionante: serve la history completa (fetch-depth: 0),
    # altrimenti il checkout shallow rende la guardia un no-op silenzioso.
    assert "fetch-depth: 0" in wf, "manca fetch-depth: 0 (anti-deletion bypassata su checkout shallow)"
    # gate anti-stale + anti-deletion vs base
    assert "diff -u" in wf, "manca il gate anti-stale (diff con la rigenerazione)"
    assert "base.sha" in wf, "manca la guardia anti-deletion vs base ref"


def test_generate_workflow_seeds_from_committed_lock():
    """Il gate anti-stale deve confrontare col lock, non con l'ultima versione.

    `pip-compile` tratta i pin gia' presenti nel file di output come vincoli e
    cambia solo cio' che il `.in` impone. Se il file di destinazione parte
    VUOTO non ha niente da preservare e risolve tutto all'ultima versione
    disponibile: il gate pretende allora che il lock committato le combaci,
    cioe' obbliga ogni PR che tocca un requirements* a inghiottire la deriva
    accumulata dagli upstream.

    Misurato su #430 aggiungendo la sola PySocks:
        da zero in /tmp     -> pysocks + 11 bump non richiesti
        sul lock committato -> pysocks, e nient'altro

    Un lock hash-verificato serve a rendere gli aggiornamenti DELIBERATI. Senza
    il seed li rendeva obbligatori e invisibili, che e' il contrario.
    """
    wf = _read(GEN_WF)
    assert "cp requirements-build-linux.lock /tmp/fresh-linux.lock" in wf, (
        "il file di destinazione non parte dal lock committato: senza seed "
        "pip-compile risolve tutto all'ultima versione e il gate anti-stale "
        "pretende la deriva"
    )

    # L'ordine va verificato DENTRO lo script dello step, non sul file intero:
    # `pip-compile --generate-hashes` compare anche nella stringa
    # CUSTOM_COMPILE_COMMAND, che sta dentro il blocco `env:` dello stesso step
    # e non e' un'invocazione: va escluso restringendosi al `run:`.
    # (La prima versione di questo test cadeva proprio li' — e un test che
    # fallisce per il motivo sbagliato non dimostra niente.)
    step = wf.split("- name: Compile lock (hashes)", 1)[1].split("- name:", 1)[0]
    script = step.split("run: |", 1)[1]
    invocazione = script.index("pip-compile --generate-hashes --allow-unsafe")
    assert script.index("cp requirements-build-linux.lock") < invocazione, (
        "il seed avviene dopo la compilazione: sovrascriverebbe il risultato"
    )
    # Il seed e' condizionale: al primo giro il lock non esiste ancora e
    # `cp` fallirebbe con `set -euo pipefail` non ancora attivo qui.
    assert "if [ -f requirements-build-linux.lock ]" in wf, (
        "il seed non e' condizionale: fallirebbe quando il lock non esiste ancora"
    )


def test_socks_support_is_declared_for_the_betfair_proxy():
    """Un proxy `socks5` senza PySocks ferma l'avvio (#430).

    `requests` non dichiara SOCKS fra le sue dipendenze: lo supporta solo con
    l'extra `requests[socks]`, che installa PySocks. `betfair_client` lo
    verifica all'avvio e si ferma con un messaggio azionabile; questo test
    fissa il fatto che la dipendenza sia DICHIARATA, cosi' che il messaggio
    resti un promemoria invece che l'unico modo di scoprirlo.
    """
    radice = ROOT
    runtime = (radice / "requirements.txt").read_text(encoding="utf-8")
    lock_runtime = (radice / "requirements-lock.txt").read_text(encoding="utf-8")
    lock_build = (radice / "requirements-build-linux.lock").read_text(encoding="utf-8")

    assert "PySocks" in runtime, "PySocks non dichiarata in requirements.txt"
    assert "PySocks==" in lock_runtime, "PySocks non pinnata in requirements-lock.txt"
    # Nel lock hash-verificato il nome e' normalizzato in minuscolo da pip-compile.
    assert "\npysocks==" in lock_build, (
        "PySocks assente dal lock hash-verificato della toolchain di build"
    )


def test_aiohttp_resta_fuori_finche_nessuno_la_importa():
    """`aiohttp` era dichiarata e non importata da nessuno (#374 follow-up 5).

    Zero occorrenze in tutto il repo, codice e test; non richiesta da telethon
    (`pyaes`, `rsa`) ne' da betfairlightweight (`requests`). Portava **35**
    advisory `pip-audit` per zero funzionalita': superficie di supply-chain a
    fondo perduto, quindi e' stata tolta.

    Questo test non vieta di riusarla: vieta di ri-DICHIARARLA senza usarla.
    Se un domani serve, si aggiunge l'import e il test smette di lamentarsi da
    solo.
    """

    radice = ROOT

    # Si guarda la RIGA DI REQUISITO, non il testo del file: qui sopra c'e' un
    # commento in requirements.txt che spiega la rimozione e contiene la parola
    # `aiohttp`. Cercare la stringa faceva risultare la dipendenza "dichiarata"
    # per colpa della propria spiegazione. (Seconda volta nello stesso test che
    # un confronto testuale mente: vedi anche il commento sull'import.)
    REQUISITO = re.compile(r"^\s*aiohttp\b", re.IGNORECASE)

    def _dichiarata(nome):
        f = radice / nome
        if not f.exists():
            return False
        return any(
            REQUISITO.match(riga)
            for riga in f.read_text(encoding="utf-8").splitlines()
            if riga.strip() and not riga.lstrip().startswith("#")
        )

    if not any(_dichiarata(n) for n in ("requirements.txt", "requirements-lock.txt")):
        return

    # Si cerca un IMPORT, non la stringa: questo file stesso contiene la parola
    # `aiohttp` una dozzina di volte, e cercarla renderebbe il test sempre
    # verde. (Prima versione: esattamente cosi'. Il sabotaggio di controllo —
    # ri-dichiarare aiohttp senza usarla — non faceva scattare niente. E' la
    # stessa trappola gia' vista su #430: un test che passa per il motivo
    # sbagliato.)
    IMPORT = re.compile(r"^\s*(?:import\s+aiohttp|from\s+aiohttp[\s.])", re.MULTILINE)
    questo_file = Path(__file__).resolve()

    # Le cartelle da NON guardare: una virtualenv nel repo contiene
    # `import aiohttp` dentro site-packages, e il test passerebbe per il motivo
    # sbagliato — la stessa trappola che questo commento dichiarava di evitare.
    # Rilievo di Claude Fable 5 su #432, fondato: ci ero cascato di nuovo.
    IGNORA = {
        ".git", ".venv", "venv", "env", "site-packages", "node_modules",
        "build", "dist", ".tox", ".mypy_cache", ".pytest_cache", "__pycache__",
    }

    usata = False
    for sorgente in radice.rglob("*.py"):
        if sorgente == questo_file or IGNORA & set(sorgente.parts):
            continue
        try:
            if IMPORT.search(sorgente.read_text(encoding="utf-8")):
                usata = True
                break
        except (OSError, UnicodeDecodeError):  # pragma: no cover - file illeggibile
            continue

    assert usata, (
        "aiohttp e' di nuovo dichiarata nei requirements ma nessun file .py la "
        "importa: e' stata rimossa perche' portava 35 advisory per zero "
        "funzionalita'. Se serve davvero, usala; altrimenti non dichiararla."
    )


def test_i_pin_del_lock_non_sono_sotto_le_versioni_note_vulnerabili():
    """Fissa i minimi raggiunti dal follow-up 5, cosi' non si torna indietro.

    Non e' un audit — quello richiede rete e non lo si fa in un test unitario.
    E' un fermo sui numeri gia' verificati con `pip-audit`: 44 rilievi -> 1.

    `pytest` e' pinnato a 9.1.1 e non al minimo 9.0.3: e' la versione con cui
    la suite gira davvero qui, quindi il lock riflette una combinazione provata
    invece del minimo teorico.

    L'ULTIMO non e' chiudibile: `requests` ha un advisory che si risolve solo
    in 2.33.0, e betfairlightweight impone `requests<2.33.0` in OGNI sua
    versione, ultima compresa (2.23.2). 2.32.5 e' il massimo consentito.
    """
    lock = (ROOT / "requirements-lock.txt").read_text(encoding="utf-8")
    for pin in ("urllib3==2.7.0", "pytest==9.1.1", "requests==2.32.5"):
        assert pin in lock, f"{pin} non piu' nel lock: non scendere sotto (#374 follow-up 5)"
    # pytest 9 esige pytest-asyncio >= 1.x: 0.24.0 dichiara `pytest<9`.
    assert "pytest-asyncio==1." in lock, (
        "pytest-asyncio deve restare 1.x: le 0.x dichiarano `pytest<9` e "
        "romperebbero la risoluzione con il pytest 9.x pinnato qui"
    )


def test_generate_workflow_triggers_on_transitive_requirements():
    # Il .in include transitivamente requirements.txt/dev/test: un bump la' deve
    # ri-triggerare l'anti-stale, altrimenti il lock diventa silenziosamente
    # obsoleto (rilievo GPT-5.6 Sol / Fable 5).
    wf = _read(GEN_WF)
    for req in ("requirements.txt", "requirements-dev.txt", "requirements-test.txt"):
        assert f'"{req}"' in wf, f"il trigger del workflow non include {req}"


def test_build_workflow_consumes_lock_fail_closed():
    wf = _read(BUILD_WF)
    assert "--require-hashes" in wf, "build-linux non installa hash-locked"
    assert "requirements-build-linux.lock" in wf, "build-linux non referenzia il lock"
    # FAIL-CLOSED: senza il lock si fallisce, NON si ripiega su un install non
    # pinnato (che riaprirebbe la supply-chain). Rilievo Fugu Ultra / GPT.
    assert "[ ! -f requirements-build-linux.lock ]" in wf, "il build non e' fail-closed sull'assenza del lock"
    assert 'pip install "pyinstaller>=6,<7"' not in wf, "non deve esserci un fallback legacy non pinnato"


@pytest.mark.parametrize("wf_name", ["build-linux.yml", "build-windows-exe.yml"])
def test_build_workflows_are_manual_or_tag_only(wf_name):
    # Decisione owner: ENTRAMBI i build pesanti (Linux + Windows) si costruiscono
    # SOLO a mano o su tag v*, mai a ogni push/PR (risparmio CI). L'anti-stale
    # del lock resta validato su PR dal workflow leggero generate-linux-lockfile.
    wf = _read(ROOT / ".github" / "workflows" / wf_name)
    assert "workflow_dispatch" in wf, f"{wf_name}: manca il trigger manuale"
    assert re.search(r"^\s*tags:", wf, re.M) and '"v*"' in wf, f"{wf_name}: manca il trigger su tag v*"
    # Robusto ai commenti (rilievo Fable 5): cerca `pull_request:` come CHIAVE a
    # inizio riga, non come sottostringa — un commento che cita "pull_request"
    # non deve far fallire il test.
    assert not re.search(r"^\s*pull_request:", wf, re.M), (
        f"{wf_name}: il build pesante non deve partire su PR (solo manuale/tag)"
    )


def test_lock_is_hash_pinned_and_path_independent():
    # Guardia attiva SOLO quando il lock e' stato committato (Round 2): prima
    # non esiste e il build resta legacy — questo test non deve fallire allora.
    if not LOCK.exists():
        pytest.skip("lock non ancora committato (build in modalita' legacy)")
    data = _read(LOCK)
    assert "--hash=sha256:" in data, "il lock non contiene hash (--generate-hashes mancato)"
    # riproducibilita': nessun path assoluto del runner deve finire nel lock
    assert "/home/runner" not in data, "path assoluto del runner nel lock (non riproducibile)"
    assert "/tmp/" not in data, "path temporaneo assoluto nel lock (non riproducibile)"
    # ogni requisito pinnato (riga a colonna 0 con ==) deve portare hash: in
    # --generate-hashes la riga del pin termina con ' \' e gli hash seguono.
    for line in data.splitlines():
        if re.match(r"^[A-Za-z0-9]", line) and "==" in line:
            assert line.rstrip().endswith("\\"), (
                f"pin senza continuazione hash: {line!r}"
            )
