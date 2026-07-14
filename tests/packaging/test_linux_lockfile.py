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


def test_generate_workflow_triggers_on_transitive_requirements():
    # Il .in include transitivamente requirements.txt/dev/test: un bump la' deve
    # ri-triggerare l'anti-stale, altrimenti il lock diventa silenziosamente
    # obsoleto (rilievo GPT-5.6 Terra / Fable 5).
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
    # il build deve ri-triggerare quando il lock/.in cambia
    assert "requirements-build-linux.in" in wf


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
