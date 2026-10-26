"""PF-GOV-DOC-WORKFLOW-ALIGN-01: CLAUDE.md e AGENTS.md dichiarano gli stessi file a merge owner del classificatore.

La lista in prosa e' quella che l'owner e gli agenti leggono; il classificatore
(`scripts/pr_autonomy_policy.OWNER_MANUAL_MERGE_FILES`) e' quello che i gate applicano. Una divergenza e' gia'
successa (#563: tre script aggiunti al classificatore, prosa ferma): ora la rende rossa questo test.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import pr_autonomy_policy as policy  # noqa: E402

DOCS = ("CLAUDE.md", "AGENTS.md")
SPEC = "docs/auto_pr_flow_spec.md"


#: Inizio e fine dell'elenco dei file a merge owner in ciascun documento. Il confronto e' limitato a quell'elenco:
#: un percorso citato altrove nel file (es. `scripts/pr_fix_loop_policy.py` nella sezione PR-flow) non lo sostituisce.
LIST_BOUNDS = {
    "CLAUDE.md": ("I documenti che definiscono cosa l'agente può mergiare:", "- (e, per fail-closed, ogni altro"),
    "AGENTS.md": ("The documents that define what the agent may merge:", "- (and, fail-closed, every other"),
}


def _owner_manual_list(doc: str) -> str:
    text = (ROOT / doc).read_text(encoding="utf-8")
    start_marker, end_marker = LIST_BOUNDS[doc]
    start = text.index(start_marker)
    return text[start:text.index(end_marker, start)]


@pytest.mark.parametrize("doc", DOCS)
@pytest.mark.parametrize("path", sorted(policy.OWNER_MANUAL_MERGE_FILES))
def test_every_owner_manual_file_is_declared_in_the_governance_docs(doc, path):
    assert f"`{path}`" in _owner_manual_list(doc), f"{path} e' a merge owner nel classificatore ma {doc} non lo elenca"


def _spec_merge_section() -> str:
    text = (ROOT / SPEC).read_text(encoding="utf-8")
    start = text.index("### 0.9 Merge")
    end = text.index("\n### ", start + 1)
    return text[start:end]


@pytest.mark.parametrize("path", sorted(policy.OWNER_MANUAL_MERGE_FILES))
def test_every_owner_manual_file_is_declared_in_the_spec_merge_section(path):
    """La spec §0.9 e' il testo normativo: nomina i workflow per nome file, gli altri per percorso completo."""
    section = _spec_merge_section()
    named = f"`{path}`" in section
    if path.startswith(".github/workflows/"):
        named = named or f"`{Path(path).name}`" in section
    assert named, f"{path} e' a merge owner nel classificatore ma {SPEC} §0.9 non lo elenca"


@pytest.mark.parametrize("doc", DOCS)
def test_no_stale_two_euro_microstake_stub(doc):
    """#561: lo stub e' il minimo exchange (`core.stake_precision.EXCHANGE_MIN_ORDER`, riferimento owner 1 EUR)."""
    text = (ROOT / doc).read_text(encoding="utf-8")
    assert not re.search(r"stub 2 EUR|2 EUR stub|stub da 2", text), f"{doc} descrive ancora lo stub da 2 EUR"
    assert "EXCHANGE_MIN_ORDER" in text
