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


@pytest.mark.parametrize("doc", DOCS)
@pytest.mark.parametrize("path", sorted(policy.OWNER_MANUAL_MERGE_FILES))
def test_every_owner_manual_file_is_declared_in_the_governance_docs(doc, path):
    text = (ROOT / doc).read_text(encoding="utf-8")
    assert f"`{path}`" in text, f"{path} e' a merge owner nel classificatore ma {doc} non lo elenca"


@pytest.mark.parametrize("doc", DOCS)
def test_no_stale_two_euro_microstake_stub(doc):
    """#561: lo stub e' il minimo exchange (`core.stake_precision.EXCHANGE_MIN_ORDER`, riferimento owner 1 EUR)."""
    text = (ROOT / doc).read_text(encoding="utf-8")
    assert not re.search(r"stub 2 EUR|2 EUR stub|stub da 2", text), f"{doc} descrive ancora lo stub da 2 EUR"
    assert "EXCHANGE_MIN_ORDER" in text
