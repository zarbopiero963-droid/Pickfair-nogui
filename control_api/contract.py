"""Contratto Control API v1: versione, capability servite, schema hash condiviso con il MCP.

Il descrittore canonico è una copia fedele di quello di pickfair-mcp-
(``pickfair_mcp.contract.version.canonical_contract_descriptor``), salvata in
``contract_v1_descriptor.json``. L'hash si calcola con lo stesso algoritmo; un test
cross-repository verifica che i due valori coincidano. L'algoritmo condiviso è
PROVISIONAL fino alla ratifica di PF-API-0.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Final

CONTRACT_VERSION: Final = "1.0.0"

# Solo letture. Nessuna route di comando è servita da questa versione.
CAPABILITIES: Final[tuple[str, ...]] = (
    "GET /v1/meta",
    "GET /v1/status",
    "GET /v1/reconciliation/status",
)

GIT_SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
DESCRIPTOR_PATH: Final = Path(__file__).with_name("contract_v1_descriptor.json")


def load_descriptor() -> dict[str, Any]:
    data = json.loads(DESCRIPTOR_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("descriptor version mismatch")
    return data


def hash_descriptor(descriptor: dict[str, Any]) -> str:
    canonical = json.dumps(descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def schema_hash() -> str:
    return hash_descriptor(load_descriptor())


def resolve_git_sha(repo_root: Path) -> str:
    """SHA del checkout letto dai file di git, senza sottoprocessi. Errore se non dimostrabile."""
    git = repo_root / ".git"
    if git.is_file():
        line = git.read_text(encoding="utf-8").strip()
        if not line.startswith("gitdir: "):
            raise ValueError("unsupported .git file")
        git = (repo_root / line[len("gitdir: "):]).resolve()
    head = (git / "HEAD").read_text(encoding="utf-8").strip()
    if head.startswith("ref: "):
        ref = head[len("ref: "):]
        ref_file = git / ref
        sha = ref_file.read_text(encoding="utf-8").strip() if ref_file.is_file() else ""
        if not sha:
            packed = git / "packed-refs"
            common = git / "commondir"
            if not packed.is_file() and common.is_file():
                packed = (git / common.read_text(encoding="utf-8").strip() / "packed-refs").resolve()
            for row in packed.read_text(encoding="utf-8").splitlines() if packed.is_file() else ():
                parts = row.split(" ")
                if len(parts) == 2 and parts[1] == ref:
                    sha = parts[0]
    else:
        sha = head
    if not GIT_SHA_RE.fullmatch(sha):
        raise ValueError("git sha not resolvable")
    return sha
