#!/usr/bin/env python3
"""VPS Autonomy Bridge — verifica di integrità di un pacchetto di rilascio PRIMA dell'installazione (WP5).

Uso (sola lettura, nessuna estrazione nel sistema):
    python3 -I verify_artifact.py ARCHIVIO.tgz SHA256_ATTESO

Controlli, tutti fail-closed (exit 1 al primo errore):
- sha256 dell'archivio uguale al valore atteso (pubblicato su GitHub, canale separato dall'archivio);
- nessuna voce con percorso assoluto, ``..``, symlink, hardlink, device o FIFO;
- ``SHA256SUMS`` interno presente, che elenca TUTTI e SOLO i file regolari dell'archivio, e ogni hash coincide;
- nessun file che somigli a materiale segreto (chiavi private, token).

La firma di provenienza (Sigstore sull'artefatto del workflow di main) la verifica lo script di installazione con
``gh attestation verify`` prima di chiamare questo controllo; qui si verifica il contenuto.
"""
from __future__ import annotations

import hashlib
import re
import sys
import tarfile
from typing import Dict, List

SECRET_PATTERNS = [re.compile(p) for p in (
    rb"-----BEGIN [A-Z ]*PRIVATE KEY-----", rb"gh[pousr]_[A-Za-z0-9]{20,}", rb"github_pat_[A-Za-z0-9_]{20,}",
    rb"xox[abp]-[A-Za-z0-9-]{10,}", rb"AKIA[0-9A-Z]{16}")]
SUMS_NAME = "SHA256SUMS"


class Invalid(Exception):
    pass


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_name(name: str) -> str:
    n = name[2:] if name.startswith("./") else name
    if not n or n.startswith("/") or any(p == ".." for p in n.split("/")) or "\\" in n:
        raise Invalid(f"UNSAFE_PATH:{name}")
    return n


def verify(archive: str, expected_sha256: str) -> List[str]:
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256 or ""):
        raise Invalid("EXPECTED_SHA_MALFORMED")
    if sha256_file(archive) != expected_sha256:
        raise Invalid("ARCHIVE_SHA_MISMATCH")
    contents: Dict[str, bytes] = {}
    with tarfile.open(archive, "r:gz") as tar:
        for m in tar.getmembers():
            name = _safe_name(m.name)
            if m.isdir():
                continue
            if not m.isfile():
                raise Invalid(f"NON_REGULAR_MEMBER:{name}")
            if name in contents:
                raise Invalid(f"DUPLICATE_MEMBER:{name}")
            f = tar.extractfile(m)
            contents[name] = f.read() if f else b""
    if SUMS_NAME not in contents:
        raise Invalid("SUMS_MISSING")
    listed: Dict[str, str] = {}
    for line in contents[SUMS_NAME].decode("utf-8", "strict").splitlines():
        m = re.fullmatch(r"([0-9a-f]{64}) [ *](.+)", line.strip())
        if not m:
            raise Invalid("SUMS_MALFORMED")
        listed[_safe_name(m.group(2))] = m.group(1)
    files = {n for n in contents if n != SUMS_NAME}
    if set(listed) != files:
        raise Invalid("SUMS_SET_MISMATCH")
    for n in sorted(files):
        if hashlib.sha256(contents[n]).hexdigest() != listed[n]:
            raise Invalid(f"FILE_SHA_MISMATCH:{n}")
        if any(p.search(contents[n]) for p in SECRET_PATTERNS):
            raise Invalid(f"SECRET_LIKE_CONTENT:{n}")
    return sorted(files)


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print("uso: verify_artifact.py ARCHIVIO.tgz SHA256_ATTESO", file=sys.stderr)
        return 2
    try:
        files = verify(argv[0], argv[1])
    except (Invalid, OSError, tarfile.TarError, UnicodeDecodeError) as exc:
        print(f"[verify] RIFIUTATO: {exc}", file=sys.stderr)
        return 1
    print(f"[verify] OK: {len(files)} file verificati")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
