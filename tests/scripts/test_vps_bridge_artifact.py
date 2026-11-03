"""WP5 (preparazione offline): verifica di integrità del pacchetto del ponte. Nessuna VPS, solo tmp_path."""
import hashlib
import io
import os
import pathlib
import sys
import tarfile

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "scripts" / "vps_bridge"))
import verify_artifact as V  # noqa: E402


def _tgz(path, files, sums=None, extra=()):
    """files: {nome: bytes}; sums: testo di SHA256SUMS (default: corretto); extra: TarInfo speciali."""
    if sums is None:
        sums = "".join(f"{hashlib.sha256(b).hexdigest()}  {n}\n" for n, b in sorted(files.items()))
    with tarfile.open(path, "w:gz") as t:
        for n, b in list(files.items()) + ([("SHA256SUMS", sums.encode())] if sums is not False else []):
            ti = tarfile.TarInfo(n); ti.size = len(b); t.addfile(ti, io.BytesIO(b))
        for ti in extra:
            t.addfile(ti)
    return path, hashlib.sha256(open(path, "rb").read()).hexdigest()


GOOD = {"bridge/contract.py": b"print('ok')\n", "bridge/README.md": b"# ponte\n"}


def test_a_well_formed_package_is_accepted(tmp_path):
    p, sha = _tgz(tmp_path / "a.tgz", GOOD)
    assert V.verify(str(p), sha) == sorted(GOOD)


def test_archive_sha_mismatch_is_rejected(tmp_path):
    p, sha = _tgz(tmp_path / "a.tgz", GOOD)
    with pytest.raises(V.Invalid, match="ARCHIVE_SHA_MISMATCH"):
        V.verify(str(p), "0" * 64)


@pytest.mark.parametrize("name", ["/etc/passwd", "../escape.py", "bridge/../../x", "a\\b"])
def test_unsafe_paths_are_rejected(tmp_path, name):
    p, sha = _tgz(tmp_path / "a.tgz", dict(GOOD, **{name: b"x"}))
    with pytest.raises(V.Invalid, match="UNSAFE_PATH"):
        V.verify(str(p), sha)


@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.CHRTYPE, tarfile.FIFOTYPE])
def test_links_devices_and_fifos_are_rejected(tmp_path, kind):
    ti = tarfile.TarInfo("bridge/evil"); ti.type = kind; ti.linkname = "/etc/shadow"
    p, sha = _tgz(tmp_path / "a.tgz", GOOD, extra=[ti])
    with pytest.raises(V.Invalid, match="NON_REGULAR_MEMBER"):
        V.verify(str(p), sha)


def test_missing_or_incomplete_sums_are_rejected(tmp_path):
    p, sha = _tgz(tmp_path / "a.tgz", GOOD, sums=False)
    with pytest.raises(V.Invalid, match="SUMS_MISSING"):
        V.verify(str(p), sha)
    partial = f"{hashlib.sha256(GOOD['bridge/README.md']).hexdigest()}  bridge/README.md\n"
    p, sha = _tgz(tmp_path / "b.tgz", GOOD, sums=partial)          # un file non elencato
    with pytest.raises(V.Invalid, match="SUMS_SET_MISMATCH"):
        V.verify(str(p), sha)


def test_a_tampered_file_is_rejected(tmp_path):
    sums = "".join(f"{hashlib.sha256(b).hexdigest()}  {n}\n" for n, b in sorted(GOOD.items()))
    tampered = dict(GOOD, **{"bridge/contract.py": b"import os; os.system('id')\n"})
    p, sha = _tgz(tmp_path / "a.tgz", tampered, sums=sums)
    with pytest.raises(V.Invalid, match="FILE_SHA_MISMATCH"):
        V.verify(str(p), sha)


def test_secret_like_content_is_rejected(tmp_path):
    files = dict(GOOD, **{"bridge/key.pem": b"-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n"})
    p, sha = _tgz(tmp_path / "a.tgz", files)
    with pytest.raises(V.Invalid, match="SECRET_LIKE_CONTENT"):
        V.verify(str(p), sha)


def test_cli_is_fail_closed(tmp_path, capsys):
    assert V.main([]) == 2
    assert V.main([str(tmp_path / "missing.tgz"), "0" * 64]) == 1
