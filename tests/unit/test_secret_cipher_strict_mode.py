"""Dedicated tests for SecretCipher.key_source classification (Phase 1).

This phase adds *non-breaking* visibility into where the master encryption
key came from.  These tests pin the four deterministic classifications:

    - ``env``            PICKFAIR_SECRET_KEY env var
    - ``file_existing``  pre-existing on-disk key file
    - ``file_generated`` freshly generated key, persisted to disk
    - ``ephemeral``      freshly generated key, persist to disk failed

They also assert that encryption / decryption semantics are unchanged across
every path, and that direct construction keeps working with a neutral default.
"""

from __future__ import annotations

import pathlib
import secrets as _stdsecrets

import pytest

from core.secret_cipher import SecretCipher


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_VALID_HEX = _stdsecrets.token_bytes(32).hex()


def _isolate_env_and_key_path(monkeypatch, key_path: pathlib.Path) -> None:
    """Scrub env var and redirect the key-file location to a tmp path."""
    monkeypatch.delenv("PICKFAIR_SECRET_KEY", raising=False)
    monkeypatch.setattr(
        SecretCipher,
        "_key_file_path",
        staticmethod(lambda: key_path),
    )


# ---------------------------------------------------------------------------
# A) env path
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.guardrail
def test_key_source_env_when_valid_env_var_set(monkeypatch, tmp_path):
    # Redirect any potential file lookup away from real HOME so the env path
    # is unambiguously the one that wins.
    _isolate_env_and_key_path(monkeypatch, tmp_path / ".pickfair" / "db.key")
    monkeypatch.setenv("PICKFAIR_SECRET_KEY", _VALID_HEX)

    cipher = SecretCipher.from_env_or_file()

    assert cipher.key_source == "env"
    # Encryption / decryption must keep working.
    token = cipher.encrypt("round-trip-env")
    assert cipher.decrypt(token) == "round-trip-env"


@pytest.mark.unit
@pytest.mark.guardrail
def test_key_source_env_invalid_hex_falls_through(monkeypatch, tmp_path):
    """Invalid env value must not be classified as ``env``."""
    _isolate_env_and_key_path(monkeypatch, tmp_path / ".pickfair" / "db.key")
    monkeypatch.setenv("PICKFAIR_SECRET_KEY", "not-a-hex-string")

    cipher = SecretCipher.from_env_or_file()

    assert cipher.key_source != "env"
    assert cipher.key_source in {"file_generated", "ephemeral"}


# ---------------------------------------------------------------------------
# B) file_existing path
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.guardrail
def test_key_source_file_existing_when_valid_file_on_disk(monkeypatch, tmp_path):
    key_path = tmp_path / ".pickfair" / "db.key"
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_text(_VALID_HEX)

    _isolate_env_and_key_path(monkeypatch, key_path)

    cipher = SecretCipher.from_env_or_file()

    assert cipher.key_source == "file_existing"
    # File must be left untouched (still present, same contents).
    assert key_path.exists()
    assert key_path.read_text().strip() == _VALID_HEX
    # Round-trip continues to work.
    token = cipher.encrypt("round-trip-file-existing")
    assert cipher.decrypt(token) == "round-trip-file-existing"


# ---------------------------------------------------------------------------
# C) file_generated path
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.guardrail
def test_key_source_file_generated_when_no_env_no_file(monkeypatch, tmp_path):
    key_path = tmp_path / ".pickfair" / "db.key"
    assert not key_path.exists()

    _isolate_env_and_key_path(monkeypatch, key_path)

    cipher = SecretCipher.from_env_or_file()

    assert cipher.key_source == "file_generated"
    assert key_path.exists(), "factory must have persisted the generated key"
    # The persisted content must be a valid 64-hex key.
    persisted = key_path.read_text().strip()
    assert len(persisted) == 64
    assert bytes.fromhex(persisted)  # does not raise
    # Round-trip continues to work.
    token = cipher.encrypt("round-trip-file-generated")
    assert cipher.decrypt(token) == "round-trip-file-generated"


# ---------------------------------------------------------------------------
# D) ephemeral path
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.guardrail
def test_key_source_ephemeral_when_persist_fails(monkeypatch, tmp_path):
    # Block the parent directory creation by planting a regular file where the
    # ".pickfair" directory is expected.  ``mkdir(parents=True, exist_ok=True)``
    # raises FileExistsError because the existing path is not a directory.
    blocker = tmp_path / ".pickfair"
    blocker.write_text("not a directory")
    key_path = blocker / "db.key"

    _isolate_env_and_key_path(monkeypatch, key_path)

    cipher = SecretCipher.from_env_or_file()

    assert cipher.key_source == "ephemeral"
    # No key file was created — the blocker is still a plain file.
    assert blocker.is_file()
    assert not key_path.exists()
    # Round-trip continues to work against the in-memory key.
    token = cipher.encrypt("round-trip-ephemeral")
    assert cipher.decrypt(token) == "round-trip-ephemeral"


# ---------------------------------------------------------------------------
# E) direct construction remains unchanged
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.guardrail
def test_direct_construction_defaults_to_unknown_key_source():
    raw = _stdsecrets.token_bytes(32)

    cipher = SecretCipher(raw)

    assert cipher.key_source == "unknown"
    # Existing encrypt/decrypt contract is preserved.
    token = cipher.encrypt("direct-ctor")
    assert token != "direct-ctor"
    assert cipher.decrypt(token) == "direct-ctor"


@pytest.mark.unit
@pytest.mark.guardrail
def test_direct_construction_accepts_explicit_key_source():
    raw = _stdsecrets.token_bytes(32)

    cipher = SecretCipher(raw, key_source="env")

    assert cipher.key_source == "env"


@pytest.mark.unit
@pytest.mark.guardrail
def test_key_source_is_read_only():
    cipher = SecretCipher(_stdsecrets.token_bytes(32))

    with pytest.raises(AttributeError):
        cipher.key_source = "tampered"  # type: ignore[misc]
