"""
Stdlib-only symmetric encryption for at-rest secret fields.

Algorithm: XOR stream cipher using shake_256 as XOF (hash-based PRF).
           Nonce is randomly generated per encryption, ensuring ciphertext
           uniqueness.  Not fake obfuscation — security depends on key secrecy.

Key source (in priority order):
  1. PICKFAIR_SECRET_KEY env var (64-char hex = 32 bytes)
  2. ~/.pickfair/db.key  (auto-generated, chmod 0600 on creation)
  3. In-process ephemeral key (last resort, logs a WARNING)

Wire format stored in DB:
  "enc:v1:<base64url(nonce_16_bytes + ciphertext)>"

Legacy plaintext values (no prefix) are passed through transparently on
read so existing data keeps working.  Next write of the same field will
encrypt it.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import secrets
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_PREFIX = "enc:v1:"
_NONCE_LEN = 16
_KEY_LEN = 32


class SecretCipher:

    def __init__(self, key: bytes, *, key_source: str = "unknown") -> None:
        if len(key) != _KEY_LEN:
            raise ValueError(f"SecretCipher: key must be {_KEY_LEN} bytes")
        self._key = key
        self._key_source = str(key_source)

    @property
    def key_source(self) -> str:
        """Deterministic classification of where the current key came from.

        Values produced by :meth:`from_env_or_file`:
            - ``"env"``            — loaded from PICKFAIR_SECRET_KEY env var
            - ``"file_existing"``  — loaded from a pre-existing key file
            - ``"file_generated"`` — newly generated key, persisted to disk
            - ``"ephemeral"``      — newly generated key, persist to disk failed

        Instances constructed directly via ``SecretCipher(key)`` (without the
        factory) default to ``"unknown"``.
        """
        return self._key_source

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------
    @classmethod
    def from_env_or_file(cls) -> "SecretCipher":
        """Load or generate the master encryption key."""
        # 1. Env var (CI / headless deployments)
        env_val = os.environ.get("PICKFAIR_SECRET_KEY", "").strip()
        if env_val:
            try:
                key = bytes.fromhex(env_val[:64])
                if len(key) == _KEY_LEN:
                    logger.debug("secret_cipher: key loaded from PICKFAIR_SECRET_KEY")
                    return cls(key, key_source="env")
            except ValueError:
                logger.warning(
                    "secret_cipher: PICKFAIR_SECRET_KEY is set but not valid hex; ignoring"
                )

        # 2. Key file
        key_path = cls._key_file_path()
        if key_path.exists():
            try:
                raw = key_path.read_bytes().strip()
                key = bytes.fromhex(raw.decode("ascii"))
                if len(key) == _KEY_LEN:
                    logger.debug("secret_cipher: key loaded from %s", key_path)
                    return cls(key, key_source="file_existing")
            except Exception as exc:
                logger.warning("secret_cipher: key file unreadable (%s); regenerating", exc)

        # 3. Generate new key and persist
        key = secrets.token_bytes(_KEY_LEN)
        persisted = False
        try:
            key_path.parent.mkdir(parents=True, exist_ok=True)
            key_path.write_text(key.hex())
            try:
                key_path.chmod(0o600)
            except Exception:
                pass
            logger.info("secret_cipher: new key generated and saved to %s", key_path)
            persisted = True
        except Exception as exc:
            logger.warning(
                "secret_cipher: could not persist key to %s (%s); "
                "using ephemeral key — existing encrypted values will be unreadable after restart",
                key_path,
                exc,
            )

        return cls(key, key_source="file_generated" if persisted else "ephemeral")

    @staticmethod
    def _key_file_path() -> Path:
        return Path.home() / ".pickfair" / "db.key"

    # ------------------------------------------------------------------
    # Crypto primitives
    # ------------------------------------------------------------------
    def _keystream(self, nonce: bytes, length: int) -> bytes:
        """Derive deterministic keystream from key + nonce via shake_256 XOF."""
        h = hashlib.shake_256()
        h.update(self._key + nonce)
        return h.digest(length)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def encrypt(self, plaintext: str) -> str:
        """Return wire-format encrypted string; empty input returned as-is."""
        if not plaintext:
            return plaintext
        data = plaintext.encode("utf-8")
        nonce = secrets.token_bytes(_NONCE_LEN)
        ks = self._keystream(nonce, len(data))
        ct = bytes(a ^ b for a, b in zip(data, ks))
        return _PREFIX + base64.b64encode(nonce + ct).decode("ascii")

    def decrypt(self, token: str) -> str:
        """Return plaintext; legacy plaintext (no prefix) passed through."""
        if not token:
            return token
        if not token.startswith(_PREFIX):
            # Legacy plaintext: return unchanged (migration transparent on next write)
            return token
        try:
            raw = base64.b64decode(token[len(_PREFIX):].encode("ascii"))
            nonce, ct = raw[:_NONCE_LEN], raw[_NONCE_LEN:]
            ks = self._keystream(nonce, len(ct))
            return bytes(a ^ b for a, b in zip(ct, ks)).decode("utf-8")
        except Exception as exc:
            logger.warning("secret_cipher: decrypt failed (%s); returning empty string", exc)
            return ""

    def is_encrypted(self, value: str) -> bool:
        return bool(value and value.startswith(_PREFIX))

    def is_legacy_plaintext(self, value: str) -> bool:
        return bool(value and not value.startswith(_PREFIX))
