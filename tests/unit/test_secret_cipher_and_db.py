"""
Tests for secret-at-rest encryption.
Verifies:
  1. SecretCipher encrypt/decrypt round-trip
  2. Encrypted values stored in DB are NOT equal to original secrets
  3. Values read back from DB match originals (transparent decryption)
  4. Legacy plaintext values are handled safely (migration passthrough)
  5. Empty strings pass through without change
"""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from core.secret_cipher import SecretCipher
from database import Database


# ===========================================================================
# SecretCipher unit tests
# ===========================================================================

@pytest.mark.unit
@pytest.mark.guardrail
def test_cipher_round_trip():
    from core.secret_cipher import SecretCipher
    import secrets as _secrets
    key = _secrets.token_bytes(32)
    cipher = SecretCipher(key)

    for plaintext in ["hunter2", "s3cr3t!", "api_hash_abc123", "A" * 200]:
        token = cipher.encrypt(plaintext)
        assert token != plaintext, "encrypted value must differ from plaintext"
        assert cipher.decrypt(token) == plaintext, "round-trip must recover plaintext"


@pytest.mark.unit
@pytest.mark.guardrail
def test_cipher_empty_passthrough():
    import secrets as _secrets
    key = _secrets.token_bytes(32)
    cipher = SecretCipher(key)

    assert cipher.encrypt("") == ""
    assert cipher.decrypt("") == ""


@pytest.mark.unit
@pytest.mark.guardrail
def test_cipher_legacy_plaintext_passthrough():
    import secrets as _secrets
    key = _secrets.token_bytes(32)
    cipher = SecretCipher(key)

    legacy = "my_old_plaintext_password"
    assert cipher.decrypt(legacy) == legacy, "legacy plaintext must pass through unchanged"
    assert cipher.is_legacy_plaintext(legacy) is True
    assert cipher.is_encrypted(legacy) is False


@pytest.mark.unit
@pytest.mark.guardrail
def test_cipher_prefix_detection():
    import secrets as _secrets
    key = _secrets.token_bytes(32)
    cipher = SecretCipher(key)

    enc = cipher.encrypt("secret")
    assert cipher.is_encrypted(enc) is True
    assert cipher.is_legacy_plaintext(enc) is False


@pytest.mark.unit
@pytest.mark.guardrail
def test_cipher_different_nonces_produce_different_ciphertexts():
    import secrets as _secrets
    key = _secrets.token_bytes(32)
    cipher = SecretCipher(key)

    plaintext = "same_secret"
    enc1 = cipher.encrypt(plaintext)
    enc2 = cipher.encrypt(plaintext)
    assert enc1 != enc2, "two encryptions of the same value must differ (random nonce)"
    assert cipher.decrypt(enc1) == plaintext
    assert cipher.decrypt(enc2) == plaintext


# ===========================================================================
# Database integration: on-disk value must differ from original secret
# ===========================================================================

def _raw_setting(db_path: str, key: str) -> str:
    """Read raw on-disk value bypassing Database decryption."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    return str(row["value"]) if row else ""


@pytest.mark.unit
@pytest.mark.guardrail
def test_password_not_stored_in_plaintext():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)

        secret = "MyBetfairP@ssw0rd!"
        db.save_password(secret)

        raw = _raw_setting(db_path, "password")
        assert raw != secret, "password must not be stored as plaintext"
        assert raw.startswith("enc:v1:"), "password must use enc:v1: prefix"

        # Read back must match original
        recovered = db.get_settings().get("password", "")
        assert recovered == secret, "decrypted password must match original"


@pytest.mark.unit
@pytest.mark.guardrail
def test_session_token_not_stored_in_plaintext():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)

        token = "session_ABCDEF1234567890"
        db.save_session(token)

        raw = _raw_setting(db_path, "session_token")
        assert raw != token, "session_token must not be stored as plaintext"
        assert raw.startswith("enc:v1:")

        recovered = db.get_settings().get("session_token", "")
        assert recovered == token


@pytest.mark.unit
@pytest.mark.guardrail
def test_betfair_credentials_not_stored_in_plaintext():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)

        db.save_credentials(
            username="my_user",
            app_key="my_app_key",
            certificate="-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----",
            private_key="-----BEGIN RSA PRIVATE KEY-----\nFAKE\n-----END RSA PRIVATE KEY-----",
        )

        raw_cert = _raw_setting(db_path, "certificate")
        raw_pk = _raw_setting(db_path, "private_key")

        assert not raw_cert.startswith("-----BEGIN"), "certificate must not be stored as plaintext"
        assert not raw_pk.startswith("-----BEGIN"), "private_key must not be stored as plaintext"
        assert raw_cert.startswith("enc:v1:")
        assert raw_pk.startswith("enc:v1:")

        # username and app_key are NOT secret fields — they stay plaintext
        raw_user = _raw_setting(db_path, "username")
        assert raw_user == "my_user", "non-secret fields must remain plaintext"

        settings = db.get_settings()
        assert settings["certificate"].startswith("-----BEGIN CERTIFICATE")
        assert settings["private_key"].startswith("-----BEGIN RSA")


@pytest.mark.unit
@pytest.mark.guardrail
def test_telegram_secrets_not_stored_in_plaintext():
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)

        db.save_telegram_settings({
            "api_id": "12345678",
            "api_hash": "abcdef1234567890abcdef1234567890",
            "session_string": "1BAAAAAAAAAA_FAKE_SESSION_STRING",
            "phone_number": "+393331234567",
            "enabled": True,
        })

        for field in ("telegram.api_id", "telegram.api_hash", "telegram.session_string"):
            raw = _raw_setting(db_path, field)
            assert raw.startswith("enc:v1:"), f"{field} must be encrypted on disk"

        # phone_number is NOT a secret field — must remain plaintext
        raw_phone = _raw_setting(db_path, "telegram.phone_number")
        assert raw_phone == "+393331234567", "phone_number is not a secret field"

        tg = db.get_telegram_settings()
        assert tg["api_id"] == "12345678"
        assert tg["api_hash"] == "abcdef1234567890abcdef1234567890"
        assert tg["session_string"] == "1BAAAAAAAAAA_FAKE_SESSION_STRING"


@pytest.mark.unit
@pytest.mark.guardrail
def test_legacy_plaintext_migration_on_read():
    """Legacy plaintext rows written before encryption must still be readable."""
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "test.db")
        db = Database(db_path)

        # Write plaintext directly into DB (simulates pre-encryption legacy row)
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO settings(key, value) VALUES('password', 'legacy_plain') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        conn.commit()
        conn.close()

        # Must be readable via DB (legacy passthrough)
        settings = db.get_settings()
        assert settings.get("password") == "legacy_plain", \
            "legacy plaintext must be returned transparently"
