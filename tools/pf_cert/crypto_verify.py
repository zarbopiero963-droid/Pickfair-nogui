"""Verifica di firme in Python puro, senza dipendenze: Ed25519 (RFC 8032 §5.1.7) e RS256 (RSASSA-PKCS1-v1_5/SHA-256).

Solo VERIFICA di firme pubbliche (manifest dell'orchestratore, token OIDC di GitHub): nessuna chiave privata, nessuna
operazione segreta, quindi nessun requisito di tempo costante. Ogni input non conforme => False, mai un'eccezione che
il chiamante potrebbe leggere come «verificato».
"""
from __future__ import annotations

import base64
import hashlib
from typing import Optional, Tuple

# ----------------------------------------------------------------------------------------------------- Ed25519
_P = 2 ** 255 - 19
_D = -121665 * pow(121666, _P - 2, _P) % _P
_Q = 2 ** 252 + 27742317777372353535851937790883648493
_SQRT_M1 = pow(2, (_P - 1) // 4, _P)
_Point = Tuple[int, int, int, int]


def _add(a: _Point, b: _Point) -> _Point:
    x1, y1, z1, t1 = a
    x2, y2, z2, t2 = b
    aa = (y1 - x1) * (y2 - x2) % _P
    bb = (y1 + x1) * (y2 + x2) % _P
    cc = 2 * t1 * t2 * _D % _P
    dd = 2 * z1 * z2 % _P
    e, f, g, h = bb - aa, dd - cc, dd + cc, bb + aa
    return e * f % _P, g * h % _P, f * g % _P, e * h % _P


def _mul(s: int, point: _Point) -> _Point:
    acc: _Point = (0, 1, 1, 0)
    while s > 0:
        if s & 1:
            acc = _add(acc, point)
        point = _add(point, point)
        s >>= 1
    return acc


def _equal(a: _Point, b: _Point) -> bool:
    return (a[0] * b[2] - b[0] * a[2]) % _P == 0 and (a[1] * b[2] - b[1] * a[2]) % _P == 0


def _recover_x(y: int, sign: int) -> Optional[int]:
    if y >= _P:
        return None
    x2 = (y * y - 1) * pow(_D * y * y + 1, _P - 2, _P) % _P
    if x2 == 0:
        return None if sign else 0
    x = pow(x2, (_P + 3) // 8, _P)
    if (x * x - x2) % _P != 0:
        x = x * _SQRT_M1 % _P
    if (x * x - x2) % _P != 0:
        return None
    if (x & 1) != sign:
        x = _P - x
    return x


def _decompress(raw: bytes) -> Optional[_Point]:
    if len(raw) != 32:
        return None
    y = int.from_bytes(raw, "little")
    sign = y >> 255
    y &= (1 << 255) - 1
    x = _recover_x(y, sign)
    return None if x is None else (x, y, 1, x * y % _P)


_GY = 4 * pow(5, _P - 2, _P) % _P
_GX = _recover_x(_GY, 0)
_G: _Point = (_GX, _GY, 1, _GX * _GY % _P)

#: prefisso DER di una SubjectPublicKeyInfo Ed25519 (OID 1.3.101.112), seguito dai 32 byte della chiave
_ED25519_SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")


def ed25519_public_key_from_spki_b64(spki_b64: str) -> Optional[bytes]:
    """I 32 byte della chiave da una SPKI DER in base64 (formato della chiave pubblicata dall'orchestratore)."""
    try:
        der = base64.b64decode(spki_b64, validate=True)
    except (ValueError, TypeError):
        return None
    if len(der) != 44 or not der.startswith(_ED25519_SPKI_PREFIX):
        return None
    return der[12:]


def ed25519_verify(public_key: bytes, message: bytes, signature: bytes) -> bool:
    if not (isinstance(public_key, bytes) and isinstance(message, bytes) and isinstance(signature, bytes)):
        return False
    if len(public_key) != 32 or len(signature) != 64:
        return False
    a = _decompress(public_key)
    r = _decompress(signature[:32])
    if a is None or r is None:
        return False
    s = int.from_bytes(signature[32:], "little")
    if s >= _Q:
        return False
    h = int.from_bytes(hashlib.sha512(signature[:32] + public_key + message).digest(), "little") % _Q
    return _equal(_mul(s, _G), _add(r, _mul(h, a)))


# ------------------------------------------------------------------------------------------------------- RS256
#: DigestInfo DER di SHA-256 (RFC 8017 §9.2, nota 1)
_SHA256_DIGEST_INFO = bytes.fromhex("3031300d060960864801650304020105000420")
#: modulo minimo accettato: 2048 bit (chiavi OIDC di GitHub)
_MIN_RSA_BITS = 2048


def b64url_decode(value: str) -> bytes:
    if not isinstance(value, str) or not value or any(c not in _B64URL for c in value):
        raise ValueError("base64url non valido")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


_B64URL = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")


def rs256_verify(n: int, e: int, message: bytes, signature: bytes) -> bool:
    """RSASSA-PKCS1-v1_5 con SHA-256: confronto ESATTO dell'encoded message (nessun parsing permissivo del padding)."""
    if not (isinstance(n, int) and isinstance(e, int) and isinstance(message, bytes) and isinstance(signature, bytes)):
        return False
    if n.bit_length() < _MIN_RSA_BITS or e not in (3, 65537) or n % 2 == 0:
        return False
    k = (n.bit_length() + 7) // 8
    if len(signature) != k:
        return False
    s = int.from_bytes(signature, "big")
    if s >= n:
        return False
    em = pow(s, e, n).to_bytes(k, "big")
    t = _SHA256_DIGEST_INFO + hashlib.sha256(message).digest()
    if k < len(t) + 11:
        return False
    expected = b"\x00\x01" + b"\xff" * (k - len(t) - 3) + b"\x00" + t
    return em == expected
