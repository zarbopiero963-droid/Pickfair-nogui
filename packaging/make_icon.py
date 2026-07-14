#!/usr/bin/env python3
"""Genera l'icona placeholder di Pickfair (``pickfair.png``, 256x256).

Placeholder pulito e riproducibile (nessuna dipendenza esterna: PNG scritto a
mano via ``zlib``), da sostituire quando c'e' un logo vero. Disegna una "P"
bianca su fondo teal arrotondato. Rigenerare con::

    python3 packaging/make_icon.py

Serve per la desktop-integration dell'AppImage Linux (icona nel menu app).
"""
from __future__ import annotations

import struct
import sys
import zlib

SIZE = 256
BG = (15, 118, 110)      # teal 700
FG = (240, 253, 250)     # teal 50 (quasi bianco)
RADIUS = 44              # raggio angoli arrotondati


def _rounded(x: int, y: int) -> bool:
    """True se il pixel (x, y) è dentro il quadrato ad angoli arrotondati."""
    r = RADIUS
    # angoli: fuori solo se oltre il raggio nell'angolo corrispondente
    cx = None
    cy = None
    if x < r and y < r:
        cx, cy = r, r
    elif x >= SIZE - r and y < r:
        cx, cy = SIZE - 1 - r, r
    elif x < r and y >= SIZE - r:
        cx, cy = r, SIZE - 1 - r
    elif x >= SIZE - r and y >= SIZE - r:
        cx, cy = SIZE - 1 - r, SIZE - 1 - r
    if cx is None:
        return True
    return (x - cx) ** 2 + (y - cy) ** 2 <= r * r


def _is_letter_p(x: int, y: int) -> bool:
    """True se (x, y) cade nella glifo "P" (composta da rettangoli)."""
    # stem sinistro
    if 80 <= x <= 110 and 64 <= y <= 192:
        return True
    # barra superiore
    if 80 <= x <= 172 and 64 <= y <= 94:
        return True
    # stem destro (metà alta)
    if 142 <= x <= 172 and 64 <= y <= 128:
        return True
    # barra centrale
    if 80 <= x <= 172 and 110 <= y <= 140:
        return True
    return False


def _png(pixels: bytes, width: int, height: int) -> bytes:
    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)  # 8-bit RGB
    raw = bytearray()
    stride = width * 3
    for y in range(height):
        raw.append(0)  # filter type 0
        raw.extend(pixels[y * stride:(y + 1) * stride])
    idat = zlib.compress(bytes(raw), 9)
    return sig + chunk(b"IHDR", ihdr) + chunk(b"IDAT", idat) + chunk(b"IEND", b"")


def build() -> bytes:
    buf = bytearray(SIZE * SIZE * 3)
    for y in range(SIZE):
        for x in range(SIZE):
            if not _rounded(x, y):
                color = (255, 255, 255)  # trasparenza non usata: fondo bianco fuori raggio
            elif _is_letter_p(x, y):
                color = FG
            else:
                color = BG
            i = (y * SIZE + x) * 3
            buf[i], buf[i + 1], buf[i + 2] = color
    return _png(bytes(buf), SIZE, SIZE)


def main() -> int:
    out = sys.argv[1] if len(sys.argv) > 1 else "packaging/pickfair.png"
    with open(out, "wb") as fh:
        fh.write(build())
    print(f"icona scritta: {out} ({SIZE}x{SIZE})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
