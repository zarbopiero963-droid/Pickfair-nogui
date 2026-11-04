"""Avvio OPZIONALE della Control API da ``headless_main`` — DISATTIVATO per impostazione predefinita.

Si attiva solo con ENTRAMBI i flag espliciti sulla riga di comando:

    --control-api-port <1-65535>  --control-api-token-file </percorso/assoluto/control_api.token>

Senza flag: nessun import del package, nessun socket, nessuna porta, nessun file. Nessuna
variabile d'ambiente e nessuna chiave di config.json attivano la Control API.

Con i flag, fail-closed sulla sola Control API (il motore continua senza API, causa nel log):
- package assente o diverso da quello verificato (pin di versione e schema hash);
- porta non valida, percorso non assoluto, cartella del token non privata (scrivibile da altri
  o di un altro utente), SHA del codice non dimostrabile, package assente, porta occupata.

Lo SHA viene letto da ``.git`` del checkout oppure da un file ``BUILD_SHA`` accanto a
``headless_main.py`` (da scrivere al packaging); altrimenti la Control API non parte.
"""

from __future__ import annotations

import logging
import os
import re
import stat
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PORT_FLAG = "--control-api-port"
TOKEN_FLAG = "--control-api-token-file"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class ControlApiLaunchError(Exception):
    """La Control API richiesta non può partire in sicurezza. `code` è fisso."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class LaunchConfig:
    port: int
    token_path: Path


def _flag_value(argv: Sequence[str], flag: str) -> str | None:
    found: list[str] = []
    for i, arg in enumerate(argv):
        if arg == flag:
            if i + 1 >= len(argv):
                raise ControlApiLaunchError("FLAG_WITHOUT_VALUE")
            found.append(argv[i + 1])
        elif arg.startswith(flag + "="):
            found.append(arg[len(flag) + 1 :])
    if len(found) > 1:
        raise ControlApiLaunchError("FLAG_REPEATED")
    return found[0] if found else None


def requested(argv: Sequence[str]) -> bool:
    """True solo se uno dei flag è presente. Nessun altro canale attiva la Control API."""
    return any(a == f or a.startswith(f + "=") for a in argv for f in (PORT_FLAG, TOKEN_FLAG))


def parse(argv: Sequence[str]) -> LaunchConfig:
    port_raw, token_raw = _flag_value(argv, PORT_FLAG), _flag_value(argv, TOKEN_FLAG)
    if port_raw is None or token_raw is None:
        raise ControlApiLaunchError("BOTH_FLAGS_REQUIRED")
    if not port_raw.isdigit() or not 1 <= int(port_raw) <= 65535:
        raise ControlApiLaunchError("PORT_INVALID")
    token_path = Path(token_raw)
    if not token_path.is_absolute() or ".." in token_path.parts:
        raise ControlApiLaunchError("TOKEN_PATH_INVALID")
    return LaunchConfig(port=int(port_raw), token_path=token_path)


def check_private_dir(directory: Path) -> None:
    try:
        info = os.lstat(directory)
    except OSError:
        raise ControlApiLaunchError("TOKEN_DIR_UNSAFE") from None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        or info.st_uid != os.getuid()
    ):
        raise ControlApiLaunchError("TOKEN_DIR_UNSAFE")


def resolve_code_sha(app_root: Path) -> str:
    build_sha = app_root / "BUILD_SHA"
    if build_sha.is_file():
        sha = build_sha.read_text(encoding="ascii").strip()
        if SHA_RE.fullmatch(sha):
            return sha
        raise ControlApiLaunchError("GIT_SHA_UNRESOLVABLE")
    try:
        from pickfair_control_api.contract import resolve_git_sha

        return resolve_git_sha(app_root)
    except (OSError, ValueError, ImportError):
        raise ControlApiLaunchError("GIT_SHA_UNRESOLVABLE") from None


def maybe_start(runtime: object, argv: Sequence[str], *, app_root: Path) -> Any | None:
    """Nessun flag => None senza effetti. Con flag: server avviato o None con causa nel log."""
    if not requested(argv):
        return None
    try:
        config = parse(argv)
        check_private_dir(config.token_path.parent)
        sha = resolve_code_sha(app_root)
        try:
            from control_api.bridge import ControlApiPinMismatch, start_control_api
        except ImportError:
            raise ControlApiLaunchError("PACKAGE_MISSING") from None
        try:
            server = start_control_api(runtime, port=config.port, token_path=config.token_path,
                                       git_sha=sha)
        except ImportError:
            raise ControlApiLaunchError("PACKAGE_MISSING") from None
        except ControlApiPinMismatch:
            raise ControlApiLaunchError("CONTRACT_PIN_MISMATCH") from None
        except OSError:
            raise ControlApiLaunchError("BIND_FAILED") from None
    except ControlApiLaunchError as exc:
        logger.error("CONTROL_API_NOT_STARTED: %s", exc.code)
        return None
    except Exception as exc:  # noqa: BLE001 - la Control API non deve mai fermare il motore
        logger.error("CONTROL_API_NOT_STARTED: %s", type(exc).__name__)
        return None
    logger.info("CONTROL_API_STARTED port=%s (sola lettura, 127.0.0.1)", server.port)
    return server


def stop(server: Any | None) -> None:
    if server is None:
        return
    try:
        server.stop()
    except Exception:  # noqa: BLE001
        logger.exception("CONTROL_API_STOP_FAILED")
