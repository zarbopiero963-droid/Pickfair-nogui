#!/usr/bin/env bash
# Setup ambiente di sviluppo/test riproducibile.
#
# Uso:
#   bash scripts/setup_dev_env.sh
#
# Nota: il venv dedicato evita il setuptools di sistema (Debian) che
# rompe la build di pyaes (dipendenza Telethon) con
# "AttributeError: install_layout".
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python3}"
VENV_DIR="${VENV_DIR:-.venv}"

"$PYTHON" -m venv --clear "$VENV_DIR"
"$VENV_DIR/bin/pip" install --quiet --upgrade pip setuptools wheel
"$VENV_DIR/bin/pip" install --quiet -r requirements.txt -r requirements-test.txt

"$VENV_DIR/bin/python" - <<'PY'
import importlib.util as u

required = ("betfairlightweight", "telethon", "aiohttp", "pytest", "requests")
missing = [name for name in required if u.find_spec(name) is None]
if missing:
    raise SystemExit(f"FAIL: dipendenze mancanti: {missing}")
print("OK: dipendenze runtime/test importabili:", ", ".join(required))
PY

echo "OK: ambiente pronto. Esegui i test con: $VENV_DIR/bin/python -m pytest -q"
