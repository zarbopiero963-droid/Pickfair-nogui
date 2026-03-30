from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "betfair"


def load_betfair_fixture(name: str) -> Any:
    path = _FIXTURE_DIR / name
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)