"""F-33 — conftest dell'harness full-scope: fixture di seed e directory degli artifact.

Gli hook del tracciatore dei requisiti stanno in ``tests/fullscope/requirements/pytest_plugin.py``, registrato dal
``conftest.py`` di root: qui verrebbero ignorati dal controller pytest-xdist (rilievo Codex su a721e09).
"""
from __future__ import annotations

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tests.fullscope.requirements import tracker as _trk  # noqa: E402


@pytest.fixture(scope="session")
def pf_seed() -> int:
    """Seed riproducibile per generatore e scenari (``PF_SEED``, default 0)."""
    return _trk.seed()


@pytest.fixture(scope="session")
def pf_artifact_dir() -> str:
    return _trk.artifact_dir(_trk.head_sha())
