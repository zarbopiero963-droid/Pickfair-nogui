"""Matrice fail-closed della config hard-stop richiesta per LIVE (PR-A).

`RuntimeController._validate_live_hard_stop_config` deve dichiarare la
config INVALIDA (e quindi bloccare il deploy gate LIVE) per qualsiasi
valore mancante, non numerico, non finito, non positivo o fuori range.
Il metodo dipende solo da `self.config`: lo testiamo su un'istanza
costruita senza il resto del runtime (nessuna rete, nessun thread).
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.runtime_controller import RuntimeController

_VALID = dict(
    max_daily_loss=50.0,
    max_drawdown_hard_stop_pct=20.0,
    max_open_exposure=200.0,
)


def _controller_with_config(**overrides):
    controller = RuntimeController.__new__(RuntimeController)
    cfg = dict(_VALID)
    cfg.update(overrides)
    controller.config = SimpleNamespace(**cfg)
    return controller


@pytest.mark.safety
@pytest.mark.invariant
def test_valid_hard_stop_config_passes():
    report = _controller_with_config()._validate_live_hard_stop_config()
    assert report["valid"] is True
    assert report["missing_fields"] == []
    assert report["invalid_fields"] == []


@pytest.mark.safety
@pytest.mark.invariant
@pytest.mark.parametrize("field", sorted(_VALID))
def test_missing_field_invalidates_config(field):
    controller = RuntimeController.__new__(RuntimeController)
    cfg = {k: v for k, v in _VALID.items() if k != field}
    controller.config = SimpleNamespace(**cfg)

    report = controller._validate_live_hard_stop_config()
    assert report["valid"] is False
    assert field in report["missing_fields"]


@pytest.mark.safety
@pytest.mark.invariant
@pytest.mark.parametrize("field", sorted(_VALID))
def test_none_field_invalidates_config(field):
    report = _controller_with_config(**{field: None})._validate_live_hard_stop_config()
    assert report["valid"] is False
    assert field in report["missing_fields"]


@pytest.mark.safety
@pytest.mark.invariant
@pytest.mark.parametrize("field", sorted(_VALID))
@pytest.mark.parametrize("dirty", [
    "abc", "", object(), [10.0],
    float("nan"), float("inf"), float("-inf"),
    0.0, -1.0, -0.0,
])
def test_dirty_values_invalidate_config(field, dirty):
    report = _controller_with_config(**{field: dirty})._validate_live_hard_stop_config()
    assert report["valid"] is False
    # Classificazione precisa: i valori sporchi non-None finiscono SEMPRE
    # in invalid_fields (parse error o bound), mai in missing_fields.
    assert field in report["invalid_fields"], (
        f"{field}={dirty!r} atteso in invalid_fields; "
        f"missing={report['missing_fields']}, invalid={report['invalid_fields']}"
    )


@pytest.mark.safety
@pytest.mark.invariant
def test_drawdown_pct_over_100_is_invalid():
    report = _controller_with_config(
        max_drawdown_hard_stop_pct=150.0
    )._validate_live_hard_stop_config()
    assert report["valid"] is False
    assert "max_drawdown_hard_stop_pct" in report["invalid_fields"]


@pytest.mark.safety
@pytest.mark.invariant
@pytest.mark.parametrize("pct", [100.0, 99.999])
def test_drawdown_pct_at_or_below_100_is_valid(pct):
    report = _controller_with_config(
        max_drawdown_hard_stop_pct=pct
    )._validate_live_hard_stop_config()
    assert report["valid"] is True
    assert "max_drawdown_hard_stop_pct" not in report["invalid_fields"]


@pytest.mark.safety
@pytest.mark.invariant
def test_config_object_missing_entirely_is_invalid():
    controller = RuntimeController.__new__(RuntimeController)
    controller.config = None
    report = controller._validate_live_hard_stop_config()
    assert report["valid"] is False
    assert set(report["missing_fields"]) == set(report["required_fields"])


@pytest.mark.safety
@pytest.mark.invariant
def test_numeric_strings_are_accepted_but_bounds_still_enforced():
    # float("25") e' legittimo: il parsing e' permissivo, i bound no.
    ok = _controller_with_config(max_daily_loss="25")._validate_live_hard_stop_config()
    assert ok["valid"] is True
    bad = _controller_with_config(max_daily_loss="-25")._validate_live_hard_stop_config()
    assert bad["valid"] is False
