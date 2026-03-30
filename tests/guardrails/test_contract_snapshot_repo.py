import inspect
import json
from pathlib import Path

import pytest


SNAPSHOT_PATH = Path("guardrails/public_api_snapshot.json")


def _load_snapshot():
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


def _import_module(path: str):
    import importlib

    return importlib.import_module(path)


def _method_param_names(callable_obj):
    sig = inspect.signature(callable_obj)
    params = []
    for name, p in sig.parameters.items():
        if name == "self":
            continue
        params.append(name)
    return params


@pytest.mark.guardrail
def test_public_api_snapshot_file_exists():
    assert SNAPSHOT_PATH.exists(), "Manca guardrails/public_api_snapshot.json"


@pytest.mark.guardrail
def test_public_api_snapshot_matches_repo():
    snapshot = _load_snapshot()

    for module_name, module_spec in snapshot.items():
        module = _import_module(module_name)

        for class_name, class_spec in module_spec.get("classes", {}).items():
            assert hasattr(module, class_name), f"{module_name}.{class_name} mancante"
            cls = getattr(module, class_name)

            for method_name, expected_params in class_spec.get("methods", {}).items():
                assert hasattr(cls, method_name), f"{module_name}.{class_name}.{method_name} mancante"
                actual_params = _method_param_names(getattr(cls, method_name))
                assert actual_params == expected_params, (
                    f"Signature cambiata per {module_name}.{class_name}.{method_name}: "
                    f"expected={expected_params} actual={actual_params}"
                )

        for func_name, expected_params in module_spec.get("functions", {}).items():
            assert hasattr(module, func_name), f"{module_name}.{func_name} mancante"
            actual_params = _method_param_names(getattr(module, func_name))
            assert actual_params == expected_params, (
                f"Signature cambiata per {module_name}.{func_name}: "
                f"expected={expected_params} actual={actual_params}"
            )