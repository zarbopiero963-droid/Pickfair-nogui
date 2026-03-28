import pytest


@pytest.mark.smoke
def test_import_dutching():
    import dutching  # noqa: F401


@pytest.mark.smoke
def test_simple_dutching_call():
    from dutching import calculate_dutching_stakes

    result = calculate_dutching_stakes([2.0, 3.0], 10)
    assert "stakes" in result
    assert "profits" in result