import pytest


@pytest.mark.smoke
def test_import_auto_throttle_module():
    import auto_throttle  # noqa: F401


@pytest.mark.smoke
def test_construct_auto_throttle():
    from auto_throttle import AutoThrottle

    throttle = AutoThrottle()
    assert throttle is not None
    assert throttle.wait() is None