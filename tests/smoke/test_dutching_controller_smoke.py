import pytest


@pytest.mark.smoke
def test_import_dutching_controller_module():
    import controllers.dutching_controller  # noqa: F401


@pytest.mark.smoke
def test_dutching_controller_class_exists():
    from controllers.dutching_controller import DutchingController

    controller = DutchingController(bus=None, runtime_controller=None)
    assert controller is not None