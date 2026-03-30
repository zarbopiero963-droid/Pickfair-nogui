import pytest


@pytest.mark.smoke
def test_import_headless_main_module():
    import headless_main  # noqa: F401


@pytest.mark.smoke
def test_headless_app_class_exists():
    from headless_main import HeadlessApp

    app = HeadlessApp()
    assert app is not None
    assert app._running is False