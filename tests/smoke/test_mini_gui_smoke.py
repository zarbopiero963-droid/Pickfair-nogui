import pytest


@pytest.mark.smoke
def test_import_mini_gui_module():
    import mini_gui  # noqa: F401