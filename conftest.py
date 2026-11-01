"""Shared pytest configuration.

Intentionally kept import-free to avoid collection-time side effects: the only plugin registered here is the F-33
requirement tracker, whose import creates no files and whose hooks act only on tests marked ``pf_requirement``.
It must live at the root so the pytest-xdist controller loads it too (``pytest -n auto tests`` does not load nested
conftests in the controller).
"""

pytest_plugins = ["tests.fullscope.requirements.pytest_plugin"]
