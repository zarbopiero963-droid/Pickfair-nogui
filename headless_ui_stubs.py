"""
Headless UI stub classes used by mini_gui.py when running without a real
display (headless server, CI, unit tests).

Extracted from mini_gui.py to reduce that module's size and to allow
importing the stubs independently of tkinter.
"""
from __future__ import annotations

from typing import Any, Callable


class _HeadlessBoolVar:
    def __init__(self, value: bool = False):
        self._value = bool(value)

    def get(self):
        return self._value

    def set(self, value):
        self._value = bool(value)


class _HeadlessStringVar:
    def __init__(self, value: str = ""):
        self._value = str(value)

    def get(self):
        return self._value

    def set(self, value):
        self._value = str(value)


class _HeadlessRoot:
    def after(self, _delay, fn=None):
        if callable(fn):
            try:
                fn()
            except Exception:
                pass
        return None

    def destroy(self):
        return None

    def protocol(self, *_args, **_kwargs):
        return None

    def withdraw(self):
        return None

    def title(self, *_args, **_kwargs):
        return None

    def geometry(self, *_args, **_kwargs):
        return None

    def grid_columnconfigure(self, *_args, **_kwargs):
        return None

    def grid_rowconfigure(self, *_args, **_kwargs):
        return None


class _DummyButton:
    def __init__(self, fn: Callable):
        self._fn = fn

    def cget(self, name: str) -> Any:
        if name == "command":
            return self._fn
        return None


class _DummyTree:
    def __init__(self):
        self.rows: list = []

    def delete(self, *_args, **_kwargs):
        self.rows = []

    def get_children(self):
        return list(range(len(self.rows)))

    def insert(self, *_args, **kwargs):
        self.rows.append(kwargs.get("values"))


class _DummyLog:
    def __init__(self):
        self.lines: list = []

    def insert(self, *args, **kwargs):
        if len(args) >= 2:
            self.lines.append(args[1])
        elif "text" in kwargs:
            self.lines.append(kwargs["text"])

    def see(self, *_args, **_kwargs):
        return None
