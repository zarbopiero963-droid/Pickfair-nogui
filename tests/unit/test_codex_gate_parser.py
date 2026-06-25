"""Tests for the codex-bug-gate comment parser (outdated detection fix).

The gate treats outdated/resolved comments as NOISE; the parser must surface the
correct `outdated` flag so already-fixed findings stop failing the gate.
"""

import importlib.util
import os

import pytest

_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "scripts",
    "parse_codex_review_comments.py",
)
_spec = importlib.util.spec_from_file_location("parse_codex_review_comments", _SCRIPT)
parser = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(parser)


@pytest.mark.unit
def test_is_outdated_explicit_bool_wins():
    assert parser._is_outdated({"outdated": True}) is True
    assert parser._is_outdated({"outdated": False}) is False
    assert parser._is_outdated({"isOutdated": True}) is True
    assert parser._is_outdated({"is_outdated": True}) is True


@pytest.mark.unit
def test_is_outdated_rest_position_fallback():
    # GitHub nulls `position` for outdated comments; an integer means live.
    assert parser._is_outdated({"position": None, "original_position": 5}) is True
    assert parser._is_outdated({"position": 3}) is False


@pytest.mark.unit
def test_is_outdated_defaults_false_without_signal():
    assert parser._is_outdated({}) is False


@pytest.mark.unit
def test_flatten_thread_marks_outdated_and_resolved():
    outdated_thread = {
        "isOutdated": True,
        "isResolved": False,
        "comments": {"nodes": [
            {"databaseId": 1, "author": {"login": "chatgpt-codex-connector"},
             "path": "betfair_client.py", "line": None, "originalLine": 753,
             "body": "stale finding", "url": "http://x"},
        ]},
    }
    resolved_thread = {
        "isOutdated": False,
        "isResolved": True,
        "comments": {"nodes": [
            {"databaseId": 2, "author": {"login": "chatgpt-codex-connector"},
             "path": "betfair_client.py", "line": 10, "originalLine": 10,
             "body": "addressed finding", "url": "http://y"},
        ]},
    }
    live_thread = {
        "isOutdated": False,
        "isResolved": False,
        "comments": {"nodes": [
            {"databaseId": 3, "author": {"login": "chatgpt-codex-connector"},
             "path": "betfair_client.py", "line": 20, "originalLine": 20,
             "body": "live finding", "url": "http://z"},
        ]},
    }

    assert parser._flatten_thread(outdated_thread)[0]["outdated"] is True
    assert parser._flatten_thread(resolved_thread)[0]["outdated"] is True
    assert parser._flatten_thread(live_thread)[0]["outdated"] is False


@pytest.mark.unit
def test_make_finding_propagates_outdated():
    outdated = parser._make_finding({
        "id": 1, "user": {"login": "chatgpt-codex-connector"},
        "path": "betfair_client.py", "original_line": 753,
        "body": "x", "outdated": True,
    })
    live = parser._make_finding({
        "id": 2, "user": {"login": "chatgpt-codex-connector"},
        "path": "betfair_client.py", "line": 20, "position": 4,
        "body": "y",
    })
    assert outdated["outdated"] is True
    assert live["outdated"] is False
    assert live["line"] == 20


@pytest.mark.unit
def test_is_codex_comment_matches_only_codex():
    assert parser._is_codex_comment({"user": {"login": "chatgpt-codex-connector"}}) is True
    assert parser._is_codex_comment({"user": {"login": "openai-codex[bot]"}}) is True
    assert parser._is_codex_comment({"user": {"login": "greptile-apps"}}) is False
    assert parser._is_codex_comment({"user": {"login": "deepsource-io"}}) is False
