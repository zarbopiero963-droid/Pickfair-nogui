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
def test_is_outdated_fail_closed_keeps_live_findings():
    # Null position alone must NOT mark a live finding outdated (fail-closed):
    # file-level comments and comments with a current line/side are still live.
    assert parser._is_outdated({"position": None, "subject_type": "file"}) is False
    assert parser._is_outdated({"position": None, "line": 10}) is False
    assert parser._is_outdated({"position": None, "side": "RIGHT"}) is False


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
def test_graphql_raises_on_errors_so_fallback_runs(monkeypatch):
    import json as _json
    monkeypatch.setattr(parser, "_run", lambda cmd: _json.dumps(
        {"errors": [{"message": "rate limited"}], "data": None}))
    with pytest.raises(RuntimeError, match="GraphQL returned errors"):
        parser._fetch_review_comments_graphql("o/r", 1)


@pytest.mark.unit
def test_graphql_raises_on_missing_review_threads(monkeypatch):
    import json as _json
    monkeypatch.setattr(parser, "_run", lambda cmd: _json.dumps(
        {"data": {"repository": {"pullRequest": None}}}))
    with pytest.raises(RuntimeError, match="missing reviewThreads"):
        parser._fetch_review_comments_graphql("o/r", 1)


@pytest.mark.unit
def test_rest_fallback_flattens_slurped_pages(monkeypatch):
    import json as _json
    # gh api --paginate --slurp yields an array of page-arrays.
    pages = [
        [{"id": 1, "user": {"login": "chatgpt-codex-connector"}}],
        [{"id": 2, "user": {"login": "deepsource-io"}}],
    ]
    monkeypatch.setattr(parser, "_run", lambda cmd: _json.dumps(pages))
    out = parser._fetch_review_comments_rest("o/r", 1)
    assert [c["id"] for c in out] == [1, 2]


@pytest.mark.unit
def test_fetch_falls_back_to_rest_on_graphql_failure(monkeypatch):
    import json as _json
    calls = {"graphql": 0, "rest": 0}

    def _fake_run(cmd):
        if "graphql" in cmd:
            calls["graphql"] += 1
            raise RuntimeError("boom")
        calls["rest"] += 1
        return _json.dumps([[{"id": 9, "user": {"login": "chatgpt-codex-connector"}}]])

    monkeypatch.setattr(parser, "_run", _fake_run)
    out = parser._fetch_review_comments("o/r", 1)
    assert calls["graphql"] == 1 and calls["rest"] == 1
    assert out[0]["id"] == 9


@pytest.mark.unit
def test_is_codex_comment_matches_only_codex():
    assert parser._is_codex_comment({"user": {"login": "chatgpt-codex-connector"}}) is True
    assert parser._is_codex_comment({"user": {"login": "openai-codex[bot]"}}) is True
    assert parser._is_codex_comment({"user": {"login": "greptile-apps"}}) is False
    assert parser._is_codex_comment({"user": {"login": "deepsource-io"}}) is False
