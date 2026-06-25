"""PR-M programma test hedge-fund grade: hard-verify PR gate (entry-point).

Phase 0 dedup (verificato): la logica di automazione PR ha ~1080 test in
tests/scripts/test_pr_automation_controller.py + flow/readiness, MA
l'ENTRY-POINT del gate, scripts/guardrail_check.py, NON aveva alcun file
di unit test dedicato: 7 funzioni su 9 senza copertura diretta e il
comportamento fail-closed su JSON malformato non testato. Qui si copre
SOLO quel gap (test-only, zero modifiche di produzione), su comportamento
REALE delle funzioni.

Contratto del gate documentato da questi test (cosa guardrail_check.py FA
e cosa NON fa):
- VALIDA fail-closed (SystemExit): file mancante / JSON invalido /
  pr_meta non-dict / pr_files non-list / TASK marker mancante o NON
  registrato / task_file_change che tocca anche file critici.
- NON enforce (by-design, layering nel pr_automation_controller): il
  per-task ``files`` allowlist e ``max_files`` (solo warning a >25 file),
  e non blocca ``.github/workflows/*`` se la PR ha un task registrato.
  Questi confini sono caratterizzati esplicitamente cosi' una eventuale
  regressione (gate piu' permissivo o piu' severo del previsto) si vede.
"""
from __future__ import annotations

import json
from unittest import TestCase

import pytest

import scripts.guardrail_check as g

ASSERTIONS = TestCase()


# ---------------------------------------------------------------------------
# load_json — MALFORMED / fail-closed
# ---------------------------------------------------------------------------

def test_load_json_missing_file_fails_closed(tmp_path):
    with ASSERTIONS.assertRaises(SystemExit):
        g.load_json(str(tmp_path / "does_not_exist.json"))


def test_load_json_invalid_json_fails_closed(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{ not valid json ", encoding="utf-8")
    with ASSERTIONS.assertRaises(SystemExit):
        g.load_json(str(p))


def test_load_json_valid_returns_parsed(tmp_path):
    p = tmp_path / "ok.json"
    p.write_text(json.dumps({"a": 1}), encoding="utf-8")
    ASSERTIONS.assertEqual(g.load_json(str(p)), {"a": 1})


# ---------------------------------------------------------------------------
# normalize_changed_files — EDGE
# ---------------------------------------------------------------------------

def test_normalize_changed_files_str_and_dict_entries():
    raw = [
        "a.py",
        {"filename": "b.py"},
        "  c.py  ",                 # trimmed
        {"filename": "  d.py  "},   # trimmed
    ]
    ASSERTIONS.assertEqual(
        g.normalize_changed_files(raw),
        ["a.py", "b.py", "c.py", "d.py"],
    )


def test_normalize_changed_files_drops_empty_and_unknown_shapes():
    raw = [
        "",                       # empty str -> dropped
        "   ",                    # whitespace -> dropped
        {"no_filename": "x"},     # dict senza filename -> dropped
        {"filename": ""},         # filename vuoto -> dropped
        None,                     # non-str/non-dict -> dropped
        42,                       # idem
        "keep.py",
    ]
    ASSERTIONS.assertEqual(g.normalize_changed_files(raw), ["keep.py"])


# ---------------------------------------------------------------------------
# extract_tasks — PASS / EDGE
# ---------------------------------------------------------------------------

def test_extract_tasks_both_marker_forms_and_dedup_order():
    text = (
        "[TASK: first_one] blah\n"
        "TASK: second_one\n"
        "# TASK: second_one\n"          # duplicato case dello stesso -> dedup
        "[TASK: First_One]"             # case-insensitive dup di first_one
    )
    ASSERTIONS.assertEqual(g.extract_tasks(text), ["first_one", "second_one"])


def test_extract_tasks_empty_text_returns_empty():
    ASSERTIONS.assertEqual(g.extract_tasks(""), [])
    ASSERTIONS.assertEqual(g.extract_tasks(None), [])


# ---------------------------------------------------------------------------
# _is_placeholder_or_invalid — BLOCK (placeholder) / PASS (valido)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["", "   ", "todo", "TODO", "....", "ha spazi", "UPPER!", "a/b"])
def test_is_placeholder_or_invalid_true_for_bad(bad):
    ASSERTIONS.assertTrue(g._is_placeholder_or_invalid(bad))


@pytest.mark.parametrize("good", ["test_suite_pr_m_hardverify_gate", "pr_guard", "a1", "x-y_z"])
def test_is_placeholder_or_invalid_false_for_valid(good):
    ASSERTIONS.assertFalse(g._is_placeholder_or_invalid(good))


# ---------------------------------------------------------------------------
# _is_allowed_task_key / _is_approved_task_family
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("task", [
    "pr_guard",                       # exact allowed
    "audit_something",                # approved prefix
    "ci_pipeline_fix",
    "runtime_xyz",
    "betfair_client_thing",
    "hardening_emergency",
    "workflow_hygiene_cleanup",
    "claude_bug_pr7a_order_reconciliation",  # claude bug pattern
])
def test_is_allowed_task_key_accepts_known_families(task):
    ASSERTIONS.assertTrue(g._is_allowed_task_key(task))


@pytest.mark.parametrize("task", [
    "random_task",
    "test_suite_pr_m_hardverify_gate",   # NON e' una famiglia approvata: serve registrazione
    "claude_bug_xx_bad",                 # non matcha il pattern claude_bug_pr<n><a>_
    "auditx_no_underscore",
])
def test_is_allowed_task_key_rejects_unknown(task):
    ASSERTIONS.assertFalse(g._is_allowed_task_key(task))


# ---------------------------------------------------------------------------
# _select_task_candidate — registrato vince anche se non famiglia approvata
# ---------------------------------------------------------------------------

def test_select_task_candidate_registered_key_passes():
    ASSERTIONS.assertTrue(
        g._select_task_candidate("test_suite_pr_m_hardverify_gate", {"test_suite_pr_m_hardverify_gate"})
    )


def test_select_task_candidate_unknown_unregistered_fails():
    ASSERTIONS.assertFalse(g._select_task_candidate("totally_unknown", set()))


# ---------------------------------------------------------------------------
# touches_critical_files
# ---------------------------------------------------------------------------

def test_touches_critical_files_detects_subset():
    changed = ["order_manager.py", "tests/x.py", "core/trading_engine.py", "README.md"]
    ASSERTIONS.assertEqual(
        sorted(g.touches_critical_files(changed)),
        ["core/trading_engine.py", "order_manager.py"],
    )


def test_touches_critical_files_none_when_absent():
    ASSERTIONS.assertEqual(g.touches_critical_files(["tests/x.py", "docs/y.md"]), [])


# ---------------------------------------------------------------------------
# resolve_task — PASS / EDGE
# ---------------------------------------------------------------------------

def test_resolve_task_registered_from_title():
    meta = {"title": "[TASK: my_registered_key] fix"}
    task, source, unknown, _ignored = g.resolve_task(meta, [], {"my_registered_key"})
    ASSERTIONS.assertEqual(task, "my_registered_key")
    ASSERTIONS.assertEqual(source, "pr_title")
    ASSERTIONS.assertEqual(unknown, [])


def test_resolve_task_unknown_recorded_not_selected():
    meta = {"title": "[TASK: not_registered] fix"}
    task, _source, unknown, _ignored = g.resolve_task(meta, [], set())
    ASSERTIONS.assertIsNone(task)
    ASSERTIONS.assertIn(("pr_title", "not_registered"), unknown)


def test_resolve_task_placeholder_ignored():
    meta = {"title": "[TASK: todo] wip"}
    task, _source, unknown, ignored = g.resolve_task(meta, [], set())
    ASSERTIONS.assertIsNone(task)
    ASSERTIONS.assertIn(("pr_title", "todo"), ignored)
    ASSERTIONS.assertEqual(unknown, [])


def test_resolve_task_from_task_file_change_paths():
    meta = {}
    task, source, _u, _i = g.resolve_task(meta, ["ops/tasks/foo.md"], set())
    ASSERTIONS.assertEqual(task, "task_file_change")
    ASSERTIONS.assertEqual(source, "changed_task_files")
    # anche ops/tasks_done/ conta come task-file change
    task2, _s2, _u2, _i2 = g.resolve_task({}, ["ops/tasks_done/bar.md"], set())
    ASSERTIONS.assertEqual(task2, "task_file_change")


def test_resolve_task_none_when_no_marker_anywhere():
    task, source, unknown, _ignored = g.resolve_task({}, ["src/x.py"], set())
    ASSERTIONS.assertIsNone(task)
    ASSERTIONS.assertIsNone(source)
    ASSERTIONS.assertEqual(unknown, [])


# ---------------------------------------------------------------------------
# validate_task_selection — il cuore fail-closed del gate (BLOCK / PASS)
# ---------------------------------------------------------------------------

def test_validate_missing_task_no_candidates_blocks():
    with ASSERTIONS.assertRaises(SystemExit):
        g.validate_task_selection(None, [], set(), [])


def test_validate_missing_task_with_unknown_candidates_blocks():
    with ASSERTIONS.assertRaises(SystemExit):
        g.validate_task_selection(None, [], set(), [("pr_title", "nope")])


def test_validate_task_file_change_plus_critical_blocks():
    with ASSERTIONS.assertRaises(SystemExit):
        g.validate_task_selection("task_file_change", ["order_manager.py"], set(), [])


def test_validate_unknown_registered_task_blocks():
    with ASSERTIONS.assertRaises(SystemExit):
        g.validate_task_selection("ghost_key", [], set(), [])


def test_validate_registered_task_passes():
    # nessuna eccezione = PASS
    g.validate_task_selection("good_key", ["order_manager.py"], {"good_key"}, [])


def test_validate_task_file_change_without_critical_passes():
    g.validate_task_selection("task_file_change", [], set(), [])


# ---------------------------------------------------------------------------
# main() end-to-end — PASS / BLOCK / MALFORMED (via cwd in tmp_path)
# ---------------------------------------------------------------------------

def _write_gate_inputs(tmp_path, *, meta, files, scope):
    (tmp_path / "pr_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (tmp_path / "pr_files_raw.json").write_text(json.dumps(files), encoding="utf-8")
    guard_dir = tmp_path / ".guardrails"
    guard_dir.mkdir(exist_ok=True)
    (guard_dir / "allowed_scope.json").write_text(json.dumps(scope), encoding="utf-8")


def test_main_pass_with_registered_task(tmp_path, monkeypatch):
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: my_key] x"},
        files=[{"filename": "tests/scripts/test_x.py"}],
        scope={"tasks": {"my_key": {"files": ["x.py"], "max_files": 8}}},
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)


def test_main_pass_with_approved_family_even_if_unregistered(tmp_path, monkeypatch):
    # Una famiglia approvata (es. audit_) passa anche senza registrazione.
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: audit_demo] x"},
        files=["tests/x.py"],
        scope={"tasks": {}},
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)


def test_main_blocks_unregistered_task(tmp_path, monkeypatch):
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: not_a_real_key] x"},
        files=["tests/x.py"],
        scope={"tasks": {"other": {}}},
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_blocks_on_malformed_pr_meta_json(tmp_path, monkeypatch):
    (tmp_path / "pr_meta.json").write_text("{ broken ", encoding="utf-8")
    (tmp_path / "pr_files_raw.json").write_text("[]", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_blocks_on_pr_files_not_a_list(tmp_path, monkeypatch):
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: my_key] x"},
        files={"not": "a list"},   # dict invece di list
        scope={"tasks": {"my_key": {}}},
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


# ---------------------------------------------------------------------------
# CARATTERIZZAZIONE dei confini: cosa il gate NON enforce (by-design)
# ---------------------------------------------------------------------------

def test_main_does_not_enforce_per_task_files_or_max_files(tmp_path, monkeypatch):
    """Boundary: guardrail_check.py valida la PRESENZA/registrazione della
    task key, NON che i file cambiati siano dentro il ``files`` del task ne'
    il ``max_files``. (L'enforcement di scope vive nel pr_automation_controller.)
    Se un giorno questo gate iniziasse a bloccare qui, questo test lo segnala."""
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: my_key] x"},
        # file FUORI dal `files` del task e ben oltre max_files=1:
        files=[f"unrelated_{i}.py" for i in range(20)],
        scope={"tasks": {"my_key": {"files": ["only_this.py"], "max_files": 1}}},
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)


def test_main_does_not_block_workflow_paths_when_task_registered(tmp_path, monkeypatch):
    """Boundary: toccare .github/workflows/* NON e' bloccato da questo gate
    (non e' in CRITICAL_FILES); la difesa sui workflow e' altrove."""
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: my_key] x"},
        files=[".github/workflows/ci.yml"],
        scope={"tasks": {"my_key": {}}},
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)
