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
- ENFORCE dalla #470 (invariante di scope, in fondo a questo file): il
  per-task ``files`` deve COINCIDERE col diff, la entry del task non si
  riscrive in silenzio, e `default` e le chiavi altrui non si toccano.
  Vale anche per `task_file_change` e per le famiglie-prefisso.
- NON enforce, e lo si dichiara: ``max_files`` (solo warning a >25 file),
  che imposto ``files`` == diff e' gia' il numero del diff; e non blocca
  ``.github/workflows/*`` in quanto tale — li blocca solo se non sono
  dichiarati, come ogni altro file.
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
        # `files` allineato al diff: dall'invariante di scope in fondo a questo
        # file, una dichiarazione che NON coincide col diff blocca.
        scope={"tasks": {"my_key": {"files": ["tests/scripts/test_x.py"], "max_files": 8}}},
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)


def test_main_pass_with_approved_family_when_registered(tmp_path, monkeypatch):
    """Una famiglia approvata (es. `audit_`) supera la validazione della task
    key anche senza essere nel registro — quel ramo non e' cambiato. Ma per
    passare il gate le serve comunque una entry, perche' senza `files` non c'e'
    scope dichiarato: vedi `test_main_blocca_il_task_senza_entry_nel_registro`."""
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: audit_demo] x"},
        files=["tests/x.py"],
        scope={"tasks": {"audit_demo": {"files": ["tests/x.py"]}}},
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

def test_main_ora_enforce_files_ma_ancora_non_max_files(tmp_path, monkeypatch):
    """Il confine si e' spostato, ed e' il test che lo prevedeva.

    La versione precedente affermava che il gate NON controlla il `files` del
    task, e chiudeva con «se un giorno questo gate iniziasse a bloccare qui,
    questo test lo segnala». E' successo: l'invariante `files` == diff e' ora
    applicato (vedi in fondo al file). Quel che resta NON applicato e'
    `max_files`, che una volta imposto `files` == diff e' pura documentazione:
    se i file coincidono col diff, il loro numero e' il numero del diff."""
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: my_key] x"},
        # `files` coincide col diff; `max_files` dichiara 1 su 20 file e NON blocca.
        files=[f"unrelated_{i}.py" for i in range(20)],
        scope={"tasks": {"my_key": {"files": [f"unrelated_{i}.py" for i in range(20)],
                                    "max_files": 1}}},
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)


def test_main_does_not_block_workflow_paths_when_declared(tmp_path, monkeypatch):
    """Boundary: toccare `.github/workflows/*` NON e' bloccato da questo gate in
    quanto tale (non e' in CRITICAL_FILES) — ma va DICHIARATO come ogni altro
    file. E' la differenza che alla #463 non c'era: li' il workflow-gate era
    toccato e non dichiarato, e passava."""
    _write_gate_inputs(
        tmp_path,
        meta={"title": "[TASK: my_key] x"},
        files=[".github/workflows/ci.yml"],
        scope={"tasks": {"my_key": {"files": [".github/workflows/ci.yml"]}}},
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)


# ---------------------------------------------------------------------------
# INVARIANTE DI SCOPE: `files` della task key == file cambiati dalla PR
#
# Chiude il bloccante su cui convergevano tutti e quattro i reviewer pagati
# sulla #469: l'invariante era scritto nella policy ma non applicato da CI, su
# un file che l'agente stesso scrive. Grok: «e' solo prosa». Sol: «affidare
# questi vincoli alla sola dichiarazione dell'agente non chiude la falla».
#
# Non era teorico. Misurato sulle 14 PR precedenti: 13 rispettavano gia'
# l'invariante, una no — la #463 (1b7b689d) dichiarava 8 file e ne toccava 12,
# fra cui `.github/workflows/pr-merge-readiness.yml` (un workflow-gate) e
# `docs/auto_pr_flow_spec.md` (un file-policy). Fuori scope dichiarato, mergiata,
# e nessuno se n'e' accorto.
# ---------------------------------------------------------------------------

def _write_gate_inputs_con_base(tmp_path, *, meta, files, scope, base_scope=None):
    """Come `_write_gate_inputs`, piu' la copia del registro dal branch base.

    Il workflow `pr-guard.yml` la scrive accanto a `pr_meta.json` e
    `pr_files_raw.json` (ha gia' `fetch-depth: 0`), cosi' il confronto col base
    non costringe `guardrail_check.py` a lanciare git: resta puro e testabile.
    """
    _write_gate_inputs(tmp_path, meta=meta, files=files, scope=scope)
    if base_scope is not None:
        (tmp_path / "allowed_scope_base.json").write_text(
            json.dumps(base_scope), encoding="utf-8"
        )


SCOPE_463 = {
    "tasks": {
        "gpt56sol_openrouter_migration": {
            "files": [
                ".guardrails/allowed_scope.json",
                ".github/workflows/pr-review-openai-gpt56-sol.yml",
            ],
            "max_files": 8,
        }
    }
}


def test_main_blocca_il_file_toccato_e_non_dichiarato(tmp_path, monkeypatch):
    """RED-FIRST — riproduce la #463: la PR tocca un workflow-gate e un
    file-policy che non ha dichiarato. Prima di questo guard passava."""
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: gpt56sol_openrouter_migration] x"},
        files=[
            ".guardrails/allowed_scope.json",
            ".github/workflows/pr-review-openai-gpt56-sol.yml",
            ".github/workflows/pr-merge-readiness.yml",   # mai dichiarato
            "docs/auto_pr_flow_spec.md",                  # mai dichiarato
        ],
        scope=SCOPE_463,
        base_scope=SCOPE_463,
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_blocca_il_file_dichiarato_e_non_toccato(tmp_path, monkeypatch):
    """L'altra direzione: scope riservato e non usato. Meno grave del caso
    sopra, ma e' il cricchetto — la entry resta nel registro per dopo."""
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: k] x"},
        files=["a.py"],
        scope={"tasks": {"k": {"files": ["a.py", "core/money_management.py"]}}},
        base_scope={"tasks": {"k": {"files": ["a.py", "core/money_management.py"]}}},
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_passa_quando_files_coincide_col_diff(tmp_path, monkeypatch):
    """Il caso normale: 13 delle 14 PR misurate stanno gia' qui."""
    coincide = {
        "tasks": {"k": {"files": [".guardrails/allowed_scope.json", "a.py"], "max_files": 2}}
    }
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: k] x"},
        files=[".guardrails/allowed_scope.json", "a.py"],
        scope=coincide,
        base_scope=coincide,
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)


def test_main_blocca_la_entry_di_un_altro_task_modificata(tmp_path, monkeypatch):
    """Riscrivere cio' che era stato concesso a un ALTRO task: need-manual,
    mai auto-merge. Qui la PR si allarga di soppiatto un'altra chiave."""
    base = {"tasks": {
        "k": {"files": [".guardrails/allowed_scope.json"]},
        "altro": {"files": ["solo_questo.py"]},
    }}
    head = {"tasks": {
        "k": {"files": [".guardrails/allowed_scope.json"]},
        "altro": {"files": ["solo_questo.py", "core/money_management.py"]},
    }}
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: k] x"},
        files=[".guardrails/allowed_scope.json"],
        scope=head,
        base_scope=base,
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_blocca_il_default_modificato(tmp_path, monkeypatch):
    """`default` vale per tutti i task che non dichiarano il proprio: alzarlo
    e' un allargamento globale, non locale alla PR."""
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: k] x"},
        files=[".guardrails/allowed_scope.json"],
        scope={"default": {"max_files": 999}, "tasks": {"k": {"files": [".guardrails/allowed_scope.json"]}}},
        base_scope={"default": {"max_files": 8}, "tasks": {"k": {"files": [".guardrails/allowed_scope.json"]}}},
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_blocca_se_manca_la_copia_base_del_registro(tmp_path, monkeypatch):
    """Fail-closed: la PR tocca il registro ma la copia base non c'e', quindi
    il confronto e' impossibile. Un controllo che si puo' saltare in silenzio
    non e' un controllo — era il punto di Sol."""
    scope = {"tasks": {"k": {"files": [".guardrails/allowed_scope.json"]}}}
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: k] x"},
        files=[".guardrails/allowed_scope.json"],
        scope=scope,
        base_scope=None,     # non scritta
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_blocca_il_task_senza_entry_nel_registro(tmp_path, monkeypatch):
    """Nessuno scope dichiarato = scope illimitato. Vale anche per le
    famiglie-prefisso (`audit_`, `ci_`, ...), che prima passavano senza
    registrazione: misurate 0 volte su 31 commit con marker, quindi chiudere
    la scappatoia non toglie niente a nessuno."""
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: audit_demo] x"},
        files=["tests/x.py"],
        scope={"tasks": {}},
        base_scope={"tasks": {}},
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


# ---------------------------------------------------------------------------
# I BUCHI CHE CODEX HA TROVATO NELLA PRIMA VERSIONE DI QUESTO GUARD (#470)
#
# Quattro P1, tutti reali, tutti riprodotti prima di essere corretti. I quattro
# reviewer pagati non li avevano visti: avevano guardato se il disegno fosse
# coerente, non se fosse aggirabile.
# ---------------------------------------------------------------------------

def test_main_blocca_il_task_sintetico_che_tocca_altro(tmp_path, monkeypatch):
    """P1 Codex: `task_file_change` saltava l'invariante per l'INTERO diff.

    Una PR con un file sotto `ops/tasks/` piu' un workflow-gate risolveva a
    `task_file_change`, e l'esenzione che avevo scritto io disattivava il
    controllo su tutto il resto. `CRITICAL_FILES` non contiene
    `.github/workflows/*`, quindi non lo fermava nemmeno quello: il gate
    passava proprio sul caso che questa PR esiste per chiudere.
    """
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "nessun marker qui"},
        files=["ops/tasks/foo.md", ".github/workflows/pr-guard.yml"],
        scope={"tasks": {}},
        base_scope={"tasks": {}},
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_blocca_la_riscrittura_muta_della_propria_entry(tmp_path, monkeypatch):
    """P1 Codex: riusare una chiave altrui equivaleva a riscriverne lo scope.

    `registry_tampering` salta la entry del task corrente — deve, perche' la
    policy impone a ogni PR di registrarsi li'. Ma cosi' bastava mettere nel
    marker una chiave gia' esistente e sostituirne i `files` col proprio diff:
    `validate_declared_scope` vedeva coincidenza esatta e il guard passava.

    La policy chiede che un'estensione in corso d'opera sia DICHIARATA nella
    `description` della chiave. Ora e' il guard a pretenderlo.
    """
    base = {"tasks": {"k": {"files": ["innocuo.py"], "description": "il task originale"}}}
    head = {"tasks": {"k": {
        # `files` riscritti sul diff nuovo, `description` intatta: muto.
        "files": [".github/workflows/pr-guard.yml", ".guardrails/allowed_scope.json"],
        "description": "il task originale",
    }}}
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: k] x"},
        files=[".github/workflows/pr-guard.yml", ".guardrails/allowed_scope.json"],
        scope=head,
        base_scope=base,
    )
    monkeypatch.chdir(tmp_path)
    with ASSERTIONS.assertRaises(SystemExit):
        g.main()


def test_main_passa_se_l_estensione_della_propria_entry_e_dichiarata(tmp_path, monkeypatch):
    """L'altra faccia: estendere i propri `files` e' legittimo se dichiarato."""
    base = {"tasks": {"k": {"files": ["innocuo.py"], "description": "il task originale"}}}
    head = {"tasks": {"k": {
        "files": [".github/workflows/pr-guard.yml", ".guardrails/allowed_scope.json"],
        "description": "il task originale. AMPLIAMENTO SCOPE DICHIARATO: aggiunto il workflow perche' ...",
    }}}
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: k] x"},
        files=[".github/workflows/pr-guard.yml", ".guardrails/allowed_scope.json"],
        scope=head,
        base_scope=base,
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)


def test_main_passa_su_una_chiave_nuova_senza_pretendere_dichiarazioni(tmp_path, monkeypatch):
    """Una chiave che nel base non esiste e' una registrazione, non un
    allargamento: non c'e' niente di precedente da dichiarare."""
    head = {"tasks": {"nuova": {"files": ["a.py", ".guardrails/allowed_scope.json"],
                               "description": "task nuovo di zecca"}}}
    _write_gate_inputs_con_base(
        tmp_path,
        meta={"title": "[TASK: nuova] x"},
        files=["a.py", ".guardrails/allowed_scope.json"],
        scope=head,
        base_scope={"tasks": {}},
    )
    monkeypatch.chdir(tmp_path)
    ASSERTIONS.assertEqual(g.main(), 0)
