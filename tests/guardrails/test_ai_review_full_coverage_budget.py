"""PF-569: Sol e Grok ricevono il diff intero della #569; oltre il tetto resta fail-closed.

Catena ESTRATTA dai workflow veri (mai una copia), tetti letti dal loro `env`."""
import ast
import re
import textwrap
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SOL = ".github/workflows/pr-review-openrouter-gpt56-sol.yml"
GROK = ".github/workflows/pr-review-xai-grok46.yml"
# Patch per file del diff completo reale della #569 (Compare API, 22d1b71...fac499d).
DIFF_569 = [1962, 3355, 913, 21014, 22159, 826, 425, 62419, 71800, 1483]
OK = "## Bloccanti\nNessun bloccante evidente\n\n## Verdetto finale\nCopertura: COMPLETA\nVisto tutto.\n"


def _env(wf):
    return list(yaml.safe_load((ROOT / wf).read_text(encoding="utf-8"))["jobs"].values())[0]["env"]


def _catena(wf):
    righe = (ROOT / wf).read_text(encoding="utf-8").splitlines()
    i = next(n for n, r in enumerate(righe) if r.strip() == "python3 <<'PY'")
    j = next(n for n in range(i + 1, len(righe)) if righe[n].strip() == "PY")
    albero = ast.parse(textwrap.dedent("\n".join(righe[i + 1:j])))
    env = _env(wf)
    ns = {"re": re, "MAX_PATCH_PER_FILE_CHARS": int(env["MAX_PATCH_PER_FILE_CHARS"]),
          "MAX_TOTAL_PATCH_CHARS": int(env["MAX_TOTAL_PATCH_CHARS"])}
    for nodo in albero.body:
        if isinstance(nodo, ast.Assign) and not {getattr(t, "id", "") for t in nodo.targets} & set(ns):
            try:
                exec(compile(ast.Module(body=[nodo], type_ignores=[]), wf, "exec"), ns)
            except Exception:
                pass                    # costanti che dipendono dall'ambiente di Actions
    defs = [n for n in albero.body if isinstance(n, ast.FunctionDef)]
    exec(compile(ast.Module(body=defs, type_ignores=[]), wf, "exec"), ns)
    return ns


def _files(sizes):
    def patch(n):
        return ("@@ -0,0 +1 @@\n" + "\n".join("+" + "x" * 98 for _ in range(n // 100 + 1)))[:n]
    return [{"filename": f"f{k}.py", "status": "modified", "patch": patch(n), "additions": 1,
             "deletions": 0, "changes": 1} for k, n in enumerate(sizes)]


def _run(c, files, compare_truncated=False):
    text, skipped, _ = c["build_patch_payload"](files)
    return text, skipped, c["rr_coverage_gaps"](files, text, compare_truncated)


@pytest.mark.parametrize("wf", [SOL, GROK])
def test_pass_full_569_diff_reaches_the_model_whole(wf):
    c, files = _catena(wf), _files(DIFF_569)
    text, skipped, gaps = _run(c, files)
    assert skipped == [] and gaps == [] and all(f["patch"] in text for f in files)
    summary = c["rr_coverage_summary"](files, text, False)
    assert summary.startswith("10/10 file con patch inviati, 0 tagliati") and summary.endswith("nessuno")
    assert c["rr_classify_output"](OK, coverage_gaps=gaps)[0] == "VALID_PASS"
    env = _env(wf)                      # margine di almeno il 25% sul diff reale
    assert int(env["MAX_PATCH_PER_FILE_CHARS"]) >= 1.25 * max(DIFF_569)
    assert int(env["MAX_TOTAL_PATCH_CHARS"]) >= 1.25 * sum(DIFF_569)


@pytest.mark.parametrize("wf", [SOL, GROK])
@pytest.mark.parametrize("case", ["per_file", "total", "compare"])
def test_block_over_budget_or_truncated_compare_is_a_gap_never_pass(wf, case):
    c, env = _catena(wf), _env(wf)
    per_file, total = int(env["MAX_PATCH_PER_FILE_CHARS"]), int(env["MAX_TOTAL_PATCH_CHARS"])
    sizes = {"per_file": [per_file + 10], "total": [per_file - 500] * (total // per_file + 2),
             "compare": [100]}[case]
    files = _files(sizes)
    text, _, gaps = _run(c, files, compare_truncated=case == "compare")
    expected = {"per_file": "1_patch_tagliate", "total": "_file_oltre_budget", "compare": "compare_api_troncata"}
    assert any(g.endswith(expected[case]) for g in gaps), gaps
    assert expected[case] in c["rr_coverage_summary"](files, text, case == "compare")
    assert c["rr_classify_output"](OK, coverage_gaps=gaps)[0] == "VALID_INCONCLUSIVE"


@pytest.mark.parametrize("wf", [SOL, GROK])
def test_comment_records_full_shas_and_measured_coverage(wf):
    t = (ROOT / wf).read_text(encoding="utf-8")
    assert "**SHA esaminati:** `{range_base}...{range_head}`" in t
    assert "**Diff inviato al modello:** {rr_coverage_summary(files, diff_text, compare_truncated)}" in t


def test_active_reviewers_share_the_budget():
    for k in ("MAX_PATCH_PER_FILE_CHARS", "MAX_TOTAL_PATCH_CHARS"):
        assert _env(SOL)[k] == _env(GROK)[k]
