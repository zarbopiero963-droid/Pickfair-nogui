"""Verificatore lato GitHub: parte dalle esecuzioni del PRE-LIVE (tabella + blocco pf-gate-decision/1, che devono
coincidere), verifica provenienza (slug + id dell'app), doppia firma, legame, build e teardown di OGNI run VPS.

Le evidenze sono costruite A MANO (non da tdpub, che filtra gia') con firme Ed25519 VERE: cosi' ogni predicato del
verificatore ha un test che fallisce se il predicato viene tolto (audit P2-V3)."""
import base64, copy, hashlib, json, pathlib, subprocess, sys
import pytest

HERE = pathlib.Path(__file__).resolve().parents[2] / "tools" / "teardown_evidence"
sys.path.insert(0, str(HERE))
import verify_teardown_evidence as V   # noqa: E402

SHA = "d" * 40; PR = 608; REPO = "o/r"
APP = {"slug": "pickfair-dedicated", "id": 5579027}
R1, R2, R3, R4, R5 = ("20261104T101341Z-586a4e26", "20261104T101340Z-139418aa", "20261104T102647Z-c253d6f0",
                      "20261104T102647Z-c88b2f4e", "20261104T102635Z-9f4d8896")
#       suite                      env                         result     reason                              run
ROWS = [("pytest-full-regression", "OFFLINE", "PASS", "OK", R1),
        ("bf-readonly-api", "BETFAIR_DELAYED_READ", "PASS", "OK", R2),
        ("ext-guardrails", "GITHUB_EXTERNAL_EVIDENCE", "PASS", "OK", R3),
        ("ext-review", "GITHUB_EXTERNAL_EVIDENCE", "NOT_RUN", "NEEDS_MANUAL_PR_MODIFIES_GITHUB", R4)]


def TD(rid, probe=False):
    checks = V.CHECKS_PROBE if probe else V.CHECKS_SANDBOX
    return {"schema": "pf-teardown/1", "units": [("pf-probe-" if probe else "pf-sbx-") + rid], "uid": None if probe else 990,
            "checks": list(checks), "check_status": {c: "ok" for c in checks}, "settle_s": 10.0, "elapsed_s": 0.1,
            "checked_at": "2026-11-04T10:00:00+00:00", "clean": True, "residue_kinds": [], "residue_count": 0,
            "slot": "pf-bfsim" if probe else "pf-sbxj0"}


@pytest.fixture
def key(tmp_path, monkeypatch):
    k = tmp_path / "k.pem"
    subprocess.run(["openssl", "genpkey", "-algorithm", "ed25519", "-out", str(k)], check=True, capture_output=True)
    pub = subprocess.run(["openssl", "pkey", "-in", str(k), "-pubout"], check=True, capture_output=True, text=True).stdout
    monkeypatch.setattr(V, "PUBKEY_B64", "".join(l for l in pub.splitlines() if "-----" not in l))
    return (k, tmp_path)


def sign(key, raw):
    k, d = key; f = d / "x"; f.write_bytes(raw)
    return subprocess.run(["openssl", "pkeyutl", "-sign", "-inkey", str(k), "-rawin", "-in", str(f)], check=True, capture_output=True).stdout


def canon(o): return json.dumps(o, sort_keys=True, separators=(",", ":")).encode()


def b64(b): return base64.b64encode(b).decode()


def evidence(key, row, side_over=None, man_over=None, raw_over=None, check_over=None):
    suite, env, result, _reason, rid = row
    man = {"schema": "pf-cert-result/1", "run_id": rid, "sha": SHA, "target": str(PR), "scenario": suite, "result": result,
           "reason": "OK", "runner_version": "pf-orch/1.4.3", "repo": REPO, "environment": env}
    man.update(man_over or {})
    mraw = canon(man)
    side = {"schema": "pf-teardown-sidecar/1", "run_id": rid, "sha": SHA, "target": str(PR), "scenario": suite, "result": result,
            "manifest_sha256": hashlib.sha256(mraw).hexdigest(), "runner_build": next(iter(V.ALLOWED_BUILDS)),
            "runner_files": {f: next(iter(v)) for f, v in V.ALLOWED_RUNNER_FILES.items()},
            "teardown": TD(rid, probe=suite == "bf-readonly-api")}
    side.update(side_over or {})
    sraw = canon(side)
    parts = {"sidecar_b64": sraw, "signature_b64": sign(key, sraw), "manifest_b64": mraw, "manifest_signature_b64": sign(key, mraw)}
    parts.update(raw_over or {})
    summary = "Evidenza di teardown.\n\n" + "\n\n".join(f"{n}: `{b64(v)}`" for n, v in parts.items())
    c = {"name": "pf-teardown-evidence/" + suite, "head_sha": SHA, "external_id": rid, "status": "completed",
         "conclusion": "neutral", "app": dict(APP), "output": {"title": "x", "summary": summary}}
    c.update(check_over or {})
    return c


def prelive(rows=ROWS, suites=None, extra="", **over):
    table = V.HEADER + "\n" + "|---" * 9 + "|\n" + "".join(f"| {s} | {e} | {r} | {why} | - | - | - | VERIFIED | {rid} |\n"
                                                             for s, e, r, why, rid in rows)
    dec = {"schema": "pf-gate-decision/1", "gate": "pf-betfair-certification", "pr": PR, "sha": SHA,
           "suites": suites if suites is not None else [{"id": s, "result": r, "reason": why, "verify": "VERIFIED", "run_id": rid}
                                                         for s, e, r, why, rid in rows]}
    summary = ("NEEDS_MANUAL\nGate: pf-betfair-certification (PRE-LIVE)\nPR: #608\n\n" + table + "\n" + extra
               + "NEEDS_MANUAL: GATE_OR_CI\n```json\n" + json.dumps(dec, separators=(",", ":")) + "\n```\n")
    c = {"name": "pf-betfair-certification", "head_sha": SHA, "external_id": f"pr{PR}-{SHA}", "status": "completed",
         "conclusion": "action_required", "app": dict(APP), "output": {"summary": summary}}
    c.update(over); return c


def world(key):
    return [prelive(), evidence(key, ROWS[0]), evidence(key, ROWS[1])]


def run(checks):
    return V.verify(REPO, PR, SHA, fetch=lambda path: {"check_runs": checks})


def fails(out, frag):
    assert out["verdict"] == "FAIL" and any(frag in r for r in out["reasons"]), out


# ---- percorso valido ---------------------------------------------------------------------------------------------
def test_every_vps_run_covered_and_valid_is_pass(key):
    out = run(world(key))
    assert out["verdict"] == "PASS", out
    assert out["required"] == [R1, R2]          # ext-* e NOT_RUN pre-sandbox esclusi


# ---- copertura: si parte dal PRE-LIVE, mai dalle evidenze trovate -------------------------------------------------
def test_missing_evidence_for_one_run_fails(key):
    fails(run(world(key)[:2]), f"{R2}:TD_CHECKS=0")


def test_partial_publication_cannot_pass(key):
    rows = ROWS + [("pytest-multi-table", "SIM", "PASS", "OK", "20261104T102616Z-aaaaaaaa")]
    w = world(key); w[0] = prelive(rows)
    fails(run(w), "20261104T102616Z-aaaaaaaa:TD_CHECKS=0")


def test_a_not_run_after_the_sandbox_is_required(key):
    # audit P2-V1: REQUIRED_REPRODUCTION_MISSING arriva DOPO la sandbox => la unit e' partita => serve l'evidenza
    rows = ROWS + [("pytest-idempotency", "SIM", "NOT_RUN", "REQUIRED_REPRODUCTION_MISSING", "20261104T102603Z-7471d325")]
    w = world(key); w[0] = prelive(rows)
    fails(run(w), "20261104T102603Z-7471d325:TD_CHECKS=0")
    w.append(evidence(key, rows[-1]))
    assert run(w)["verdict"] == "PASS"


def test_an_excluded_row_with_published_evidence_is_still_verified(key):
    w = world(key); w.append(evidence(key, ROWS[2], side_over={"teardown": {"schema": "pf-teardown/1", "clean": False}}))
    fails(run(w), f"{R3}:TEARDOWN_NOT_VERIFIED")


def test_duplicate_evidence_fails(key):
    w = world(key); w.append(copy.deepcopy(w[1]))
    fails(run(w), f"{R1}:TD_CHECKS=2")


def test_duplicate_run_id_in_the_prelive_fails(key):
    rows = ROWS + [("pytest-other", "SIM", "PASS", "OK", R1)]
    w = world(key); w[0] = prelive(rows)
    fails(run(w), f"{R1}:RUN_ID_DUPLICATED_IN_PRELIVE")


# ---- PRE-LIVE: provenienza e parsing rigoroso ---------------------------------------------------------------------
@pytest.mark.parametrize("app", [{"slug": "evil", "id": 5579027}, {"slug": "pickfair-dedicated", "id": 1}, {}])
def test_prelive_from_another_app_fails(key, app):
    w = world(key); w[0]["app"] = app
    fails(run(w), "PRELIVE_CHECKS=0")


def test_two_prelive_checks_fail(key):
    w = world(key); w.append(prelive())
    fails(run(w), "PRELIVE_CHECKS=2")


def test_prelive_identity_must_match(key):
    w = world(key); w[0]["external_id"] = f"pr607-{SHA}"
    fails(run(w), "PRELIVE_EXTERNAL_ID")


def test_prelive_signature_column_must_be_verified(key):
    # tabella e JSON coerenti fra loro (UNVERIFIED in entrambi): il run resta comunque non verificato => FAIL
    w = world(key)
    w[0]["output"]["summary"] = (w[0]["output"]["summary"].replace(f"VERIFIED | {R1}", f"UNVERIFIED | {R1}")
                                 .replace(f'"verify":"VERIFIED","run_id":"{R1}"', f'"verify":"UNVERIFIED","run_id":"{R1}"'))
    fails(run(w), f"{R1}:PRELIVE_SIGNATURE_NOT_VERIFIED")


def test_a_broken_row_in_the_table_fails_instead_of_disappearing(key):
    # audit P2-V2: un a-capo nel reason spezzava la riga, che spariva dall'elenco richiesto
    w = world(key)
    w[0]["output"]["summary"] = w[0]["output"]["summary"].replace(f"| PASS | OK | - | - | - | VERIFIED | {R2} |",
                                                                  f"| PASS | O\nK | - | - | - | VERIFIED | {R2} |")
    fails(run(w), "PRELIVE_TABLE_")


def test_table_and_decision_json_must_agree(key):
    w = world(key)
    w[0] = prelive(suites=[{"id": s, "result": r, "verify": "VERIFIED", "run_id": rid} for s, e, r, why, rid in ROWS[:3]])
    fails(run(w), "PRELIVE_TABLE_DECISION_MISMATCH")


def test_decision_json_of_another_pr_fails(key):
    w = world(key); w[0]["output"]["summary"] = w[0]["output"]["summary"].replace('"pr":608', '"pr":607')
    fails(run(w), "PRELIVE_DECISION_IDENTITY")


def test_prelive_without_vps_runs_is_not_pass(key):
    fails(run([prelive([r for r in ROWS if r[1] == "GITHUB_EXTERNAL_EVIDENCE"])]), "PRELIVE_NO_VPS_RUNS")


# ---- evidenza: provenienza, nome, campi ----------------------------------------------------------------------------
@pytest.mark.parametrize("app", [{"slug": "github-actions", "id": 15368}, {"slug": "pickfair-dedicated", "id": 2}])
def test_evidence_from_another_app_fails(key, app):
    w = world(key); w[1]["app"] = app
    fails(run(w), "TD_CHECK_FROM_OTHER_SOURCE")


def test_evidence_on_another_sha_fails(key):
    w = world(key); w[2]["head_sha"] = "e" * 40
    fails(run(w), f"{R2}:TD_CHECKS=0")


def test_evidence_check_name_must_match_the_suite(key):
    w = world(key); w[1]["name"] = "pf-teardown-evidence/altro"
    fails(run(w), f"{R1}:TD_CHECK_NAME")


@pytest.mark.parametrize("missing", ["sidecar_b64", "signature_b64", "manifest_b64", "manifest_signature_b64"])
def test_incomplete_evidence_fails(key, missing):
    w = world(key); s = w[1]["output"]["summary"]
    w[1]["output"]["summary"] = "\n\n".join(p for p in s.split("\n\n") if not p.startswith(missing + ":"))
    fails(run(w), f"{R1}:TD_EVIDENCE_INCOMPLETE")


def test_a_field_repeated_twice_is_rejected(key):
    w = world(key); w[1]["output"]["summary"] += "\n\nmanifest_b64: `" + b64(b"{}") + "`"
    fails(run(w), f"{R1}:TD_EVIDENCE_INCOMPLETE")


def test_invalid_base64_is_fail_not_traceback(key):
    w = world(key); w[1]["output"]["summary"] = w[1]["output"]["summary"].replace("sidecar_b64: `", "sidecar_b64: `A", 1)
    fails(run(w), f"{R1}:")


# ---- firme e legame -----------------------------------------------------------------------------------------------
def test_modified_manifest_bytes_break_the_signature(key):
    raw = canon({"schema": "pf-cert-result/1"})
    w = world(key); w[1] = evidence(key, ROWS[0], raw_over={"manifest_b64": raw})
    fails(run(w), f"{R1}:MANIFEST_SIGNATURE_INVALID")


def test_modified_sidecar_bytes_break_the_signature(key):
    w = world(key); w[1] = evidence(key, ROWS[0], raw_over={"sidecar_b64": canon({"schema": "x"})})
    fails(run(w), f"{R1}:SIDECAR_SIGNATURE_INVALID")


def test_signature_by_another_key_fails(key, monkeypatch):
    w = world(key)
    monkeypatch.setattr(V, "PUBKEY_B64", "MCowBQYDK2VwAyEAGOCShUVxqBgNHHwlwN4JTGfSVBemrL7WByXgNXNx8yI=")
    fails(run(w), "SIDECAR_SIGNATURE_INVALID")


def test_a_validly_signed_manifest_of_another_run_fails_the_binding(key):
    other = canon({"schema": "pf-cert-result/1", "run_id": R1, "sha": SHA, "target": str(PR), "scenario": ROWS[0][0],
                   "result": "PASS", "reason": "ALTRO", "repo": REPO, "environment": "OFFLINE"})
    w = world(key); w[1] = evidence(key, ROWS[0], raw_over={"manifest_b64": other, "manifest_signature_b64": sign(key, other)})
    fails(run(w), f"{R1}:BINDING_SHA256")


def test_a_sidecar_used_as_manifest_fails(key):
    w = world(key)
    side = json.loads(base64.b64decode(w[1]["output"]["summary"].split("sidecar_b64: `")[1].split("`")[0]))
    fake = canon(side)
    side2 = dict(side, manifest_sha256=hashlib.sha256(fake).hexdigest())
    w[1] = evidence(key, ROWS[0], side_over=side2, raw_over={"manifest_b64": fake, "manifest_signature_b64": sign(key, fake)})
    fails(run(w), f"{R1}:MANIFEST_SCHEMA")


def test_sidecar_schema_is_required(key):
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"schema": "pf-cert-result/1"})
    fails(run(w), f"{R1}:SIDECAR_SCHEMA")


# ---- coerenza dei campi --------------------------------------------------------------------------------------------
@pytest.mark.parametrize("where,k,v,frag", [
    ("side", "run_id", "20261104T000000Z-00000000", "SIDECAR_RUN_ID"), ("man", "run_id", "20261104T000000Z-00000000", "MANIFEST_RUN_ID"),
    ("side", "sha", "e" * 40, "SIDECAR_SHA"), ("man", "sha", "e" * 40, "MANIFEST_SHA"),
    ("side", "target", "607", "SIDECAR_TARGET"), ("man", "target", "607", "MANIFEST_TARGET"),
    ("side", "scenario", "altro", "SIDECAR_SCENARIO"), ("man", "scenario", "altro", "MANIFEST_SCENARIO"),
    ("side", "result", "FAIL", "RESULT_MISMATCH"), ("man", "result", "FAIL", "RESULT_MISMATCH"),
    ("man", "repo", "x/y", "MANIFEST_REPO"), ("man", "environment", "SIM", "MANIFEST_ENVIRONMENT"),
    ("side", "runner_build", "pf-orch/9.9.9", "RUNNER_BUILD_NOT_AUTHORIZED"),
])
def test_every_field_is_cross_checked(key, where, k, v, frag):
    w = world(key)
    w[1] = evidence(key, ROWS[0], side_over={k: v} if where == "side" else None, man_over={k: v} if where == "man" else None)
    fails(run(w), f"{R1}:{frag}")


@pytest.mark.parametrize("f", ["orch.py", "certd.py", "tdpub.py"])
def test_unauthorized_runner_files_fail(key, f):
    w = world(key); rf = {x: next(iter(v)) for x, v in V.ALLOWED_RUNNER_FILES.items()}; rf[f] = "0" * 64
    w[1] = evidence(key, ROWS[0], side_over={"runner_files": rf})
    fails(run(w), f"{R1}:RUNNER_FILE_NOT_AUTHORIZED:{f}")


# ---- teardown: pulito, completo, di questo run --------------------------------------------------------------------
@pytest.mark.parametrize("mutate,frag", [
    (lambda t: t.update(clean=False), "TEARDOWN_NOT_VERIFIED"),
    (lambda t: t.update(residue_kinds=["procs_uid990"]), "TEARDOWN_NOT_VERIFIED"),
    (lambda t: t.update(residue_count=1), "TEARDOWN_NOT_VERIFIED"),
    (lambda t: t.update(error="X"), "TEARDOWN_NOT_VERIFIED"),
    (lambda t: t["check_status"].update(locks="error"), "TEARDOWN_NOT_VERIFIED"),
    (lambda t: t.update(schema="pf-teardown/0"), "TEARDOWN_NOT_VERIFIED"),
    (lambda t: t.update(checks=[], check_status={}), "TEARDOWN_NOT_VERIFIED"),
    (lambda t: (t["checks"].remove("sysvipc"), t["check_status"].pop("sysvipc")), "TEARDOWN_UNIT_OR_CHECKS"),
    (lambda t: t.update(units=["pf-sbx-20261104T000000Z-00000000"]), "TEARDOWN_UNIT_OR_CHECKS"),
])
def test_teardown_predicates(key, mutate, frag):
    td = TD(R1); mutate(td)
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown": td})
    fails(run(w), f"{R1}:{frag}")


def test_dirty_regression_base_fails(key):
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown_base": {**TD(R1), "clean": False}})
    fails(run(w), f"{R1}:TEARDOWN_BASE_NOT_VERIFIED")


def test_probe_with_the_probe_check_set_passes_and_with_sandbox_set_fails(key):
    assert run(world(key))["verdict"] == "PASS"
    w = world(key); w[2] = evidence(key, ROWS[1], side_over={"teardown": {**TD(R2, probe=True), "uid": 990}})
    fails(run(w), f"{R2}:TEARDOWN_UNIT_OR_CHECKS")


# ---- infrastruttura --------------------------------------------------------------------------------------------------
def test_github_unreachable_is_infra_error_never_pass():
    def boom(path): raise ConnectionError("down")
    assert V.verify(REPO, PR, SHA, fetch=boom)["verdict"] == "INFRA_ERROR"


@pytest.mark.parametrize("pr,sha", [("x", SHA), (PR, "z" * 40)])
def test_bad_input_fails(pr, sha):
    assert V.verify(REPO, pr, sha, fetch=lambda p: {"check_runs": []}) == {"verdict": "FAIL", "reasons": ["INPUT"]}


def test_a_row_with_an_extra_pipe_fails(key):
    # un '|' dentro una cella sposta le colonne: la riga deve far fallire, non essere letta di traverso
    w = world(key)
    w[0]["output"]["summary"] = w[0]["output"]["summary"].replace(f"| PASS | OK | - | - | - | VERIFIED | {R2} |",
                                                                  f"| PASS | O|K | - | - | - | VERIFIED | {R2} |")
    fails(run(w), "PRELIVE_TABLE_ROW_CELLS")


# ---- audit di chiusura: P2-1 (tabella == JSON su ogni campo usato) -------------------------------------------------
NR = ("pytest-idempotency", "SIM", "NOT_RUN", "REQUIRED_REPRODUCTION_MISSING", "20261104T102603Z-7471d325")


def test_table_reason_disagreeing_with_the_json_cannot_hide_a_required_run(key):
    # la tabella dice pre-sandbox (non richiesto), il JSON dice post-sandbox: prima dava PASS senza evidenza
    rows = ROWS + [NR]
    dec = [{"id": s, "result": r, "reason": why, "verify": "VERIFIED", "run_id": rid} for s, e, r, why, rid in rows]
    w = world(key); w[0] = prelive(ROWS + [NR[:3] + ("SELECTION_ABSENT_NEEDS_TESTS", NR[4])], suites=dec)
    fails(run(w), "PRELIVE_TABLE_DECISION_MISMATCH")


def test_table_signature_disagreeing_with_the_json_verify_fails(key):
    dec = [{"id": s, "result": r, "reason": why, "verify": "UNVERIFIED" if rid == R1 else "VERIFIED", "run_id": rid}
           for s, e, r, why, rid in ROWS]
    w = world(key); w[0] = prelive(suites=dec)
    fails(run(w), "PRELIVE_TABLE_DECISION_MISMATCH")


@pytest.mark.parametrize("k,v", [("id", "altro"), ("result", "FAIL"), ("reason", "ALTRO"), ("verify", "UNVERIFIED"),
                                 ("run_id", "20261104T000000Z-00000000")])
def test_every_decision_field_is_compared_with_the_table(key, k, v):
    dec = [{"id": s, "result": r, "reason": why, "verify": "VERIFIED", "run_id": rid} for s, e, r, why, rid in ROWS]
    dec[0][k] = v
    w = world(key); w[0] = prelive(suites=dec)
    fails(run(w), "PRELIVE_TABLE_DECISION_MISMATCH")


def test_a_non_dict_suite_in_the_decision_fails(key):
    dec = [{"id": s, "result": r, "reason": why, "verify": "VERIFIED", "run_id": rid} for s, e, r, why, rid in ROWS] + ["x"]
    w = world(key); w[0] = prelive(suites=dec)
    fails(run(w), "PRELIVE_TABLE_DECISION_MISMATCH")


def test_a_pre_sandbox_not_run_vps_row_is_not_required(key):
    rows = ROWS + [("pytest-x", "SIM", "NOT_RUN", "SELECTION_ABSENT_NEEDS_TESTS", "20261104T102603Z-11111111")]
    w = world(key); w[0] = prelive(rows)
    out = run(w)
    assert out["verdict"] == "PASS" and "20261104T102603Z-11111111" not in out["required"], out


def test_a_pass_row_with_a_pre_sandbox_reason_is_still_required(key):
    # needs(): l'esenzione vale solo per NOT_RUN, mai per un run eseguito
    rows = ROWS + [("pytest-x", "SIM", "PASS", "SELECTION_ABSENT_NEEDS_TESTS", "20261104T102603Z-11111111")]
    w = world(key); w[0] = prelive(rows)
    fails(run(w), "20261104T102603Z-11111111:TD_CHECKS=0")


def test_github_reason_is_not_a_vps_exemption(key):
    # NIT-A: NEEDS_MANUAL_PR_MODIFIES_GITHUB su una riga VPS non esenta
    rows = ROWS + [("pytest-x", "SIM", "NOT_RUN", "NEEDS_MANUAL_PR_MODIFIES_GITHUB", "20261104T102603Z-11111111")]
    w = world(key); w[0] = prelive(rows)
    fails(run(w), "20261104T102603Z-11111111:TD_CHECKS=0")


# ---- audit di chiusura: P2-2 (un test per ogni predicato di parsing sopravvissuto) ---------------------------------
def _sub(w, a, b, count=1):
    s = w[0]["output"]["summary"]; assert a in s; w[0]["output"]["summary"] = s.replace(a, b, count)


def test_two_table_headers_fail(key):
    w = world(key); _sub(w, V.HEADER, V.HEADER + "\n" + "|---" * 9 + "|\n\n" + V.HEADER)
    fails(run(w), "PRELIVE_TABLE_HEADER")


def test_missing_separator_fails(key):
    w = world(key); _sub(w, "|---" * 9 + "|\n", "")
    fails(run(w), "PRELIVE_TABLE_SEPARATOR")


def test_a_row_without_pipes_fails(key):
    w = world(key); _sub(w, f"| PASS | OK | - | - | - | VERIFIED | {R2} |", f"| PASS | OK | - | - | - | VERIFIED | {R2}")
    fails(run(w), "PRELIVE_TABLE_ROW_SHAPE")


def test_two_decisions_fail(key):
    w = world(key); _sub(w, "NEEDS_MANUAL: GATE_OR_CI", '{"schema":"pf-gate-decision/1"}\nNEEDS_MANUAL: GATE_OR_CI')
    fails(run(w), "PRELIVE_DECISION_COUNT")


def test_a_second_non_compact_decision_fails(key):
    w = world(key); _sub(w, "NEEDS_MANUAL: GATE_OR_CI", '{"schema": "pf-gate-decision/1"}\nNEEDS_MANUAL: GATE_OR_CI')
    fails(run(w), "PRELIVE_DECISION_COUNT")


@pytest.mark.parametrize("a,b", [(f'"sha":"{SHA}"', '"sha":"' + "e" * 40 + '"'),
                                 ('"gate":"pf-betfair-certification"', '"gate":"altro"'),
                                 ('"suites":[', '"suites":{"x":[')])
def test_decision_identity_sha_and_gate(key, a, b):
    w = world(key)
    if b.endswith("[") and "suites" in b:
        _sub(w, ']}\n```', ']}}\n```')
    _sub(w, a, b)
    fails(run(w), "PRELIVE_DECISION_IDENTITY")


def test_prelive_not_completed_is_not_used(key):
    w = world(key); w[0]["status"] = "in_progress"
    fails(run(w), "PRELIVE_CHECKS=0")


def test_evidence_not_completed_is_not_used(key):
    w = world(key); w[1]["status"] = "in_progress"
    fails(run(w), "TD_CHECK_FROM_OTHER_SOURCE")


def test_pagination_reads_beyond_the_first_page(key):
    w = world(key)
    filler = [{"name": f"ci-{i}", "head_sha": SHA, "status": "completed", "app": {"slug": "x", "id": 1}} for i in range(100)]
    pages = {1: filler, 2: w}
    out = V.verify(REPO, PR, SHA, fetch=lambda p: {"check_runs": pages.get(int(p.rsplit("page=", 1)[1]), [])})
    assert out["verdict"] == "PASS", out


def test_too_many_pages_is_infra_error_not_pass(key):
    full = [{"name": "ci", "head_sha": SHA, "status": "completed", "app": {}}] * 100
    out = V.verify(REPO, PR, SHA, fetch=lambda p: {"check_runs": full})
    assert out["verdict"] == "INFRA_ERROR", out


# ---- audit di chiusura: NIT-B / NIT-C ------------------------------------------------------------------------------
@pytest.mark.parametrize("mutate", [
    lambda t: t.update(uid=None, checks=list(V.CHECKS_PROBE), check_status={c: "ok" for c in V.CHECKS_PROBE}),  # sandbox spacciata per probe
    lambda t: t.update(units=t["units"] + ["pf-sbx-altro"]),                                                    # unit in piu'
    lambda t: t.update(units=["x-pf-sbx-" + R1]),                                                               # match per sottostringa
    lambda t: t.update(units=["pf-probe-" + R1]),                                                               # probe con uid e set sandbox
    lambda t: t.update(uid=True),                                                                               # uid non intero
])
def test_teardown_unit_kind_is_exact(key, mutate):
    td = TD(R1); mutate(td)
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown": td})
    fails(run(w), f"{R1}:TEARDOWN_UNIT_OR_CHECKS")


def BASE():
    t = TD("x"); t["units"] = ["pf-sbx-base-0123456789ab-a1b2c3"]; return t


def test_a_clean_regression_base_passes(key):
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown_base": BASE()})
    assert run(w)["verdict"] == "PASS"


@pytest.mark.parametrize("mutate", [
    lambda t: t.update(checks=["cgroup"], check_status={"cgroup": "ok"}),
    lambda t: t.update(units=["pf-sbx-" + R1]),
    lambda t: t.update(units=t["units"] * 2),
    lambda t: t.update(uid=None),
    lambda t: t.update(clean=False),
])
def test_regression_base_must_be_a_complete_sandbox_teardown(key, mutate):
    tb = BASE(); mutate(tb)
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown_base": tb})
    fails(run(w), f"{R1}:TEARDOWN_BASE_NOT_VERIFIED")


@pytest.mark.parametrize("k,frag", [("repo", "MANIFEST_REPO"), ("environment", "MANIFEST_ENVIRONMENT")])
def test_manifest_repo_and_environment_are_mandatory(key, k, frag):
    w = world(key); e = evidence(key, ROWS[0])
    man = json.loads(base64.b64decode(e["output"]["summary"].split("manifest_b64: `")[1].split("`")[0])); man.pop(k)
    mraw = canon(man)
    w[1] = evidence(key, ROWS[0], side_over={"manifest_sha256": hashlib.sha256(mraw).hexdigest()},
                    raw_over={"manifest_b64": mraw, "manifest_signature_b64": sign(key, mraw)})
    fails(run(w), f"{R1}:{frag}")


def test_probe_must_declare_exactly_the_probe_check_set(key):
    # una probe (uid null) che dichiara i controlli della sandbox non e' una probe: il set e' deciso dal tipo di unit
    td = {**TD(R2, probe=True), "checks": list(V.CHECKS_SANDBOX), "check_status": {c: "ok" for c in V.CHECKS_SANDBOX}}
    w = world(key); w[2] = evidence(key, ROWS[1], side_over={"teardown": td})
    fails(run(w), f"{R2}:TEARDOWN_UNIT_OR_CHECKS")


# ---- re-audit: P1-1 (NOT_RUN pre-sandbox: il produttore pubblica sempre, con teardown null) --------------------------
# elenco scritto a mano (non letto dal codice): togliere un motivo dal verificatore deve far fallire un test
PRE_SANDBOX_LITERAL = ["NEEDS_DATA_SUSPENDED_MARKETS", "REQUIRES_SEPARATE_GO_LIVE", "SELECTION_ABSENT_NEEDS_TESTS"]
PS = ("pytest-x", "SIM", "NOT_RUN", "SELECTION_ABSENT_NEEDS_TESTS", "20261104T102603Z-11111111")


def ps_evidence(key, row=PS, **over):
    side = {"teardown": None, "reason": row[3]}; side.update(over.pop("side", {}))
    man = {"reason": row[3]}; man.update(over.pop("man", {}))
    return evidence(key, row, side_over=side, man_over=man)


@pytest.mark.parametrize("why", PRE_SANDBOX_LITERAL)
def test_pre_sandbox_not_run_with_published_null_teardown_passes(key, why):
    row = PS[:3] + (why, PS[4])
    w = world(key); w[0] = prelive(ROWS + [row]); w.append(ps_evidence(key, row))
    out = run(w)
    assert out["verdict"] == "PASS" and PS[4] in out["required"], out


@pytest.mark.parametrize("why", PRE_SANDBOX_LITERAL)
def test_each_pre_sandbox_reason_without_evidence_is_not_required(key, why):
    w = world(key); w[0] = prelive(ROWS + [PS[:3] + (why, PS[4])])
    out = run(w)
    assert out["verdict"] == "PASS" and PS[4] not in out["required"], out


@pytest.mark.parametrize("over,frag", [
    ({"side": {"teardown": TD(PS[4])}}, "PRE_SANDBOX_WITH_TEARDOWN"),
    ({"side": {"teardown_base": TD("x")}}, "PRE_SANDBOX_WITH_TEARDOWN"),
    ({"side": {"reason": "REQUIRED_REPRODUCTION_MISSING"}}, "PRE_SANDBOX_REASON_MISMATCH"),
    ({"man": {"reason": "REQUIRED_REPRODUCTION_MISSING"}}, "PRE_SANDBOX_REASON_MISMATCH"),
    ({"man": {"repo": "x/y"}}, "MANIFEST_REPO"),
    ({"side": {"runner_build": "pf-orch/9"}}, "RUNNER_BUILD_NOT_AUTHORIZED"),
])
def test_pre_sandbox_evidence_is_still_fully_checked(key, over, frag):
    w = world(key); w[0] = prelive(ROWS + [PS]); w.append(ps_evidence(key, **over))
    fails(run(w), f"{PS[4]}:{frag}")


def test_null_teardown_on_a_run_that_started_is_fail(key):
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown": None})
    fails(run(w), f"{R1}:TEARDOWN_ABSENT")


def test_blocked_pre_sandbox_is_never_pass(key):
    row = ("pytest-x", "SIM", "BLOCKED", "INFRA_ERROR_NO_CLEAN_SANDBOX_SLOT", PS[4])
    w = world(key); w[0] = prelive(ROWS + [row]); w.append(evidence(key, row, side_over={"teardown": None}))
    fails(run(w), f"{PS[4]}:TEARDOWN_ABSENT")


# ---- re-audit: P2-1 (mutanti sopravvissuti) e NIT-2 ----------------------------------------------------------------
def test_sidecar_and_manifest_agreeing_against_the_prelive_row_fail(key):
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"result": "FAIL"}, man_over={"result": "FAIL"})
    fails(run(w), f"{R1}:RESULT_MISMATCH")


def test_missing_runner_file_key_fails(key):
    rf = {x: next(iter(v)) for x, v in V.ALLOWED_RUNNER_FILES.items()}; rf.pop("orch.py")
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"runner_files": rf})
    fails(run(w), f"{R1}:RUNNER_FILE_NOT_AUTHORIZED:orch.py")


def test_base_unit_with_trailing_garbage_fails(key):
    tb = BASE(); tb["units"] = [tb["units"][0] + "XYZ"]
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown_base": tb})
    fails(run(w), f"{R1}:TEARDOWN_BASE_NOT_VERIFIED")


def test_malformed_run_id_in_the_prelive_fails(key):
    w = world(key); w[0] = prelive(ROWS + [("pytest-x", "SIM", "PASS", "OK", "not-a-run-id")])
    fails(run(w), "pytest-x:RUN_ID_MALFORMED")


def test_github_env_on_a_non_ext_suite_is_not_exempt(key):
    w = world(key); w[0] = prelive(ROWS + [("pytest-x", "GITHUB_EXTERNAL_EVIDENCE", "PASS", "OK", PS[4])])
    fails(run(w), f"{PS[4]}:TD_CHECKS=0")


def test_exemption_set_is_exactly_the_three_pre_sandbox_reasons():
    assert sorted(V.PRE_SANDBOX_NOT_RUN) == PRE_SANDBOX_LITERAL


def test_a_pass_row_with_a_pre_sandbox_reason_and_null_teardown_is_fail(key):
    # l'esenzione dal teardown vale solo per NOT_RUN: un PASS senza teardown non passa mai, anche se tutto e' coerente
    row = ("pytest-x", "SIM", "PASS", "SELECTION_ABSENT_NEEDS_TESTS", PS[4])
    w = world(key); w[0] = prelive(ROWS + [row]); w.append(ps_evidence(key, row))
    fails(run(w), f"{PS[4]}:TEARDOWN_ABSENT")


# ---- re-audit 2 ------------------------------------------------------------------------------------------------------
def test_only_pre_sandbox_vps_runs_is_never_pass(key):
    # P2-A: un PRE-LIVE senza alcuna unit VPS avviata non e' verificato, anche se l'evidenza pre-sandbox e' coerente
    ext = [r for r in ROWS if r[1] == "GITHUB_EXTERNAL_EVIDENCE"]
    w = [prelive(ext + [PS]), ps_evidence(key)]
    fails(run(w), "PRELIVE_NO_VPS_RUNS")


def test_github_env_needs_the_exact_ext_dash_prefix(key):
    # NIT-A: "extfoo" non e' una suite ext-*: resta richiesta
    w = world(key); w[0] = prelive(ROWS + [("extfoo", "GITHUB_EXTERNAL_EVIDENCE", "PASS", "OK", PS[4])])
    fails(run(w), f"{PS[4]}:TD_CHECKS=0")


def test_only_ext_rows_with_published_evidence_is_never_pass(key):
    # una riga ext-* con evidenza pubblicata entra in required ma non e' un run VPS: niente PASS a vuoto
    ext = [r for r in ROWS if r[1] == "GITHUB_EXTERNAL_EVIDENCE"]
    w = [prelive(ext), evidence(key, ext[0])]
    fails(run(w), "PRELIVE_NO_VPS_RUNS")


# ---- revisione conclusiva ------------------------------------------------------------------------------------------
def test_probe_suites_are_exactly_the_catalog_probes():
    assert V.PROBE_SUITES == {"bf-readonly-api"}


def test_a_sandbox_suite_with_a_probe_teardown_fails(key):
    # P2-1: il tipo di unit e' legato alla suite: pytest-full-regression non puo' presentare un teardown da probe
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown": TD(R1, probe=True) | {"units": ["pf-probe-" + R1]}})
    fails(run(w), f"{R1}:TEARDOWN_UNIT_OR_CHECKS")


def test_the_probe_suite_with_a_sandbox_teardown_fails(key):
    w = world(key); w[2] = evidence(key, ROWS[1], side_over={"teardown": TD(R2) | {"units": ["pf-sbx-" + R2]}})
    fails(run(w), f"{R2}:TEARDOWN_UNIT_OR_CHECKS")


@pytest.mark.parametrize("uid", [0, -1])
def test_sandbox_uid_must_be_a_real_unprivileged_uid(key, uid):
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown": TD(R1) | {"uid": uid}})
    fails(run(w), f"{R1}:TEARDOWN_UNIT_OR_CHECKS")


def test_base_uid_zero_fails(key):
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown_base": BASE() | {"uid": 0}})
    fails(run(w), f"{R1}:TEARDOWN_BASE_NOT_VERIFIED")


def test_checks_failed_is_never_clean(key):
    w = world(key); w[1] = evidence(key, ROWS[0], side_over={"teardown": TD(R1) | {"checks_failed": []}})
    fails(run(w), f"{R1}:TEARDOWN_NOT_VERIFIED")


@pytest.mark.parametrize("row", [
    ("pytest-x", "LIVE_NOT_EXECUTED", "NOT_RUN", "REQUIRES_SEPARATE_GO_LIVE", PS[4]),
    ("pytest-x", "SIM", "NOT_RUN", "SELECTION_ABSENT_NEEDS_TESTS", PS[4]),
])
def test_an_exempt_vps_row_still_needs_a_verified_signature(key, row):
    # P2-2: l'esenzione poggia sul motivo del manifest: se il PRE-LIVE non l'ha verificato, niente PASS
    w = world(key)
    w[0] = prelive(ROWS + [row])
    w[0]["output"]["summary"] = (w[0]["output"]["summary"].replace(f"VERIFIED | {PS[4]}", f"INVALID | {PS[4]}")
                                 .replace(f'"verify":"VERIFIED","run_id":"{PS[4]}"', f'"verify":"INVALID","run_id":"{PS[4]}"'))
    fails(run(w), "pytest-x:PRELIVE_SIGNATURE_NOT_VERIFIED")


def test_ext_rows_are_not_judged_on_their_signature_here(key):
    # le righe ext-* sono evidenza GitHub, fuori dal teardown: il loro stato resta nella conclusione del PRE-LIVE
    rows = [r if r[0] != "ext-review" else r for r in ROWS]
    w = world(key); w[0] = prelive(rows)
    w[0]["output"]["summary"] = (w[0]["output"]["summary"].replace(f"VERIFIED | {R4}", f"UNVERIFIED | {R4}")
                                 .replace(f'"verify":"VERIFIED","run_id":"{R4}"', f'"verify":"UNVERIFIED","run_id":"{R4}"'))
    assert run(w)["verdict"] == "PASS"


def test_prelive_conclusion_is_reported(key):
    w = world(key); w[0]["conclusion"] = "action_required"
    assert run(w)["prelive_conclusion"] == "action_required"


def test_missing_runner_build_fails(key):
    w = world(key); s = {"runner_build": None}
    w[1] = evidence(key, ROWS[0], side_over=s)
    fails(run(w), f"{R1}:RUNNER_BUILD_NOT_AUTHORIZED")


@pytest.mark.parametrize("ext_row_runs", [True])
def test_ran_on_vps_ignores_ext_and_pre_sandbox_but_counts_real_runs(key, ext_row_runs):
    # sopravvissuti L164/L165: una riga VPS NOT_RUN dopo la sandbox (con evidenza) conta come run VPS reale
    nr = ("pytest-idempotency", "SIM", "NOT_RUN", "REQUIRED_REPRODUCTION_MISSING", "20261104T102603Z-7471d325")
    ext = [r for r in ROWS if r[1] == "GITHUB_EXTERNAL_EVIDENCE"]
    w = [prelive(ext + [nr]), evidence(key, nr)]
    assert run(w)["verdict"] == "PASS"
    # una riga GITHUB non ext- e' un run VPS: conta e richiede evidenza
    odd = ("pytest-y", "GITHUB_EXTERNAL_EVIDENCE", "PASS", "OK", "20261104T102603Z-22222222")
    w = [prelive(ext + [odd]), evidence(key, odd)]
    assert run(w)["verdict"] == "PASS"


@pytest.mark.parametrize("verdict,code", [("PASS", 0), ("FAIL", 1), ("INFRA_ERROR", 2)])
def test_cli_exit_codes(monkeypatch, capsys, verdict, code):
    import runpy
    monkeypatch.setattr(sys, "argv", ["v", "--repo", REPO, "--pr", str(PR), "--sha", SHA])
    import urllib.request
    def fake(*a, **k):
        raise ConnectionError("down")
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    src = pathlib.Path(V.__file__).read_text()
    src = src.replace("    res = verify(a.repo, a.pr, a.sha)", f"    res = {{'verdict': {verdict!r}}}")
    with pytest.raises(SystemExit) as e:
        exec(compile(src, "v", "exec"), {"__name__": "__main__"})
    assert e.value.code == code
