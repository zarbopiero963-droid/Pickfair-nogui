"""BRIDGE-02 (preparazione offline): contratto unico pf-test-request/2. Nessuna rete, nessuna VPS."""
import copy
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "scripts" / "vps_bridge"))
import contract as C  # noqa: E402

REPO = "zarbopiero963-droid/Pickfair-nogui"
NOW = 1_800_000_000
CATALOG = {"schema": C.CATALOG_SCHEMA, "version": 2, "suites": [
    {"id": "core", "args": ["tests/core"], "environment": "SIM"},
    {"id": "e2e", "args": ["tests/e2e", "-q"], "environment": "OFFLINE"},
]}
TRUSTED = C.catalog_sha256(CATALOG)


def _req(**over):
    r = {"schema": C.SCHEMA, "repo": REPO, "pr": 607, "head_sha": "3" * 40, "lane": "development", "suites": ["core"],
         "catalog_sha256": TRUSTED, "nonce": "a" * 32, "requested_by": "zarbopiero963-droid",
         "created_at_epoch": NOW - 10, "run_id": 7, "run_attempt": 1}
    r.update(over)
    return r


def _verify(req, **kw):
    kw.setdefault("catalog", CATALOG)
    kw.setdefault("trusted_catalog_sha256", TRUSTED)
    kw.setdefault("seen_nonces", ())
    return C.verify_request(req, repo=REPO, now_epoch=NOW, **kw)


def test_a_valid_development_request_is_accepted():
    assert _verify(_req()) == ("development", ("core",))


def test_certification_needs_the_owner_go():
    with pytest.raises(C.Rejected) as e:
        _verify(_req(lane="certification"))
    assert e.value.reason == "CERTIFICATION_REQUIRES_OWNER_GO"
    assert _verify(_req(lane="certification"), owner_go_certification=True)[0] == "certification"


@pytest.mark.parametrize("lane", ["live", "LIVE", "prod", ""])
def test_there_is_no_live_lane(lane):
    with pytest.raises(C.Rejected) as e:
        _verify(_req(lane=lane), owner_go_certification=True)
    assert e.value.reason == "LANE"


@pytest.mark.parametrize("mutate,reason", [
    (lambda r: r.update(schema="pf-test-request/1"), "SCHEMA"),
    (lambda r: r.update(repo="someone/else"), "REPO"),
    (lambda r: r.update(pr=0), "PR"),
    (lambda r: r.update(pr=True), "PR"),
    (lambda r: r.update(head_sha="3" * 39), "HEAD_SHA"),
    (lambda r: r.update(nonce="z" * 32), "NONCE"),
    (lambda r: r.update(created_at_epoch=NOW - C.MAX_TTL_S - 1), "EXPIRED"),
    (lambda r: r.update(created_at_epoch=NOW + 120), "EXPIRED"),
    (lambda r: r.update(suites=["core; rm -rf /"]), "SUITE_UNKNOWN"),
    (lambda r: r.update(suites=["unknown-suite"]), "SUITE_UNKNOWN"),
    (lambda r: r.update(suites=[]), "SUITES"),
    (lambda r: r.update(suites=["core", "core"]), "SUITES_DUPLICATE"),
    (lambda r: r.update(catalog_sha256="0" * 64), "CATALOG_MISMATCH"),
    (lambda r: r.update(extra_args=["-k", "x"]), "FIELDS"),
    (lambda r: r.pop("nonce"), "FIELDS"),
    (lambda r: r.update(requested_by=""), "REQUESTER"),
    (lambda r: r.update(run_attempt=0), "RUN_ATTEMPT"),
])
def test_malformed_or_hostile_requests_are_rejected(mutate, reason):
    r = _req(); mutate(r)
    with pytest.raises(C.Rejected) as e:
        _verify(r)
    assert e.value.reason == reason


def test_a_replayed_nonce_is_rejected():
    with pytest.raises(C.Rejected) as e:
        _verify(_req(), seen_nonces=["a" * 32])
    assert e.value.reason == "REPLAY"


def test_a_tampered_catalog_is_rejected_even_if_the_request_matches_it():
    tampered = copy.deepcopy(CATALOG); tampered["suites"][0]["args"] = ["tests/core", "--evil"]
    with pytest.raises(C.Rejected) as e:
        _verify(_req(catalog_sha256=C.catalog_sha256(tampered)), catalog=tampered)
    assert e.value.reason == "CATALOG_MISMATCH"        # il catalogo fidato e' quello di main, non quello inviato


def test_an_unknown_trusted_catalog_is_fail_closed():
    with pytest.raises(C.Rejected) as e:
        _verify(_req(), trusted_catalog_sha256="")
    assert e.value.reason == "TRUSTED_CATALOG_UNKNOWN"


@pytest.mark.parametrize("bad,reason", [
    ({"schema": "x", "suites": []}, "CATALOG_SCHEMA"),
    ({"schema": C.CATALOG_SCHEMA, "suites": [{"id": "ab", "args": []}, {"id": "ab", "args": []}]}, "CATALOG_DUPLICATE_ID"),
    ({"schema": C.CATALOG_SCHEMA, "suites": [{"id": "xy", "args": [], "environment": "LIVE"}]}, "CATALOG_LIVE_SUITE"),
    ({"schema": C.CATALOG_SCHEMA, "suites": [{"id": "xy", "args": "tests/core"}]}, "CATALOG_ARGS"),
])
def test_malformed_catalogs_are_rejected(bad, reason):
    with pytest.raises(C.Rejected) as e:
        C.suite_index(bad)
    assert e.value.reason == reason


def test_idempotency_key_and_check_names_are_unique_and_stable():
    a = C.idempotency_key(_req(suites=["e2e", "core"]))
    assert a == C.idempotency_key(_req(suites=["core", "e2e"], nonce="b" * 32))   # il nonce non cambia l'identita'
    assert a != C.idempotency_key(_req(suites=["core"]))
    assert C.check_run_name("certification", "core") == "pf-test/core"
    assert C.check_run_name("development", "core") == "pf-dev-test/core"
    with pytest.raises(C.Rejected):
        C.check_run_name("live", "core")


def test_compat_report_never_invents_suites():
    vps = {"entries": [{"id": "pytest-suite-core", "selection": ["tests/core"]}]}
    rep = C.compat_report(CATALOG, vps)
    assert rep["verdict"] == "INCOMPATIBLE" and rep["missing"] == ["e2e"]
    vps["entries"].append({"id": "pytest-suite-e2e", "selection": ["tests/e2e"]})
    assert C.compat_report(CATALOG, vps)["verdict"] == "COMPATIBLE"


def test_a_declared_equivalence_counts_only_if_the_vps_id_exists():
    cat = {"schema": C.CATALOG_SCHEMA, "suites": [{"id": "mutation", "args": ["scripts/run_mutation_guardrails.py"]}]}
    assert C.compat_report(cat, {"entries": [{"id": "mutation-guardrails-catalog", "selection": []}]},
                           {"mutation": "mutation-guardrails-catalog"})["verdict"] == "COMPATIBLE"
    assert C.compat_report(cat, {"entries": []}, {"mutation": "mutation-guardrails-catalog"})["verdict"] == "INCOMPATIBLE"
