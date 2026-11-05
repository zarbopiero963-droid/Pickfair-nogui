#!/usr/bin/env python3
"""PF-TEARDOWN-EVIDENCE-01 — verificatore lato GitHub (Claude B), fail-closed.

Parte dall'elenco COMPLETO delle esecuzioni dichiarate dal check PRE-LIVE `pf-betfair-certification` (pubblicato
dall'app attesa sullo SHA esatto), NON dall'elenco dei sidecar trovati. Per ogni esecuzione VPS richiesta:
  - esattamente UN check `pf-teardown-evidence/<scenario>` dell'app attesa con external_id == run_id (duplicati => FAIL);
  - sidecar e manifest principale estratti dal check, ciascuno con la propria firma Ed25519 verificata con la chiave
    pubblica FISSATA (mai una chiave fornita dal check);
  - sha256(manifest) == sidecar.manifest_sha256; run_id, sha, PR, scenario ed esito coerenti fra riga PRE-LIVE,
    manifest e sidecar; head_sha del check == SHA verificato;
  - runner_build e runner_files nell'elenco autorizzato;
  - teardown (e teardown_base, se presente) ben formato e pulito con ogni controllo dichiarato eseguito.
Limite dichiarato: la firma signature_ed25519_b64 del blocco pf-gate-decision/1 non e' verificata qui (la chiave del
classifier non e' fissata); la fiducia nella tabella poggia su slug + id dell'app, SHA e stato del check.
Esiti: PASS solo se tutto torna; FAIL con motivi; INFRA_ERROR se GitHub non e' leggibile (mai PASS).
Il PASS riguarda SOLO il teardown (e l'integrita' delle evidenze): non sostituisce la conclusione del check PRE-LIVE,
che e' riportata nell'output (`prelive_conclusion`) e va valutata a parte per il merge.
Uso:  verify_teardown_evidence.py --repo o/r --pr 608 --sha <40hex>   (exit 0 PASS, 1 FAIL, 2 INFRA_ERROR)
"""
import argparse, base64, hashlib, json, os, re, subprocess, sys, tempfile, urllib.request

PUBKEY_B64 = "MCowBQYDK2VwAyEAGOCShUVxqBgNHHwlwN4JTGfSVBemrL7WByXgNXNx8yI="   # SHA256:DLb/jf5wcDUQkabvv1KbGzWQRQbkMvaO6tMRYmmcGBw
APP_SLUG = "pickfair-dedicated"
APP_ID = 5579027                                    # id numerico dell'app (lo slug da solo puo' cambiare)
PRELIVE = "pf-betfair-certification"
TD_PREFIX = "pf-teardown-evidence/"
ALLOWED_BUILDS = {"pf-orch/1.4.3+teardown-evidence-1"}
ALLOWED_RUNNER_FILES = {   # sha256 dei file del runner autorizzati (build del kit); un altro valore => build non autorizzata
    "orch.py": {"92ac813ec78afd8c543ec63f43391e79ef7c2d601c6a8e9015da4eeebe3fc631"},
    "certd.py": {"fea7137af0174ce601c358d1bd7e95892ed1403734f869f6d816932a57ec9f65"},
    "tdpub.py": {"b7bd7d2151730ab691dae1fef561ebc74054629f3a4ff62f83739ff5a739ed31"},
}   # bundlelib.py: valore di produzione non noto offline, non fissato (dichiarato)
PRE_SANDBOX_NOT_RUN = {"REQUIRES_SEPARATE_GO_LIVE", "NEEDS_DATA_SUSPENDED_MARKETS",
                       "SELECTION_ABSENT_NEEDS_TESTS"}   # NOT_RUN VPS senza unit avviata; ogni altro NOT_RUN va verificato
CHECKS_SANDBOX = ["cgroup", "locks", "mount_unit", "privatetmp", "procs_uid", "sysvipc", "unit_state"]
CHECKS_PROBE = ["cgroup", "mount_unit", "privatetmp", "unit_state"]
PROBE_SUITES = {"bf-readonly-api"}   # le sole suite eseguite come probe VPS (orch.probe); ogni altra e' sandbox
HEADER = "| Suite | Env | Result | Reason | Dataset id | Dataset captured_to | Dataset sha256 | Signature | Manifest run |"
VPS_ENVS_EXCLUDED = {"GITHUB_EXTERNAL_EVIDENCE"}   # righe che non eseguono unit sulla VPS: nessun teardown
BASE_UNIT_RE = re.compile(r"^pf-sbx-base-[0-9a-f]{12}-[0-9a-f]{6}$")   # base di regressione (orch.regression_compare)
SHA_RE = re.compile(r"^[0-9a-f]{40}$"); RUN_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$")


def gh_json(path, fetch=None):
    if fetch:
        return fetch(path)
    req = urllib.request.Request("https://api.github.com" + path, headers={"Accept": "application/vnd.github+json",
                                                                            "User-Agent": "pf-verify-teardown"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def check_runs(repo, sha, fetch=None):
    out = []
    for page in range(1, 21):
        rows = gh_json(f"/repos/{repo}/commits/{sha}/check-runs?filter=all&per_page=100&page={page}", fetch).get("check_runs", [])
        out += rows
        if len(rows) < 100:
            return out
    raise RuntimeError("TOO_MANY_PAGES")


def ed25519_ok(raw, sig):
    pem = "-----BEGIN PUBLIC KEY-----\n" + PUBKEY_B64 + "\n-----END PUBLIC KEY-----\n"
    with tempfile.TemporaryDirectory() as d:
        for n, b in (("k.pem", pem.encode()), ("m", raw), ("s", sig)):
            open(os.path.join(d, n), "wb").write(b)
        r = subprocess.run(["openssl", "pkeyutl", "-verify", "-pubin", "-inkey", os.path.join(d, "k.pem"), "-rawin",
                            "-in", os.path.join(d, "m"), "-sigfile", os.path.join(d, "s")], capture_output=True)
        return r.returncode == 0


def prelive_rows(summary):
    """Tabella PRE-LIVE, parsing RIGOROSO: solo il blocco fra l'header esatto e la prima riga vuota; ogni riga deve avere
    esattamente 9 celle (altrimenti ValueError => FAIL). Mai una riga saltata in silenzio."""
    lines = summary.splitlines()
    idx = [i for i, l in enumerate(lines) if l.strip() == HEADER]
    if len(idx) != 1:
        raise ValueError("TABLE_HEADER")
    i = idx[0] + 1
    if i >= len(lines) or lines[i].replace(" ", "") != "|---" * 9 + "|":
        raise ValueError("TABLE_SEPARATOR")
    rows = []
    for line in lines[i + 1:]:
        if not line.strip():
            break
        if not (line.startswith("|") and line.rstrip().endswith("|")):
            raise ValueError("TABLE_ROW_SHAPE")
        cells = [c.strip() for c in line.strip()[1:-1].split("|")]
        if len(cells) != 9:
            raise ValueError("TABLE_ROW_CELLS")
        rows.append({"suite": cells[0], "env": cells[1], "result": cells[2], "reason": cells[3], "signature": cells[7], "run": cells[8]})
    if not rows:
        raise ValueError("TABLE_EMPTY")
    return rows


def decision(summary, pr, sha):
    """Blocco JSON pf-gate-decision/1 dello stesso summary: deve esserci UNA volta e combaciare con PR e SHA."""
    key = '{"schema":"pf-gate-decision/1"'
    if summary.count("pf-gate-decision/1") != 1 or summary.count(key) != 1:   # anche una seconda in forma non compatta
        raise ValueError("DECISION_COUNT")
    d, _end = json.JSONDecoder().raw_decode(summary, summary.index(key))
    if d.get("gate") != PRELIVE or str(d.get("pr")) != str(pr) or d.get("sha") != sha or not isinstance(d.get("suites"), list):
        raise ValueError("DECISION_IDENTITY")
    return d


def td_clean(td):
    return (isinstance(td, dict) and td.get("schema") == "pf-teardown/1" and td.get("clean") is True and "error" not in td
            and "checks_failed" not in td and td.get("residue_kinds") == [] and td.get("residue_count") == 0 and isinstance(td.get("checks"), list)
            and td["checks"] and td.get("check_status") == {c: "ok" for c in td["checks"]})


def field(summary, name):
    """Campo ancorato a inizio riga, ESATTAMENTE una occorrenza, base64 validato; altrimenti None (=> FAIL)."""
    ms = re.findall(r"^" + re.escape(name) + r": `([A-Za-z0-9+/=]+)`$", summary, re.M)
    if len(ms) != 1:
        return None
    try:
        return base64.b64decode(ms[0], validate=True)
    except (ValueError, TypeError):
        return None


def verify(repo, pr, sha, fetch=None):
    reasons = []
    if not SHA_RE.match(sha) or not str(pr).isdigit():
        return {"verdict": "FAIL", "reasons": ["INPUT"]}
    try:
        runs = check_runs(repo, sha, fetch)
    except Exception as e:  # noqa: BLE001
        return {"verdict": "INFRA_ERROR", "reasons": [f"GITHUB:{type(e).__name__}"]}
    def from_app(c):
        app = c.get("app") or {}
        return (app.get("slug") == APP_SLUG and app.get("id") == APP_ID and c.get("head_sha") == sha
                and c.get("status") == "completed")
    pre = [c for c in runs if c.get("name") == PRELIVE and from_app(c)]
    if len(pre) != 1:
        return {"verdict": "FAIL", "reasons": [f"PRELIVE_CHECKS={len(pre)}"]}
    pre = pre[0]
    if pre.get("external_id") != f"pr{pr}-{sha}":
        reasons.append("PRELIVE_EXTERNAL_ID")
    summary = (pre.get("output") or {}).get("summary") or ""
    try:
        rows = prelive_rows(summary)
        dec = decision(summary, pr, sha)
    except ValueError as e:
        return {"verdict": "FAIL", "reasons": [f"PRELIVE_{e}"]}
    # tabella == JSON su TUTTI i campi che il verificatore usa (audit di chiusura P2-1): reason decide needs(), la colonna
    # Signature decide PRELIVE_SIGNATURE_NOT_VERIFIED
    tab = sorted((r["suite"], r["result"], r["reason"], r["signature"], r["run"]) for r in rows)
    js = sorted((str(x.get("id")), str(x.get("result")), str(x.get("reason")), str(x.get("verify")), str(x.get("run_id")))
                for x in dec["suites"] if isinstance(x, dict))
    if tab != js or len(js) != len(dec["suites"]):
        return {"verdict": "FAIL", "reasons": ["PRELIVE_TABLE_DECISION_MISMATCH"]}
    td_checks = [c for c in runs if str(c.get("name", "")).startswith(TD_PREFIX)]
    published = {c.get("external_id") for c in td_checks}
    def needs(r):
        if r["env"] in VPS_ENVS_EXCLUDED and r["suite"].startswith("ext-"):   # Env non e' nel JSON: serve anche il prefisso
            return r["run"] in published                       # escluse, ma un'evidenza pubblicata si verifica comunque
        if r["result"] == "NOT_RUN" and r["reason"] in PRE_SANDBOX_NOT_RUN:
            return r["run"] in published
        return True                                            # ogni altro run VPS (anche NOT_RUN dopo la sandbox)
    required = [r for r in rows if needs(r)]
    for r in rows:   # re-audit finale P2-2: anche una riga VPS esente deve avere il manifest verificato dal PRE-LIVE
        if not (r["env"] in VPS_ENVS_EXCLUDED and r["suite"].startswith("ext-")) and r not in required \
                and r["signature"] != "VERIFIED":
            reasons.append(f"{r['suite']}:PRELIVE_SIGNATURE_NOT_VERIFIED")
    def ran_on_vps(r):   # re-audit 2 P2-A: almeno un run VPS che ha avviato una unit, mai un PASS a vuoto
        exempt = r["env"] in VPS_ENVS_EXCLUDED and r["suite"].startswith("ext-")
        pre_sandbox = r["result"] == "NOT_RUN" and r["reason"] in PRE_SANDBOX_NOT_RUN
        return not exempt and not pre_sandbox
    if not any(ran_on_vps(r) for r in required):
        reasons.append("PRELIVE_NO_VPS_RUNS")
    foreign = [c for c in td_checks if not from_app(c)]
    # Fail-closed voluto: chiunque possa creare check sul commit (anche un workflow) puo' far fallire la verifica (DoS,
    # mai un PASS falso). Si indaga l'origine; non si ignora mai un'evidenza con il nome giusto.
    if foreign:
        reasons.append(f"TD_CHECK_FROM_OTHER_SOURCE={len(foreign)}")          # nome giusto, provenienza sbagliata
    seen = set(); covered = []
    for row in required:
        rid = row["run"]
        if not RUN_RE.match(rid):
            reasons.append(f"{row['suite']}:RUN_ID_MALFORMED"); continue
        if rid in seen:
            reasons.append(f"{rid}:RUN_ID_DUPLICATED_IN_PRELIVE"); continue
        seen.add(rid)
        if row["signature"] != "VERIFIED":
            reasons.append(f"{rid}:PRELIVE_SIGNATURE_NOT_VERIFIED")
        mine = [c for c in td_checks if from_app(c) and c.get("external_id") == rid]
        if len(mine) != 1:
            reasons.append(f"{rid}:TD_CHECKS={len(mine)}"); continue
        try:
            bad = check_one(repo, pr, sha, row, mine[0])
        except Exception as e:  # noqa: BLE001 - qualunque forma inattesa => FAIL, mai traceback ne' PASS
            bad = [f"MALFORMED:{type(e).__name__}"]
        if bad:
            reasons += [f"{rid}:{b}" for b in bad]
        else:
            covered.append(rid)
    verdict = "PASS" if not reasons and covered and len(covered) == len(required) else "FAIL"
    return {"verdict": verdict, "reasons": reasons, "required": [r["run"] for r in required], "covered": covered,
            "prelive_conclusion": pre.get("conclusion")}   # informativo: il PASS copre solo il teardown


def is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


def td_kind_ok(td, rid, suite):
    """Tipo di unit ESATTO: sandbox = una sola unit pf-sbx-<rid>, uid intero, controlli CHECKS_SANDBOX; probe = una sola
    unit pf-probe-<rid>, uid null, controlli CHECKS_PROBE. Nessuna unit in piu', nessun match per sottostringa."""
    if suite not in PROBE_SUITES and td.get("units") == ["pf-sbx-" + rid]:   # re-audit finale P2-1: tipo legato alla suite
        return is_int(td.get("uid")) and td["uid"] > 0 and td.get("checks") == CHECKS_SANDBOX
    if suite in PROBE_SUITES and td.get("units") == ["pf-probe-" + rid]:
        return td.get("uid") is None and td.get("checks") == CHECKS_PROBE
    return False


def check_one(repo, pr, sha, row, c):
    rid = row["run"]
    if c.get("name") != TD_PREFIX + row["suite"]:
        return ["TD_CHECK_NAME"]
    s = (c.get("output") or {}).get("summary") or ""
    side, ssig, man, msig = (field(s, n) for n in ("sidecar_b64", "signature_b64", "manifest_b64", "manifest_signature_b64"))
    if not all((side, ssig, man, msig)):
        return ["TD_EVIDENCE_INCOMPLETE"]
    if not ed25519_ok(side, ssig):
        return ["SIDECAR_SIGNATURE_INVALID"]
    if not ed25519_ok(man, msig):
        return ["MANIFEST_SIGNATURE_INVALID"]
    sj, mj = json.loads(side), json.loads(man)
    if not isinstance(sj, dict) or not isinstance(mj, dict):
        return ["JSON_NOT_OBJECT"]
    bad = []
    if sj.get("schema") != "pf-teardown-sidecar/1": bad.append("SIDECAR_SCHEMA")
    if mj.get("schema") != "pf-cert-result/1": bad.append("MANIFEST_SCHEMA")      # un sidecar non vale mai come manifest
    if sj.get("manifest_sha256") != hashlib.sha256(man).hexdigest(): bad.append("BINDING_SHA256")
    for k, want in (("run_id", rid), ("sha", sha), ("target", str(pr)), ("scenario", row["suite"])):
        if str(sj.get(k)) != want: bad.append(f"SIDECAR_{k.upper()}")
        if str(mj.get(k)) != want: bad.append(f"MANIFEST_{k.upper()}")
    if not (sj.get("result") == mj.get("result") == row["result"]): bad.append("RESULT_MISMATCH")
    if sj.get("runner_build") not in ALLOWED_BUILDS: bad.append("RUNNER_BUILD_NOT_AUTHORIZED")
    rf = sj.get("runner_files") or {}
    for f, allowed in ALLOWED_RUNNER_FILES.items():
        if rf.get(f) not in allowed: bad.append(f"RUNNER_FILE_NOT_AUTHORIZED:{f}")
    if mj.get("repo") != repo: bad.append("MANIFEST_REPO")                   # orch li scrive sempre: assenti => FAIL
    if mj.get("environment") != row["env"]: bad.append("MANIFEST_ENVIRONMENT")
    td = sj.get("teardown")
    if row["result"] == "NOT_RUN" and row["reason"] in PRE_SANDBOX_NOT_RUN:
        # re-audit P1-1: nessuna unit avviata => il produttore pubblica teardown null. Si accetta SOLO se sidecar e
        # manifest firmati confermano lo stesso NOT_RUN pre-sandbox, senza teardown ne' base (firme/legame/campi sopra)
        if td is not None or "teardown_base" in sj: bad.append("PRE_SANDBOX_WITH_TEARDOWN")
        if not (sj.get("reason") == mj.get("reason") == row["reason"]): bad.append("PRE_SANDBOX_REASON_MISMATCH")
        return bad
    if td is None: bad.append("TEARDOWN_ABSENT")                     # nessuna unit (es. BLOCKED pre-sandbox): mai PASS
    elif not td_clean(td): bad.append("TEARDOWN_NOT_VERIFIED")
    elif not td_kind_ok(td, rid, row["suite"]): bad.append("TEARDOWN_UNIT_OR_CHECKS")
    if "teardown_base" in sj:
        tb = sj["teardown_base"]
        if not (td_clean(tb) and is_int(tb.get("uid")) and tb["uid"] > 0 and tb.get("checks") == CHECKS_SANDBOX
                and isinstance(tb.get("units"), list) and len(tb["units"]) == 1 and BASE_UNIT_RE.match(str(tb["units"][0]))):
            bad.append("TEARDOWN_BASE_NOT_VERIFIED")
    return bad


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--repo", required=True); ap.add_argument("--pr", required=True)
    ap.add_argument("--sha", required=True); a = ap.parse_args()
    res = verify(a.repo, a.pr, a.sha)
    print(json.dumps(res, indent=1, sort_keys=True))
    sys.exit(0 if res["verdict"] == "PASS" else (2 if res["verdict"] == "INFRA_ERROR" else 1))
