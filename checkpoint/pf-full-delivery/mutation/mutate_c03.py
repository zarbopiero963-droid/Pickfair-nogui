"""Mutation testing F026 v2 / F2 / F018 (+ regressione mutanti F001). Baseline obbligatoria.
Uso: python mutate_v2.py <tree> <python> <netguard> <out.json>"""
import json, os, shutil, subprocess, sys, tempfile
TREE, PY, NET, OUT = sys.argv[1:5]
RC = "core/runtime_controller.py"
MUTANTS = [
 ('M0_BASELINE', 'core/runtime_controller.py', None, None),
 ('MC1_F027_poll_only_active', 'core/runtime_controller.py', '(RuntimeMode.ACTIVE, RuntimeMode.PAUSED, RuntimeMode.LOCKDOWN)', '(RuntimeMode.ACTIVE,)'),
 ('MC2_F027_poll_when_stopped', 'core/runtime_controller.py', '(RuntimeMode.ACTIVE, RuntimeMode.PAUSED, RuntimeMode.LOCKDOWN)', '(RuntimeMode.ACTIVE, RuntimeMode.PAUSED, RuntimeMode.LOCKDOWN, RuntimeMode.STOPPED)'),
 ('MC3_F027_auto_next_outside_active', 'core/runtime_controller.py', 'return "" if mode is RuntimeMode.ACTIVE else f"runtime_not_active:{getattr(mode, \'value\', mode)}"', 'return ""'),
 ('MC4_F027_live_emergency_reads_sim', 'core/runtime_controller.py', 'if getattr(self, "_settlement_source_before_emergency", "") == "LIVE":', 'if False:'),
 ('MC5_F028_releases_uncertain', 'core/runtime_controller.py', 'c["status"] == "SUBMITTED" and c["bet_id"]', 'c["status"] in ("SUBMITTED", "AMBIGUOUS", "INFLIGHT") and c["bet_id"]'),
 ('MC6_F028_sim_ignores_matched', 'core/runtime_controller.py', '                        and float(o.get("sizeMatched") or 0.0) == 0.0}', '}'),
 ('MC7_F028_live_ignores_settled', 'core/runtime_controller.py', 'if bid and bid not in settled:', 'if bid:'),
 ('MC8_F028_never_called', 'core/runtime_controller.py', 'self._release_final_unmatched_orders()   # PF-ALL-F028', 'pass'),
 ('MC9_F031_sim_partial_cancelled_dropped', 'simulation_broker.py', 'if status == "CANCELLED" and float(o.get("sizeMatched") or 0.0) > 0.0:', 'if False:'),
 ('MC10_F030_not_attached', 'core/runtime_controller.py', '        self._attach_single_entry_liquidity(payload)\n', ''),
 ('MC11_F030_unknown_allowed', 'core/risk_gate.py', 'if payload.get("liquidity_required") is True:', 'if False:'),
 ('MC12_F030_side_swapped', 'core/liquidity.py', 'ladder_key = "availableToBack" if str(side).upper() == "BACK" else "availableToLay"', 'ladder_key = "availableToLay" if str(side).upper() == "BACK" else "availableToBack"'),
 ('MC13_F030_crosses_inverted', 'core/liquidity.py', '        return book_price >= order_price', '        return book_price <= order_price'),
 ('MC14_F030_runner_status_ignored', 'core/liquidity.py', 'return None, "LIQUIDITY_RUNNER_NOT_ACTIVE"', 'pass'),
 ('MC15_RS_candidate_triggers_stop', 'core/runtime_controller.py', 'real_reason = stop_rules.exposure_stop_reason(limit, max(0.0, total - candidate)) if candidate > 0.0 \\\n            else reason', 'real_reason = reason'),
 ('MC16_RS_real_at_limit_no_stop', 'core/runtime_controller.py', '            if trigger:\n                self.risk_stop(real_reason)  # pre-submit: violazione effettiva', '            if False:\n                self.risk_stop(real_reason)  # pre-submit: violazione effettiva'),
 ('MC17_F030_auto_next_not_attached', 'core/runtime_controller.py', "        self._attach_single_entry_liquidity(submit_payload)   # PF-ALL-F030: anche l'ingresso del ciclo\n", ''),
]
res = []
for name, rel, a, b in MUTANTS:
    d = tempfile.mkdtemp(prefix="mutv2_"); tree = os.path.join(d, "t")
    shutil.copytree(TREE, tree, ignore=shutil.ignore_patterns(".git", "__pycache__"))
    p = os.path.join(tree, rel); s = open(p).read()
    if a is not None:
        if s.count(a) != 1:
            res.append({"mutant": name, "applied": False, "count": s.count(a)}); print(name, "NOT_APPLIED", s.count(a), flush=True); continue
        open(p, "w").write(s.replace(a, b))
    env = dict(os.environ, PYTHONPATH=NET, PF_PY=PY, PF_NETGUARD=NET, HOME=d, PF_TREE=tree)
    pr = subprocess.run([PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-o", "log_level=CRITICAL", "-n", "4", "tests/f001", "tests/f026", "tests/f029", "tests/acceptance/test_issue437_pr28c.py"],
                        cwd=tree, env=env, capture_output=True, text=True, timeout=1800)
    tail = pr.stdout.strip().splitlines()[-1] if pr.stdout.strip() else pr.stderr[-200:]
    failed = [l.split("::")[-1][:90] for l in pr.stdout.splitlines() if l.startswith("FAILED")]
    killed = pr.returncode != 0
    res.append({"mutant": name, "applied": True, "summary": tail, "killed": killed, "killing_tests": failed[:6]})
    shutil.rmtree(d, ignore_errors=True)
    print(name, ("BASELINE_FAIL" if killed else "BASELINE_PASS") if a is None else ("KILLED" if killed else "SURVIVED"), tail, flush=True)
json.dump(res, open(OUT, "w"), indent=1)
