"""Mutation testing F026 v2 / F2 / F018 (+ regressione mutanti F001). Baseline obbligatoria.
Uso: python mutate_v2.py <tree> <python> <netguard> <out.json>"""
import json, os, shutil, subprocess, sys, tempfile
TREE, PY, NET, OUT = sys.argv[1:5]
RC = "core/runtime_controller.py"
MUTANTS = [
 ("M0_BASELINE", RC, None, None),
 ("MV1_autoreset_on_available_drawdown", RC, "        cash_drawdown = self._cash_drawdown_pct()\n        if cash_drawdown is None:\n            return False",
  "        cash_drawdown = available_drawdown\n        if cash_drawdown is None:\n            return False"),
 ("MV2_autoreset_when_cash_unknown", RC, "        if cash_drawdown is None:\n            return False", "        if cash_drawdown is None:\n            cash_drawdown = float(\"inf\")"),
 ("MV3_reset_ignores_settlement", RC, "            if event_key and market_id not in settled:", "            if False and event_key and market_id not in settled:"),
 ("MV4_reset_frees_live_keys", RC, "                keep_keys.add(event_key)\n", "                pass\n"),
 ("MV5_settlement_not_marked", RC, "        self._mark_market_settled((payload or {}).get(\"market_id\"))\n", "        pass\n"),
 ("MV6_F2_no_release_without_table_id", RC, "        market_id = str((payload or {}).get(\"market_id\") or \"\").strip()\n        if not market_id:\n            return\n        uncertain_keys",
  "        market_id = \"\"\n        if not market_id:\n            return\n        uncertain_keys"),
 ("MV7_F2_releases_uncertain", RC, "        certain = [t for t in matches if str(t.get(\"current_event_key\")) not in uncertain_keys]", "        certain = list(matches)"),
 ("MV8_F2_releases_ambiguous_multi", RC, "        if len(certain) == 1 and len(matches) == 1:", "        if certain:"),
 ("MV9_account_missing_exposure_is_zero", RC, "        if funds.get(\"available\") is None or funds.get(\"exposure\") is None:", "        if funds.get(\"available\") is None:"),
 ("MV10_R1_bankroll_synced_to_cash", RC, "        self.risk_desk.sync_bankroll(available_f)\n        self._processed_bankroll_sync_keys.add(settlement_key)",
  "        self.risk_desk.sync_bankroll(available_f + abs(float(funds.get('exposure') or 0.0)))\n        self._processed_bankroll_sync_keys.add(settlement_key)"),
 ("MV11_F018_rebuild_disabled", RC, "        result[\"unattributed\"] = unattributed\n", "        result[\"unattributed\"] = unattributed\n        return result\n"),
 ("MV12_F018_exchange_failure_ok", RC, "            result.update(ok=False, reason=f\"EXCHANGE_ORDERS_UNAVAILABLE:{type(exc).__name__}\")",
  "            pass"),
 ("MV13_F018_absent_order_kept_live", RC, "                else:\n                    result[\"closed\"].append(c[\"customer_ref\"])\n                    continue",
  "                else:\n                    is_uncertain = False"),
 ("MV14_F018_start_active_anyway", RC, "        if self._reconciliation_required:\n            self.mode = RuntimeMode.PAUSED", "        if False:\n            self.mode = RuntimeMode.PAUSED"),
 ("MV15_F018_resume_bypass", RC, "        if getattr(self, \"_reconciliation_required\", False):", "        if False:"),
 ("MV16_F018_uncertain_not_marked", RC, "                uncertain[c[\"event_key\"]] = table_id\n", "                pass\n"),
 ("MV17_cash_peak_not_rebased", RC, "            self._cash_equity_peak = float(account[\"cash_equity\"])   # il reset ribasa il picco (OC-04)", "            pass"),
 ("MF29a_recovery_table_reassigned_while_occupied", "core/table_manager.py", "                    and not t.current_event_key\n", ""),
 ("MF29b_recovery_beyond_table_count", "core/table_manager.py", "                    and t.table_id <= self.table_count\n                ]", "                ]"),
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
    pr = subprocess.run([PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-o", "log_level=CRITICAL", "-n", "4", "tests/f001", "tests/f026", "tests/f029"],
                        cwd=tree, env=env, capture_output=True, text=True, timeout=1800)
    tail = pr.stdout.strip().splitlines()[-1] if pr.stdout.strip() else pr.stderr[-200:]
    failed = [l.split("::")[-1][:90] for l in pr.stdout.splitlines() if l.startswith("FAILED")]
    killed = pr.returncode != 0
    res.append({"mutant": name, "applied": True, "summary": tail, "killed": killed, "killing_tests": failed[:6]})
    shutil.rmtree(d, ignore_errors=True)
    print(name, ("BASELINE_FAIL" if killed else "BASELINE_PASS") if a is None else ("KILLED" if killed else "SURVIVED"), tail, flush=True)
json.dump(res, open(OUT, "w"), indent=1)
