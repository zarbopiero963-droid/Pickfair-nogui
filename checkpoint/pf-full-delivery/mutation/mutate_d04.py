"""Mutation testing F026 v2 / F2 / F018 (+ regressione mutanti F001). Baseline obbligatoria.
Uso: python mutate_v2.py <tree> <python> <netguard> <out.json>"""
import json, os, shutil, subprocess, sys, tempfile
TREE, PY, NET, OUT = sys.argv[1:5]
RC = "core/runtime_controller.py"
MUTANTS = [
 ('M0_BASELINE', 'core/runtime_controller.py', None, None),
 ('MC8v_F028_never_called', 'core/runtime_controller.py', '                self._release_final_unmatched_orders()   # PF-ALL-F028, dopo i settlement del giro\n', '                pass\n'),
 ('MD1_F032_growth_ignores_uncertain', 'core/runtime_controller.py', '        if self._uncertain_reservations_map():\n            return None\n        return (float(account', '        return (float(account'),
 ('MD2_F032_growth_on_invalid_snapshot', 'core/runtime_controller.py', '        if baseline is None or baseline <= 0.0 or not account.get("ok"):', '        if baseline is None or baseline <= 0.0:'),
 ('MD3_F032_baseline_not_persisted', 'core/runtime_controller.py', '            self.db.save_settings({self._EXPANSION_KEY: json.dumps(state)})', '            pass'),
 ('MD4_MM_unknown_growth_expands', 'core/money_management.py', '            growth_pct = growth if growth is not None else 0.0', '            growth_pct = growth if growth is not None else 100.0'),
 ('MD5_MM_legacy_peak_growth', 'core/money_management.py', '            expansion_growth_given="expansion_growth_snapshot" in signal,', '            expansion_growth_given=False,'),
 ('MD6_F032_reset_cycle_no_rebase', 'core/runtime_controller.py', '        self._start_expansion_cycle("reset_cycle")   # F032', '        pass   # F032'),
 ('MD7_RSR12_not_restored', 'core/runtime_controller.py', '        self._restore_recovery_tables()                                # RSR-12, prima di F018\n', '        pass\n'),
 ('MD8_RSR12_not_persisted_on_settlement', 'core/runtime_controller.py', '        self._persist_recovery_tables()                                     # RSR-12\n', '        pass\n'),
 ('MD9_RSR12_unreadable_not_fail_closed', 'core/runtime_controller.py', '            self._recovery_state_unreadable = True\n', '            self._recovery_state_unreadable = False\n'),
 ('MD10_RSR12_signal_path_old_count', 'core/runtime_controller.py', '        active_recovery_count = self._active_recovery_count()   # RSR-12: anche i tavoli in recovery senza ordini', "        active_recovery_count = len([t for t in self.table_manager._tables.values() if getattr(t, 'in_recovery', False) and t.current_event_key])"),
 ('MD11_F032_first_start_no_baseline', 'core/runtime_controller.py', '            self._start_expansion_cycle("first_start", funds=funds)', '            pass'),
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
        comp = subprocess.run([PY, "-m", "py_compile", p], capture_output=True, text=True)
        if comp.returncode != 0:
            # mutante non valido: non compila. Non e' KILLED (mandato CD-04, lotto C).
            res.append({"mutant": name, "applied": True, "invalid": True, "error": comp.stderr[-300:]})
            print(name, "INVALID_DOES_NOT_COMPILE", flush=True); shutil.rmtree(d, ignore_errors=True); continue
    env = dict(os.environ, PYTHONPATH=NET, PF_PY=PY, PF_NETGUARD=NET, HOME=d, PF_TREE=tree)
    pr = subprocess.run([PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-o", "log_level=CRITICAL", "-n", "4", "tests/f001", "tests/f026", "tests/f029", "tests/roserpina41", "tests/acceptance/test_issue437_pr28c.py"],
                        cwd=tree, env=env, capture_output=True, text=True, timeout=1800)
    tail = pr.stdout.strip().splitlines()[-1] if pr.stdout.strip() else pr.stderr[-200:]
    failed = [l.split("::")[-1][:90] for l in pr.stdout.splitlines() if l.startswith("FAILED")]
    killed = pr.returncode != 0
    res.append({"mutant": name, "applied": True, "summary": tail, "killed": killed, "killing_tests": failed[:6]})
    shutil.rmtree(d, ignore_errors=True)
    print(name, ("BASELINE_FAIL" if killed else "BASELINE_PASS") if a is None else ("KILLED" if killed else "SURVIVED"), tail, flush=True)
json.dump(res, open(OUT, "w"), indent=1)
