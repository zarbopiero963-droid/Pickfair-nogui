"""Mutazione mirata della correzione v22 (CL22-AUDIT-DECISION-001)."""
import subprocess, sys, pathlib
ROOT = pathlib.Path(sys.argv[1]); PY = sys.argv[2]
T = "tests/close02/test_close02_v22_risk_decision_audit.py"
R, D = "core/runtime_controller.py", "controllers/dutching_controller.py"
MUT = [
 ("U01 rifiuto non registrato", R, '        self._record_risk_decision("REJECTED", reason, signal)\n        self.bus.publish(', '        self.bus.publish('),
 ("U02 ammissione non registrata", R, '        self._record_risk_decision("APPROVED", decision.reason or "approved", signal,\n                                   stake=decision.recommended_stake)  # PROPOSTA close02 v22\n', ''),
 ("U03 niente persistenza", R, '            self.db.insert_audit_event(record)', '            pass'),
 ("U04 limiti non registrati", R, '            "limits": {k: getattr(getattr(self, "config", None), k, None) for k in self._RISK_DECISION_LIMITS},', '            "limits": {},'),
 ("U05 versione non registrata", R, '            "config_version": self._config_version(),', '            "config_version": None,'),
 ("U06 segnale intero nel record", R, '        for k in self._RISK_DECISION_FIELDS:\n            record[k] = signal.get(k)', '        record.update(signal)'),
 ("U07 get_status senza decisioni", R, '        data["risk_decisions"] = dict(', '        data["_nascosto"] = dict('),
 ("U08 fallimenti non contati", R, '            state["persist_failures"] += 1', '            pass'),
 ("U09 corsia GUI non registrata", D, '                self._record_block(blocco, payload, "gui_manual")\n', ''),
 ("U10 origine sbagliata", R, '            origin = "copy" if signal.get("copy_meta") else ("pattern" if signal.get("pattern_meta") else "signal")', '            origin = "gui_manual"'),
 ("U11 ultimo rifiuto non aggiornato", R, '            state["last_rejection"] = {"reason": record["reason"], "origin": origin, "ts": record["ts"]}', '            pass'),
]
res = []
for name, f, old, new in MUT:
    p = ROOT / f; src = p.read_text()
    if src.count(old) != 1:
        res.append((name, "NON_APPLICABILE")); continue
    p.write_text(src.replace(old, new))
    try:
        r = subprocess.run([PY, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", T], cwd=ROOT, capture_output=True, text=True, timeout=300)
        res.append((name, "UCCISO" if r.returncode != 0 else "SOPRAVVISSUTO"))
    finally:
        p.write_text(src)
for n, s in res: print(f"{s:16} {n}")
print("uccisi", sum(s == "UCCISO" for _, s in res), "/", len(res))
