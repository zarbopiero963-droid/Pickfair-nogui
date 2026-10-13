"""Mutazione mirata della correzione v24 (CL26-CASH-PARTIAL-001)."""
import subprocess, sys, pathlib
ROOT = pathlib.Path(sys.argv[1]); PY = sys.argv[2]
T = "tests/close02/test_close02_v24_cashout_partial.py"
E, H = "cashout_executor.py", "cashout_residual_handler.py"
MUT = [
 ("W01 parziale come successo", E, "        if self._cents(matched) < self._cents(requested):\n            remaining", "        if False:\n            remaining"),
 ("W02 confronto senza centesimi", E, "        if self._cents(matched) < self._cents(requested):\n            remaining", "        if matched < requested:\n            remaining"),
 ("W03 residuo non riportato", E, '            event["remaining"] = remaining  # PROPOSTA close02 v24 (CL26)', '            pass'),
 ("W04 ledger parziale come MATCHED", E, '            return "PARTIAL"\n        return "MATCHED"', '            return "MATCHED"\n        return "MATCHED"'),
 ("W05 severita' bassa", H, 'severity = "HIGH" if (status in {"ERROR", "FAILURE", "PARTIAL"}) else "INFO"', 'severity = "HIGH" if (status in {"ERROR", "FAILURE"}) else "INFO"'),
 ("W06 eseguito/residuo non persistiti", H, '            record["matched"] = payload.get("matched")\n            record["remaining"] = payload.get("remaining")', '            pass'),
 ("W07 stato sbagliato", E, '            self._fail("partial_fill", status="PARTIAL", payload=payload, bet_id=bet_id,', '            self._fail("partial_fill", status="FAILURE", payload=payload, bet_id=bet_id,'),
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
