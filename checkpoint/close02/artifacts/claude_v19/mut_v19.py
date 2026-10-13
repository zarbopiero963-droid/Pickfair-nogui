"""Mutazione mirata della correzione v19 (CL18-CASH-LEDGER-001, CL19-CASH-REF-001)."""
import subprocess, sys, pathlib
ROOT = pathlib.Path(sys.argv[1]); PY = sys.argv[2]
T = "tests/close02/test_close02_v19_cashout_ledger.py"
F = "cashout_executor.py"
MUT = [
 ("V01 ledger mai scritto", F, "        if self.ledger is not None:\n            try:\n                ledger_id", "        if False:\n            try:\n                ledger_id"),
 ("V02 scrittura fallita ignorata", F, '                self._fail(f"ledger_write_failed:{exc}", status="REJECTED", payload=payload)\n                return', '                pass'),
 ("V03 esito non registrato", F, "        self._record_outcome(ledger_id, self._ledger_status(result), result)", "        pass"),
 ("V04 ambiguo come fallito", F, '        if result.get("order_unknown"):\n            return "AMBIGUOUS"', '        if result.get("order_unknown"):\n            return "FAILED"'),
 ("V05 eccezione non registrata", F, '            self._record_outcome(ledger_id, "AMBIGUOUS", None, reason=f"place_exception:{exc}")', '            pass'),
 ("V06 abbinato come PLACED", F, 'return "MATCHED" if matched > 0.0 else "PLACED"', 'return "PLACED"'),
 ("V07 ref = source", F, '            "customer_ref": derive_customer_ref("co", {', '            "customer_ref": str(payload.get("source", "") or "") or derive_customer_ref("co", {'),
 ("V08 ref senza mercato", F, '"market_id": str(payload["market_id"]), "selection_id"', '"selection_id"'),
 ("V09 wiring senza ledger", "cashout_wiring.py", "safety_layer=SafetyLayer(), ledger=ledger)", "safety_layer=SafetyLayer(), ledger=None)"),
 ("V10 risposta non salvata", F, '"response": dict(result) if isinstance(result, dict) else None,', '"response": None,'),
 ("V11 headless senza ledger", "headless_main.py", "            ledger=self.db,  # PROPOSTA close02 v19 (CL18-CASH-LEDGER-001)\n", ""),
]
res = []
for name, f, old, new in MUT:
    p = ROOT / f; src = p.read_text()
    if src.count(old) != 1:
        res.append((name, "NON_APPLICABILE")); continue
    p.write_text(src.replace(old, new))
    try:
        r = subprocess.run([PY, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", T], cwd=ROOT, capture_output=True, text=True)
        res.append((name, "UCCISO" if r.returncode != 0 else "SOPRAVVISSUTO"))
    finally:
        p.write_text(src)
for n, s in res: print(f"{s:16} {n}")
print("uccisi", sum(s == "UCCISO" for _, s in res), "/", len(res))
