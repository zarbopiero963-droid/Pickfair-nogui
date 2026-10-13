"""Mutazione mirata della correzione v20 (CL20-PAR-MANUAL-001)."""
import subprocess, sys, pathlib
ROOT = pathlib.Path(sys.argv[1]); PY = sys.argv[2]
T = "tests/close02/test_close02_v20_parity_lanes.py"
R, D = "core/runtime_controller.py", "controllers/dutching_controller.py"
MUT = [
 ("P01 recovery ignorata", R, '        if getattr(self, "_startup_recovery_blocked", ""):\n            return f"startup_recovery_blocked:{self._startup_recovery_blocked}"', '        pass'),
 ("P02 config corrotta ignorata", R, '        if corrupt:\n            return "config_corrotta:" + ",".join(corrupt)', '        pass'),
 ("P03 loss/risk stop ignorati", R, '        reason = self._loss_block_reason() or self._exposure_stop(trigger=False)', '        reason = ""'),
 ("P04 drawdown 10 ignorato", R, '            if validators.reaches_limit(loss, dd_eur):\n                return "drawdown_hard_stop_active"', '            pass'),
 ("P05 drawdown con > invece di >=", R, '            if validators.reaches_limit(loss, dd_eur):\n                return "drawdown_hard_stop_active"', '            if loss > dd_eur:\n                return "drawdown_hard_stop_active"'),
 ("P06 equity non finita ammessa", R, '        if not (math.isfinite(bankroll) and math.isfinite(peak)):\n            return "equity_non_verificabile"', '        pass'),
 ("P07 gate manuale assente", D, '            blocco = self._runtime_block_reason()  # PROPOSTA close02 v20 (CL20)\n            if blocco:\n                return self._fail(f"Nuovo rischio bloccato ({blocco})")\n\n            if not self._bus_available():', '            if not self._bus_available():'),
 ("P08 gate precheck assente", D, '        blocco = self._runtime_block_reason()  # PROPOSTA close02 v20 (CL20)\n        if blocco:\n            return self._fail(f"Nuovo rischio bloccato ({blocco})")\n\n        try:', '        try:'),
 ("P09 gate che solleva ammette", D, '            return "gate_rischio_runtime_illeggibile"', '            return ""'),
 ("P10 esposizione solo tavoli", D, '        total_fn = getattr(self.runtime, "_open_exposure_total", None)', '        total_fn = None'),
 ("P11 cap assoluto precheck assente", D, '            if cap_reason:\n                return self._fail(f"Esposizione oltre il cap configurato ({cap_reason})",\n                                  batch_exposure', '            if False:\n                return self._fail(f"Esposizione oltre il cap configurato ({cap_reason})",\n                                  batch_exposure'),
 ("P12 emergency/daily ignorato", R, '        if self._daily_loss_entry_blocked():\n            return "emergency_stop_active"\n        reason = self._loss_block_reason()', '        reason = self._loss_block_reason()'),
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
