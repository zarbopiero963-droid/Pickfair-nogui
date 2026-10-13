"""Mutazione mirata della correzione v21 (CL21-RACE-ADMIT-001)."""
import subprocess, sys, pathlib
ROOT = pathlib.Path(sys.argv[1]); PY = sys.argv[2]
T = "tests/close02/test_close02_v21_admission_race.py"
R, D = "core/runtime_controller.py", "controllers/dutching_controller.py"
MUT = [
 ("A01 segnale senza lock", R, '        with self._admission_lock():\n            self._on_signal_received_serial(signal)', '        self._on_signal_received_serial(signal)'),
 ("A02 reload senza lock", R, '        with self._admission_lock():  # PROPOSTA close02 v21 (CL21)\n            self._reload_config_serial(reset_session=reset_session)', '        self._reload_config_serial(reset_session=reset_session)'),
 ("A03 auto-trade senza lock", R, '        with self._admission_lock():  # PROPOSTA close02 v21 (CL21)\n            return self._evaluate_and_maybe_submit_auto_next_trade_serial(', '        if True:\n            return self._evaluate_and_maybe_submit_auto_next_trade_serial('),
 ("A04 manuale senza lock", D, '        with self._admission_lock():\n            return self._manual_bet_serial(payload)', '        return self._manual_bet_serial(payload)'),
 ("A05 execute dutching senza lock", D, '        with self._admission_lock():  # PROPOSTA close02 v21 (CL21)\n            return self._execute_impl_serial(payload)', '        return self._execute_impl_serial(payload)'),
 ("A06 lock dutching sempre nullo", D, '        return lock if lock is not None and hasattr(lock, "__enter__") else contextlib.nullcontext()', '        return contextlib.nullcontext()'),
 ("A07 lock non rientrante condiviso per istanza", R, '        self._risk_admission_lock = threading.RLock()', '        self._risk_admission_lock = threading.RLock() if False else __import__("contextlib").nullcontext()'),
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
