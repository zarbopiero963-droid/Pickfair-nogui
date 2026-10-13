"""Mutazione mirata della correzione v23 (CL25-CFG-VALID-001, CL24-CFG-CONFLICT-NORM-001)."""
import subprocess, sys, pathlib
ROOT = pathlib.Path(sys.argv[1]); PY = sys.argv[2]
T = "tests/close02/test_close02_v23_config_valid.py"
S = "services/setting_service.py"
MUT = [
 ("V01 validazione salvataggio assente", S, "        self._validate_roserpina_payload(payload)\n", ""),
 ("V02 negativi ammessi al salvataggio", S, "            if not math.isfinite(number) or number < 0:", "            if not math.isfinite(number):"),
 ("V03 non finiti ammessi al salvataggio", S, "            if not math.isfinite(number) or number < 0:", "            if number < 0:"),
 ("V04 testo ammesso al salvataggio", S, "            except (TypeError, ValueError):\n                bad.append(key)\n                continue", "            except (TypeError, ValueError):\n                continue"),
 ("V05 negativo float caricato valido", S, '        if value < 0 and key.startswith("roserpina."):\n            # PROPOSTA close02 v23', '        if False:\n            # PROPOSTA close02 v23'),
 ("V06 negativo int caricato valido", S, '        if value < 0 and key.startswith("roserpina."):  # PROPOSTA close02 v23 (CL25)', '        if False:'),
 ("V07 hard-stop negativo valido", S, "        if parsed < 0:\n", "        if False:\n"),
 ("V08 zero rifiutato", S, "            if not math.isfinite(number) or number < 0:", "            if not math.isfinite(number) or number <= 0:"),
 ("V09 confronto conflitti testuale", S, "        if str(a) == str(b):\n            return True\n        try:", "        return str(a) == str(b)\n        try:"),
 ("V10 conflitto mai rilevato", S, "                if conflicts:\n                    raise RuntimeError", "                if False:\n                    raise RuntimeError"),
 ("V11 negativo hard-stop non marcato", S, "            self._mark_corrupt(key if key in data else (fallback_key or key))\n", ""),
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
