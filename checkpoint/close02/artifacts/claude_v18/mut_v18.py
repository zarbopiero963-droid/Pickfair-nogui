"""Mutazione mirata della correzione v18 (CL17-RST-GHOST-001). Ogni mutante deve far fallire i test v18."""
import subprocess, sys, pathlib
ROOT = pathlib.Path(sys.argv[1]); PY = sys.argv[2]
T = "tests/close02/test_close02_v18_remote_exposure.py"
MUT = [
 ("G01 seed non chiamato", "core/runtime_controller.py", 'self.remote_exposure.seed(recovery_result.get("remote_active_orders") or [])', 'pass'),
 ("G02 nessun refresh all'ammissione", "core/runtime_controller.py", 'total_exposure = self._open_exposure_total(refresh=True)', 'total_exposure = self._open_exposure_total()'),
 ("G03 lettura fallita => rilascio", "core/runtime_controller.py", '            logger.warning("Esposizione remota: lettura correnti fallita, nessun rilascio (%s)", exc)\n            return', '            snapshot = []'),
 ("G04 somma senza netting", "core/remote_exposure.py", 'worst = max(worst, -pnl)', 'worst = worst + max(Decimal(0), -pnl)'),
 ("G05 arrotondamento non per eccesso", "core/remote_exposure.py", 'rounding=ROUND_CEILING', 'rounding="ROUND_HALF_EVEN"'),
 ("G06 non modellabile ignorato", "core/remote_exposure.py", '            if self._unmodeled:\n                return math.inf', '            pass'),
 ("G07 residuo sempre abbinato", "core/remote_exposure.py", 'pnl += Decimal(0) if wins else -r', 'pnl += r * (o["price"] - 1) if wins else -r'),
 ("G08 LAY al prezzo minimo", "core/remote_exposure.py", 'high = max(o["avg_price"], o["price"])', 'high = min(o["avg_price"], o["price"])'),
 ("G09 BACK al prezzo massimo", "core/remote_exposure.py", 'low = min(o["avg_price"], o["price"])', 'low = max(o["avg_price"], o["price"])'),
 ("G10 totale ignora infinito", "core/runtime_controller.py", '        if not (math.isfinite(tables) and math.isfinite(remote)):\n            return float("inf")', ''),
 ("G11 recovery senza istantanea", "core/state_recovery.py", '"remote_active_orders": list(startup.get("remote_orders") or []),', '"remote_active_orders": [],'),
 ("G12 rilascio fuori scope", "core/remote_exposure.py", '                if scope is not None and old["market_id"] not in scope:\n                    continue', ''),
 ("G13 ordini nuovi nel registro", "core/remote_exposure.py", '                fresh = by_id.get(bet_id)', '                fresh = by_id.get(bet_id)\n            for _bid, _r in by_id.items():\n                if _bid and _bid not in self._orders:\n                    self._add(_r)\n            for bet_id, old in list(self._orders.items()):\n                fresh = by_id.get(bet_id)'),
 ("G14 handicap ignorato", "core/remote_exposure.py", 'or (handicap is not None and (not handicap.is_finite() or handicap != 0))', 'or False'),
 ("G15 riga non modellabile in refresh rilasciata", "core/remote_exposure.py", '                if not bad:\n                    self._unmodeled.pop(key, None)', '                self._unmodeled.pop(key, None)'),
 ("G16 liability residuo LAY a 0", "core/remote_exposure.py", 'pnl += -r * (o["price"] - 1) if wins else Decimal(0)', 'pnl += Decimal(0)'),
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
