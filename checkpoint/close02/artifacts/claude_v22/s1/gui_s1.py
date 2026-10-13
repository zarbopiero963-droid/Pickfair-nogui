"""close02 (Claude) — L6 S1 dal vivo (Linux, Xvfb): avvia la mini GUI reale (force_simulation=True, nessun
LIVE, nessuna rete), apre la scheda Roserpina, raccoglie testi delle etichette e valori dei campi, salva
screenshot. Fonte S1 (clausola derivata, non verbatim): «Mostrare nome, unità, stato e valore effettivo su
Windows e Linux». Windows: non disponibile => NOT VERIFIED."""
import json, os, subprocess, sys, time
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import tkinter as tk
import mini_gui

app = mini_gui.MiniPickfairGUI(force_simulation=True)
app.tabs.set("Roserpina")
for _ in range(30):
    app.update(); time.sleep(0.05)
labels, entries = [], []


def walk(w):
    for c in w.winfo_children():
        cls = c.__class__.__name__
        try:
            if cls in ("CTkLabel", "Label"):
                labels.append(str(c.cget("text")))
            elif cls in ("CTkCheckBox",):
                labels.append("[checkbox] " + str(c.cget("text")))
            elif cls in ("CTkEntry", "Entry"):
                entries.append(str(c.get()))
        except Exception:
            pass
        walk(c)


walk(app.tab_roserpina)
out_png = os.environ["SHOT"]
subprocess.run(["import", "-window", "root", out_png], check=False)
json.dump({"labels": labels, "entries": entries}, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False)
print(len(labels), "etichette,", len(entries), "campi")
app.destroy()
