"""#311 §3.4: Wizard di prima configurazione — VISTA sottile (Toplevel a 5 step).

Tutta la logica vive in `wizard.py` (puro, testato in CI): qui solo widget e il
threading delle sonde (mai bloccare il main thread Tk; esito riportato via `after`).
Il wizard NON scrive mai la config da solo: alla fine consegna i valori al chiamante
(`on_finish`), che li applica passando dal percorso Salva ESISTENTE (gate inclusi).
Non attiva mai la modalità REALE (step 5 = checklist informativa).
"""

import threading

import customtkinter as ctk

from . import wizard

_W = 620          # larghezza contenuti/wraplength
_OK, _KO = "#66bb6a", "#ef5350"


class WizardWindow(ctk.CTkToplevel):
    """Finestra del wizard. Dipendenze INIETTATE dal chiamante (app):

    - `initial`: dict con i prefill (bot_token/chat_id/csv_path) dalla config viva;
    - `builder_factory`: (chat_id) -> ParserBuilder del parser ATTIVO per quella chat
      (la chat LIVE dello step 2, non uno snapshot), o None se assente;
    - `checklist_provider`: () -> lista (ok, label) per lo step 5 (wizard.final_checklist
      sulla config viva + parser attivo);
    - `on_finish(values)`: applica token/chat/csv al form e salva (gate esistenti).
    """

    _TITLES = ("1/5 · Token del bot", "2/5 · Chat sorgente", "3/5 · Parser sul messaggio reale",
               "4/5 · Percorso CSV", "5/5 · Checklist finale")

    def __init__(self, master=None, *, initial=None, builder_factory=None,
                 checklist_provider=None, on_finish=None):
        super().__init__(master)
        self.title("🧙 Wizard di prima configurazione")
        initial = initial if isinstance(initial, dict) else {}
        self._builder_factory = builder_factory
        self._checklist_provider = checklist_provider
        self._on_finish = on_finish
        self._step = 0
        self._passed = [False] * 5      # step superati (gate del pulsante Avanti)
        self._verified = {}             # step -> valore VERIFICATO (anti esito stantio)
        self._probe_running = False

        self._title_lbl = ctk.CTkLabel(self, text="", font=ctk.CTkFont(size=14, weight="bold"))
        self._title_lbl.pack(anchor="w", padx=14, pady=(12, 4))
        self._body = ctk.CTkFrame(self, fg_color="transparent")
        self._body.pack(fill="both", expand=True, padx=14, pady=4)
        self._result_lbl = ctk.CTkLabel(self, text="", anchor="w", justify="left",
                                        wraplength=_W, font=ctk.CTkFont(size=12))
        self._result_lbl.pack(fill="x", padx=14, pady=4)
        nav = ctk.CTkFrame(self, fg_color="transparent")
        nav.pack(fill="x", padx=14, pady=(4, 12))
        self._btn_back = ctk.CTkButton(nav, text="◀ Indietro", width=110,
                                       command=self._go_back)
        self._btn_back.pack(side="left")
        self._btn_next = ctk.CTkButton(nav, text="Avanti ▶", width=110,
                                       command=self._go_next)
        self._btn_next.pack(side="right")

        # Campi condivisi fra step (prefill dalla config viva).
        self._e_token = ctk.CTkEntry(self._body, width=_W, show="•")
        self._e_token.insert(0, str(initial.get("bot_token", "") or ""))
        self._e_chat = ctk.CTkEntry(self._body, width=260)
        self._e_chat.insert(0, str(initial.get("chat_id", "") or ""))
        self._e_csv = ctk.CTkEntry(self._body, width=_W)
        self._e_csv.insert(0, str(initial.get("csv_path", "") or ""))
        self._msg_box = ctk.CTkTextbox(self._body, width=_W, height=140)
        self._hint = ctk.CTkLabel(self._body, text="", anchor="w", justify="left",
                                  wraplength=_W, font=ctk.CTkFont(size=11),
                                  text_color="gray")
        self._action_btn = ctk.CTkButton(self._body, text="", width=220)
        self._extra_btn = ctk.CTkButton(self._body, text="", width=220)
        self._check_lbls = [ctk.CTkLabel(self._body, text="", anchor="w",
                                         justify="left", wraplength=_W) for _ in range(5)]
        self._render()

    # ── navigazione ─────────────────────────────────────────────────────────
    def _go_back(self):
        if self._step > 0:
            self._step -= 1
            self._render()

    def _go_next(self):
        if self._step >= 4:
            self._finish()
            return
        if not self._passed[self._step]:
            self._result_lbl.configure(
                text="⛔ Completa prima la verifica di questo step.", text_color=_KO)
            return
        if self._verified.get(self._step) != self._step_snapshot(self._step):
            # Valore MODIFICATO dopo il ✅ (CodeRabbit #354): l'esito è stantio —
            # mai avanzare (e quindi mai salvare) su un valore mai verificato.
            self._passed[self._step] = False
            self._result_lbl.configure(
                text="✏️ Valore modificato dopo la verifica: ripeti la verifica.",
                text_color=_KO)
            return
        self._step += 1
        self._render()

    def _step_snapshot(self, step):
        """Valore corrente rilevante per lo step (confrontato col valore VERIFICATO):
        copre qualsiasi via di edit — tastiera, incolla dal menu, modifica
        programmatica — senza dipendere dai binding Tk. Lo step chat dipende anche
        dal token (chat verificata con un token poi cambiato → da riverificare); lo
        step parser dipende anche dalla chat (il parser attivo è risolto per-chat)."""
        if step == 0:
            return self._e_token.get()
        if step == 1:
            return (self._e_token.get(), self._e_chat.get())
        if step == 2:
            return (self._e_chat.get(), self._msg_box.get("1.0", "end"))
        if step == 3:
            return self._e_csv.get()
        return None

    def _render(self):
        """Mostra i widget dello step corrente (gli altri sono nascosti)."""
        for w in (self._e_token, self._e_chat, self._e_csv, self._msg_box,
                  self._hint, self._action_btn, self._extra_btn, *self._check_lbls):
            w.pack_forget()
        self._title_lbl.configure(text=self._TITLES[self._step])
        self._result_lbl.configure(text="", text_color="gray")
        self._btn_back.configure(state="normal" if self._step else "disabled")
        self._btn_next.configure(text="Fine ✔" if self._step == 4 else "Avanti ▶")
        step = self._step
        if step == 0:
            self._hint.configure(text="Incolla il token del bot creato con @BotFather, "
                                      "poi premi il test. Il token non compare mai nei log.")
            self._hint.pack(anchor="w", pady=(0, 4))
            self._e_token.pack(anchor="w", pady=4)
            self._action_btn.configure(text="🔌 Prova connessione (getMe)",
                                       command=self._run_token_probe)
            self._action_btn.pack(anchor="w", pady=6)
        elif step == 1:
            self._hint.configure(text="Aggiungi il bot come ADMIN alla chat/canale, invia "
                                      "un messaggio di prova, inserisci il Chat ID e premi "
                                      "«Controlla ora». (Listener fermo: altrimenti consuma "
                                      "lui gli update.)")
            self._hint.pack(anchor="w", pady=(0, 4))
            self._e_chat.pack(anchor="w", pady=4)
            self._action_btn.configure(text="📡 Controlla ora", command=self._run_chat_probe)
            self._action_btn.pack(anchor="w", pady=6)
        elif step == 2:
            self._hint.configure(text="Incolla un messaggio segnale REALE del canale: lo "
                                      "valuto col Parser Personalizzato ATTIVO (configuralo "
                                      "prima nella scheda 🧩 Parser se manca).")
            self._hint.pack(anchor="w", pady=(0, 4))
            self._msg_box.pack(anchor="w", pady=4)
            self._action_btn.configure(text="🧪 Valuta messaggio", command=self._run_parser_check)
            self._action_btn.pack(anchor="w", pady=6)
        elif step == 3:
            self._hint.configure(text="Percorso del CSV letto da XTrader (identico nella "
                                      "sorgente segnali di XTrader). La scrittura di prova "
                                      "crea SOLO l'header e non tocca mai un CSV operativo.")
            self._hint.pack(anchor="w", pady=(0, 4))
            self._e_csv.pack(anchor="w", pady=4)
            self._action_btn.configure(text="🔎 Verifica percorso",
                                       command=lambda: self._run_csv_check(False))
            self._action_btn.pack(anchor="w", pady=(6, 2))
            self._extra_btn.configure(text="📄 Scrivi CSV di prova",
                                      command=lambda: self._run_csv_check(True))
            self._extra_btn.pack(anchor="w", pady=2)
        else:
            self._render_checklist()

    # ── sonde in thread (mai bloccare Tk) ───────────────────────────────────
    def _run_async(self, fn, on_done):
        if self._probe_running:
            return
        self._probe_running = True
        self._result_lbl.configure(text="⏳ Verifica in corso…", text_color="gray")

        def worker():
            # L'esito va SEMPRE riconsegnato al main thread (review Fable #354):
            # se la sonda solleva e il thread muore in silenzio, `_probe_running`
            # resterebbe True per sempre → tutte le sonde bloccate su ⏳ eterna.
            try:
                res = fn()
            except Exception as ex:   # noqa: BLE001 — fail-closed: SOLO la classe dell'errore (mai token/URL grezzi)
                res = wizard.StepResult(
                    False, f"Verifica fallita: errore imprevisto ({type(ex).__name__}).")
            try:
                self.after(0, lambda: self._probe_done(res, on_done))
            except Exception:   # noqa: BLE001 — finestra/Tk distrutti durante la sonda: niente da aggiornare
                pass
        threading.Thread(target=worker, daemon=True).start()

    def _probe_done(self, res, on_done):
        """Esito sonda nel main thread Tk. La finestra può essere stata CHIUSA mentre
        la sonda era in corso (timeout 10s): l'`after` pende sull'interprete, non sul
        widget, quindi qui va verificato che la finestra esista ancora (Fable #354)."""
        self._probe_running = False
        try:
            alive = bool(self.winfo_exists())
        except Exception:   # noqa: BLE001 — interprete Tk già smontato: come finestra chiusa
            alive = False
        if alive:
            on_done(res)

    def _show(self, step_idx, res, snapshot=None):
        """Registra l'esito; su ✅ memorizza il valore SONDATO (`snapshot`, catturato
        all'avvio della sonda: un edit durante i 10s di probe non può spacciarsi per
        verificato — CodeRabbit #354)."""
        self._passed[step_idx] = bool(res.ok)
        if res.ok:
            self._verified[step_idx] = snapshot
        self._result_lbl.configure(text=("✅ " if res.ok else "⛔ ") + res.message,
                                   text_color=_OK if res.ok else _KO)

    def _run_token_probe(self):
        token = self._e_token.get()
        self._run_async(lambda: wizard.check_token(token),
                        lambda res: self._show(0, res, snapshot=token))

    def _run_chat_probe(self):
        token, chat = self._e_token.get(), self._e_chat.get()
        self._run_async(lambda: wizard.check_chat(token, chat),
                        lambda res: self._show(1, res, snapshot=(token, chat)))

    def _run_parser_check(self):
        chat = self._e_chat.get()
        builder = (self._builder_factory(chat.strip())
                   if self._builder_factory else None)
        if builder is None:
            self._show(2, wizard.StepResult(
                False, "Nessun Parser Personalizzato attivo: configuralo nella "
                       "scheda 🧩 Parser e riapri il wizard."))
            return
        text = self._msg_box.get("1.0", "end")
        self._show(2, wizard.check_parser(builder, text), snapshot=(chat, text))

    def _run_csv_check(self, do_write):
        path = self._e_csv.get()
        self._run_async(lambda: wizard.check_csv(path, do_write=do_write),
                        lambda res: self._show(3, res, snapshot=path))

    def _render_checklist(self):
        items = self._checklist_provider() if self._checklist_provider else []
        for lbl, (ok, text) in zip(self._check_lbls, items):
            lbl.configure(text=("✅ " if ok else "⛔ ") + text,
                          text_color=_OK if ok else _KO)
            lbl.pack(anchor="w", pady=2)
        self._hint.configure(text="La checklist è informativa: il wizard NON attiva la "
                                  "modalità Reale (si passa dai gate della tab 🛡️ Sicurezza). "
                                  "Premi «Fine ✔» per salvare token/chat/CSV nella config.")
        self._hint.pack(anchor="w", pady=(8, 0))
        self._passed[4] = True

    def _finish(self):
        if self._on_finish is not None:
            self._on_finish({"bot_token": self._e_token.get().strip(),
                             "chat_id": self._e_chat.get().strip(),
                             "csv_path": self._e_csv.get().strip()})
        self.destroy()
