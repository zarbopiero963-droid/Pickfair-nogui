# close02 — Finding F1..F6: PROPOSTE (non risolti)
Base: main_ro 028cee9074c3ab6c2a8014449a1897ae8faebc93, worktree isolato close02/wt, nessun push.
Stato di ogni finding: APERTO — patch = proposta; resta aperto fino alla sequenza autorizzata #505.

## Esecuzioni
- RED v1 (red_run.txt): 10 failed / 4 passed. ERRATA: i 3 test F3 in LIVE venivano rifiutati dal deploy gate (NO-GO) prima del controllo drawdown → RED F3 per motivo sbagliato e test "sotto 10 €" vacuo. Harness corretto (stub test-only di enforce_deploy_gate + asserzione di non vacuità).
- RED v2 su codice 028cee9 (red_run_v2.txt): 10 failed / 4 passed, tutti per il motivo atteso (0 deploy_gate_no_go).
- GREEN con patch (green_run.txt): 14/14 passed.
- Regressione suite completa (-o addopts=""): baseline 028cee9 = 1 failed / 7253 passed / 14 skipped / 1 xfailed (pr04_quater tkinter, preesistente); con patch = 20 failed / 7248 passed (19 nuovi):
  - 13: doppi di test DB senza `transaction()` (F6 usa db.transaction) — impatto harness, da aggiornare i doppi;
  - 3: test_issue437_pr28 tolleranza 1e-9 (#393 H-14) — conflitto con Q3 "nessuna tolleranza"; 0.1+0.2 float ora supera 0.3;
  - 3: test_dutching_max_win_guard con stake >10 € — bloccati dal nuovo limite owner 10,00 € (atteso per Q3).

## F1 — capitale a rischio > 10 € accettato se la % lo consente (manual_bet)
Fix: limite assoluto OWNER_MAX_CAPITAL_AT_RISK_EUR=10.00 su esposizione tavoli + ordine, sempre (anche bankroll 0); _table_total_exposure eccezione/NaN/negativo → inf → blocco.
Concorrenza: check-then-act non atomico tra manual_bet concorrenti (esposizione letta prima dell'invio) — residuo, serve prenotazione esposizione sotto lock. Recovery: dopo riavvio l'esposizione dipende dal table_manager ricostruito. Fail-closed: sì per eccezione/non finito; tablemanager assente = 0 (nessun tavolo) — da confermare. Nota: duplication_guard acquisito prima dei check e non rilasciato sui fail (comportamento preesistente).
## F2 — LAY misurato come stake
Fix: validators.order_exposure_or_inf (LAY=liability), lato/quota invalidi → inf → blocco. Stessa analisi di F1.
## F3 — drawdown solo in % (LIVE)
Fix: perdita assoluta peak−bankroll ≥ 10,00 € → lockdown+rifiuto; la % resta controllo aggiuntivo. Concorrenza: bankroll/peak letti senza lock del risk_desk (residuo). Recovery: dipende dalla persistenza di equity_peak (non verificata). Fail-closed: valori non finiti/illeggibili → rifiuto.
## F4 — drawdown non applicato in SIM; equity NaN
Fix: controllo in tutte le modalità; NaN/inf → "equity_non_verificabile".
## F5 — tolleranza 1e-9 su cap/limiti
Fix: confronto Decimal esatto (repr del float), uguaglianza ammessa, illeggibile → nega. Rischio: errori float dei chiamanti (0.1+0.2) ora bloccano al cap esatto (direzione fail-closed). Decisione aperta: quantizzare al centesimo (Q3 "Decimal") renderebbe 10.000000001 = 10.00 ammesso, in contrasto col test RED F5 → TECNICA/OWNER da chiarire.
## F6 — config_version assente
Fix: roserpina.config_version = precedente+1 nella stessa transazione dei valori; versione corrotta → ValueError (nessun reset). Concorrenza: atomico in-process (RLock del Database); multi-processo: BEGIN deferred, read-modify-write possibile lost update → servirebbe BEGIN IMMEDIATE. Recovery: rollback della transazione lascia valori e versione coerenti.

## v2 (13/10/2026, correzioni)
- F1: rimosso il 10 EUR hardcoded. Fonte verificata: #426/5656005416 P02/P06 "Esposizione complessiva massima €10" = limite della CAMPAGNA #351 (non costante di prodotto); #426/6131980558 §1 = esposizione espressa in €. manual_bet ora usa il cap configurato `max_open_exposure` (stessa regola A2 del runtime: assente => nessun cap; illeggibile => nega). Il test F1 configura 10.00 citando la fonte; nuovo test: con cap configurato 20 EUR, 11 EUR passa.
- F5/float: somme e prodotti in Decimal sugli importi canonici (repr del valore sorgente): validators.money_sum, order_exposure in Decimal, table_manager.total_exposure in Decimal, money_management/runtime/A2 usano money_sum. 0.1+0.2 = 0.3 esatto; 10.000000001 > 10 resta superamento.
- Tolleranza 1e-9: NON e' regola owner. #393 contiene solo la decisione owner LAY=liability (4974645091/4974754190); "H-14" e' un id di finding (core/risk_gate.py:26). La tolleranza nasce dal contratto tecnico PR28 (test_issue437_pr28). Test pr28 aggiornati a money_sum/order_exposure.
- 13 doppi DB in 10 file di test: aggiunto transaction() (nullcontext).
- RED v3 (028cee9): 11 failed / 5 passed (test_f5_float_sum fallisce per AttributeError: money_sum non esiste — RED strutturale, non comportamentale). GREEN v3: 16/16. Suite completa con patch v2: 1 failed (pr04_quater tkinter, identico al baseline) / 7269 passed.
- Residuo: il drawdown 10 EUR resta costante nel codice: fonte #426/6131980558 §2 "Il codice dovrà rispettare il limite monetario" + Q5. Da confermare se va invece reso configurabile.

## v3 (13/10/2026) — decisioni owner #426/6141902809
- D1 drawdown: validators.effective_drawdown_eur: chiave nuova `roserpina.max_drawdown_abs` (RoserpinaConfig.max_drawdown_abs, load/save); assente ⇒ tetto campagna #351 10.00; valida ⇒ min(valore, 10.00); presente ma non valida (NaN/≤0/testo) ⇒ rifiuto "drawdown_threshold_non_valido". Distinta dal cap di esposizione. Residuo: "inferiore solo se validata" — la validazione è solo tecnica (finito > 0); nessun flusso di convalida owner/test della soglia inferiore.
- D2 cap mancante: open_exposure_cap_reason e runtime A2: max_open_exposure assente ⇒ "max_open_exposure_assente" (blocco nuovo rischio) in runtime, auto-next e manual_bet; nessun default (RoserpinaConfig.max_open_exposure resta None). Operazioni di riduzione del rischio (cancel/cashout) non passano da questi gate: NON verificato con test dedicato in questa iterazione.
- Fixture di test aggiornate con cap esplicito di test (1000.0, etichettato) in 9 file (runtime/cycle/auto-trade/pr26a/max_win_guard/best_price).
- pr04_quater [True]: causa = ambiente. customtkinter non installato nel venv di test (presente solo in requirements-build-linux) ⇒ mini_gui usa i widget fallback; DISPLAY=:7 presente sul box ⇒ la variante real_widgets non viene saltata; _FallbackComboBox inoltra `command=` a ttk.Combobox (mini_gui.py:182-184, chiamata a :919-925) ⇒ TclError "-command". Senza DISPLAY: 1 passed, 1 skipped. Difetto latente nel fallback (basso impatto, solo GUI senza customtkinter), non regressione.
