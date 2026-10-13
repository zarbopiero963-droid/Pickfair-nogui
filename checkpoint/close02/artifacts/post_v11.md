## CLOSE-02 v11: t78u (12:26). Zero AI, nessuna chiamata a pagamento, nessuna modifica a #505
1. **Lavoro svolto**
   - **Patch v7** (`close02_findings_proposal_v7.patch`, sha 4fa2514e…):
     - firma pubblica `Database.transaction()` invariata; nuovo metodo privato `_transaction_immediate(busy_timeout_ms)` con `BEGIN IMMEDIATE` e busy timeout limitato a 2 s, ripristinato dopo l'uso;
     - contesa persistente sul lock ⇒ `CONFIG_CONFLICT:LOCK_CONTENTION`, fail-closed, nessuna scrittura parziale;
     - i test usano `monkeypatch` (guardrail rispettato).
   - **L7b**: ricostruiti i 21 record LOAD (i 20 respinti + RSR-20).
     - Sonda v3 a 028cee9: assente, override, override uguale al default del codice, formato alternativo, vuoto, `abc`, riavvio in un PROCESSO NUOVO sullo stesso file DB (con tipo).
     - Predicati attesi dichiarativi e valutatore separato (`eval_pred.py`).
   - **Preflight rafforzato** `preflight_L7b.py`, un controllo per ogni rilievo Sol (S1-S8, più sha, money e cid), con 12 test negativi per mutazione (`test_preflight_L7b.py`): **12/12 passati**, ogni rilievo re-iniettato viene intercettato.
   - **Controllo di completezza indipendente** `indep_check_L7b.py`: non importa builder, preflight né valutatore. Estrae dalla fonte grezza #461 la riga LOAD e la sezione di controllo (chiave, tipo, "vuoto=non impostato") e verifica sha, copertura e scenari drawdown.
2. **Gap chiusi**: nessuno (restano aperti fino alla sequenza #505 autorizzata).
3. **Test**
   - Regressione completa con v7: **7343 passati, 0 falliti**, 15 saltati, 2 xfail (v6: 7329/0). Le 15 regressioni della v12 sono risolte.
   - RACE-001 + lock: tutti verdi.
   - **CL02-DD-001**: RED a 028cee9 **10 falliti / 2 passati**; GREEN con v7 **12/12**.
   - L7b: preflight **21/21 PASS**, controllo indipendente **21/21 completi**.
4. **Finding**
   - **CL02-DD-001, P0 / LIVE-BLOCKING**: a 028cee9 il drawdown di 10 € non è applicato. Perdite di 10.00 e 11.00 € passano in SIM e LIVE, anche con una soglia del 50%. Il limite solo percentuale vale solo in LIVE.
     - Fonti owner: #426/6141136157 Q5, #426/6141450045 Q3 (>= 10,00 €), #426/6141902809 D1.
     - Test (SIM e LIVE): stop a >= 10.00 €; nessuno stop a 9.99 €; la soglia % non indebolisce i 10 €.
     - La correzione è nella patch di proposta (v7, già presente dalla v6); va collegata alla futura sequenza #505 autorizzata. Il finding resta APERTO.
   - **Rilievo Sol #1 (default)**, applicando le regole owner vigenti:
     - chiave assente ⇒ si cita il valore owner vigente se esiste (D2 "missing cap" per RSR-17/38/39/40; D1/Q3 drawdown per RSR-20/41; "vuoto=non impostato" dalla fonte per RSR-16..19);
     - altrimenti NESSUN default normativo: il default del codice è comportamento attuale non ratificato, nel campo impl, non come atteso.
     - Candidati **PENDING_OWNER** (l'assenza cambia il rischio monetario o LIVE e nessuna regola la copre):
       - RSR-02 max_single_bet_pct (codice 18.0)
       - RSR-03 max_total_exposure_pct (35.0)
       - RSR-04 max_event_exposure_pct (18.0)
       - RSR-15 max_stake_abs (10000.0 €)
       - RSR-12 max_recovery_tables (2)
       - RSR-31 allow_recovery (True)
       - RSR-13 commission_pct (4.5, incide sul P/L)
5. **Denominatore e blocchi**
   - Denominatore: 316 record, 4146 casi.
   - Lotto L7b: **21 record interi, 305 casi**, tutti con osservato a 028cee9 e verdetto: 199 NON_CONFORME, 95 CONFORME, 11 NON_APPLICABILE. Preflight 21/21, controllo indipendente 21/21. **Non inviato a Sol** (nessuna autorizzazione).
   - L8 (SAVE): NON ancora riallineato alle nuove regole (S1-S8): residuo.
6. **Costi**: 0 € in questo ciclo. Cumulato 3.518447, residuo 16.481553.
   - Tempo agente misurato: 12:16–12:26 (~10 min) per patch v7 + DD-001 + L7b.
   - Ricostruzione L7b: ~6 min per 21 record (~17 s/record).
   - Tempo macchina: build 0.2 s, sonda v3 2.4 s, preflight 0.1 s, controllo indipendente 0.2 s.
7. **Prossima attività**
   - Riallineare L8 (SAVE) con S1-S8 e il controllo indipendente.
   - Proposta di lotto L7b (~0.35 €, tetto 0.66 €, NON chiamata) solo su autorizzazione owner.
   - Pacchetto consolidato PENDING_OWNER.

SHA256SUMS_v11 (20/20 verificati). Backup: /home/box/pickfair_backup/close02_v11_1225.tgz + patch v7. Lotto `lot_R_L7b_LOAD21.json` sha 15e50493….
