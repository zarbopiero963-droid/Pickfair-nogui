## CLOSE-02 v12: L8b SAVE + dipendenze PENDING_OWNER in L7b (12:28). Zero AI, nessuna modifica a #505
1. **Lavoro svolto**
   - **L8b**: 20 record SAVE ricostruiti con le regole di L7b.
     - Clausola P1 verbatim dalla riga di fase SAVE (#461/6085510337); D1-D3 ancorate a P1; O1 drawdown (owner #426/6141136157 + Q3 #426/6141450045) su RSR-20 e RSR-41.
     - Predicati dichiarativi; valutatore separato `eval_pred_save.py`.
     - Sonde a 028cee9:
       - percorso GUI reale `MiniPickfairGUI._save_roserpina_settings` → `save_roserpina_config` su DB file;
       - scenari ok / guasto sulla chiave / guasto con valore precedente / guasto sulla versione;
       - nuova sonda "salvataggio invariato" `probe_save_same_v2` per tutte le 20 chiavi;
       - sonda drawdown con 10.00 / 9.99 / 11.00 €.
   - **Preflight** `preflight_L8b.py`: S1-S8 adattati a SAVE + S9 (fixture non inseribile dalla GUI va dichiarata) + sha/money/cid.
   - **Controllo indipendente** `indep_check_L8b.py`: non importa builder, preflight né valutatori; estrae la riga SAVE e le sezioni di controllo dalla fonte grezza.
     - Al primo giro **ha intercettato un difetto reale**: a RSR-41-SAVE (max_drawdown_abs) mancavano i casi della regola owner drawdown. Corretto.
   - **Nuovo stato di verdetto `NON_ESERCITABILE`**, quando il guasto iniettato non viene mai raggiunto: la versione non è mai scritta a 028cee9 e le chiavi RSR-38..41 non sono scritte dal percorso di salvataggio. Così non si scrive un CONFORME/NON_CONFORME senza aver esercitato lo scenario.
   - **L7b**: segnati i 7 record con dipendenza PENDING_OWNER (RSR-02, 03, 04, 12, 13, 15, 31).
     - Campo `pending_owner` con la decisione richiesta, i casi interessati (P2-positive e D2-boundary) e `stato_oracolo=PROVVISORIO_PENDING_OWNER`.
     - Lettura in attesa: chiave assente ⇒ assenza segnalata (flag/avviso) o None, ancorata solo a P1/P2 verbatim; il default del codice NON è l'atteso.
     - Se l'owner ratifica un valore, l'oracolo diventa: "assente ⇒ valore owner citato, con segnalazione".
     - Esito attuale sotto l'oracolo provvisorio: NON_CONFORME (il default viene applicato in silenzio).
2. **Gap chiusi**: nessuno.
3. **Test**
   - Mutazioni preflight L7b + L8b: **25/25 passati** (12 L7b + 13 L8b, incluso un test che verifica che il controllo indipendente intercetti uno scenario rimosso).
   - L8b: preflight **20/20 PASS**, controllo indipendente **20/20 completi**.
   - L7b, dopo la marcatura: preflight 21/21, controllo indipendente 21/21.
   - Regressione completa del codice invariata dalla v11 (patch v7: 7343/0); nessun cambiamento di codice in questo ciclo.
4. **Finding**
   - L8b a 028cee9: **252 casi**: 96 CONFORME, 96 NON_CONFORME, 60 NON_ESERCITABILE.
     - Principali: versione di configurazione mai persistita; messaggio "salvata" mostrato anche senza versione nel DB; chiavi RSR-38..41 mai salvate, con messaggio di successo comunque mostrato; drawdown 10 € non applicato (CL02-DD-001).
     - Atomicità sul guasto della chiave: CONFORME (nessuna scrittura parziale, nessun successo).
   - Tutti i finding restano aperti.
5. **Denominatore e blocchi**
   - Denominatore: 316 record, 4146 casi.
   - Record interi con preflight rafforzato + controllo indipendente: **41** (L7b LOAD 21 / 305 casi; L8b SAVE 20 / 252 casi), di cui 7 con oracolo dell'assenza provvisorio (PENDING_OWNER).
   - Decisione owner in attesa: 7 default di assenza (pacchetto consolidato).
6. **Costi**: 0 € (cumulato 3.518447, residuo 16.481553).
   - Tempo agente misurato: 12:26:56–12:28:13 per L8b + marcature (~1.5 min attivi, 20 record ≈ 4-5 s/record più la verifica).
   - Tempo macchina: build 0.25 s, valutazione 0.1 s, preflight 0.1 s, controllo indipendente 0.2 s.
7. **Prossima attività**
   - Pacchetto PENDING_OWNER consolidato (7 chiavi).
   - Fase RESTART (38 record L2b) con le stesse regole.
   - Nessuna revisione Sol senza autorizzazione.

SHA256SUMS_v12 (18/18 verificati): `record_specific_L7b.json` b0d327bb…, `record_specific_L8b.json` c0e7294f…, `preflight_L8b.py` a25d2fab…, `indep_check_L8b.py` 89b2d557…. Backup: /home/box/pickfair_backup/close02_v12_1228.tgz.
