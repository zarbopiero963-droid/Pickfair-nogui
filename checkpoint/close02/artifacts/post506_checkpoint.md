## Checkpoint di continuità close02 (13/10/2026, ~15:50 ora di Roma)

**Solo continuità del lavoro**: non è integrazione, non è revisione e non è certificazione indipendente. Nessuna PR, nessun merge, nessun deploy, nessun LIVE. **Gate A: NON PASS.** Spesa AI aggiuntiva: 0 €.

- **Branch:** `checkpoint/gate-a-close02`, da `028cee9`. **Commit esatto:** `477a8977713bb84a8820990cdc3f57bb4d118d7d`.
- **Contenuto:** `checkpoint/close02/` con CHECKPOINT.md, patch v13 (verde) e v14 (**WIP, NON VERIFICATO**) da applicare con `git apply`, più `mut_all.py`. Gli SHA dei file remoti coincidono con quelli locali.
  - v13: sha256 6fef237dc74a37ec…
  - v14: sha256 13c1bf8d2988a49c…
- **Stato dei test (locali):**
  - v13: suite completa 7502 passed, 0 failed (xdist e seriale); RED→GREEN v12 e v13; mutanti v12 9/10 (M51 equivalente), v13 13/13.
  - v14 WIP: 23/23 test nuovi verdi (23 RED su v13). Suite completa e mutazione completa M1–M80 **non eseguite**: la mutazione si è fermata a M29 per il limite d'uso, e M8 e M27 sono sopravvissuti nel sottoinsieme di test, da rieseguire sulla suite completa.
- **Controlli prima del push:** nessuna credenziale (solo password fittizie di test preesistenti); fixture v14 sintetiche, nessun estratto conto reale; `git apply --check` dei due patch su 028cee9 riuscito.
- **Finding aperti:**
  - semantica `betIds`+`groupBy=MARKET` non confermata;
  - picco delle installazioni base (v14 WIP; decisione owner https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426#issuecomment-6146717266);
  - confronto del picco con `availableToBetBalance` invece dell'equity;
  - lotti Gate A L4, L6 e CASH NOT_READY; L1/L2 in attesa di revisione indipendente.
- **Prossime attività:**
  1. v14: suite completa, mutazione M1–M80, report;
  2. aggiunta al branch di sonde, preflight, indep-check, eval e record JSON;
  3. lotti L4, L6 e CASH.
