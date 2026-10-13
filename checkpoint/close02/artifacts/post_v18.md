## close02 — aggiornamento v18 (13/10/2026, 14:32–15:15 ora di Roma) — Zero AI, niente Sol, niente #505

Patch **v12** (protezioni) e **v13** (migrazione pre-v9, isolata) = **SOLO PROPOSTA**: nessun merge, deploy o LIVE, nessuna chiamata reale a Betfair, nessun ordine. **Gate A: NON PASS.**
Ratifica owner della migrazione pre-v9 (opzione A) pubblicata alla lettera su #426: https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426#issuecomment-6145958246 (read-back identico).

### 1. Lavoro svolto
- **(a) Netto conservativo per daily-loss e drawdown (v12).**
  - Nuova `pnl_label.conservative_pnl`. Con P/L verificato, simulato o senza commissione, oppure in perdita, si usa il netto così com'è.
  - Con una vincita a commissione **stimata** si usa `max(0, min(netto, lordo − lordo×4,5% − 0,01 €))`.
  - Fonte del limite: Betfair Italia, https://support.betfair.it/app/answers/detail/a_id/3754/: «Il tasso base di mercato (Market Base Rate, MBR) è la commissione massima applicata su un particolare mercato, che in Italia è pari al 4,5%». Sull'arrotondamento: «L'importo della commissione da pagare è arrotondato alle due cifre decimali più vicine. Per importi che terminano in 0,005 è prevista un'approssimazione per eccesso». Da qui il margine di 0,01 €.
  - Conferma: https://www.betfair.it/aboutUs/commissioni/ («percentuale base di mercato del 4.5% … fissa e non sará possibile ottenere alcun tipo di sconto»).
  - **TECNICA_DELEGATA:** se il lordo è assente o illeggibile, la vincita è considerata interamente a rischio (vale 0).
  - Il valore conservativo è usato sia nella proiezione del pending-stop durevole sia nell'applicazione al daily-loss.
  - **Drawdown:** usa già il saldo verificato (`get_account_funds`). È stato trovato e corretto un difetto della base: l'applicazione transitoria del P/L alzava `equity_peak` (anche da P/L stimato). Ora il picco viene ripristinato e l'hook di persistenza è disattivato durante quel passaggio.
- **(b) Semantica `betIds` + `groupBy=MARKET` (solo documentazione pubblica, nessuna chiamata).**
  - Fonte: https://betfair-developer-docs.atlassian.net/wiki/spaces/1smk3cen4v3lu3yomq5qye0ni/pages/2687749/listClearedOrders.
  - `betIds`: «Optionally restricts the results to the specified bet IDs. A maximum of 1000 betId's are allowed in a single request».
  - `groupBy`: «How to aggregate the lines, if not supplied then the lowest level is returned».
  - Nota `settledDateRange`: «…there is no way to get the returned market rollup to only include bets settled in a certain date range, it will always return the overall position from the market including all settlements».
  - Roll-up (pages/2687679): `commission` vale N a livello BET/SIDE e Y a livello MARKET.
  - **Non è confermato** che `betIds` filtri prima del rollup. Per questo `CLEARED_MARKET_ROLLUP_VERIFIES = False`: la verifica da rollup è disattivata e il P/L resta **STIMATO**. Il codice resta e i test v11 lo esercitano con il flag attivo.
- **M39:** ucciso con prova (vedi punto 4).
- **(c) Migrazione pre-v9, opzione A (v13, patch isolata).**
  - Nuovo `core/pre_v9_migration.py` con le funzioni `inspect` e `ratify`, più la CLI operatore `scripts/pre_v9_migration.py`.
  - **Prima della migrazione:** con REINIT_REQUIRED ogni nuovo rischio è bloccato, anche se il picco è presente (`migrazione_pre_v9_richiesta`).
  - **Report all'operatore:** valori grezzi e interpretati del picco e del daily-loss, ciascuno con la sua provenienza (DB o file); copie e motivo del risk-stop; righe di emergenza; ordini incerti (saghe PENDING/SUBMITTED/PLACED/PARTIAL/ROLLBACK_PENDING); bloccanti; digest sha256.
  - **Sblocco** solo con operatore dichiarato, digest digitato uguale allo stato mostrato e nessun bloccante. Sono bloccanti: dato mancante, illeggibile o copie incoerenti (picco o daily-loss), risk-stop illeggibile, ordini incerti o illeggibili, migrazione già eseguita o non richiesta.
  - **Nessun valore monetario scritto:** le copie mancanti sono copiate byte per byte; si aggiorna solo il registro. Risk-stop, emergenza e daily-loss non vengono toccati.
  - **Commit e audit:** si scrive il registro su DB e poi su file. Il registro è l'unione delle due copie, quindi finché una contiene ancora REINIT il sistema resta bloccato. Audit append-only (INTENTO → COMPLETATA, COMPLETATA_RECUPERATA dopo un kill tra commit e audit, RIFIUTATA) più una riga DB.

### 2. Lacune
- **Installazioni base reali (028cee9):** non hanno mai persistito il picco equity. La migrazione resta quindi **bloccata** (`equity_peak_mancante`) e richiede una riconciliazione documentata. Formato e fonte di questa riconciliazione **non sono definiti**: serve una decisione owner (PENDING_OWNER) e nessun percorso è stato implementato.
- **Mutanti v9–v11 (M1–M44) non rieseguiti in blocco su v12:** lo script non era più disponibile. Sono stati rieseguiti M39 e 9 nuovi mutanti v12. I file toccati da v12 sono soltanto `pnl_label` e l'applicazione del settlement.
- **M51 (hook picco attivo durante l'applicazione transitoria) è equivalente:** `apply_closed_pnl` aggiorna il picco in linea prima di `_touch`, quindi l'hook non può scattare. La disattivazione è solo difesa in profondità.
- Con risk-stop attivo dopo il riavvio, il sizing viene ancora calcolato e il blocco avviene al ricontrollo pre-submit (nessun `CMD_`). È un comportamento preesistente, non modificato.
- 5 asserzioni di test esistenti sono cambiate al valore conservativo (19.10→19.09, 7.64→7.63, 12.5→12.49), con commento.

### 3. Test
- **RED/GREEN v12:** a 028cee9 16 falliti e 1 passato (controllo: vincita verificata contata intera) su 17, registrati in `red_v12.txt`; con v12 17/17 GREEN.
- **RED/GREEN v13:** sul patch v12 24 falliti e 1 passato (controllo: primo avvio autentico); con v13 25/25 GREEN. Copertura, un test per condizione:
  - C1 non operativo prima della migrazione;
  - C2/C2b report e CLI con conferma digitata;
  - C3 picco mancante, niente inventato;
  - C4 daily-loss mancante;
  - C5/C5b illeggibile;
  - C6 copie incoerenti;
  - C7 ordini incerti;
  - C8 digest e operatore;
  - C9 valori invariati, audit, sblocco;
  - C10 risk-stop ed emergenza restano attivi;
  - C11/C11b perdita del giorno (−8 €) e picco preservati;
  - C12 una tantum;
  - C13 **kill -9 reali** (SIGKILL) a ciascuno dei 7 passi, più C13b sei kill in sequenza.
- **Esito kill -9:** nessuno sblocco senza entrambe le copie del registro; valori monetari invariati; nuova ratifica completa; audit sempre chiuso.
- **Regressione completa** sull'albero pulito con patch applicata: v12 7477 passed, 15 skipped, 2 xfailed (xdist 3:42, seriale 6:53); v13 7502 passed, 15 skipped, 2 xfailed (xdist 3:45, seriale 7:17). 0 failed.
- **Mutazione:**
  - v12: 9/10 uccisi (M39, M45–M50, M52, M53); M51 equivalente.
  - v13: 13/13 uccisi (M54–M66).
  - M45 (proiezione sul netto stimato) è sopravvissuto alla prima esecuzione ed è stato ucciso con un nuovo test sul pending-stop.
- **L7b** con v13: 315 CONFORMI, 0 NON_CONFORMI, 11 N.A. **L9 RESTART** con v13: 357/357 CONFORMI. Invariati rispetto a v11.

### 4. Risultati
- **M39 chiuso con prova:** `test_a_verifica_solo_a_mercato_completo_seconda_bet_a_profitto_zero` (seconda bet a profitto 0, atteso `["STIMATO","VERIFICATO"]`) uccide il mutante: `mutation_v12.txt`, 1 fallito.
  - Il test fissa la specifica: verificato solo a mercato completo.
  - Il percorso è comunque disattivato di default da (b).
- Una vincita stimata non può più compensare una perdita oltre il limite documentato. Esempio: limite 10 €, vincita lorda 0,44 € e poi perdita di 10,415 €. Con il netto stimato la perdita è 9,9948 € (nessuno stop); con il conservativo è 10,0048 € (stop).
- Il picco del drawdown non viene più gonfiato da P/L transitori: era un difetto della base.

### 5. Blocchi
- Riconciliazione documentata per le installazioni senza picco persistito: **PENDING_OWNER**.
- La semantica `betIds` prima del rollup non è confermata dalla documentazione pubblica: P/L STIMATO.
- Gate A: NON PASS.

### 6. Costi
- 0 € di spesa AI aggiuntiva, nessuna chiamata Betfair, nessun ordine.

### 7. Prossima attività
- Decisione owner su formato e fonte della riconciliazione documentata del picco per le installazioni base.
- Eventuale riesecuzione in blocco di M1–M44 su v13, con lo script ricostruito.

Artefatti (locali): `close02_findings_proposal_v12.patch` (sha256 f42e1f226a1d4424…), `close02_v11_to_v12_delta.patch` (c7e83df5799d9044…), `close02_findings_proposal_v13.patch` (6fef237dc74a37ec…), `close02_v12_to_v13_delta.patch` (96ab56da3a3ccbc5…, 4 file), `red_v12.txt`, `red_v13.txt`, `mutation_v12.txt`, `mutation_v13.txt`, `patched_full_v18*.txt`, `patched_full_v19*.txt`, `record_specific_L7b_v13.json`, `record_specific_L9_v13.json`, `SHA256SUMS_v18`.
