## close02 — aggiornamento v17 (13/10/2026, 14:10–14:33 ora di Roma) — Zero AI, niente Sol, niente #505

Patch **v11 = SOLO PROPOSTA** (nessun merge/deploy/LIVE, nessuna chiamata reale a Betfair, nessun ordine). **Gate A: NON PASS.** Migrazione pre-v9: **PENDING_OWNER**, nessun percorso di sblocco.

### 1. Lavoro svolto (regola owner t80u (3), #426/6143637861)
- **(a) Commissione da listClearedOrders.** Secondo la tabella Betfair "listClearedOrders – Roll-up Fields Available", `commission` **non esiste a livello BET/SIDE** e c'è solo da `groupBy=MARKET` in su (cumulativa sul mercato).
  - Il poller leggeva `row["commission"]` dalle righe per-bet: un percorso morto, ora rimosso.
  - Nuovo: dopo il fetch per-bet, il poller chiede il rollup `groupBy=MARKET` filtrato sulle sole bet del bot di quel giro. La commissione è considerata **attribuibile solo se** `betCount` coincide esattamente con le bet del giro e `profit` sommato coincide al centesimo. Viene passata solo sull'ultima riga del mercato.
  - Il motore marca `VERIFICATA/VERIFICATO` solo se l'importo coincide al centesimo con il totale commissione di mercato applicato.
  - Rollup assente, in errore, non attribuibile o diverso ⇒ `STIMATO` (+ `commission_mismatch`). Il settlement non viene mai bloccato.
- **(b) Nessun aumento di rischio su P/L stimato.** Nel percorso auto-next/cycle executor l'invio del prossimo trade (progressione) è bloccato (`AUTO_TRADE_SKIPPED_PNL_STIMATO`) se il P/L dipende da una commissione non verificata.
  - Ammesso solo con P/L `VERIFICATO`, `SIMULATO`, oppure con commissione applicata = 0 (P/L = lordo riportato).
  - Stato assente o sconosciuto, oppure valori illeggibili ⇒ blocco.
  - Le decisioni che riducono il rischio restano: target raggiunto, ciclo chiuso, stake non valido sono applicati **prima** del gate; daily-loss, emergency, drawdown e cashout non sono toccati.
  - Nuovo modulo unico `core/pnl_label.py`, usato da gate, GUI e Telegram.
- **(c) Etichetta visibile.**
  - **GUI**: storico Risk Desk mostra `9.55 (VERIFICATO|STIMATO|SIMULATO)`, letto dalla riga o da `payload_json`; se manca ⇒ STIMATO. Lo storico ora salva `pnl_status`/`commission_status`.
  - **Telegram**: ogni alert con `net_pnl`/`pnl`/`realized_pnl` nei dettagli aggiunge `• P/L: x (ETICHETTA)`.
  - **SIM**: P/L del broker simulato = `SIMULATO`/`SIMULATA` (nessun denaro reale).
- **Fixture aggiornate** (dichiarate): 7 builder di payload di test con commissione > 0 ora dichiarano `"pnl_status": "VERIFICATO"`; senza questo campo il gate li blocca, correttamente. Il fake del poller risponde `[]` al rollup MARKET non scriptato. Un test del poller conta solo le chiamate per-bet.

### 2. Lacune
- **Equivalenza argomentata del mutante M39** (commissione passata su ogni riga del mercato, non solo l'ultima): il confronto avviene sul totale *cumulativo* di mercato, quindi una riga intermedia risulta verificata solo se il suo cumulativo coincide già con la commissione finale (es. bet successive a profitto 0), e in quel caso l'etichetta è vera. Non è stato trovato un input che lo distingua. **Mutante equivalente, non ucciso.**
- Il filtro `betIds` + `groupBy=MARKET` è inteso come applicato prima del rollup. Se Betfair aggregasse anche bet esterne, `betCount`/`profit` non coinciderebbero ⇒ STIMATO (fail-closed). Non è verificabile senza chiamate reali (vietate).
- Le **protezioni** (daily-loss) usano ancora il netto stimato. È conservativo solo se la commissione reale ≥ stima; il bankroll sync da `get_account_funds` resta la fonte vera.
- Migrazione pre-v9: PENDING_OWNER.

### 3. Test
- RED/GREEN `test_close02_v11.py`: **base 028cee9 18 falliti / 3 passati (i 3 controlli) → v11 21/21 passati**.
- **Mutazioni rieseguite davvero su v11** (tutta la suite close02 per ogni mutante):
  - v9 M1–M7, M9–M24: **23/23 uccise**;
  - v10 M25–M32: **8/8 uccise**;
  - v11 M33–M44: **11/12 uccise** (M39 equivalente, vedi sopra).
  - Log completo `mutation_v11.txt`.
- Regressione completa v11 (seriale): **7459 passati / 0 falliti** (15 skip, 2 xfail), ≥ v10 (7438).
- **L7b**: base 103/212/11; **con v11 315 CONFORMI / 0 NON_CONFORMI / 11 N.A.**; preflight 21/21 PASS; indip t80u 7/7; test_t80u 9/9.
- **L9 RESTART**: base 258/99; **con v11 357/357 CONFORMI**; preflight 38/38 OK; indip 38/38 completi.

### 4. Finding
- **Chiusi (con la patch)**: commissione Betfair estratta dal livello documentato; nessuna progressione su P/L stimato; etichetta in GUI e Telegram; il percorso morto `commission` per-bet è rimosso (era un finding: non poteva mai verificare nulla).
- **Aperti**: protezioni sul netto stimato (vedi §2); semantica del filtro `betIds` + MARKET da confermare con documentazione/prova owner; migrazione pre-v9.

### 5. Blocchi
Migrazione pre-v9 = PENDING_OWNER. Gate A NON PASS. Nessun merge/deploy/LIVE.

### 6. Costi
0 chiamate a pagamento, 0 AI, 0 chiamate Betfair. Tempo agente misurato ~23 min (14:10–14:33).

### 7. Prossima attività
Su autorizzazione:
- (a) protezioni con margine conservativo sulla commissione stimata (limite superiore documentato);
- (b) conferma documentale/owner della semantica `betIds` + `groupBy=MARKET`;
- (c) decisione owner sulla migrazione pre-v9.

Artefatti (locali): `close02_findings_proposal_v11.patch` (sha256 0d0fb41b4d7f8d5b…, + .bak), delta `close02_v10_to_v11_delta.patch` (927604e347e11125…, 15 file), `red_v11.txt`, `green_v11.txt`, `mutation_v11.txt`, `patched_full_v17_serial.txt`, `record_specific_L7b_patched.json`, `record_specific_L9_patched.json`.
