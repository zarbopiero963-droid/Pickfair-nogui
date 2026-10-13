## close02 — aggiornamento v16 (13/10/2026, 13:56–14:12 ora di Roma) — Zero AI, niente Sol, niente #505

Patch **v10 = SOLO PROPOSTA** (nessun merge/deploy/LIVE, nessun ordine Betfair). **Gate A: NON PASS.** La migrazione delle installazioni pre-v9 resta **PENDING_OWNER**: nessun percorso di sblocco è stato implementato.

### 1. Lavoro svolto
- **P/L con commissione etichettata (t80u regola 3, seconda parte)**:
  - Ogni risultato P/L di `core/pnl_engine.py` porta `commission_status` (`STIMATA`/`VERIFICATA`) e `pnl_status` (`STIMATO`/`VERIFICATO`). Vale per aggregatore market-net, chiusura posizione pubblicata (`RUNTIME_CLOSE_POSITION`), mark-to-market, settlement simulato e settlement cleared.
  - Il P/L è `VERIFICATA/VERIFICATO` **solo** se Betfair riporta una commissione (`reported_commission`) che coincide al centesimo con quella applicata.
  - Se l'importo riportato è diverso o NaN: resta `STIMATA` + `commission_mismatch=True`. Senza dato del broker: `STIMATA` (la costante di policy 4,5 % non è una verifica).
- **Segnali LOAD distinti per i 10 cap opzionali** (`max_daily_loss`, `max_drawdown_hard_stop_pct`, `max_drawdown_abs`, `max_open_exposure`, `max_session_loss`, `max_exposure_stop`, `max_recovery_chase_abs`, `max_order_exposure`, `max_market_exposure`, `max_event_exposure_abs`):
  - assente ⇒ `absent_optional_keys` + WARNING;
  - vuoto ⇒ `empty_keys` + WARNING (resta "non impostato");
  - corrotto ⇒ `corrupt_keys` + ERROR ⇒ nuovo rischio bloccato dal gate config corrotta già esistente.
- **Assente vs esplicito uguale al default** (RSR-14/23/27/29/36): le chiavi che ricadono sul default del codice sono elencate in `defaulted_keys` + WARNING "default del codice (non ratificato)".
- La sonda LOAD v3 ora riporta anche i nuovi campi flag (lo strumento è lo stesso, mostra solo più dati).

### 2. Lacune
- **Nessun percorso reale verifica oggi la commissione**: lo stato `VERIFICATA` richiede un `reported_commission` Betfair coincidente.
- Il poller cleared non estrae ancora la commissione da Betfair: in pratica tutto il P/L è etichettato `STIMATO`. Così è onesto, ma l'etichetta non è ancora usata dalla GUI/Telegram (non toccate).
- Non c'è ancora alcuna regola su **decisioni che aumentano il rischio basate su P/L stimato** (es. target di ciclo): oggi i gate t80u bloccano il nuovo rischio solo quando `commission_pct` è assente o non valida.
- **Migrazione pre-v9**: PENDING_OWNER, invariata.

### 3. Test
- RED/GREEN `test_close02_v10.py`: **base 028cee9 20/20 falliti → v10 20/20 passati**.
- Mutazioni v10: **8/8 uccise** (M25–M32; M27 uccisa dopo l'aggiunta del test sulla chiusura posizione pubblicata). Le mutazioni v9 (23/23) restano valide: i file toccati sono diversi da quelli della v9, eccetto `setting_service` (funzione nuova separata).
- Regressione completa v10 (seriale): **7438 passati / 0 falliti** (15 skip, 2 xfail), ≥ v9 (7418).
- **L7b**:
  - base 103 CONFORMI / 212 NON_CONFORMI / 11 N.A.; **con v10 315 CONFORMI / 0 NON_CONFORMI / 11 N.A.** (i 32 casi residui della v9 sono chiusi).
  - preflight 21/21 PASS, controllo indipendente t80u 7/7, test_t80u 9/9.
- **L9 RESTART**: base 258/99; **con v10 357/357 CONFORMI**; preflight 38/38 OK, controllo indipendente 38/38 completi.

### 4. Finding
- **Chiusi (con la patch)**: etichetta P/L stimato/verificato; 32 casi L7b (assente/vuoto/corrotto/override, assente vs default esplicito).
- **Nuovo comportamento da notare**: un cap opzionale **corrotto** ora blocca tutto il nuovo rischio, non più solo il proprio controllo. È fail-closed, coerente con il gate config corrotta.
- **Aperti**:
  - estrazione della commissione Betfair nel poller cleared;
  - uso dell'etichetta in GUI/Telegram;
  - decisioni che aumentano il rischio basate su P/L stimato;
  - migrazione pre-v9 (owner).

### 5. Blocchi
Migrazione pre-v9 = PENDING_OWNER (nessuno sblocco implementato). Gate A NON PASS. Nessun merge/deploy/LIVE.

### 6. Costi
0 chiamate a pagamento, 0 AI. Tempo agente misurato ~16 min (13:56–14:12).

### 7. Prossima attività
Su autorizzazione:
- (a) estrazione commissione da listClearedOrders, così il P/L può diventare VERIFICATO;
- (b) regola: nessuna decisione che aumenta il rischio basata su P/L `STIMATO`;
- (c) etichetta visibile in GUI/Telegram;
- (d) decisione owner sulla migrazione pre-v9.

Artefatti (locali): `close02_findings_proposal_v10.patch` (sha256 1e8cc2464ab71d7a…, + .bak), delta `close02_v9_to_v10_delta.patch` (6ffd4ab768fd4eb7…, 5 file), `red_v10.txt`, `green_v10.txt`, `mutation_v10.txt`, `patched_full_v16_serial.txt`, `record_specific_L7b_patched.json`, `record_specific_L9_patched.json`.
