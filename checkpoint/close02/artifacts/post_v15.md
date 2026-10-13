## close02 — aggiornamento v15 (13/10/2026, 13:17–13:56 ora di Roma) — Zero AI, niente Sol, niente #505

Patch **v9 = SOLO PROPOSTA** (nessun merge, nessun deploy, nessun LIVE, nessun ordine Betfair). **Gate A: NON PASS.**

### 1. Lavoro svolto
- **Regole owner t80u nel codice** (#426/6143637861):
  - (1) `max_single_bet_pct`, `max_total_exposure_pct`, `max_event_exposure_pct`, `max_stake_abs` assenti, vuote, non valide o non verificabili ⇒ blocco di tutto il nuovo rischio. Il gate sta in `_on_signal_received` dopo il controllo dei campi obbligatori e PRIMA di tavolo, sizing, builder, executor e broker; c'è anche in `_loss_block_reason` (auto-next / pre-submit) e nel dutching manuale. Config mancante = non verificabile = blocco. Il cashout (che riduce il rischio) resta disponibile.
  - (2) `allow_recovery` o `max_recovery_tables` assente/non valida ⇒ `allow_recovery=False` (mai True per assenza). `max_recovery_tables=0` solo se è questa chiave a mancare: un massimo esplicito valido resta com'è.
  - (3) `commission_pct` assente/non valida ⇒ `commission_pct=None` (nessun 4,5 inventato) + blocco del nuovo rischio.
  - Al LOAD l'assenza è segnalata (`absent_keys` + WARNING per ogni chiave).
- **RSR-38..40** (`max_order_exposure`, `max_market_exposure`, `max_event_exposure_abs`) ora sono campi di `RoserpinaConfig`: caricati dopo il riavvio e salvati solo se impostati.
- **Mutante M10 eliminato**: con perdita giornaliera parziale (8 € su 10, nessun breach e nessuna emergenza), dopo kill -9 sparisce la riga daily-loss. Il riavvio ora blocca e il blocco è **durevole** (marcato nel registro, resta anche al riavvio successivo). Prima M10 sopravviveva.
- **Emergenza**: se la riga emergency sparisce dopo essere stata scritta ⇒ si riparte in emergenza (registro `emergency`).
- **Picco a ogni nuovo massimo in sessione**: callback `RiskDesk.on_new_peak` → persistenza. Il picco non viene mai scritto prima di essere riletto all'avvio (evita di sovrascrivere 1000 con 989).
- **Limite "tutte le copie spariscono = primo avvio"**: se il registro protezioni manca (DB e file) ma il DB contiene stato di rischio scritto dal runtime (daily_loss/risk_stop/emergency/picco) ⇒ `risk_ledger_mancante`, blocco durevole. Le saghe ordini NON contano come prova: la prima versione le usava e bloccava in modo permanente avvii legittimi (rilevato dalla sonda L9 e corretto).
- La fixture della sonda RESTART ora dichiara esplicitamente le 7 chiavi owner valide (tranne quella sotto prova): senza di esse t80u blocca, giustamente.

### 2. Lacune
- Il **primo avvio autentico** resta indistinguibile se spariscono insieme il DB intero e i file marker: non c'è un'ancora esterna. La copertura è parziale ma fail-closed su ogni DB con storia.
- **Migrazione delle installazioni esistenti (pre-v9)**: hanno stato daily-loss ma nessun registro ⇒ al primo avvio v9 risultano bloccate in modo durevole e non esiste un percorso operatore di sblocco. **Serve una decisione owner** (ratifica di una baseline da parte dell'operatore?). Nessun percorso di sblocco è stato inventato.
- Il P/L usa ancora una commissione 4,5 % propria (`pnl_engine`, `betfair_service` sim) senza etichetta "stimata vs verificata" (regola 3, seconda parte). Non è coperto da v9: è un finding.
- L7b con la patch: 32 casi ancora NON_CONFORMI, tutti fuori da t80u (RSR-14/16..20/23/27/29/36/38..41: assente/vuoto/corrotto non distinguibili per segnale sui cap opzionali).

### 3. Test
- RED/GREEN su 3 file (v9 + t81u + RST): **base 028cee9 59 falliti / 16 passati → v9 75/75 passati**.
- Mutazioni: **23/23 uccise** (M1–M7, M9–M24; M17/M18 riverificate dopo la correzione sulle saghe).
- Regressione completa v9: **7418 passati / 0 falliti** (15 skip, 2 xfail), sia in seriale sia con -n4 (≥ v8).
- **L9 RESTART**: base 258 CONFORMI / 99 NON_CONFORMI; con v9 **357/357 CONFORMI**; preflight 38/38 OK, controllo indipendente 38/38 completi (entrambe le viste).
- **L7b**: preflight 21/21 PASS, controllo indipendente t80u 7/7, test_t80u 9/9. Base 103 CONFORMI / 212 NON_CONFORMI; con v9 283 / 32. I 7 record owner (O2) sono tutti CONFORMI.

### 4. Finding
- Chiusi (con la patch): t80u (1)(2)(3) LOAD/APPLY; RSR-38..40 al riavvio; M10; picco in sessione; registro emergenza; registro sparito con storia.
- Aperti: migrazione pre-v9 (owner); P/L con commissione stimata non etichettata; distinzione assente/vuoto/corrotto sui cap opzionali (32 casi L7b).

### 5. Blocchi
Decisione owner sulla migrazione delle installazioni esistenti. Gate A NON PASS. Nessun merge/deploy/LIVE.

### 6. Costi
0 chiamate a pagamento, 0 AI. Tempo agente misurato ~39 min (13:17–13:56).

### 7. Prossima attività
Su autorizzazione: (a) decisione owner sulla migrazione pre-v9; (b) etichetta P/L "stimato/verificato" per la commissione (regola 3); (c) segnali assente/vuoto/corrotto per i cap opzionali (32 casi L7b).

Artefatti (locali): patch `close02_findings_proposal_v9.patch` (sha256 6d132bc590cbd482…, + .bak), delta `close02_v8_to_v9_delta.patch` (c66e2079f7b854bb…, 11 file), `red_v9.txt`, `green_v9.txt`, `mutation_v9.txt`, `patched_full_v15*.txt`, `record_specific_L9_patched.json`, `record_specific_L7b_patched.json`.
