## PF-OWNER-CLOSE-ALL-02 — v13: fase RESTART ricostruita (38 record, regole L7b/L8b)
Zero AI, nessun #505, nessuna chiamata Sol. Nessun merge, deploy, LIVE od ordine Betfair. Il patch resta solo proposta (v7, 4fa2514e, invariato). **Gate A: NON PASS.**

**1. Lavoro svolto**
- `probe_restart_v2` gira a 028cee9 con terminazione **reale**:
  - il processo A esegue il commit della chiave, crea un ordine aperto (saga PENDING, BACK 5 € @2.0) e poi chiama `os.kill(SIGKILL)`, con returncode −9 verificato;
  - il processo B è **nuovo** (pid diverso) e lavora sullo **stesso file SQLite su disco**: carica la config, avvia `RuntimeController.start()` in modalità SIM con DB e SettingsService reali (exchange simulato) e invia un segnale per verificare l'ammissione.
- Scenari per ciascuna chiave:
  - `commit_poi_kill`;
  - `kill_in_transazione` (scrittura non committata, kill dentro `transaction()`);
  - `riavvio_esito_incerto` (il fetch degli ordini exchange all'avvio fallisce);
  - `riavvio_ordine_fantasma` (ordine attivo sull'exchange assente dal DB).
- Per le chiavi drawdown (RSR-05, 06, 07, 20, 41): memoria della perdita con picco A di 1000 € e saldo B di 989 / 990,00 / 990,01 €, cioè perdite di 11,00 / 10,00 / 9,99 €.
- L9 contiene: predicati dichiarativi (`eval_pred_restart.py`), preflight S1–S8 + SHA + O1, il controllo indipendente `indep_check_L9.py` (regex sulla riga `| RESTART |` e sulle sezioni `### RSR-NN` della fonte grezza #461) e i test di mutazione.

**2. Gap chiusi davvero**
- Il RESTART ora ha prove dinamiche reali (SIGKILL, nuovo processo, stesso DB) con oracolo specifico per clausola su tutti e 38 i record.
- Fonti con SHA verificato: #461/6085510337 per la riga di fase, P1/P2 verbatim, e #461/6085510893 per le sezioni RSR-22..41. Questa seconda fonte prima mancava ed è stata trovata dal controllo indipendente.
- Le clausole D1–D3 sono marcate **DERIVATA_NON_VERBATIM** (D02 R3), non come normative.
- Difetto di sorgente L2b su RSR-29: la `clause_map` era malformata (D1/D2 spezzati su "[0.1-3…"). L'ho ricostruita dal testo canonico e registrata nel campo `difetto_sorgente_L2b`.
- Nessun finding viene dichiarato risolto.

**3. Test e risultati**
- 38 record e 357 casi: **259 CONFORME, 98 NON_CONFORME**.
- Preflight: 38/38. Controllo indipendente: 38/38.
- Mutazioni: 18/18 rilevate (13 sul preflight, 4 sul controllo indipendente, più il caso base pulito).
- Esiti CONFORMI:
  - il valore committato sopravvive al kill −9 e al nuovo processo con tipo corretto, enum incluso (34 chiavi);
  - la scrittura non committata è invisibile (38/38);
  - la saga aperta sopravvive e viene ripubblicata come RECOVER_ORDER (38/38);
  - con recovery ok l'ammissione è consentita (38/38).
- La suite completa non è stata rieseguita, perché in v13 nessun codice prodotto è cambiato.

**4. Finding (aperti, nessuno risolto)**
- **CL02-RST-001 (P0, blocca il LIVE).** Esito incerto all'avvio: `recover()` restituisce `ok=False, STARTUP_RECONCILIATION_REQUIRED, startup_mode=BLOCKED`, ma `start()` imposta `RuntimeMode.ACTIVE` e il segnale arriva al sizing. Viola D2 "nessuna ammissione durante recovery" (38/38 NON_CONFORME).
- **CL02-RST-002 (P0, blocca il LIVE).** La tabella `startup_orders` non viene mai creata: nessun CREATE TABLE nel codice, mentre `startup_order_exists`, `load_startup_orders` e `upsert_startup_order` la interrogano. Il risultato è `OperationalError: no such table`, che `_is_missing_in_db` trasforma in False: l'ordine fantasma viene **ignorato in silenzio**, si passa a `LIVE_FLAT` e il sistema torna operativo (38/38 NON_CONFORME).
- **CL02-RST-003 (P0, blocca il LIVE).** La memoria del drawdown si perde al riavvio: `equity_peak` non è persistito. B riparte con picco = saldo corrente e, con perdite di 11,00 € e 10,00 €, ammette il segnale, contro la regola owner (#506/6141450045: stop a ≥ 10,00 €; #506/6141136157). Esito 10/10 NON_CONFORME. Il caso 9,99 € risulta CONFORME, ma è banale: dopo il riavvio niente blocca.
- **Già noto.** RSR-38..41 sono persistite nel DB ma assenti da `RoserpinaConfig` a 028cee9, quindi non vengono conservate nel runtime (12 casi). Il patch v6/v7 le copre solo come proposta.

**5. Blocchi residui e denominatore**
- RESTART: 38 record e 357 casi, con 98 casi NON_CONFORMI su 3 finding nuovi più 1 noto. Nessun record è READY: le non conformità sono aperte e servirebbero fix proposti con prova RED→GREEN prima della sequenza #505.
- Non ancora sondato al riavvio: lo stato daily-loss e risk-stop, che hanno codice di persistenza (`restore_daily_loss`, `restore_risk_stop`) ma nessuna prova kill −9 in questo lotto.
- PENDING_OWNER: restano invariati i 7 marcati. Delle chiavi RESTART, RSR-04, 12, 13, 15 e 31 hanno `pending_owner=true`, ma qui gli oracoli usano il valore committato e non il default. **Nessuna chiave nuova.**

**6. Costi**
- 0 € in questo lotto, nessuna chiamata AI.
- Cumulativo invariato: 3,518447 speso e 16,481553 residuo.
- Tempo agente misurato: 12:28:50 → 12:33:30 (circa 5 min).

**7. Prossima attività**
- Patch proposto (non applicato a main) con test RED a base e GREEN con patch per RST-001 (start resta non attivo se la recovery non è ok), RST-002 (schema `startup_orders` e fail-closed su errore), RST-003 (persistenza di picco e memoria drawdown).
- Probe kill −9 per daily-loss e risk-stop.
- Poi rieseguire L9.

SHA256SUMS_v13: `record_specific_L9.json` c7908a24…, `probe_restart_v2.json` 1d06cf94…, `lot_R_L9_RESTART38.json` be7438ef…. Backup: `close02_v13_1233.tgz`.
