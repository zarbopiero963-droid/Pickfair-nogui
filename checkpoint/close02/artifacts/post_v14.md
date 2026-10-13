## PF-OWNER-CLOSE-ALL-02 — v14: ratifica t80u applicata a L7b + patch RST (t81u) + RESTART L9 rieseguito
Zero AI, nessun Sol, nessun #505. Nessun merge, deploy, LIVE od ordine Betfair. Il patch v8 è **solo proposta**. Nessun finding è dichiarato risolto. **Gate A: NON PASS.**

**1. Lavoro svolto**
- Ratifica t80u pubblicata su #426 con read-back identico: https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426#issuecomment-6143637861 (sha dfb7cef0…).
- L7b, 7 record (RSR-02, 03, 04, 12, 13, 15, 31):
  - rimosso `PROVVISORIO_PENDING_OWNER`;
  - casi assente/default con provenienza `OWNER_DECISION` che cita #426/6143637861, e ancore verbatim alla regola e alla frase "Not an approval of …";
  - casi non validi con oracolo owner aggiuntivo;
  - nuova clausola O2 con casi P/N/B dinamici (`probe_owner_t80u.py`: processo nuovo, DB su file, chiave assente / "abc" / controllo con tutte le chiavi valide).
- Patch proposto v8 (sha 02c3a2b1…), in aggiunta a v7:
  - **RST-001/002**: recovery non `ok` (esito incerto, errore di merge, errore DB sugli ordini startup, eccezione in `recover`) ⇒ runtime in PAUSED, `start()` restituisce `started=False`, `resume()` rifiutato. Gate su `_on_signal_received`, su `_evaluate_and_maybe_submit_auto_next_trade` e su entrambi i punti `CMD_QUICK_BET`. Tabella `startup_orders` aggiunta allo schema. Un errore nella verifica o registrazione degli ordini startup rende la recovery incompleta (fail-closed).
  - **RST-003**: nuovo `core/risk_persistence.py`. Il picco equity ha due copie (riga DB + file accanto al DB) e vale il massimo delle copie leggibili. Un registro delle protezioni scritte (DB + file) fa sì che una copia illeggibile, o tutte le copie mancanti dopo una scrittura, blocchino il nuovo rischio (`drawdown_state_non_verificabile`). Una scrittura del picco fallita blocca finché un nuovo tentativo non riesce. Risk-stop con riga e marker spariti dopo l'attivazione ⇒ `risk_stop_state_mancante`. Daily-loss con riga sparita dopo la scrittura ⇒ breach (fail-closed).
- Probe kill −9 su daily-loss e risk-stop (`probe_protections_restart.py`) a 028cee9 e con il patch.
- RESTART L9 rieseguito a 028cee9 e con v8.

**2. Gap chiusi davvero**
- PENDING_OWNER per le 7 chiavi: chiuso sul piano **normativo**. La regola owner è citata e gli oracoli sono definitivi. Il codice NON è conforme (vedi punto 4).
- Su RST-001/002/003 esiste ora una prova RED a 028cee9 e GREEN con v8. Restano **aperti** fino alla sequenza autorizzata #505.

**3. Test e risultati**
- L7b: 21 record, 326 casi. Preflight 21/21, controllo indipendente 21/21, più il controllo owner t80u 7/7. Mutazioni: 8/8 t80u e 52/52 complessive (L7b, L8b, L9, t80u).
- Test RST (31): a 028cee9 **21 falliti e 10 passati**, con v8 **31/31 passati**.
- Nei test (a) ci sono spie su tutti i punti capaci di generare ordini: sizing (`mm.calculate`), trading_engine, executor, broker (place/replace/submit/execute) e bus `CMD_*`. Coprono 4 tipi di segnale, auto-next-trade, `resume()` e un secondo `start()`, per 4 scenari di recovery fallita. Il controllo discriminante (recovery ok ⇒ si raggiunge il sizing) passa.
- Test (b) eseguiti con kill −9 reale:
  - picco con perdita 11,00 / 10,00 / 9,99 €;
  - kill ripetuti (1000 → 995 → 992 → riavvio a 990: bloccato);
  - riga del picco corrotta ("abc", "nan", "-5", JSON errato), riga mancante con copia file presente, tutte le copie mancanti;
  - primo avvio autentico non bloccato;
  - risk-stop dopo kill ripetuti, con riga corrotta e con righe mancanti;
  - daily-loss dopo kill ripetuti, con riga corrotta e con riga mancante.
- Mutazioni sul patch: 9/10 rilevate. **M10 (controllo del registro daily-loss disattivato) sopravvive**: il caso di riga daily-loss mancante è già bloccato a 028cee9 dall'emergency stop persistito, quindi quel controllo non ha un test che lo renda indispensabile.
- Regressione completa con v8: **7374 passati, 0 falliti** (`patched_full_v14.txt`).
- L9 RESTART: a 028cee9 357 casi, 258 CONFORME e 99 NON_CONFORME; con v8 348 CONFORME e 9 NON_CONFORME. Preflight 38/38 e controllo indipendente 38/38 su entrambi.

**4. Finding (tutti aperti)**
- **t80u, codice vs regola owner** (probe APPLY dinamico, 028cee9):
  - chiave assente o non valida ⇒ il codice carica 18 / 35 / 18 / 10000 / 2 / True / 4,5 in silenzio e il segnale **raggiunge il sizing** (7/7 NON_CONFORME sul caso assente);
  - `allow_recovery="abc"` ⇒ False, CONFORME;
  - gli altri 6 casi non validi sono NON_CONFORMI.
  - Serve un patch proposto separato, non ancora fatto.
- **CL02-RST-001/002/003**: confermati RED a base e GREEN con v8 (vedi punto 3).
- **Correzione di un oracolo L9**: per RSR-41 la soglia committata è 7,5 €, più severa di 10 €, quindi con perdita 9,99 € lo stop è atteso. A 028cee9 il caso è NON_CONFORME, da cui i 99 casi invece di 98.
- **Residuo con v8 (9 casi)**: RSR-38, 39 e 40 sono persistite nel DB ma ancora assenti da `RoserpinaConfig`.
- **Daily-loss e risk-stop al riavvio sono già robusti a 028cee9** con kill −9: il breach viene rifiutato come `emergency_stop_active` e il risk-stop resta attivo. Con v8 il primo rifiuto è `max_open_exposure_assente` (gate D2 di v7), ma lo stop resta attivo.
- **Harness**: il test interno `test_close02_findings._DB` ora conserva le impostazioni in memoria, così la persistenza del picco riesce e il test isola il gate D2.

**5. Blocchi residui e denominatore**
- RESTART: 38 record e 357 casi, 99 NON_CONFORMI a base e 9 con v8. Nessun record è READY.
- Mancano ancora:
  - un patch per t80u (blocco del nuovo rischio, niente recovery, niente commissione inventata);
  - il caricamento in config di RSR-38..40;
  - un test che renda indispensabile il controllo del registro daily-loss (M10);
  - la scrittura durevole del picco quando il bankroll cresce durante il runtime: oggi avviene solo all'avvio e al gate del segnale.
- Limite noto: se spariscono insieme sia la riga protetta sia **entrambe** le copie del registro (DB e file), lo stato non si distingue da un primo avvio.

**6. Costi**
- 0 €, nessuna chiamata AI. Cumulativo invariato: 3,518447 speso, 16,481553 residuo.
- Tempo agente misurato: 12:34:10 → 13:17 (circa 43 min, di cui circa 28 min in tre esecuzioni della suite completa).

**7. Prossima attività**
- Patch proposto t80u con test RED→GREEN e spie sul sizing.
- RSR-38..40 nel runtime.
- Test M10.
- Persistenza del picco a ogni nuovo massimo durante il runtime.

SHA256SUMS_v14: patch v8 02c3a2b1…, L7b d059ee1b…, L9 c2878bbe…, L9_patched 43228ad0…, probe_owner_t80u 3f4a9e02…. Backup: `close02_v14_1316.tgz`.
