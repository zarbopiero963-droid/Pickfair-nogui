# Ripristino del registro dei superamenti LIVE illeggibile (M148)

Decisione owner M148 (#628): un registro dei superamenti LIVE illeggibile mantiene Pickfair **fail-closed**.
Questa procedura serve a uscire dal blocco **solo** ricostruendo un registro leggibile da fonti verificabili e
riconciliando davvero ogni conto. Non è un modo per togliere lo stop: è un modo per tornare a poter dimostrare che
non c'è più rischio.

Valido dalla #638 (`ef2da782`, M147) in avanti. Si appoggia a [db_backup_restore.md](db_backup_restore.md) e a
[incident_playbook.md](incident_playbook.md).

## Quando si applica

`get_status()["live_exposure"]` mostra `registry_unreadable`, oppure `reset_emergency()` risponde:

- `reason: esposizione_live_irrisolta` con `accounts_pending: ["betfair|LIVE|<vincolo_illeggibile>|EUR"]` (registro
  illeggibile);
- `reason: esposizione_live_irrisolta` con `accounts_pending: ["betfair|LIVE|<conto_non_identificato>|EUR"]`
  (M149: obbligo LIVE con conto non identificabile, per esempio sessione LIVE senza login o perdita non attribuibile;
  **nessuno sweep la certifica** e non viene mai attribuita al conto collegato);
- `reason: registro_incoerente_obbligo_live` (M149: Emergency Stop LIVE ma registro vuoto o mancante).

Il registro è la chiave `emergency_live_accounts` della tabella `settings` del database (formato v2:
`{"version": 2, "accounts": {ambito: {"state": "PENDING"|"VERIFIED", "breaches": [...]}}}`).

## Divieti assoluti

Ognuno di questi è un aggiramento dello stop, non una procedura.

| Vietato | Perché |
|---|---|
| Cancellare la chiave o scriverci `""` / `{}` | Registro vuoto = «nessun vincolo»: misurato in M148, con il libro giornaliero illeggibile `reset_emergency()` **passa** con un ordine ancora aperto |
| Scrivere `"state": "VERIFIED"` a mano | Solo lo sweep di Pickfair, con quel conto connesso, può verificare un conto. L'app non distingue un VERIFIED scritto a mano |
| Togliere un conto dal registro | Un conto omesso non viene più riconciliato |
| Sovrascrivere o cancellare il database originale corrotto, il marker `<db>.daily_loss_pending.json` o i log | Sono le evidenze |
| Inventare superamenti, perdite, sessioni, settlement o identità di conto | La storia si ricostruisce solo da fonti che la dimostrano |
| Modificare `daily_loss_state`, lo stato dello stop di rischio o la catena `audit_events` per accorciare il blocco | Il libro illeggibile resta un superamento fino al nuovo giorno UTC, per progetto |
| Usare l'API `reset_emergency()` prima del passo 8 | |
| Cancellare o modificare il giornale di sicurezza `<db>.safety_pending.json` (M151) | Contiene un Emergency Stop dichiarato che il db non ha reso durevole, la protezione delle coperture e gli obblighi LIVE in sospeso (`emergency_live_pending_bindings`, M151-bis): all'avvio prevale sul db. È un'evidenza e un blocco, non un file temporaneo |
| Modificare `emergency_settlement_source` o `emergency_unresolved_exposure` | Sono l'evidenza dell'obbligo LIVE (M149): cancellarle insieme al registro è l'unico aggiramento che l'app non rileva |
| Scrivere la data di attribuzione (passo 4.4) senza inventario firmato o con una data futura | La data scioglie i superamenti non identificati: senza prove è un'attribuzione inventata |

## Ruoli

Due operatori distinti: **O1** esegue, **O2** controlla e firma. Nessun passo successivo al 4 senza la firma di O2.
L'owner autorizza ogni connessione a Betfair LIVE, anche in sola lettura (passo 7).

## Passi

### 1. Fermare e congelare

1. STOP del runtime (GUI o headless). Nessun avvio in trading finché la procedura non è chiusa.
2. Verificare che nessun processo tenga aperto il database (precondizioni di [db_backup_restore.md](db_backup_restore.md)).
3. Se una delle due cose non è certa: **fermarsi qui**.

### 2. Conservare le evidenze (mai modificarle)

In una cartella `recovery/M148-<data-ora>/`, in sola lettura dopo la copia:

1. copia byte per byte di `pickfair.db` e, se presenti, `pickfair.db-wal` e `pickfair.db-shm` (un solo insieme);
2. copia di `pickfair.db.daily_loss_pending.json` e (M151) del giornale di sicurezza `pickfair.db.safety_pending.json`,
   se presenti; il giornale resta al suo posto (all'avvio ripristina l'Emergency Stop non durevole sul db);
3. sha256 di ogni file copiato e dell'originale (Linux `sha256sum <file>`, Windows `certutil -hashfile <file> SHA256`):
   **devono coincidere**;
4. il valore corrotto della chiave e il suo sha256. Con la CLI `sqlite3`, se installata:
   ```bash
   sqlite3 pickfair.db "SELECT value FROM settings WHERE key='emergency_live_accounts'" > registro_corrotto.txt
   sha256sum registro_corrotto.txt
   ```
   La CLI non è garantita (es. Windows). Alternativa identica su Windows e Linux: salvare queste righe come
   `leggi_registro.py` ed eseguire `python leggi_registro.py pickfair.db` (apertura in sola lettura):
   ```python
   import hashlib, sqlite3, sys
   db = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
   riga = db.execute("SELECT value FROM settings WHERE key = 'emergency_live_accounts'").fetchone()
   valore = (riga[0] if riga else "") or ""
   with open("registro_corrotto.txt", "w", encoding="utf-8", newline="") as f:
       f.write(valore)
   print(hashlib.sha256(valore.encode("utf-8")).hexdigest())
   ```
5. versione di Pickfair (SHA del commit), log del runtime, `get_status()` dell'ultimo avvio.

O2 ricalcola gli hash e firma l'elenco.

### 3. Diagnosi: cosa è illeggibile

Su una **copia** del database, per ciascuna chiave di `settings` annotare leggibile / illeggibile:

- `emergency_live_accounts` (il registro);
- `daily_loss_state` (libro giornaliero per ambito);
- `emergency_unresolved_exposure`, `emergency_live_verify_only`;
- stato dello stop di rischio e della sessione di rischio;
- la catena del registro della sessione di rischio in `audit_events`, verificata con `core.risk_session_ledger.load()`
  (una catena non verificabile non è una fonte).

### 4. Ricostruire il registro candidato (su una copia)

**Regola d'ingresso:** un conto LIVE entra nel candidato se **una qualsiasi** fonte lo nomina. **Tutti i conti
entrano `PENDING`**, sempre, anche se una fonte li dava verificati: la verifica vale solo dopo il passo 7.

Fonti, in quest'ordine:

1. **Backup integro e verificato**, anteriore alla corruzione: `scripts/db_restore_validate.py` = `PASS` e
   chiave leggibile. Da lì: conti e storia dei superamenti. Stato forzato a `PENDING`.
2. **Fonti durevoli del database corrente**, per i conti comparsi dopo il backup o se il backup manca:
   - ambiti LIVE in `daily_loss_state` (perdita o superamento);
   - ambiti nelle sessioni del registro della sessione di rischio (`delta.scopes`), solo se la catena è verificabile;
   - `financial_scope` nei `payload_json` di `orders` e `settlement_effects`;
   - `required_accounts` in `emergency_unresolved_exposure`.
3. **Inventario dei conti — OBBLIGATORIO**, sempre, anche quando le fonti 1 e 2 sembrano complete: ogni conto
   Betfair LIVE mai configurato su questa installazione (credenziali, configurazione, log). Con `daily_loss_state` ed
   `emergency_unresolved_exposure` illeggibili un conto coinvolto può comparire **solo** qui (verificato da Claude D).
   Nel dubbio il conto **entra**. Senza inventario firmato da O2 il candidato non si scrive.

La storia di un superamento (`key`, `kind`, `limit`, `loss`, `risk_session_id`, `source`, giorno) si scrive **solo**
se una fonte la dimostra campo per campo. Altrimenti `"breaches": []` e una nota nel dossier.

**Se un conto non è identificabile** (ambito non verificabile, quarantena legacy, fonte contraddittoria):
il candidato **non si scrive**, il blocco resta e si apre una richiesta owner.

4. **Sentinella «conto non identificato» (M149).** Se il registro contiene `betfair|LIVE|<conto_non_identificato>|EUR`,
   oppure il libro giornaliero contiene superamenti LIVE senza conto (`betfair|LIVE||EUR`, `betfair|UNVERIFIED||EUR`),
   la sentinella si scioglie **solo** così:
   1. l'inventario firmato da O2 identifica con prove **tutti** i conti a cui quei superamenti possono appartenere
      (ordini, settlement, estratti conto dell'exchange); nel dubbio entrano tutti i conti dell'inventario;
   2. il candidato contiene quei conti `PENDING` e **non** contiene la sentinella;
   3. nella stessa operazione si scrive la **data di attribuzione**, la data UTC di oggi, nella chiave
      `emergency_live_unidentified_attributed_through` della tabella `settings` (formato `AAAA-MM-GG`, mai futura);
      va riportata nel dossier e nell'audit del passo 6.

   La data vale **solo** per i superamenti giornalieri non identificati fino a quel giorno: al riavvio Pickfair non
   ricrea la sentinella per quelli, e resta l'obbligo di riconciliare i conti del candidato (passo 7). Ogni nuovo
   obbligo senza conto (Emergency Stop manuale, stop LIVE senza login) ricrea **sempre** la sentinella. Data assente,
   non valida o futura = nessuna attribuzione: la sentinella torna e il blocco resta.

### 5. Validare e firmare il candidato

1. Il candidato si legge con `RuntimeController._parse_live_registry()` senza errori.
2. Ogni stato è `PENDING`; nessun conto delle fonti del passo 4 manca.
3. `scripts/db_restore_validate.py` = `PASS` sulla copia candidata.
4. O2 confronta il candidato con le fonti e firma il dossier: fonti usate, conti, storia, hash prima e dopo.

### 6. Applicare il candidato e registrare l'audit

A processo fermo, sulla copia candidata già validata:

1. scrivere il candidato nella chiave `emergency_live_accounts`;
2. aggiungere in `audit_events` un evento `{"type": "M148_REGISTRY_RECOVERY", ...}` con hash dell'originale,
   registro corrotto, registro ripristinato, fonti, O1 e O2. È un'aggiunta: la catena del registro della sessione di
   rischio non ne è toccata;
3. passaggio al database ripristinato come in [db_backup_restore.md](db_backup_restore.md) («Cutover»), conservando
   l'originale nella cartella del passo 2.

### 7. Riconciliare **ogni** conto

1. Avvio di Pickfair **senza trading**: Emergency Stop ancora attivo, `live_exposure.accounts` con tutti i conti
   `PENDING`, nessun `registry_unreadable`. Se no: tornare al passo 1.
2. Per **ogni** conto del registro, con autorizzazione owner alla connessione LIVE in **sola lettura**: connettere
   quel conto e lasciare che Pickfair esegua la verifica. Il conto diventa `VERIFIED` solo così.
3. Per ogni conto, sull'exchange, l'operatore verifica **anche** ciò che zero ordini aperti non dimostra:
   - ordini aperti (eseguibili) e parzialmente abbinati;
   - posizioni abbinate su mercati non ancora regolati;
   - ordini regolati (cleared orders) dalla data dell'ultimo stato verificato, confrontati con `settlement_effects`
     e con la perdita per ambito in `daily_loss_state`;
   - attribuzione corretta di ogni perdita al proprio conto.
4. Ogni differenza = **stop**: il conto resta `PENDING` e si apre una richiesta owner.

### 8. Reset con doppia conferma

1. Solo con **tutti** i conti `VERIFIED` e `live_exposure.state` = `VERIFICATA`.
2. Doppia conferma manuale: O1 e O2 firmano nel dossier l'autorizzazione al reset (`reset_emergency()` oggi **non**
   ha una doppia conferma nell'app: è un controllo procedurale, vedi «Limiti»).
3. `reset_emergency()`. Se risponde `esposizione_live_irrisolta`: **non** forzare, tornare al passo 7.
4. Stop di rischio rimasto (es. `risk_stop_state_illeggibile`, `session_loss_breached:sessione_di_rischio_illeggibile`):
   reset G1 con doppia conferma e audit a catena (GUI «RESET SESSIONE DI RISCHIO» o headless
   `--risk-session-reset`). Con il libro giornaliero illeggibile il G1 risponde `daily_loss_breached` fino al **nuovo
   giorno UTC**: è voluto, non si accorcia.

### 9. Chiusura

Dossier completo (evidenze, hash, fonti, firme, esiti di ogni passo) allegato all'incidente. Se un passo non è
dimostrabile: il blocco resta e la procedura si ferma lì.

## Prova offline (M148, FATTO)

Banco della matrice M147 di Claude D, py3.11, offline, 3 esecuzioni su 3, nelle 4 celle `illeggibile`
(GIORNALIERO e SESSIONE × SIM/LIVE):

| Passo | Esito |
|---|---|
| Stallo prima della procedura | ES e stop di rischio restano dopo la chiusura reale delle esposizioni |
| Conservazione | copia byte per byte con lo stesso sha256 dell'originale, invariata dopo il ripristino |
| Subito dopo il ripristino, prima di riconciliare | `reset_emergency()` **rifiutato**, entrambi i conti `PENDING` |
| Dopo la riconciliazione reale di A e B | ES tolto, entrambi i conti `VERIFIED` |
| Stop di rischio residuo | `daily_loss_breached` lo stesso giorno; G1 accettato il giorno UTC successivo |

Nota di banco (verifica di Claude D): rieseguita da D la prova dà 7/8; la cella GIORNALIERO/SIM può non raggiungere
la precondizione dello stallo (l'ES si toglie già prima e resta solo lo stop di rischio). È un limite del banco, non
della procedura. L'oracolo indipendente di D sulla procedura (`6b3bee37`) dà 4/4, 3 esecuzioni su 3.

Errori dell'operatore misurati:

| Errore | Esito |
|---|---|
| Conto B omesso dal candidato | reset rifiutato: Pickfair ri-registra B dal libro giornaliero leggibile dello stesso giorno |
| `VERIFIED` scritto a mano | reset rifiutato, per la stessa ragione |
| Chiave svuotata, libro leggibile | reset rifiutato, per la stessa ragione |
| **Chiave svuotata e libro giornaliero illeggibile** | su `d1de60eb` **reset accettato con un ordine di B aperto: FAIL-OPEN**; da M149 **rifiutato** (`registro_incoerente_obbligo_live`) |

Su `d1de60eb` le difese automatiche dei primi tre casi valevano **solo** se il superamento era nel libro giornaliero
leggibile dello stesso giorno. Da M149 il registro vuoto con un obbligo LIVE è sempre fail-closed; `VERIFIED` scritto
a mano e un conto omesso restano non rilevabili in generale: valgono i divieti.

## Limiti dichiarati

- **Registro vuoto con obbligo LIVE (R7) — corretto in M149.** Fino a `d1de60eb` l'app trattava un registro vuoto
  come «nessun vincolo» (R7 progettato in M147 e non implementato; stessa radice del P1 di Claude D sull'esito LIVE
  senza conto). Da M149: un obbligo LIVE senza conto registra la sentinella; un Emergency Stop LIVE (sede salvata
  `emergency_settlement_source = "LIVE"`) o un esito LIVE irrisolto su disco con registro vuoto è un'incoerenza
  (`registro_incoerente_obbligo_live`): nessun conto certifica, reset rifiutato. Resta non rilevabile solo chi svuota
  a mano **insieme** il registro, `emergency_settlement_source` ed `emergency_unresolved_exposure`: vietato.
- **`VERIFIED` scritto a mano** e **conto omesso**: non rilevati dall'app in generale; vietati dalla procedura.
- **Doppia conferma di `reset_emergency()`**: procedurale (firme nel dossier), non applicata dall'app. Il G1 dello
  stop di rischio la applica.
- **Connessione LIVE in sola lettura** (passo 7): richiede l'autorizzazione owner; con LIVE OFF la procedura si ferma
  al passo 6 e il blocco resta.
