# Percorso MCP — contratto operativo consolidato

Revisione documentale 2026-10-07. Questo documento consolida il mandato owner;
non certifica runtime, test funzionali, installazione o autorizzazione LIVE.
Le sigle PRxx sono schede #461, non numeri di pull request GitHub.

## Entry point e preflight

Per «Lavora sul percorso MCP di Pickfair» partire obbligatoriamente da
[Pickfair-nogui #491](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/491).
Ordine di lettura: **#491 → #489 → #461 → #426 → #453 → #351 quando serve
collaudo → pickfair-mcp- #1**. Leggere corpi e commenti successivi, AGENTS.md,
CLAUDE.md e le specifiche obbligatorie; non lavorare dalla sola memoria.

| Fonte | Ruolo |
|---|---|
| #491 | Entry point di coordinamento MCP e prossima scheda |
| #489 | VIGENTE / SUPERATO / RINVIATO / ESCLUSO / LIMITE NOTO / DA CHIARIRE |
| #461 | Schede, requisiti e dipendenze; non obbliga a 60 nuove patch |
| #426 | Decisioni owner di prodotto, rischio e governance |
| #453 | Finding, follow-up e prove tecniche, senza decisioni prodotto autonome |
| #351 | Prove funzionali e certificazioni con perimetro e SHA |
| pickfair-mcp- #1 | Adapter subordinato al motore Pickfair |

Baseline di questo consolidamento: main
`e068a14deb9571e5c86de2113422f5d61b2a4375`; ultimo merge #493;
#490, #492 e #493 MERGIATE; zero PR aperte al preflight.
Questa fotografia scade dopo ogni merge e non sostituisce una nuova lettura
di main, PR aperte e decisioni. Prima di aprire la PR verificare di nuovo.

Una PR attiva per repository: una in Pickfair-nogui e una in pickfair-mcp-
possono coesistere. La serialità è interna al repository, non globale.
Una PR non correlata aperta nello stesso repository blocca un nuovo task.
Non lavorare su main; riparare una PR esistente sul suo branch/current head.

## Gerarchia e Phase 0

1. Decisioni owner più recenti e pertinenti, soltanto per i punti modificati.
2. AGENTS.md / CLAUDE.md / specifiche operative.
3. #491.
4. #489.
5. #461 / #453 nei rispettivi ruoli.
6. Phase 0 sul main corrente.
7. #351 per collaudo/certificazione.
8. pickfair-mcp- #1.
9. Issue storiche per i sottorequisiti ancora vigenti.

Phase 0 accerta il codice, NON modifica decisioni prodotto. Un commento tecnico
NON supera una decisione owner. Un vecchio corpo issue NON prevale su una
decisione owner successiva. Una feature non aggiornata individualmente NON è
automaticamente cancellata. Le vecchie code sono STORICO, non nuove istruzioni.

Per ogni scheda e dopo ogni merge rifare Phase 0: SHA → requisito/fonte →
decisione → comportamento atteso → call path → prova → scope → residuo.
Usare DIFETTO CONFERMATO, GIÀ CORRETTO CON PROVA, IPOTESI SMENTITA oppure
EVIDENZA INSUFFICIENTE / BLOCKED_ENV. Per un difetto naturale: red-first → fix
→ green. Un sabotaggio dimostra sensibilità del test, non un bug naturale.
Una prova insufficiente non è PASS. Nessuna patch duplicata per già corretto.

Fotografia storica 06/10 su `aba1340c`: **26 difetti confermati**,
**PR33 EVIDENZA INSUFFICIENTE / BLOCKED_ENV** finché non rieseguita,
**0 GIÀ CORRETTO CON PROVA**, salvo nuova prova sul main corrente.
#492/#493 cambiano reviewer, non provano la risoluzione di questi finding.

## Dipendenze riconciliate

La seguente è la sequenza tecnica corrente per MCP autorizzata dal mandato
owner di consolidamento del 07/10 e dalle decisioni #426 del 06/10. Sostituisce
le vecchie code amministrative MCP; non cancella i requisiti delle schede.

| PR | Dipendenza tecnica reale | Vecchia serialità | Ordine corrente autorizzato | Motivo |
|---|---|---|---|---|
| PR26-a | Produttori già esistenti e contratto identità engine | PR26 dopo PR25 | **Prima PR26-a customer_ref/provenance**, poi PR26 authority | Runtime pubblica CMD_QUICK_BET senza customer_ref; engine lo richiede prima della normalizzazione. Sbloccare identità stabile e retry idempotente senza indebolire validazione |
| PR26 authority | PR26-a; Q452/#478 già chiusa; censimento ingressi | PR25 → PR26 | PR26-a → PR26 → PR27 | Centralizzare adapter e autorità prima dello sweep; PR25 è consumer futuro, non import/prerequisito per centralizzare l'engine già esistente |
| PR27 | Autorità/adapter PR26, incluso rifiuto locale degli invalidi | PR26 → PR27; commento MCP proponeva il contrario | PR26 → PR27 | Lo sweep deve provare tutti gli ingressi convergenti; PR26 conserva controlli esistenti e chiude F10/F15 specifici, senza rinviare input safety |
| PR28 | Contratto numerico PR27; config/risk correnti | PR27 → PR28 | PR27 → PR28 → PR29 | Prima definire/enforce cap e liability; poi rendere atomico controllo+riserva. reload_config ha owner primaria PR28 |
| PR29 | Limiti/liability PR28 | PR28 → PR29; commento MCP diceva PR29/28 | PR28 → PR29 → PR30 | Non riservare con unità/cap non definiti; includere pending/AMBIGUOUS e prova concorrente |
| PR08 | Catalogo dominio, DB e sync disponibili; nomi univoci e freshness | PR08 → PR07 → PR06 (callback GUI e parser) | Core safety → PR08 resolver neutro; PR06/PR07 trasporti/UI successivamente | DeterministicResolver e CatalogSyncService non importano parser/listener/GUI; sync.run_sync è indipendente. Conservare delta/upsert/no-wipe; niente Provider legacy o LIMIT 1 ambiguo |
| PR43 | Posizioni/identità, order authority, MM/risk, book, STOP, reconciliation | PR42 → PR43 | Core safety + PR25/P35 + PR37/38 → PR43 → PR44 → PR45 | Il calcolo single-account non importa follower. #426 esclude master/copy/follow MCP; PR42 replica in futuro il risultato locale, non produce il contratto cashout |
| PR59 | Tutti i componenti del dossier totale, inclusa verifica update/rollback | PR58 → PR59 | **PR58 → PR59 totale → PR60**; gate go-live corrente conservato | #461 PR59 impone PR58; nessuna decisione owner ha rinviato updater dal go-live. Caso B MCP LIVE senza updater NON autorizzato: punto owner aperto, nessuna deroga tecnica |

Riscontri statici sulla baseline: `core/runtime_controller.py::_on_signal_received`
e `reload_config`; `core/trading_engine.py::_normalize_request`;
`services/deterministic_resolver.py`; `services/catalog_sync_service.py`;
`cashout_router.py`, `cashout_executor.py` (catena locale, non dispatcher follower).
Questa PR non esegue né certifica le riproduzioni runtime delle prossime schede.

### Prossima PR e sequenza unica

Finire questa PR documentale con merge owner; rileggere main e PR aperte.
La **prossima scheda tecnica è PR26-a customer_ref/provenance dei produttori**.
Se già corretta con prova sul nuovo main, registrare la prova e passare alla
prima scheda non soddisfatta della sequenza; non aprire un fix artificiale.

Sequenza MCP Pickfair:
**PR26-a → PR26 authority → PR27 → PR28 → PR29 → PR30 → PR31 → PR32 →
PR33 → PR34 → PR35 → PR36 → PR37 → PR38 → PR08 → PR11 → PR09 → PR12
(FIXED/MM; MANUAL escluso finché deciso) → PR15 → PR16 → PR17 → PR19 →
PR20 → PR22 → PR23 → PR24 → PR25 → PR43 → PR44 → PR45 → PR46 → PR47
(audit necessario) → PF-API-0 → PF-API-1 → PF-API commands**.

PR38 DIRECT/TTL e PR20 anomaly escalation non sono cancellate per omissione
dalla vecchia lista sintetica. UI/trasporto delle schede rinviati solo dove
esclusivamente Telegram o non necessari al percorso; enforcement e business
rimangono obbligatori. PR28/PR29 implementano sui componenti esistenti:
PR23 dopo ne verifica le formule/precedenze, non è un motivo per lasciare cap
non enforced. Analogamente PR37 definisce la barriera che PR43 integra.
PR33 BLOCKED_ENV ferma il passo dipendente, non autorizza API trading in anticipo.

Nel repo MCP in parallelo: foundation/governance, CI, no-bypass e test/fixture
provvisorie dichiarate. Non congelare payload mutanti finché il contratto
Pickfair non è stabilizzato; niente trading operativo collegato solo a stub.

## Ownership finding — una sola patch primaria

| Finding | Owner PR primaria | PR integrazione | Prova richiesta |
|---|---|---|---|
| CUSTOMER_REF_REQUIRED / provenance | PR26-a (parte PR26) | PR26, PF-API-0 | Single/dutching/timer: identità prima engine; duplicato/restart, nessun ref nuovo al retry |
| NaN/Inf/bool trasversali | PR27 | PR26 residui F10/F15, PF-API | Invalidi zero side effect/trasporto, tutti ingressi; non duplicare fix client #488 |
| Side assente→BACK | PR26 | PR27 | Mancante/errato bloccato, BACK/LAY validi parità; preservare P13/P15 |
| LAY liability | PR28 | PR29, PR23 | stake × (price−1), soglia ±epsilon e parità consumer |
| Exposure reservation | PR29 | PR30/32/34 | Due processi, cap insufficiente, kill fra reserve/send, nessun rilascio su AMBIGUOUS |
| reload_config | PR28 | PR29/31/34, PF-API | Config patch preserva tavoli, esposizione, identità/reconciliation e pending dopo restart |
| AMBIGUOUS order/cancel/replace | PR30 | PR31 | Response lost, niente retry cieco né FAILED/SUCCESS senza prova remota |
| Startup reconciliation | PR31 | PR34, PF-API-0 | Fetch/auth incompleto non consente ACTIVE, current+cleared/ghost |
| Fencing/lock | PR32 | PR29/31/34 | Cross-process ownership, owner stale escluso prima effetto |
| Saga/outbox atomiche | PR33 | PR34 | SQLite temp reale, crash drill transazioni; BLOCKED_ENV finché manca prova |
| SIM durability | PR34 | PR28/31/33 | Broker/DB convergono dopo kill/restart, daily-loss durevole |
| AEAD | PR35 | PR51/52 | Bitflip/wrong key/migrazione non distruttiva, nessuna key ephemeral LIVE |
| STOP barrier | PR37 | PR28/43/45, PF-API | Zero nuove entrate, cancel/hedge pertinenti via authority; EMERGENCY nessun bypass |
| Mixed/netting/Void | PR43 | PR44/45 | Oracle PnL/vettore per target, estranei invariati; Void non settlement exchange |
| Fraction/percentage | PR44 | PR45 | P34 soglia+frazione, residui/minimi/liquidità, niente arrotondamento che aumenta rischio |
| P35 lifecycle | PR25 | PR45 | Pendenti durevoli, nuovo comando non sostituisce, scadenza market close |
| AMBIGUOUS cashout | PR45 | PR25/31 | Reconciliation esito, residuo e intento senza secondo hedge cieco |
| EventBus/drain | PR46 | PR17/37, PF-API | Backpressure, teardown, comando accettato, crash/drain senza perdita occultata |
| Correlation/audit | PR47 | PR26-a, PF-API-0, MCP-07 | request_id→operation_id→customer_ref→bet_id ricostruibili, log redatti |

## Contratti prodotto vigenti

**PAUSE:** blocca nuovi ordini; NON cancella unmatched; NON avvia cashout.
**STOP/RISK_STOP:** blocca nuovi ordini; cancella unmatched pertinenti;
tenta cashout delle posizioni pertinenti via authority Pickfair.
**EMERGENCY:** barriera massima immediata, nessuna eccezione generica.
**RESUME:** soltanto dopo nuova readiness completa.
Non toccare ordini manuali non pertinenti. Distinguere cashout ordinario dalla
riduzione rischio ordinata da STOP: vietare ogni invio indistintamente impedirebbe
l'hedge richiesto. Tutti i gate applicabili, identità, limiti e audit restano;
tentativo bloccato/incerto NON è chiusura riuscita. Nessun importo fisso €10/€10
appartiene a STOP/cashout: i limiti monetari owner sono contratto risk separato.

Telegram può essere rinviato come ingresso per il primo MCP, NON eliminato.
PR18/PR21 e collaudi esclusivamente Telegram non bloccano primo MCP/go-live MCP.
Le capacità business equivalenti vanno collaudate via MCP. Quando MCP è ingresso
operativo, **Telegram disconnected NON blocca LIVE**: PF-API-0 deve sostituire
quel gate con readiness control-plane/MCP, mantenendo tutti gli altri gate.

Nessun tool MCP master/copy/follow/fan-out. PR39–PR42 non sono prerequisiti
del primo MCP; restano nel perimetro totale Telegram. Vecchie catene seriali
non li trascinano nel cashout single-account PR43.

**MANUAL via MCP = DECISIONE ANCORA APERTA.** Non scegliere/implementare/dedurre
la semantica dal vecchio Telegram o da FIXED/MM. Al punto che la richiede:
**STOP → domanda owner → annotazione #426 → attesa**. Le parti indipendenti
FIXED/MM possono procedere; non dichiarare MANUAL certificato o master chiusa.

Switch SIM ↔ LIVE (con il relativo saldo fittizio/reale) soltanto su comando
owner esplicito secondo contratto Pickfair e decisione SIM/LIVE 07/10. App Key,
feed, broker, saldo ed execution mode sono assi distinti; Delayed App Key NON
autorizza ordini reali. Mai auto-LIVE; comando owner NON bypassa readiness.
P35: nuovo comando NON sostituisce/cancella automaticamente il pendente;
resta fino alla scadenza/chiusura mercato. Nessun hosting remoto o endpoint
pubblico: prima fase soltanto locale.

## SIM / LIVE — decisione owner 07/10/2026 (autorevole)

**SIM** usa la vera Betfair Delayed App Key e dati/mercati/quote reali dal percorso
Delayed; stesso motore Pickfair, stessi contratti, stesso Money Management, stessi
Risk/Safety gate, stessi comandi, stessi lifecycle e stesse funzionalità di LIVE.
Bankroll/saldo fittizio; BACK/LAY, cancel/replace, cashout, P/L ed exposure simulati;
reconciliation con il corrispondente comportamento SIM.
Esecuzione finale: **SimulationBroker**. Nessun ordine reale deve essere inviato a Betfair.

**LIVE** usa la vera Betfair Live App Key e dati/mercati/quote reali dal percorso
Live; stesso motore Pickfair della SIM, stessi contratti, stesso Money Management,
stessi Risk/Safety gate, stessi comandi, stessi lifecycle e stesse funzionalità.
Bankroll/saldo reale Betfair; BACK/LAY, cancel/replace, cashout, P/L, exposure e
reconciliation reali. Gli ordini vengono inviati a Betfair soltanto se tutti i gate
LIVE sono soddisfatti.

SIM e LIVE sono «una goccia d'acqua l'una dell'altra» dal punto di vista funzionale.
La differenza sta nel confine di esecuzione e nelle credenziali/modalità:
- SIM: MCP → Control API → Pickfair command/runtime contract → MM / Risk / Safety → SimulationBroker + Delayed App Key + bankroll fittizio.
- LIVE: MCP → Control API → Pickfair command/runtime contract → MM / Risk / Safety → Betfair reale + Live App Key + bankroll reale.

Vietato creare una SIM semplificata; vietato creare logiche business separate per modalità;
vietato creare un MM diverso per modalità; vietato creare un cashout diverso per modalità;
vietato creare un resolver diverso per modalità; vietato creare un command contract diverso
per modalità; vietato creare un comportamento MCP diverso per modalità.

Feed ≠ execution mode. Delayed App Key NON significa dati fittizi. SIM NON significa
mockare il mondo esterno: usa dati Betfair reali delayed e non espone denaro reale
all'esecuzione. Live App Key NON deve essere usata in SIM. Delayed App Key NON deve
essere usata per piazzare ordini reali. LIVE richiede Live App Key e readiness LIVE completa.
Saldo, App Key, execution mode e feed mode sono concetti distinti: non dedurre
l'execution mode dalla sola App Key. Modalità soltanto SIM e LIVE;
vietato introdurre LIVE_DATA_SIM; vietato introdurre PAPER;
vietato introdurre REAL; vietato introdurre HYBRID; salvo futura autorizzazione owner.

Saldi: **SIM = saldo/bankroll fittizio**; **LIVE = saldo reale Betfair**.
Combinazioni normali: Delayed App Key + SIM bankroll; Live App Key + real bankroll.

**SIM/LIVE PARITY** (requisito autorevole). Per ogni capacità MCP/Pickfair disponibile
in entrambe: stesso schema input, stesso schema output, stessi codici errore applicabili,
stesso request_id, operation_id, correlation_id e customer_ref, stesso resolver, stessi
controlli numerici, stessi limiti configurati salvo eccezioni mode-specific dichiarate,
stesso MM, stesso risk model, stessa semantica STOP, stessa semantica cashout, stessa
gestione AMBIGUOUS, stessa recovery/reconciliation per quanto applicabile, stesso audit
trail. Differenze ammesse soltanto quelle inevitabili fra SimulationBroker e Betfair
real-money execution.

**MCP: un solo set di tool** (place_order, cashout, cancel, replace, stop, resume,
reconcile): lo stesso tool funziona in SIM o LIVE in base allo stato autorevole
Pickfair; MCP non implementa due motori.
Vietato creare `sim_place_order`; vietato creare `live_place_order`;
vietato creare `sim_cashout`; vietato creare `live_cashout`;
nessun tool duplicato per modalità salvo necessità tecnica dimostrata e autorizzazione owner.

Switch di modalità: MCP può richiedere SIM/LIVE solo su comando esplicito owner;
MCP non decide autonomamente di andare LIVE; comando owner ≠ bypass dei gate;
LIVE non pronto → BLOCKED, nessun ordine reale, nessun fallback automatico,
nessun passaggio implicito SIM→LIVE.

Readiness SIM: Delayed App Key disponibile/valida, SimulationBroker operativo, bankroll
simulato inizializzato, runtime sano, MM/Risk/Safety disponibili, reconciliation SIM
coerente, Control API/MCP compatibili, feed freshness secondo contratto Delayed.
Readiness LIVE: tutto il pertinente della SIM più Live App Key, sessione/autenticazione
Live, saldo/account reale, Betfair transport reale, reconciliation reale, tutti i gate
real-money, gate installazione/backup/H24 richiesti per il collaudo LIVE e readiness
MCP/control-plane. Telegram disconnected NON blocca LIVE quando MCP è l'ingresso operativo.

PR15 — separazione mode-scoped: peak SIM non contamina drawdown LIVE; balance snapshot
mode-scoped; state/counter persistenti separati o identificati per modalità; SIM→LIVE
non trasforma il bankroll SIM nel riferimento di rischio LIVE.
La futura PR15 deve implementare/testare questo contratto.

#### Matrice SIM/LIVE e test obbligatori

| Funzione | SIM | LIVE | Parità |
|---|---|---|---|
| resolver | sì | sì | stesso risultato logico |
| preview | sì | sì | stesso contratto |
| BACK | simulato | reale | stesso percorso prima del broker |
| LAY | simulato | reale | stesso percorso prima del broker |
| dutching | simulato | reale | stesso calcolo |
| cancel | simulato | reale | stessa semantica |
| replace | simulato | reale | stessa semantica |
| cashout | simulato | reale | stesso calcolo/intento |
| exposure | simulata | reale | stesso modello |
| P/L | simulato | reale | stesso modello |
| STOP | simulato | reale | stessa semantica |
| AMBIGUOUS | simulato/testabile | reale | stesso lifecycle |
| reconciliation | SIM | Betfair | stesso contratto di stato |
| audit | sì | sì | stesso schema |

Test negativi obbligatori:
- SIM non può raggiungere Betfair order transport;
- SIM non può utilizzare Live App Key per piazzare ordini;
- LIVE non può usare SimulationBroker per fingere successo;
- cambio modalità non deve perdere exposure;
- cambio modalità non deve perdere pending intent;
- reload/restart non deve contaminare saldo SIM con saldo LIVE;
- peak/drawdown/stato MM non devono contaminarsi fra modalità;
- customer_ref mantiene la stessa semantica;
- stessa richiesta non può produrre prima ordine SIM e poi ordine LIVE per replay/retry;
- reconnect MCP non deve ripetere mutazioni precedenti.

**Gate di certificazione SIM/LIVE** — stato NOT_RUN finché eseguiti con prova sullo SHA:
crash/restart; concurrency; SQLite reale; SimulationBroker reale; Betfair reale/LIVE
quando autorizzato; whole-wiring cross-repo. Documentazione o CI verde non li rendono PASS.

## Control API e adapter

**Nuova Control API**, NON `betfair_market_api.py`: in-process, loopback-only,
autenticata, versionata. Read tramite servizi autorevoli; mutazioni tramite
runtime/command contract. Nessun accesso diretto Betfair o DB dal layer API,
nessuna copia MM/risk/cashout/reconciliation. La vecchia API ha hardening PR36
se distribuita, o ritiro esplicito verificato; non è il nuovo control-plane.

**Una sola tabella corrente delle route è nel corpo di pickfair-mcp- #1**.
Non usare route dei vecchi commenti. PATCH /v1/config; un solo /v1/cashout con
target/selection/market/all/fraction/percentage/Void/intent/lifecycle nel body.
Pending operations: GET /v1/operations?status=pending (stessa risorsa delle
operazioni, non una seconda FSM). Campi/payload completi vengono stabilizzati
in PF-API-0/1/commands, senza inventare semantiche prodotto.

Ogni mutazione: request_id, operation_id e correlation_id, esito asincrono
machine-readable consultabile. Nessun retry cieco; rifiutare campi extra,
NaN/Inf/bool numerici. Version mismatch **CONTRACT_INCOMPATIBLE fail-closed**;
/v1/meta espone version/capabilities/git SHA/schema hash. Capability negotiation
e compatibility matrix prima delle mutazioni, anche dopo reconnect.

pickfair-mcp NON importa BetfairClient, TradingEngine o OrderManager; NON apre
il DB Pickfair, neppure in sola lettura; NON usa credenziali Betfair; NON parla
direttamente con Betfair; NON duplica formule MM, stake calculation, cashout
o reconciliation; NON implementa master/copy/follow. Tutto passa dalla Control API.

| Passo | Contratto |
|---|---|
| PF-API-0 | Neutral command contract, readiness MCP-aware, provenance/correlation/customer_ref |
| PF-API-1 | Read-only Control API |
| PF-API commands | Comandi mutanti via runtime/command contract |
| MCP-01 | Foundation/governance/version handshake/no-bypass |
| MCP-02 | Discovery/read-only |
| MCP-03 | Config SIM |
| MCP-04 | Trading SIM |
| MCP-05 | Cashout/STOP/resume/reconcile |
| MCP-06 | LIVE gated |
| MCP-07 | Certificazione E2E/cross-repo |

PF-API-0 → PF-API-1 → PF-API commands.
Numerazione nominale delle schede (NON ordine dei gate di certificazione):
MCP-01 → MCP-02 → MCP-03 → MCP-04 → MCP-05 → MCP-06 → MCP-07.

**Ordine operativo autorizzato:** MCP-01 → MCP-02 → MCP-03 → MCP-04 →
MCP-05 → MCP-07 (SIM) → MCP-06 → MCP-07 (finale LIVE/cross-repo).
La parte SIM di MCP-07 si esegue dopo MCP-05 e PRIMA di MCP-06; il dossier
finale completa MCP-07 dopo LIVE. Non imporre LIVE per certificare sola SIM.

## Milestone separate e prove #351

### A. MCP SIM

- [ ] Core ordini/risk della sequenza, customer_ref e audit, resolver PR08 per nome.
- [ ] Feed, saldi, MM FIXED/MM, SL/TP/trailing, dutching, cashout/P35 provati.
- [ ] PF-API-0/read/commands e MCP-01…05 reali, non solo stub.
- [ ] SIM E2E: restart/reconnect, duplicate request, response lost, AMBIGUOUS,
  STOP/resume, cashout, reload config, version mismatch, no-bypass.
- [ ] Auth/schema/capability, prompt injection, network allowlist e secret scan.
- [ ] Prove su SHA esatti dei due repo; DB temporaneo reale/concurrency/chaos.
- [ ] Readiness SIM e matrice SIM/LIVE della decisione 07/10, prove negative incluse;
  Delayed App Key reale, SimulationBroker, bankroll fittizio.

MCP-06 LIVE non serve. MANUAL resta fuori fino alla decisione. Il PASS SIM
non promuove LIVE, installato o chiusura totale; chiusura master richiede
risolvere anche le decisioni aperte pertinenti.

### B. MCP LIVE

- [ ] Tutti i requisiti SIM più comando owner esplicito, sessione/Live key/cert.
- [ ] Readiness LIVE completa (Live App Key, sessione Live, saldo reale, transport e
  reconciliation reali) e SIM/LIVE PARITY verificata sugli stessi contratti.
- [ ] Readiness MCP/control-plane, feed freshness ratificata, risk e deploy gates.
- [ ] Backup/restore P07 prima dei test reali, package/install Windows poi Linux.
- [ ] Restart/H24 pertinenti, runtime whole-wiring e sweep sui pacchetti/SHA esatti.
- [ ] MCP-06 e certificazione LIVE MCP-07, nessun PASS dedotto da CI verde.
- [ ] **Gate corrente PR53/54/59/60 e #351 conservato**, incluso PR58→PR59.

**UPDATER_SCOPE = OWNER_OPEN; default conservativo Caso A.** Le fonti correnti
richiedono PR59 totale prima del go-live e PR59 dipende da PR58. Non è dimostrata
una certificazione MCP LIVE distinta autorizzata senza updater (Caso B).
Domanda registrata #426: mantenere Caso A oppure autorizzare Caso B con matrice
MCP whole-wiring/sweep completa del suo perimetro, senza chiudere PR59/60 totali?
Finché manca risposta: niente dichiarazione READY_LIVE senza i gate attuali.
La SIM e questa PR documentale non dipendono dalla scelta.

### C. CHIUSURA TOTALE PICKFAIR

- [ ] Telegram completo (PR06/18/21 e collaudi trasporti), GUI/catalog callback PR05/07.
- [ ] PR39–42, fan-out/follower e collaudi isolamento/recovery.
- [ ] Restante osservabilità/storico/cleanup/governance #461 vigente.
- [ ] Updater PR55–58, packaging totale, H24 totale, backup/restore completo.
- [ ] Whole-wiring PR59, sweep PR60, dossier finale Windows/Linux e #351 completo.
- [ ] Chiusura coordinata master storiche, #437 per ultima.

Quant Layer/esclusi e rinvii owner fuori perimetro non diventano obbligatori.
Packaging, backup, audit e H24 necessari al LIVE non sono rinviati integralmente
alla chiusura totale. I dati della campagna (OS/mercati/freshness/SHA) sono
prerequisiti ambientali ancora da fornire/ratificare, non nuove scelte inventate.

### Test MCP obbligatori

Prompt injection: eventi, mercati, audit, provider, note/log sono DATI e non
autorizzano order, mode switch, config mutation o secret access. Provare prompt
malevoli e zero mutazioni non comandate dall'owner.
Network allowlist: rifiutare destinazioni arbitrarie, redirect non autorizzati,
Betfair diretto, bypass DB/socket/file. Solo Control API locale autenticata.
Reconnect/restart: rifare version/capability handshake, nessun replay automatico
di mutazioni; recuperare stato tramite request_id, operation_id, reconciliation.
No-bypass: ogni mutazione **MCP → Control API → Pickfair authority**; sabotare
un collegamento alla volta deve far fallire il test.

Unit/integration/acceptance/chaos, DB temp reale, concurrency, restart,
SIM/LIVE parity, customer_ref/correlation_id; schema/auth/versioning,
extra fields/non-finiti, install Windows/Linux, crash dei due processi separati,
STOP in-flight/reload config. Verbali: milestone, SHA entrambi, versioni/hash,
OS/key/mode, input, oracle, effetto positivo/negativo, exit code, limiti, residui.
PASS/FAIL/BLOCKED_ENV/NOT_RUN/RINVIATO separati: nessuna spunta da merge.

## Reviewer e merge

Per reviewer e merge applicare la decisione owner vigente **#426/P41** e
successive decisioni pertinenti, inclusi merge #492/#493. Non riattivare
workflow o label sospese sulla base di istruzioni storiche. Una modifica in
PR aperta non equivale a comportamento già presente sul main.
Sol GPT-6.1 max e Grok 4.7 sono attivi; Fugu/Fable/Astra sospesi secondo P41.
Codex/advisory: trattare rilievi reali, indisponibilità non inventa approvazione.
Verificare review reali current-head e check settled/readiness/zero thread;
gate file-policy resta merge manuale owner. Questa PR tocca AGENTS/CLAUDE:
**NON auto-mergiare**; né attivare una nuova PR core prima della conclusione.

## Agent readiness contract

SE RICEVI «Lavora sul percorso MCP di Pickfair», FAI:
1. Leggi #491.
2. Leggi #489.
3. Leggi #461.
4. Leggi #426.
5. Leggi #453.
6. Leggi #351 se serve collaudo.
7. Leggi pickfair-mcp- #1 e AGENTS/CLAUDE/spec.
8. Verifica main e PR aperte.
9. Identifica milestone SIM / LIVE / totale.
10. Identifica scheda corrente nella sequenza unica, prima PR26-a dopo questa PR.
11. Rifai Phase 0 sul main corrente e dopo ogni merge.
12. Lavora una sola PR nel repository.
13. Non inventare decisioni aperte (MANUAL; eventuale Caso B updater).
14. Non dichiarare DONE senza prove e review current-head.

## Verifica delle issue esterne

`docs/governance/mcp_issue_sections.json` contiene le sezioni operative gestite
nei corpi delle issue. Sono copie di verifica, non una roadmap alternativa.
I test offline proteggono testo/puntatori e numerazione; NON certificano da soli
che le issue GitHub siano sincronizzate. Prima di READY leggere i corpi live e
confrontare le sezioni esatte con questo file. I commenti storici conservano
prove/fonte/data; i loro ordini concorrenti sono marcati superati puntualmente.
