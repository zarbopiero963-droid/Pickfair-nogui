# Changelog

All notable changes to Pickfair are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Changed
- **#562 P0 — nessuna build doppia dopo il ripristino di uno snapshot (`scripts/pr_trusted_dispatcher.py`):**
  - causa (rollback T252): cursore dei commenti e tabella `reruns` tornano indietro insieme allo snapshot, quindi i
    rerun e le transizioni già eseguiti venivano rigiocati come nuovi;
  - ogni build porta l'identità dell'intento (metadato Buildkite `pf_intent` = tid). Prima di creare si cercano le
    build remote dello stesso intento: in corso ⇒ adottata, conclusa ⇒ `recovered` senza ripubblicare, non
    verificabile ⇒ nessuna build e `pending` subito sull'head (`RECOVERY_UNCERTAIN`, poi `error`; Codex P1 su #583). Dopo la creazione si ricontrolla, per due
    istanze concorrenti;
  - build ritrovata conclusa: si ripubblica solo se nessuno status con build uguale o più recente è consegnato
    (correlazione per numero di build: un PASS vecchio non copre mai un FAIL più recente); `seq` riportato indietro dallo snapshot: metadato `pf_pr`, si recupera il `seq` più alto della PR;
    la transizione si riallinea all'ultima valutazione remota se è dello stesso head, altrimenti prende un tid
    oltre il massimo (Sol e Codex su #583); stesso intento con un altro evento ⇒ evento riallineato, con un'altra env ⇒
    conflitto, nessuna build (Codex su #583);
  - mai l'esito di una build vecchia sopra lo status di una più recente: confronto per numero di build prima di ogni
    pubblicazione o riconsegna finale; `success` non verificabile non si pubblica (Codex su #583);
  - rerun rifiutati se il commento ha più di 6 ore (orario GitHub) o se la PR manca dall'ultimo elenco delle aperte.

  Test RED→GREEN su snapshot SQLite reali (rerun concluso, in corso, crash fra creazione e registrazione, Buildkite
  illeggibile o a pagina piena, rerun nuovo legittimo, due istanze, cambio di head, transizioni rigiocate) e
  mutazioni `td_p0_*`. Limiti dichiarati in `docs/trusted_dispatcher.md`.
  - **#562 P1 — readiness di SHA superati:** verificato che non trattengono la coda trusted oltre il giro che vede il
    nuovo head. La build viene annullata nello stesso giro che crea l'overlap nuovo; quella in attesa non ha build. Due test
    di regressione; nessuna modifica di codice (`docs/trusted_dispatcher.md`).
- **#562 PR-A — suite completa automatica su ogni PR (`scripts/pr_trusted_dispatcher.py`,
  `scripts/pr_full_suite_pipeline.py`):**
  - con la nuova chiave obbligatoria `full_suite.enabled: true` il dispatcher trusted avvia a ogni transizione e
    rerun anche la pipeline `pf-dedicated-ci` della lane non fidata, sul commit **esatto** e sul branch della PR, e
    pubblica il contesto `full-suite`;
  - `success` solo se la build è `passed` sul commit dell'head e contiene tutti i 49 step attesi
    (`FULL_SUITE_STEP_KEYS`), ognuno `passed` e non soft-fail; build fallita ⇒ `failure`; step mancanti o stato
    incoerente ⇒ `error`;
  - gli step (Onde W1–W4, equivalenti ai workflow Actions su PR, mapping verificato da test) li genera
    `scripts/pr_full_suite_pipeline.py`, caricato dallo step di avvio da `origin/main`, mai dal checkout della PR;
  - una configurazione senza `full_suite` è `NOT CONFIGURED`.
  - la simulazione di merge (`w4-merge-simulation`) ricrea il proprio branch con `git checkout -B`: nel checkout
    persistente dell'agent una seconda build non fallisce più per il branch già esistente (Sol su #584).
  - il recupero dopo uno snapshot (P0, #583) vale anche per la suite: ricerca per intento sul commit dell'head, massimo
    della PR con il solo filtro `pf_pr` (le build stanno sui branch della PR), consegna correlata e `pending` di recupero
    sul contesto `full-suite` e sulla pipeline `pf-dedicated-ci`, mai su quelli di un guard trusted.
  - suite ritrovata conclusa dopo uno snapshot: conserva l'esito consegnato da quella build (la readiness non attende
    fino all'`error`); il timeout della suite resta in config anche con `full_suite.enabled: false`, così un job già
    nell'archivio arriva al proprio esito durante un rollback (Codex su #584).

  - con `readiness_wait` e `full_suite` entrambe attive, `readiness_wait.max_seconds` ≥ `full_suite.build_timeout_seconds`
    (la readiness attende anche lo status `full-suite`).
  - **readiness prematura (#562 P0):** con la full-suite attiva la readiness non parte e non passa senza la full-suite
    dello **stesso intento** (stesso `delivery_id`, PR e SHA). Servono l'esito finale pubblicato dal dispatcher e lo
    stesso esito come ultimo status `full-suite` sull'head; assenza, `pending` o incoerenza ⇒ attesa, mai successo.
    Con la suite `failure`/`error` la readiness è negativa senza build. Un `success` della readiness senza suite
    superata si pubblica `error`. `full_suite.enabled: true` richiede `readiness_wait.enabled: true`.

  Test RED→GREEN, 12 mutazioni nuove (gate del dispatcher 120/120).
- **#562 F1 — paginazione GitHub reale nel dispatcher (`scripts/pr_trusted_dispatcher.py`), regressione di #576:**
  - nel collaudo T252 una PR con 107 check run aveva il link `next` nella forma `/repositories/1189713160/…`, che il
    client rifiutava: la readiness non partiva mai (fail-closed, nessun PASS) e Grok ha fatto rollback;
  - il link `next` ora è validato in modo stretto da `GitHub.next_page_url`: host esatto, stesso path nella forma
    `/repos/<repo>` o `/repositories/<TRUSTED_REPO_ID>` (id fisso), stessi parametri senza duplicati, `page`
    successiva. La pagina si chiede all'URL canonico ricostruito dal client, mai a quello ricevuto;
  - lo stesso difetto latente su `pulls` e `issues/comments` oltre i 100 elementi è chiuso dalla stessa funzione.

  Test RED→GREEN (107 check su 2 pagine, 19 link manomessi, conteggi incoerenti, riavvio e head nuovo); il finto
  GitHub dei test usa ora i link reali. 6 mutazioni nuove (gate del dispatcher 108/108).
- **#562 — readiness non bloccante (`scripts/pr_trusted_dispatcher.py`, OWNER GO opzione A):**
  - con la nuova chiave obbligatoria `readiness_wait` il dispatcher crea la build `pr-merge-readiness` solo quando
    gli altri check dell'head sono chiusi da `settle_polls` giri (letti da `commits/<sha>/check-runs` e `…/statuses`),
    senza attendere i propri self-check;
  - durante l'attesa nessun agent Buildkite è occupato: la readiness lanciata al push, con 300 s di attesa, falliva
    su ogni PR la cui CI durava di più (#575, build #16);
  - oltre `max_seconds` (7200) ⇒ `error`; un head nuovo sostituisce l'attesa; lo stato dell'attesa sopravvive al
    riavvio.

  Test RED→GREEN, 12 mutazioni nuove (gate del dispatcher 102/102).
- **#562 — documentazione della lane trusted riallineata (`docs/ci_trusted_lane.md`):**
  - il token GitHub della lane è un token di installazione dell'App `pickfair-trusted-status`, in sola lettura, scritto
    in tmpfs da `pf-lane-token-mint.service` e rinnovato da `pf-lane-token-refresh.timer`; prima il documento
    descriveva un file statico in `LoadCredential` (deriva D3);
  - aggiunto il runbook di emergenza senza VPS (§5): ripiego su GitHub Actions, nessun failover automatico, e nessuno
    status pubblicato a mano;
  - il limite «NOT VERIFIED end-to-end» sostituito dal riferimento alla certificazione E2E.
  - requisito della CLI corretto in `gh` ≥ 2.48: la readiness usa `gh api --paginate --slurp`, assente in 2.45/2.47
    (gate Sol/Grok `UNKNOWN` sulla lane, #575 build #17).

  Nessuna modifica al codice.
- **#562 D2: risultato del guard caricato come artifact su Buildkite (`scripts/ci/pr_overlap_guard.sh`,
  `scripts/ci/pr_merge_readiness.sh`):** nella campagna E2E reale (#573, build `pr-overlap-guard` #17 e
  `pr-merge-readiness` #10) il dispatcher non trovava `.trusted-guard/<guard>.json` tra gli artifact e pubblicava
  `error` «risultato del guard assente» (fail-closed). Con `BUILDKITE` impostata il wrapper esegue
  `buildkite-agent artifact upload` del risultato, anche con verdetto FAIL, e conserva l'exit del guard; upload non
  riuscito ⇒ exit 1. Fuori da Buildkite (workflow Actions) nulla cambia. Test RED->GREEN e mutazioni manuali.
- **#562 D1 — credenziale di systemd 255 `0440 root:root` letta via ACL (`scripts/pr_trusted_dispatcher.py`):**
  il collaudo VPS di Grok (c.6450558425) ha mostrato che `LoadCredential` crea il file `root:root` nella directory
  `0550 root:root` e lo rende leggibile al servizio con una voce ACL, mentre il controllo esigeva il gruppo del
  processo: `check-config --check-secrets` e `serve --dry-run` uscivano `NOT CONFIGURED`. Ora il gruppo può essere
  root o quello del processo e la ACL del file, letta sul descrittore aperto, deve ammettere solo root e il servizio,
  senza permessi ad «altri»; ACL malformata o illeggibile ⇒ rifiutata. Restano invariati directory, proprietario,
  permessi esattamente 0440, `O_NOFOLLOW` e 0400/0600 fuori da systemd. Test RED->GREEN e mutazioni nuove.
- **#562 — dispatcher trusted: credenziali di systemd e modalità di collaudo (`scripts/pr_trusted_dispatcher.py`):**
  - `LoadCredential` di systemd 255 espone il token 0440: accettato solo nella directory `$CREDENTIALS_DIRECTORY` del
    servizio (directory di root o dell'utente del servizio e non scrivibile da gruppo o altri, file di root o
    dell'utente del servizio, gruppo del
    processo, permessi esattamente 0440); fuori resta 0400/0600;
  - `serve --dry-run`: collaudo senza scritture esterne (trasporto solo GET, publisher che rifiuta, nessuna build né
    status, archivio separato `<state_db>.dry-run`), con le build e gli status attesi nel log; il ramo DRY-RUN
    precede anche gli annullamenti pendenti (review indipendente); limiti documentati (seconda review owner): il
    ciclo `creating → dispatched → publishing` non si percorre senza build reali e si certifica nel collaudo reale,
    il token GitHub di lettura viene comunque coniato, l'archivio `.dry-run` va rimosso per ripetere la prova;
    fallback e riconsegna provati senza scritture;
  - test RED->GREEN e mutazioni nell'area `trusted_dispatcher`.
- **#562 — dispatcher trusted della lane Buildkite (`scripts/pr_trusted_dispatcher.py`), NOT CERTIFIED:**
  - **polling autenticato dell'API GitHub al posto del webhook HTTPS** (decisione owner PF-562 opzione B): nessun web
    server; ogni `poll_interval_seconds` (60 s, configurabile 10..600) legge le PR aperte con paginazione completa
    (`Link rel=next`, al massimo 50 pagine), ogni elemento verificato con tipi stretti; risposta incompleta,
    malformata, errore API, rate limit o credenziali mancanti/scadute ⇒ nessuna transizione e nessuno status;
  - credenziali di lettura da `github.token_command` (token breve con scadenza, rinnovato prima della scadenza e
    scartato su 401; token classico con scope rifiutato), separate dal publisher; solo GET su
    `/repos/{repo}/pulls`, `/repos/{repo}/issues/comments` e `/repos/{repo}/collaborators/{login}/permission`;
  - stato per PR in SQLite (`pr_state` con contatore `seq`): nuova PR, nuovo commit, chiusa/riaperta, ritorno a uno
    SHA già visto e uscita dalla bozza danno ciascuno una transizione con id stabile `p<PR>-t<seq>-<sha12>`; la
    riconciliazione dopo il riavvio confronta lo stato salvato con quello osservato; chiusura solo dopo due giri
    consecutivi senza la PR, che annulla le build;
  - rerun manuale `/pf-trusted-rerun <sha40>` in un commento della PR: autore in `rerun.allowed_logins`, commento
    non modificato, PR aperta e idonea, SHA uguale all'head corrente; id `p<PR>-r<commento>-<sha12>`, registrato in
    `reruns` (mai rieseguito due volte), comandi precedenti al primo avvio ignorati; autore account utente con
    permesso ATTUALE di scrittura sul repository (`admin`/`maintain`/`write` letto da `collaborators/{login}/permission`;
    `author_association` non basta: Sol su `dab1e94`), permesso non verificabile ⇒ lettura annullata, allowlist iniziale
    `zarbopiero963-droid` (OWNER GO PF-570); rifiutato = solo registro, il
    rerun non tocca i required check e il verdetto resta del guard;
  - un head nuovo annulla le build del vecchio, creazione ambigua riconciliata senza seconda creazione, ripresa dopo
    il riavvio; avvia solo `pr-overlap-guard` e `pr-merge-readiness` su `main` con le cinque variabili del contratto
    di Grok (#562 c.6438092317 §5): le pipeline trusted eseguono solo script di `main`;
  - `success` solo con build `passed` e risultato `PASS` del guard sullo stesso head; tutto il resto `failure` o
    `error`; pubblicazione solo tramite il comando configurato (helper root di Grok);
  - configurazione tutta obbligatoria: chiave mancante o sconosciuta ⇒ `NOT CONFIGURED`, il servizio non parte; punti
    infrastrutturali non decisi `BLOCKED_CONTRACT` in `docs/trusted_dispatcher.md`, con la checklist dei collaudi
    reali per Grok;
  - smoke trusted: senza contesto PR del dispatcher il guard esce con motivo esplicito («contesto PR verificato
    assente»), mai con l'errore di argparse né con un numero di PR di ripiego; lo step va portato sul wrapper
    `scripts/ci/pr_merge_readiness.sh` (configurazione Buildkite, a cura di Grok);
  - rilievi della review indipendente corretti prima della PR: head che torna con un force-push, pubblicazione
    fallita ritentata, una sola istanza sull'archivio (`flock`) con transizioni condizionate, build orfane di una
    creazione ambigua annullate, stesso `updated_at` senza sostituzione, `Content-Length` non ASCII, COMMIT
    fallito, timeout del publisher in configurazione;
  - rilievi Codex su #569: `pending` ritentato mentre la build gira, risposta 5xx non JSON alla creazione trattata
    come ambigua e riconciliata; migrazione idempotente di un archivio creato da una versione precedente (Sol); annullamento non confermato ritentato e risposta HTTP troncata trattata come errore di trasporto (Codex); audit indipendente di liveness: ogni risposta di creazione non interpretabile ambigua, elenco vuoto o pieno non prova l'assenza, risultato ritentato, annullamenti non confermati ricordati (stato `canceling`), fallback sul timeout per le eccezioni inattese; obbligo di annullamento persistente e indipendente dallo stato, nessuno stato finale con build da annullare (Codex); audit avversariale degli invarianti: finestra di annullamento dall'adozione, fallback anche in pubblicazione, `pending` durante la riconciliazione, redelivery dopo la pulizia ancora replay;
  - rilievi Sol sul range completo: `pending` fallito ritentato anche quando l'artifact del risultato non è ancora
    scaricabile; esito `publish_failed` riconsegnato ogni 300 s finché è il più recente per quel guard della PR e dello
    SHA (mai sopra l'esito di un'altra PR sullo stesso commit); una valutazione nuova di uno SHA sostituisce i job
    attivi più vecchi dello stesso SHA e guard anche di altre PR (Codex P1),
    sull'head corrente di una PR aperta (mai un vecchio `success` lasciato valido, mai un esito vecchio sopra uno nuovo);
    consegna registrata in `final_published` (colonna nuova, migrazione idempotente): un esito mai consegnato non
    finisce `done` alla conferma di un annullamento (Codex P2 su `59b071e`); test del dispatcher saltati in modo esplicito fuori da POSIX (`fcntl`);
  - integrazione con l'infrastruttura di Grok (#562 c.6444963640): publisher `pf-status-publish-client` sul socket locale
    (stessa argv, nessun privilegio al dispatcher), token da `pf-dispatch-github-token`, descrizioni nel contratto del
    socket (ASCII stampabile ≤ 140, mai `` ` `` `$` `\`, sostituzioni visibili, originale nei log, invio rifiutato fuori
    formato), artifact `.trusted-guard/<guard>.json`; rischio residuo del token Buildkite admin registrato;
  - rerun: permesso attuale di scrittura letto dall'API (Sol su `dab1e94`), limite inferiore persistente del cursore
    dei commenti (Codex P2 su `893390a`: nessun comando anteriore al primo avvio, stesso secondo compreso), id di
    commento fuori misura trattato come comando malformato;
  - test offline con un finto GitHub paginato e un finto Buildkite; le prove HMAC del webhook sono sostituite dalle
    prove sulle credenziali di lettura e sul perimetro degli endpoint; mutazioni nell'area `trusted_dispatcher` e una
    nell'area `trusted_gate`.
- **PF-569 AI REVIEW FULL COVERAGE RECOVERY — budget del diff di Sol e Grok (owner GO 27/10/2026):**
  - tetti portati a 150.000 caratteri per file e 400.000 totali per GPT-6.1 Sol e Grok 4.7 (prima 7.000/24.000
    e 5.000/18.000), dimensionati sul diff della #569 con margine per il passaggio al polling (PF-562): il dispatcher trusted
    e i suoi test arrivano interi al modello;
  - ogni commento di review registra gli SHA completi esaminati e la copertura misurata del payload inviato
    (`rr_coverage_summary`); comportamento fail-closed invariato (buco misurato = `VALID_INCONCLUSIVE`);
  - nuovo guard `tests/guardrails/test_ai_review_full_coverage_budget.py` sulla catena estratta dai workflow.
- **#566 — deduplica dei check run fail-closed (`scripts/pr_automation_controller.dedupe_github_check_runs`):**
  - sull'head corrente nessun check run omonimo viene più scartato, salvo la copia esatta della stessa check run
    (stesso id, stato e tempi). Prima vinceva il più recente fra i check omonimi della stessa sorgente, quindi un PASS di un altro
    workflow run, di un job fratello o di un rerun parziale nascondeva un FAIL sullo stesso SHA;
  - i tentativi superati li esclude GitHub: i check run vanno letti con `filter=latest` (il default), che restituisce
    solo l'ultimo tentativo di ogni job; con `filter=all` il tentativo fallito resta e blocca;
  - `summarize_current_head_check_state`: una conclusione inconcludente dà `current_head_inconclusive` /
    `needs_manual` invece di `ready`;
  - test RED/GREEN con dati di run reali (rerun su `49518cc`, check concorrenti su `b4e6604`), compresi i casi dei
    job fratelli e del rerun di un solo fratello (rilievi GPT-6.1 Sol su #568); cinque mutazioni nell'area
    `check_dedupe`.
- **PF-GOV-DOC-WORKFLOW-ALIGN-01, Fase 2 — `overlap-check` sullo script trusted, diagnostica separata:**
  - `.github/workflows/pr-overlap-guard.yml` gira solo su `pull_request` ed esegue lo script canonico
    `scripts/ci/pr_overlap_guard.sh` (corretto su `main` dalla Fase 1) da un checkout del commit base, mai del codice
    della PR; token in un file 0600, permessi in sola lettura; il nome del check resta `overlap-check`;
  - nuovo `.github/workflows/pr-overlap-guard-diagnostic.yml`: collaudo manuale con identità distinta
    (`overlap-diagnostic`), solo `workflow_dispatch` e solo da `main`, che non può soddisfare il required check;
  - test su script e workflow: regola «una sola PR aperta» sul codice vero, unico workflow che pubblica
    `overlap-check`, nessuno step saltabile (`if:`/`shell:`), nessun `env` che ridefinisca `GITHUB_*`/`RUNNER_*`, un
    PASS (diagnostico o duplicato) non maschera il FAIL in merge readiness;
  - cambi di comportamento dichiarati del check: tutte le pagine dei file, PR illeggibile = FAIL, PR da fork o con
    head diverso = FAIL, nessuna PR aperta = FAIL, base diversa da `main` = FAIL; il check gira anche su `edited`,
    così un cambio di base lo ricalcola.
- **PF-GOV-DOC-WORKFLOW-ALIGN-01, Fase 1 — governance allineata a #561 e #563, token Actions accettato:**
  - `CLAUDE.md`, `AGENTS.md` e `docs/auto_pr_flow_spec.md` §0.9 (estensione documentale autorizzata) elencano i
    tre script della lane trusted fra i file a merge owner, la spec anche i cinque workflow reviewer per nome, e
    descrivono lo stub Microstake come minimo exchange (`core.stake_precision.EXCHANGE_MIN_ORDER`, riferimento owner
    1 EUR; in LIVE BACK 1000 / LAY 1.01, in SIM un prezzo che non incrocia il book) invece di 2 EUR;
  - nuovo test di coerenza fra il classificatore `OWNER_MANUAL_MERGE_FILES` e i tre documenti;
  - `scripts/ci/trusted_pr_guard.py` rifiutava il `GITHUB_TOKEN` reale di Actions (`[A-Za-z0-9_]{20,255}`, trovato
    collaudando su #564). Ora il token è validato come valore sicuro per l'header: ASCII stampabile senza spazi, da
    20 a 8192 caratteri, FAIL prima di qualunque richiesta; nuova mutazione `tg_token_header_injection`;
  - il passaggio di `.github/workflows/pr-overlap-guard.yml` allo script canonico e la diagnostica manuale separata
    sono la Fase 2, in una PR successiva: introdotti qui, il loro `overlap-check` girerebbe sullo script vecchio di
    `main` e sarebbe rosso per costruzione (decisione owner: bootstrap in due fasi).

### Added
- **Lane Buildkite trusted (#562) — `scripts/ci/pr_overlap_guard.sh` e `scripts/ci/pr_merge_readiness.sh`:** due
  controlli senza argomenti per le pipeline trusted su `main`. PR da `PF_PR_NUMBER`/`PF_PR_HEAD_SHA`, dati solo dalla
  GitHub API, nessun codice della PR eseguito. Verificano repository, PR aperta, base nella storia di main, head nello
  stesso repository e uguale a quello atteso (riletto a fine valutazione). Token da file 0600 in sola lettura (token
  classico con scope rifiutato), mai stampato, nessun redirect seguito. Overlap guard fail-closed: tutte le pagine,
  PR illeggibile = FAIL. Readiness: PASS solo con `can_merge` vero e gate Sol/Grok PASS. Contratto in
  `docs/ci_trusted_lane.md`. PASS mai con label `manual-review-required` o file a merge owner nel diff; i tre script
  sono registrati a merge owner (`scripts/pr_autonomy_policy.py` e pattern dei reviewer Sol/Grok); label riletta a fine attesa; rename con percorso di origine; 151 test, 28 mutazioni uccise.

### Changed
- **PF-MICROSTAKE-OWNER-FINAL-RECTIFICATION-01 — soglia owner 1 EUR e adapter Microstake LIVE (decisione owner
  24/10/2026):** il minimo del singolo ordine non è più 2 EUR ma il riferimento owner di 1 EUR, in una fonte unica
  (`core.stake_precision.EXCHANGE_MIN_ORDER`) letta da engine, `OrderRouter`, saga e safety layer. Da 1 EUR in su
  l'ordine va diretto; sotto, Microstake nativo con stub temporaneo di 1 EUR (il più piccolo piazzamento accettato),
  riservato come esposizione reale entro i fondi disponibili. Audit di ogni uso di 2 EUR in
  `docs/microstake_native.md` §6-bis: le soglie di prezzo della scala Betfair restano invariate.
  - Nuovo adapter LIVE `core/microstake_live_port.py`: stessa saga e stesso esecutore della SIM sopra il
    `BetfairClient` reale (stub BACK 1000 / LAY 1.01 con `customerOrderRef`, `sizeReduction` single-shot, replace
    con nuovo betId, riconciliazione da `listCurrentOrders`, prova del book da `listMarketBook`), store separato
    `microstake.sagas.live`, saga che rifiuta una policy non coerente con la porta.
  - `BetfairClient.place_bet(customer_order_ref=...)` (opzionale, default invariato) e `BetfairClient.reduce_order`.
  - Engine: adapter LIVE solo per il `BetfairClient` reale, mai riusato in SIM, dietro `is_live_allowed()` e il
    circuit breaker degli invii; recovery per modalità con blocco delle saghe pendenti dell'altra modalità.
  - Review indipendente del diff completo: replace in `TIMEOUT` = esito ignoto (mai rollback); lettura dell'esito
    da `listClearedOrders` per gli ordini spariti; recovery che chiude lo stub sparito senza abbinato, attiva
    l'ordine richiesto già abbinato, riparte da una saga FAILED senza ordine e usa l'importo dello stub persistito
    (saghe da 2 EUR precedenti); il Microstake LIVE alimenta il circuit breaker.
  - Mandato owner PF-561-LIVE-MICROSTAKE-P1-HARDENING-01: rollback dichiarato solo con la rilettura dell'ordine a
    residuo zero (P1 riprodotto: `cancel_orders` ok=True con l'istruzione fallita chiudeva ROLLED_BACK con lo stub
    vivo); invarianti del coordinatore su ROLLED_BACK/ACTIVE; recovery LIVE che non completa mai un vecchio ingresso
    (casi A-D, SIM invariata); rilievi Codex (recovery senza gate LIVE, replace con stub abbinato, identità del
    replacement) corretti; gate d'ingresso riletto dentro la saga LIVE prima di riduzione e replace (STOP armato
    durante la saga ⇒ cancellazione verificata, mai l'ingresso) e protezione delle saghe LIVE anche quando il
    recovery SIM fallisce (rilievi Codex P1 su beff2ef); il gate richiede anche il runtime ACTIVE (STOP/PAUSA
    dell'utente), safe mode non in lockdown e sessione valida (review indipendente del diff completo, P1).
  - Test con la vecchia soglia riallineati a 1,00/0,99. LIVE resta NO-GO: nessun ordine reale, accettazione Betfair
    da verificare nel collaudo (elenco in `docs/microstake_native.md` §6).
- **PF-STAKE-PRECISION-MICROSTAKE-01 / -FINAL-01 — stake per difetto al centesimo, Microstake preservato (decisione
  owner 24/10/2026):** emerso dalla prima sessione integrata della Fase 4. Dopo un settlement il money management
  chiedeva `1.9647599999999998`: il Microstake lo bloccava (`invalid_size_precision`) e sopra il minimo l'exchange
  riceveva `3.08595`. Ora lo stake inviato è lo stake economico troncato al centesimo (`core/stake_precision.py`,
  ROUND_DOWN, nessuna tolleranza: 1,9999999999 è 1,99).
  - Il money management calcola la stessa formula in aritmetica razionale esatta sugli operandi: l'errore binario
    si corregge alla fonte e nessuna divisione periodica si arrotonda prima del troncamento (rilievo Codex P2).
  - Normalizzazione prima della riserva: money management e bet manuale.
  - Difesa del contratto nell'engine, prima di risk gate e della scelta diretto/Microstake.
  - Dutching e hedge invariati.
  - Corretto anche il cashout delle posizioni Microstake, che non trovava la partita (identità del bot in
    `Database.get_bot_active_orders`).
  - Aspettative superate dalla decisione riallineate: rettifica 17/10 (0,1002 → 0,10), F032 (stake troncato),
    `test_pr512_recovery_fixes` (l'engine tronca invece di rifiutare; il coordinatore rifiuta ancora).

  Test: `tests/core/test_stake_precision_microstake_01.py`, `tests/f026/test_stake_precision_e2e.py`. Mutazioni
  `sp_*`. LIVE resta NO-GO: Microstake LIVE non implementato, adapter nella PR successiva (rettifica owner, soglia
  1 EUR).
- **Governance post-#556: una review AI senza bloccanti non è un PASS se il verdetto non è conclusivo (owner
  merge):** su #556 il classificatore di #552 registrava `VALID_PASS` per verdetti che dicevano «troncato», «NON
  VERIFICABILI», «merge non approvato». Un PASS richiede ora tre condizioni:
  - la riga strutturata `Copertura: COMPLETA` in testa al verdetto;
  - nessuna conclusione negativa che la contraddica;
  - nessun buco di copertura misurato dal workflow (`rr_coverage_gaps`: file oltre budget, patch tagliate,
    Compare API troncata).

  Altrimenti la review è `VALID_INCONCLUSIVE`: escalation owner (`STOP_OWNER`), nessuna deroga, nessun tentativo
  in più a pagamento. Il gate rilegge anche i commenti già pubblicati. Prompt di Sol e Grok aggiornati: non
  chiedono più «Non approvare merge.».

  Test: i cinque verdetti reali di #556 (prima tutti PASS) e casi avversariali, anche end-to-end sullo script
  vero dei due workflow (`tests/scripts/test_pr_review_recovery.py`). Mutazioni ad hoc: 18/18 uccise.
- **PF-POST-556 Phase 2 — un solo contratto per la lettura del conto (avvio, `get_status`, settlement, ripresa;
  decisione owner):** `RuntimeController._apply_funds_reading` con quattro stati espliciti: `FUNDS_VERIFIED`,
  `BALANCE_ZERO_COMMITTED`, `ACCOUNT_EMPTY_VERIFIED`, `FUNDS_UNVERIFIABLE`.
  - Il bankroll non si azzera più per un disponibile 0. Prima lo facevano l'avvio e il settlement con lo zero
    «confermato»: falso drawdown e LOCKDOWN.
  - Lo zero marca la cassa non verificata con motivo esplicito.
  - Il settlement tratta un campo mancante come avvio e `get_status`.
  - Al ritorno dei fondi la prima lettura d'ingresso valida riallinea bankroll e cassa, anche headless.
  - Lo stato del conto è esposto in `get_status`, con ordine delle letture garantito.

  Test: `tests/unit/test_p2_zero_balance_unified.py`. Aspettative superate dalla decisione owner riallineate
  (`test_bankroll_sync_never_zeroes_...`, motivi `BALANCE_ZERO_COMMITTED`, fake con `exposure`). LIVE resta NO-GO.
- **#554 — cap evento del lotto Dutching con la stessa misura della bet manuale e dei segnali (P3,
  preesistente):** il `precheck` del lotto misurava solo il tavolo con la propria chiave. Restavano fuori i tavoli
  da segnale e manuali dello stesso evento, e quelli dello stesso mercato; un'eccezione valeva 0 (fail-open). Ora
  lotto e bet manuale passano da `RuntimeController.controller_event_exposure(entry, event_key=…)`, che conta i
  tavoli dello stesso evento, mercato o chiave, ciascuno una volta, con riserve pendenti ed esiti incerti
  compresi e la liability per i LAY. Rifiuti:
  - identità evento assente ⇒ `Esposizione evento non verificabile`;
  - misura assente, eccezione, valore non finito o negativo ⇒ `Esposizione evento oltre limite`, anche per la bet
    manuale, che prima ripiegava sul tavolo della chiave.

  Review indipendente sul diff, correzioni con test RED→GREEN:
  - il tavolo del lotto ora salva `event_id` (prima, lotti identificati dal solo id su mercati diversi non si
    vedevano);
  - il gate d'identità usa le regole della misura (un id di soli spazi o 0 non vale);
  - il `RiskMiddleware` conserva `event_id`;
  - seconda review sul diff finale: l'id evento ha una forma canonica (123.0 e «123» sono lo stesso evento) e il
    bus non trasforma più un nome evento assente in «None».
  - Codex P1 su `e0dacbb`: la forma canonica dell'id è condivisa (`validators.canonical_event_id`) anche dal
    matcher del runtime, così un tavolo da segnale salvato con id `123.0` resta nel cap di un ingresso con `"123"`.

  Rimosso il vecchio `DutchingController._event_current_exposure`. Test:
  `tests/unit/test_554_dutching_event_cap_unified.py`; test doppi dei runtime finti allineati alla nuova misura;
  mutazioni `i554_*`. LIVE resta NO-GO.
- **#543 — ricalcolo dei fondi del SimulationBroker non piu' quadratico (follow-up PFC-005, P2 prestazioni):**
  - ordini e fill raggruppati per mercato in una passata; memo per mercato indirizzato dal contenuto (chiave =
    tutti i campi letti, nessuna invalidazione esplicita); id del fill cercato solo fra quelli dell'ordine;
    mercati regolati fuori dal memo;
  - memo solo nel caso normale (id/mercato/lato/stato `str`, chiavi dei fill presenti, numeri col loro tipo),
    altrimenti calcolo senza memo (review indipendente su 5392139);
  - 2000 ordini / 100 mercati: piazzamenti da ~18,6 s a ~2,2-3,6 s, `get_account_funds` da ~18 ms a ~7 ms;
    1000 / 50: da ~3,2 s a ~0,5-0,8 s (macchina a 4 CPU, `tests/stress/bench_sim_funds.py`); contabilita' identica;
  - prova: test differenziale contro il calcolo di prima (`tests/unit/test_543_sim_funds_recompute.py`),
    mutazioni `i543_*`. Dettaglio in `docs/performance/sim_funds_recompute.md`.

- **#532 G7 — chiusura del perimetro SIM (criterio owner, 23/10/2026):**
  - prove G7-a/b/c, #545 e #546 rieseguite su `fd5e3c4` (prima di questa PR): 214 test PASS;
  - nuovo `tests/acceptance/test_g7_closure_verified_zero_entries.py`: con zero verificato nessun percorso d'ingresso
    produce un ordine (segnale, Microstake, lotto Dutching, bet manuale, auto-next); con fondi validi le entrate
    riprendono; due piazzamenti concorrenti oltre il disponibile: ne passa uno solo;
  - `docs/verification/continuous_formal_verification.md` (riga G7) e `docs/traceability/requirements.json`:
    perimetro G7 SIM comprovato. Esiti incerti dell'exchange e parita' SIM/LIVE con dati Betfair reali NON
    VERIFICATI, spostati in #351/#506; LIVE NO-GO.

- **#546 D2 + #549 + #546 D1 — ingressi del DutchingController e zero verificato (follow-up PFC-007, P2/P3):**
  - D2: lotto Dutching e bet manuale rileggono il conto fuori dal lock e, sotto il lock, confrontano il disponibile
    fresco con impegnato + riserve non ancora a mercato (compresa la propria) prima di qualsiasi ordine
    (`RuntimeController.read_funds_for_controller_entry` / `controller_entry_funds_reason`). Fondi insufficienti ⇒
    `fondi_insufficienti:…`, lettura non verificabile ⇒ `fondi_non_verificabili`; tavolo e chiave rilasciati.
    Prima con una cassa vecchia l'ordine partiva e lo fermava solo l'exchange.
  - #549: `manual_bet` applica `max_event_exposure_pct`, contando anche i tavoli da segnale dello stesso evento
    (`controller_event_exposure`, anche per `market_id`); prima il cap evento non era valutato sulla bet manuale.
    Una bet manuale senza nome né id evento e' rifiutata (`Esposizione evento non verificabile`).
  - D1: dopo un settlement uno zero senza `error` ha motivo `BALANCE_ZERO_VERIFIED` (non piu'
    `BALANCE_ZERO_AMBIGUOUS_OR_FALLBACK`) e registra lo snapshot (cassa del RiskDesk invariata); bankroll invariato, auto-next saltato.
  - Test: `tests/unit/test_546_d2_549_controller_entry_funds.py` (catena reale fino al SimulationBroker).
  - Mutazioni `i549_*` / `i546d2_*` in `guardrails/mutation_catalog.json`; `manual_bet_admission_not_atomic` riallineata
    alla nuova chiamata con la lettura dei fondi.

- **PF-REVIEW-RECOVERY-01 (#550) — review AI incomplete, retry controllato, deroga owner verificabile:**
  - Sol e Grok classificano ogni tentativo prima di scrivere marker (`VALID_PASS`, `VALID_BLOCKERS`,
    `INCOMPLETE`, `TIMEOUT`, `PROVIDER_ERROR`, `CREDITS_EXHAUSTED`, `UNKNOWN`): il marker di completamento
    solo con le due sezioni «Bloccanti»/«Verdetto finale» valide. Prima (#547) 32 token senza sezioni
    scrivevano il marker e bloccavano ogni nuovo tentativo; un marker scritto dal modello nell'output veniva
    pubblicato.
  - Al massimo 2 tentativi per reviewer e range (marker `-pr-review-attempt`); poi nessuna chiamata a pagamento
    e una sola richiesta `REVIEW_UNKNOWN_OWNER_DECISION`. Output del modello sanificato (nessun commento HTML).
  - `scripts/pr_autonomy_policy.py`: `validate_review` legge tentativi e decisione owner; `reviewer_gate`
    `RERUN_REVIEW_ONCE` / STOP owner / `PASS_WITH_OWNER_WAIVER`; `parse_owner_waiver` (riga `PF-REVIEW-WAIVER`
    scritta a mano dall'owner, per PR, head esatto e reviewer; mai tramite app o dall'agente; mai su bloccanti
    o crediti); `merge_decision` accetta la deroga solo per l'head valutato; nuovo comando `review-gate`.
  - Merge readiness riporta `ai_review_gate` (informativo, dai commenti grezzi della REST API).
  - Test: `tests/scripts/test_pr_review_recovery.py` esegue il Python reale dei due workflow con GitHub e
    modello finti.

- **#545 — snapshot SIM non scritto mai come esito certo (follow-up PFC-006, roadmap #461, P1):**
  - `TradingEngine`: un esito con `snapshot_persisted: false` (piazzamento, batch) e' AMBIGUOUS
    (`PERSISTED_NOT_CONFIRMED`): riga AMBIGUOUS, tavolo, chiave anti-duplicato e dedupe trattenuti; dopo un crash
    F018 la riserva risulta incerta, non chiusa. Prima: riga SUBMITTED, e dopo un crash ordine sparito, riserva
    liberata e un intento diverso sullo stesso evento ammesso;
  - cancellazione (`response_confirms_cancel`, adapter TTL e cashout): non confermata se lo snapshot non e'
    scritto. Prima: confermata, e dopo il riavvio l'ordine tornava `EXECUTABLE` e si abbinava;
  - hedge del cashout (`OrderRouter._normalize_sim`): `AMBIGUOUS`/`order_unknown`, bet registrata, nessun retry;
  - `SimulationBroker.is_durable` / `ensure_durable`: il broker sa se l'ultima scrittura dello snapshot e' fallita
    e la ritenta (stato intero). Finche' non torna durevole l'engine nega ogni nuova entrata
    (`SIM_SNAPSHOT_NON_DUREVOLE`), cosi' sostituzioni, fill differiti e settlement non scritti non si sommano a
    nuova esposizione; le uscite protettive non passano dal gate.
  - Microstake: la saga dichiara anche l'esito dello snapshot FINALE (`snapshot_final_persisted`). Lo snapshot e' lo
    stato intero, quindi uno intermedio fallito e riparato dal finale non diventa AMBIGUOUS nell'engine (saga ACTIVE,
    broker durevole); un finale non scritto resta AMBIGUOUS. Il flag di durabilita' si aggiorna sotto lo stesso lock
    della scrittura, e la cancellazione non e' confermata anche col flag annidato nel wrapper `result`.
    Solo SIM, nessun effetto LIVE.
- **PFC-008 — percentuali di rischio obbligatorie e cap sulla cassa verificata (roadmap #461, M019 + M020;
  decisione owner #426 c.6369802091):**
  - le quattro percentuali Roserpina (`target_profit_cycle_pct`, `max_single_bet_pct`, `max_total_exposure_pct`,
    `max_event_exposure_pct`) non hanno piu' un default di codice: assenti => `<chiave>_non_configurato`, testo,
    bool, NaN/inf, <= 0 o > 100 => `<chiave>_non_valido`; Money Management, Dutching (precheck e bet manuale) e
    cap temporaneo Microstake negano ogni nuovo ordine. Prima il loader metteva 3/18/35/18 in silenzio e nel
    Dutching un NaN rendeva sempre falso il confronto (fail-open);
  - chiavi recovery assenti => nessun recovery (`allow_recovery` False, `max_recovery_tables` 0; prima True e 2);
  - i cap percentuali di capacita'/esposizione si calcolano sulla **cassa complessiva verificata** (disponibile +
    impegnato della stessa lettura, `RiskDesk.cash_equity_current`) e l'esposizione corrente si sottrae una sola
    volta: cassa 100, esposizione 20, cap 35% => 15 di nuova esposizione (prima 8). La cassa e' verificata solo da
    uno snapshot coerente (SIM: disponibile + impegnato == `total`; riserve unmatched e liability LAY gia'
    nell'esposizione); altrimenti nuove entrate BLOCKED con diagnostica (`cassa_non_verificata:<motivo>`), senza
    ripiego. L'esposizione dello snapshot fuori dai tavoli del bot (bet manuali, altre app) si sottrae anch'essa;
    una lettura arrivata tardi non riporta in vita una cassa superata. L'hedge del cashout (uscita protettiva,
    `protective` solo dall'OrderRouter) non viene bloccato da percentuali mancanti, cassa non verificata o
    headroom del cap totale esaurito: resta il limite dei fondi disponibili (#548). Il segnale singolo ricontrolla il cap totale
    sulla lettura fresca d'ingresso (impegnato esterno cresciuto o cassa scesa dopo l'ultima lettura registrata =>
    `supera_max_total_exposure` prima di `CMD_QUICK_BET`; rilievo Codex P1 su 45c9aa2). Il TradingEngine pubblica l'ACK sincrono
    `QUICK_BET_SUBMIT_ACK` sull'ordine accettato dal broker: la riserva risulta a mercato e non e' contata due volte;
    un ordine AMBIGUOUS resta contato in entrambi i modi (fail-closed). Sulla lettura fresca si ricontrollano anche
    singolo ordine ed evento (`fresh_caps_reason`); bankroll e cassa di una lettura si applicano insieme dopo il
    controllo di `read_at` (una lettura vecchia arrivata tardi non riscrive il bankroll; rilievi Codex su 4ce44a7). P&L realizzato, snapshot e
    ripristino spostano la soglia delle letture: una lettura iniziata prima del settlement non rivalida la cassa
    superata (audit PF-FINANCIAL-IMPACT-GATE-01). Una lettura d'ingresso superata da una
    lettura piu' recente del RiskDesk si ricontrolla anche su quella (rilievo Codex su 0dbadbb). Stake target, difesa, lockdown e drawdown non cambiano base e l'escalation a LOCKDOWN
    precede i nuovi blocchi;
  - `RoserpinaConfig` non ha piu' default per le quattro percentuali ne' per il recovery; le fixture dei test le
    dichiarano esplicitamente (`tests/risk_config_fixtures.py`);
  - Dutching: con bankroll <= 0 o config assente i controlli di esposizione non vengono piu' saltati (fail-open);
  - GUI F10, configurazione guidata: se una percentuale manca o non e' valida, avviso nella barra in alto con il
    pulsante "Configura Money Management" e apertura della tab Roserpina all'avvio (solo in quel caso); ogni campo
    spiega a cosa serve; campi vuoti, mai precompilati; il salvataggio con campo vuoto o fuori da (0, 100] viene
    annullato con errore esplicito; dopo un salvataggio valido nessuna richiesta, neppure ai riavvii;
  - M020: rimosso da `config.example.json` il blocco `roserpina` (non letto da nessun codice) che suggeriva un
    `max_daily_loss` fisso di 50 EUR; test di guardia sui default (loader, dataclass, esempio).

- **PFC-007 — fondi non verificabili non valgono come saldo (roadmap #461, M013 PF-FIN-02 residuo; #544):**
  - `BetfairClient.get_account_funds`: un `availableToBetBalance`/`exposure` assente, non numerico o non finito
    e' `None`, non 0.0: il servizio lo marca `BALANCE_FIELD_MISSING`;
  - `BetfairService.get_account_funds` conserva l'`error` del broker (es. `FILL_LEDGER_INCONSISTENT` del SIM) e
    `reserved_unmatched`; un valore non finito diventa `BALANCE_NOT_FINITE`, mai un saldo;
  - `RuntimeController`: un payload con `error` non e' un conto valido (`_account_from_funds` ok=False). All'avvio
    (`_applica_fondi_avvio`) bankroll e baseline del ciclo restano invariati e nessuna baseline viene persistita su
    dati non validi; dopo il settlement nessuna sincronizzazione (`SYNC_FAILED_BALANCE_UNAVAILABLE`); `get_status`
    allinea il bankroll solo a una lettura valida (prima un NaN arrivava a `sync_bankroll`);
  - `TradingEngine`: con fondi in errore il cap temporaneo Microstake non e' disponibile (BLOCKED);
  - `StreamingFeed` passa `numberOfWinners` dalla marketDefinition: il SimulationBroker calcola l'esposizione del
    mercato a vincitore unico anche sui book dello stream (Dutching LAY);
  - il gate dei fondi delle nuove entrate (PR28-e) NON conta due volte le riserve (verificato); il possibile
    doppio conteggio nei cap percentuali di Money Management (base = disponibile netto piu' esposizione) resta
    invariato in attesa di decisione owner; 1 mutazione nuova, `funds_gate_snapshot_error_ignored` ora equivalente
    (seconda difesa dietro il nuovo controllo).
- **PFC-006 — G7-c: riavvio con ordini SIM non abbinati (roadmap #461, decisione owner #531):**
  - il `SimulationBroker` scrive lo snapshot di reload (`simulation_state`) dopo ogni operazione che cambia
    ordini o fondi, prima di rispondere: piazzamento, batch, cancellazione (singola, parziale, di tutto il
    mercato), sostituzione, fill differiti e settlement. Prima lo scriveva solo il servizio, all'aggiornamento
    del book o alla disconnessione: un crash subito dopo un piazzamento faceva sparire l'ordine e liberava la
    sua riserva; uno dopo una cancellazione resuscitava l'ordine `EXECUTABLE`, che con l'abbinamento
    differito poteva abbinarsi;
  - la cancellazione di tutto il mercato scrive anche la cronologia per-bet (prima no);
  - uno snapshot di reload NON scritto (errore del DB) si dichiara nell'esito dell'operazione
    (`snapshot_persisted: false`, anche per batch, sostituzione, cancellazione e book con fill differiti o
    settlement) e si registra come errore, invece di restare un successo silenzioso (Codex su #542);
  - dopo il riavvio l'ordine a riposo riserva gli stessi fondi, si abbina, si cancella e si regola senza doppi
    conteggi; un ordine rifiutato non scrive nulla;
  - la saga Microstake continua a chiamare `persist_snapshot()` e a contarne i fallimenti; il test che
    contava le scritture sul DB ora verifica il contenuto dell'ultimo snapshot; 3 mutazioni nuove (area
    `recovery`).
- **PFC-005 — G7-b: transizioni dei fondi SIM senza doppio conteggio (roadmap #461, decisione owner #531):**
  - un ordine a riposo si abbina (anche in parte, in piu' volte) quando un book APERTO successivo lo
    raggiunge; mai su mercato sospeso, senza stato o runner non ACTIVE (PF-EXEC-05); la riserva diventa
    esposizione abbinata e il disponibile non cambia;
  - registro cronologico dei fill (`state.fills`) persistito: il settlement e il riavvio ripercorrono i fill
    nell'ordine in cui sono avvenuti; uno stato salvato prima lo ricostruisce dagli ordini; registro incoerente
    con l'abbinato => ordini rifiutati e settlement `LEDGER_INCONSISTENT` (fail-closed);
  - il disponibile e' cassa meno caso peggiore del MERCATO, calcolato per scenario sulle puntate abbinate e sul
    non abbinato solo dove fa perdere: niente piu' disponibile negativo dalla compensazione del PositionLedger
    a quote diverse, niente vincita condizionata contata come incassata prima del settlement; su un mercato a
    vincitore unico un Dutching LAY gamba per gamba riserva la liability peggiore, non la somma; senza
    `numberOfWinners == 1` le esposizioni dei runner si sommano (prudente);
  - la liquidita' gia' presa dagli ordini SIM resta consumata quando il book viene riletto (anche dopo un
    riavvio): si abbina solo l'incremento; un livello esaurito esce dal book invece di diventare "illimitato";
  - una copertura che non aumenta l'esposizione passa anche con disponibile negativo (PF-EXEC-03); una gamba
    di batch con lato non stringa non pesa sui fondi;
  - registro dei fill validato al caricamento: una voce malformata e' un rifiuto pulito (fondi
    `error: FILL_LEDGER_INCONSISTENT`, ordini rifiutati, settlement `LEDGER_INCONSISTENT`), mai un'eccezione;
    un registro incoerente blocca gli ordini su tutti i mercati, uscite comprese (prudente);
  - rilievi Codex su #542: un `fill_id` ripetuto nel registro e' incoerente (il ledger scartava il doppione e
    la cassa risultava sovrastimata); un book con prezzi/dimensioni non numerici non abbina nessun ordine a
    riposo (prima si fermava a meta' ciclo); la liquidita' consumata sopravvive a sospensioni e scale vuote e,
    per uno stato precedente a PFC-005, si ricostruisce dai fill;
  - ogni ordine deve tornare con i suoi fill anche per runner, lato e quota media, non solo per importo; un
    prezzo o una dimensione NaN/infinita nel book non abbina (prima una dimensione NaN valeva "illimitata",
    anche al piazzamento);
  - una voce del registro scartata al caricamento lo marca inaffidabile in modo persistente (`fills_corrupted`,
    azzerato solo da `reset()`); con il registro non verificabile neanche l'abbinamento differito avviene; una
    quota illeggibile non cancella la liquidita' consumata; una dimensione negativa non vale "illimitata";
  - modello della liquidita' consumata riscritto (rilievi Sol e Codex su #542): si toglie da ogni livello della
    scala e da tutti i runner, decade solo se la sua quota sparisce dalla scala di un book aperto e valido; un
    book e' valido solo con struttura corretta e quote > 1.0; gli id dei fill sono sempre liberi;
  - abbinamento differito con priorita' temporale piena (l'ordine piu' vecchio esaurisce i livelli raggiungibili
    prima del successivo) e solo se il PositionLedger vivo torna con la ricostruzione dei fill, come al
    settlement (altrimenti nessun fill: il mercato resterebbe non regolabile);
  - due test che codificavano il credito anticipato (disponibile 1010 prima del settlement di BACK 10@3 +
    LAY 10@2, i due esiti sono +10 o 0) aggiornati a 1000; 4 mutazioni nuove, 3 di PFC-004 riallineate.
- **PFC-004 — G7-a: il SimulationBroker riserva i fondi come Betfair (roadmap #461, decisione owner #531):**
  - un ordine non abbinato riserva lo stake (BACK) o la liability (LAY) sul caso peggiore del runner: una
    copertura che riduce l'esposizione non chiede fondi in piu' (le uscite protettive non si bloccano);
  - la riserva si ricava dagli ordini `EXECUTABLE` (residuo `size - matched_size`): si libera a cancellazione
    (anche parziale), sostituzione, decadenza e settlement, e dopo un riavvio si ricalcola senza doppi conteggi;
  - oltre il disponibile il broker risponde `FAILURE`/`INSUFFICIENT_FUNDS` e non memorizza nulla; un batch che
    non sta nei fondi non piazza nessuna gamba; una sostituzione oltre i fondi lascia l'ordine originale;
  - `get_account_funds()` espone `reserved_unmatched`; `available` e `exposure` includono la riserva, `total`
    invariato;
  - i 4 test G7 di accettazione passano senza `xfail`; due test che codificavano il difetto (disponibile 996
    con ordini a riposo; conto SIM da 1000 sotto un bankroll da 500.000) aggiornati; 4 mutazioni nuove nel
    catalogo (aree `funds`, `lay_liability`, `exposure`, `cashout`).
- **PFC-003 — suite completa e mutation in parallelo (M157, roadmap #461; merge owner):**
  - i tre job che eseguono l'intera suite (`_module-ultra-check` backstop, `merge-simulation-hard`, `pr-check`)
    girano `pytest -n auto --dist worksteal -m "not serial"` e poi `pytest -m serial`: selezioni complementari,
    verificate da `tests/guardrails/test_pfc003_parallel_backstop.py` eseguendo i comandi del workflow su un
    mini-repository (ogni test una volta sola);
  - `scripts/run_mutation_guardrails.py --jobs N`: una sandbox per worker, risultati nell'ordine del catalogo e
    report identico al run seriale; `mutation-guardrails` usa `--jobs 4`;
  - indipendente da PFC-002 (#539, chiusa senza merge): la deduplica delle esecuzioni non e' inclusa.

- **PFC-001 — test deterministici e veloci, nessuna modifica di produzione (roadmap canonica ratificata
  PF-ROADMAP-108-AUTONOMOUS-EXECUTION-01, #528):**
  - `tests/f026/child_f026.py`: le attese fisse (1,5 s dopo ogni segnale, 4 s prima del settlement) diventano
    condizioni verificabili: l'ordine del segnale ha lasciato `INFLIGHT` (oppure c'è un esito o un rifiuto), ogni
    mercato chiuso ha il proprio `RUNTIME_CLOSE_POSITION` elaborato, e il bus non ha callback in coda o in
    esecuzione per 3 letture consecutive. Scadenza 30 s registrata in `R["waits"]`; nessuna aspettativa dei test
    cambia. `tests/f026`: 460 s → 318 s in seriale.
  - `test_retry_does_not_stack_overflow`: backoff a 0 nel test (dormiva 181 s reali, il 15% della suite) e nuova
    asserzione sugli 11 tentativi effettivi; un codice che smette di ritentare ora fa fallire il test.
  - `tests/acceptance/test_issue437_pr28d.py`: attese positive (`Event.wait`, `join`) da 1–2 s a 30 s; un Event già
    impostato ritorna subito, sotto carico il thread del test non fallisce più per tempo (falso rosso su #535). Le
    attese negative restano brevi.
  - Marker `serial` in `pytest.ini` per i test con widget Tk reali sul display X condiviso, con guard
    `tests/guardrails/test_serial_marker_pfc001.py`: un test (o una fixture che richiede) usa un display reale se
    legge `DISPLAY`, costruisce una radice Tk (`Tk`/`CTk`/`Toplevel`) o una `MiniPickfairGUI` senza
    `test_mode=True` (rilievo Codex P2 su 0fd0ba2). README: comando parallelo + passo seriale.

- **Fix loop: tetto per PR portato da 5 a 10 cicli (PF-QA-AUTONOMOUS-CONTINUATION-01, mandato owner
  20/10/2026):** `MAX_FIX_LOOP_ITERATIONS_PER_PR = 10` in `scripts/pr_fix_loop_policy.py`. È un massimo,
  non un obiettivo: stop anticipato per `REVIEW_CHURN`, prenotazione prima della patch, nessun reset,
  deroga owner puntuale oltre il tetto e tutti i gate invariati. Una deroga storica registrata sotto il
  nuovo tetto (es. `AT_COUNT=5`) rende il ledger di quella PR non valido (fail-closed). Allineati
  CLAUDE.md, AGENTS.md (anche il blocco «Normative owner repair limit»), `docs/auto_pr_flow_spec.md`
  (§0.15, §11–§13) e `docs/hard_verify_spec.md`; un test rifiuta ogni copia normativa ancora a 5.
  Il freno anti-loop sulle esecuzioni del controller (`max_total_controller_runs`, fisso a 8) è ora il
  tetto + 3 (13): con 8 i cicli 9–10 non arrivavano al merge.

### Fixed

- **#536 PF-RUNTIME-LOCK-01 — autorità esclusiva del runtime: lock OS + identità del lock + lease con fencing
  token (mandato owner PF-536-ISOLATED-IMPLEMENTATION-01):** su main 152613b il lock `<db>.runtime.lock` proteggeva
  l'inode, non il percorso: cancellato o sostituito il file, un secondo runtime partiva sullo stesso database e i
  due operavano senza coordinamento (ordini, auto-next, reset di ciclo su tavoli divergenti). Ora
  (`core/runtime_authority.py`, `docs/runtime_authority.md`): identità `(st_dev, st_ino)` del lock verificata a
  ogni operazione; tabella **additiva** `runtime_lease` (riga singola, `CREATE TABLE IF NOT EXISTS`) con
  `fencing_token` monotono acquisita in `BEGIN IMMEDIATE`, scavalcabile solo se scaduta o se il detentore è
  dimostrabilmente morto sullo stesso host, rinnovata dall'heartbeat (TTL 30 s / 10 s) e rifiutata se scaduta;
  fence vincolato al `Database`: ogni scrittura (`transaction`, `_settlement_transaction`, autocommit
  `INSERT/UPDATE/DELETE/REPLACE`) verifica owner e token dentro la stessa transazione, altrimenti
  `RuntimeFenceViolation` e rollback. `start()` rifiuta con `RUNTIME_LEASE_HELD` se la lease è di un altro runtime
  vivo. Verifica fail-closed prima di segnale, auto-next, cashout, `reset_cycle`, sweep F028 e di ogni invio del
  `TradingEngine` (`assert_runtime_authority`), ricontrollo dopo l'invio (`RUNTIME_AUTHORITY_LOST_AFTER_SEND`),
  `is_live_allowed` False (verifica completa di lock e lease); verifica anche nel `CashoutExecutor` subito prima
  dell'hedge (`cashout_wiring.cabla_catena_cashout(runtime_authority_check=...)`, cablato da headless e GUI), perché
  il bus è asincrono e la verifica al routing non copre il piazzamento. Dopo la review di `fc8f136`: fence e
  scrittura autocommit nella stessa `BEGIN IMMEDIATE`; autorità rilasciata allo STOP = blocco; autorità persa ⇒
  nuova epoca solo dopo STOP; TTL unmatched senza autorità = nessun cancel; rinnovo e acquisizione misurati dopo
  l'attesa del lock del DB; nessun I/O sul DB sotto il lock dell'autorità (niente deadlock). Dopo la review di
  `125aa05`: verifica nei cancel del cashout, del residuo e del TTL (`CashoutCancelAdapter(authority_check=...)`),
  verifica prima di persistere l'intento, STOP che attende le operazioni in volo (`authority_operation`,
  `RUNTIME_AUTHORITY_DRAIN_SEC`), rilascio fallito che trattiene l'autorità, rinnovo concorrente annullato; anche
  l'hedge del cashout in volo è contato e atteso dallo STOP; `begin_transaction` fenced, emergency stop senza
  cancel-all di mercato da un runtime superato, candidato TTL conservato se l'autorità blocca il cancel, START
  rifiutato se l'autorità cade durante l'avvio, scadenza della lease ritentata allo STOP; `transaction()` con fence
  in `BEGIN IMMEDIATE` (l'upgrade da lettura a scrittura falliva se l'heartbeat rinnovava nel mezzo); TTL degli
  unmatched con esito del cancel locale alla chiamata (`CashoutCancelAdapter.cancel_with_outcome`) e presa in carico
  del `bet_id` prima del broker, così due giri sovrapposti non cancellano due volte lo stesso ordine. Autorità persa ⇒ LOCKDOWN in memoria **senza cancel-all**, lease scaduta subito così il
  proprietario legittimo subentra, evento `RUNTIME_AUTHORITY_LOST`. Limiti dichiarati: finestra TOCTOU fra verifica
  e chiamata all'exchange (segnalata, lasciata alla riconciliazione del nuovo proprietario), stesso host e
  orologio condiviso, filesystem di rete non supportato, Windows MANUAL_ONLY. Test HARD REAL in
  `tests/acceptance/test_issue536_runtime_authority.py` (due processi OS, crash, riavvio, lock cancellato e
  sostituito, lease scaduta con processo vivo, DB indisponibile, STOP fallito/ripetuto/concorrente); mutazioni
  `i536_*` nel catalogo.
- **#461 PR29-c — un settlement, un solo effetto finanziario, sulla sola riserva che identifica
  (PF-PR29-C-FINANCIAL-IDEMPOTENCY-01, PF-C29-01 .11/.13/.18):** su main 409bcf9 quattro consegne concorrenti dello
  stesso settlement portavano il debito di recovery da 10 a 40 (S7); con due bet sullo stesso mercato nessun tavolo
  si liberava (S2); il settlement della propria bet non chiudeva l'AMBIGUOUS (S3); la chiusura con `table_id`
  liberava la riserva di un altro evento e raddoppiava la perdita (S5/S6); la chiave appuntata da F018 restava per
  sempre (S9). Ora: catena d'identità durevole `bet_id → customer_ref → reservation_id → table_id`
  (`reservation_id` assegnato da `TableManager.activate`, persistito nella riga `orders`, ripristinato da F018);
  nuovo registro durevole `settlement_effects` (tabella **additiva**, `CREATE TABLE IF NOT EXISTS`, nessuna colonna
  esistente toccata) scritto con lo stato dei tavoli di recovery nella stessa transazione `BEGIN IMMEDIATE`:
  replay, concorrenza, due processi sullo stesso DB e riavvio producono l'effetto una volta sola; P&L realizzato una
  volta (flag `realized_applied`); effetto in memoria annullato se la registrazione perde la corsa o fallisce;
  rilascio legato all'identità (`TableManager.release_identified_reservation`, anche per i lotti); incertezza
  AMBIGUOUS per riserva e lifecycle che non libera il tavolo di un altro intento. Senza registrazione dimostrata
  (riga non marcata, registro illeggibile, corsa persa) si applica al realizzato solo una perdita, mai una vincita
  (nessun fail-open dei limiti di perdita); il P&L in memoria non entra nel capitale né nel gate fondi. I
  settlement trattenuti restano nel registro con il loro payload e si riconciliano a una nuova consegna, con
  `reconcile_held_settlements()` o all'avvio dopo F018; all'avvio un `held` in attesa di una lettura fallita
  tiene il runtime in PAUSED. Review su e2bb36e: nessuna registrazione senza lo stato dei tavoli; chiave della
  chiusura di posizione con l'identità della riserva; esiti del lifecycle senza identità che non liberano tavolo
  né chiave; AMBIGUOUS senza tavolo chiuso dal settlement della propria riserva. Review su 29903af: chiave
  consapevole della riserva in ogni fase; tavolo della riserva assente => held in attesa; auto-next con tavolo e
  riserva persistiti nella request; snapshot dello stato tavoli scritto sotto il lock dei tavoli; incertezza
  AMBIGUOUS chiusa solo dall'esito dello stesso intento. Review su 70821fd: identità della riserva nella chiave
  di ogni chiusura di posizione; F018 riusa il tavolo persistito dell'auto-next; esito della scrittura dello stato
  tavoli sotto il lock; aggiornamento per tavolo della mappa di recovery dentro la transazione (più processi);
  effetto non registrato senza checkpoint né auto-next e in coda di ritentativo. Review su 7a4f2c3: ogni
  scrittura della mappa di recovery (anche ordinaria, di ritentativo o di reset) è un insieme di variazioni
  rispetto alla vista del processo, sommate dentro la transazione (anche due processi sullo stesso tavolo).
  Review su c6d2338: un solo runtime attivo per database (lock esclusivo del sistema operativo all'avvio,
  rifiuto `RUNTIME_INSTANCE_ACTIVE`, lock rilasciato da STOP e da un avvio non riuscito, START/STOP serializzati); chiave della chiusura di posizione con identità canonica (`customer_ref`,
  `reservation_id` solo in sua assenza) ed effetti non registrati (e held per lettura fallita) ritentati a ogni giro del poller, con il realizzato marcato;
  avvio fallito smontato prima del rilascio del lock. **Semantica cambiata,
  dichiarata:** un settlement senza chiave o senza identità dimostrabile non libera più nulla (prima liberava il
  tavolo univoco del mercato); `RUNTIME_CLOSE_POSITION` non rilascia più in modo incondizionato la chiave
  anti-duplicato del payload. Adeguati i test che codificavano il rilascio senza identità (uno, pr26a, dipendeva
  dal difetto S5). Limiti residui in `docs/roserpina_bankroll_equity.md` (sezione PR29-c). Nessuna attività LIVE,
  Dutching disattivato.

- **#461 PR29-b — un lotto Dutching a mercato non libera la riserva (PF-C29-01.17):** nella riconciliazione un
  lotto con gambe a mercato o abbinate (stato EXECUTED) non libera piu' tavolo, esposizione e chiave anti-duplicato:
  la posizione e' viva fino al settlement. Il rilascio avviene solo su esito terminale certo e privo di rischio di
  ogni gamba (FAILED, CANCELLED, ROLLED_BACK), una volta sola. Corretto il test invariante che codificava il rilascio
  su ogni terminale. Il rilascio del lotto (riconciliazione e settlement) e' legato alla sua riserva: tavolo e
  chiave si liberano solo se il tavolo porta ancora la riserva del lotto, cioe' event_key e batch_id nel meta
  (`TableManager.release_batch_reservation`, atomico; il tavolo ricostruito al riavvio da F018 porta il `batch_id` del lotto); un tavolo riassegnato o riusato per lo stesso evento da un'altra
  riserva non si tocca, ma il P&L del lotto entra comunque nel suo recovery (stato RECOVERY con debito, ACTIVE senza); su un tavolo gia' libero il P&L del lotto resta
  contabilizzato. Su un tavolo gia' libero o riassegnato a un altro evento la chiave del lotto non si tocca (ne' release
  ne' unpin): un nuovo intento la acquisisce e la appunta prima di attivare il suo tavolo, e senza identita' della
  registrazione non si distingue da quella del vecchio lotto. Limite dichiarato (fail-closed): una chiave appuntata
  rimasta orfana blocca l'evento fino al riavvio. Dutching resta dormiente (decisione owner #531).

- **#461 PR29-a — il rilascio segue la riserva, mai un esito che non la chiude (PF-C29-01.03, .13):**
  `QUICK_BET_SUCCESS` non rilascia più tavolo, esposizione o chiave anti-duplicato. In produzione l'engine lo
  emette per `DUPLICATE_BLOCKED` (richiesta non inviata perché lo stesso intento è già in volo): un replay dello
  stesso `CMD_QUICK_BET` liberava la riserva viva dell'originale, lasciando l'esposizione fuori da cap e gate
  dei fondi (riprodotto sul runtime reale). Un SUCCESS di completamento marca soltanto la riserva come a mercato;
  quello di un doppione non tocca nulla. Con anti-duplicazione disattivata un segnale identico (stesso
  `customer_ref`) è rifiutato prima di riservare un secondo tavolo (`ingresso_gia_in_volo`); la riconsegna di un
  intento già consegnato al trasporto (DB durevole, anche dopo riavvio) è rifiutata prima di riservare un tavolo
  (`ingresso_gia_eseguito`, verifica fallita => `ammissione_non_verificabile`), anche nell'auto-next. Con più processi
  sullo stesso DB, `QUICK_BET_DUPLICATE` riporta `duplicate_in_process` (deciso sotto il lock del motore): se
  l'intento è stato preso da un altro processo, la riserva mai inviata di questo processo si rilascia (stesso
  tavolo, evento e `customer_ref`, non a mercato); un replay nello stesso processo la conserva. La proprietà della
  riserva la decide solo il `customer_ref` ammesso dal motore di questo processo (mai le chiavi ripopolate dal DB
  condiviso al recovery, che possono essere di un altro processo; un doppione nato dal solo `correlation_id` di un altro ordine
  rilascia la propria riserva), e un CMD senza `table_id` (auto-next) risolve il tavolo dal `customer_ref`. Verifica
  dell'identità e rilascio sono un'unica operazione atomica (`TableManager.force_unlock_if_unplaced_reservation`):
  un tavolo liberato e riassegnato nel frattempo, anche allo stesso evento, non si tocca. La proprietà locale la
  scarica solo il contesto che l'ha registrata, mai una richiesta con lo stesso ref negata prima del dedup
  (vale anche per la chiave di dedup in memoria, preesistente: un retry con nuovo correlation_id resta bloccato). Un
  SUCCESS di completamento non rilascia e non chiude l'incertezza AMBIGUOUS: resta fino a FAILED o
  settlement (fail-closed). Il rilascio del doppione
  esterno non tocca l'incertezza (una riserva mai inviata non è mai incerta) e trattiene la chiave se l'evento è incerto. Il rilascio resta
  a FAILED certo, sweep F028 e settlement. Corretto il test PR28-e che codificava il rilascio su SUCCESS.

- **#461 PR28-e — gate esplicito dei fondi sulle nuove entrate (PF-CAP-09 / PF-CAP-19, PF-EXEC-04):**
  un segnale o un auto-next con esposizione (BACK = stake, LAY = liability) oltre i fondi del conto viene
  rifiutato con `fondi_insufficienti:...`, mai ridimensionato; prima partiva se stava nel bankroll interno
  anche con meno fondi sul conto. Regola, sotto il lock d'ammissione: le riserve dei tavoli il cui ordine
  non è confermato a mercato prima dell'inizio della lettura del conto, compresa la nuova entrata, devono
  stare nel disponibile. Il tavolo registra il primo esito a mercato (ACCEPTED/PARTIAL/FILLED/SUCCESS,
  `TableManager.mark_placed`, correlato all'ordine tramite il `customer_ref` registrato sulla riserva;
  senza `table_id`, come nell'auto-next, `mark_placed_by_ref`; un esito tardivo di un ordine rilasciato non
  marca un'altra riserva; marca azzerata a ogni rilascio; mai su AMBIGUOUS); `pending_exposure` somma
  le riserve non ancora riflesse, comprese quelle già a mercato e poi rilasciate se piazzate dopo l'inizio
  della lettura (tracce con TTL tecnico `PLACED_TOMBSTONE_TTL_S`; una lettura più vecchia del TTL è fondi non
  verificabili). Lo snapshot del settlement conserva l'`error` del servizio; un auto-next senza tavolo è
  rifiutato (`riserva_tavolo_mancante`). Gli ordini già a mercato non contano due volte; l'impegnato del conto non
  copre nulla, perché può essere di posizioni esterne al bot (rilievo Sol su #531). Ordini ricostruiti al
  riavvio (F018) e confermati vivi = già a mercato; esiti incerti restano addebitati. Fondi illeggibili,
  incompleti (senza `exposure`) o con errore => `fondi_non_verificabili`, nessuna entrata (prima restava il
  bankroll di ripiego, PF-CAP-19); `BetfairService.get_account_funds` marca ora con `error` gli zeri di
  ripiego (lettura fallita o nessun client) e le risposte del broker senza `available` o `exposure`, che
  prima sembravano un conto letto. In SIM il saldo è
  quello del `SimulationBroker`, che però non riserva fondi sugli ordini (**G7, aperto**, difetto
  finanziario confermato dall'owner, fase M6): in SIM il gate non vede gli ordini aperti e la parità
  SIM/LIVE sui fondi non è certificata. Riproduzione RED in `tests/acceptance/test_g7_sim_funds_reservation.py`
  (`xfail(strict=True)`).
  Il segnale legge il conto fuori dal lock, nella stessa finestra di rete della best price; l'auto-next usa
  lo snapshot del settlement. Hedge e uscite protettive esclusi (PF-EXEC-03). Le corsie dormienti
  Dutching/bet manuale non sono ancora coperte: blocca la loro attivazione. Fixture di test allineate
  alla forma reale di `BetfairService.get_account_funds` (sempre con `exposure`).

- **#523 — Dutching e bet manuale conformi ai gate (corsie ancora dormienti):** lato BACK/LAY
  obbligatorio su ogni gamba e sul bet manuale (mai BACK per default, anche nel `RiskMiddleware`; alias
  in conflitto rifiutati; il lato esplicito della gamba prevale sul `bet_type` del lotto, usato solo come
  fallback per una gamba senza campi di lato; regola unica `validators.leg_side` in middleware e controller:
  campo di lato presente ma vuoto, di soli spazi, None o sconosciuto => rifiuto, mai ereditato dal lotto); cap, dedupe, tavolo e ammissione sotto il lock d'ammissione dei segnali, con
  barriera cashout atomica sul lotto e fence fino all'engine; bet manuale con esposizione reale (liability
  LAY) su cap e tavolo, che riserva sempre il controller; dedupe acquisito dopo i controlli e rilasciato
  solo se proprio, errore del dedupe => rifiuto (prima fail-open); chiave del lotto acquisita e fissata
  solo nella fase di ammissione, `precheck`/preflight privi di effetti permanenti; identificativi del
  Dutching (mercato, selezioni, quote, stake totale e stake calcolato di ogni gamba) validati con i
  validatori condivisi prima di ogni riserva (una gamba non valida, anche con stake nullo o negativo dal
  riparto, rifiuta il lotto intero: zero ordini); lotto invalidato se una gamba è negata dal fence o una
  sorella in coda ha il fence stale (nessuna gamba non ancora partita esce, anche con cashout limitato a
  una selezione); identificativi storici rifiutati all'ammissione. Esito del lotto (#529, scelta owner F):
  nessun evento di gamba rilascia tavolo o chiave del lotto (fail-closed); i settlement dal poller non
  rilasciano la chiave fissata (resta fino al riavvio o a `DuplicationGuard.clear`) e il tavolo solo se
  attribuibile; eccezione preesistente e dormiente: la chiusura mark-to-market (`auto_close_enabled`,
  default off) li libera con altre gambe aperte, trasferita a PR30.
  **PF-EXEC-02 non completo**: esito, parziale, ribilanciamento, copertura e persistenza del lotto sono
  della PR30. Test con catena reale e broker SIM in `tests/acceptance/test_issue523_dutching_manual_bet.py`.
- **#525 — gli orchestratori CI si avviano di nuovo:** `ci-master-gate` e `ci-dynamic-intelligent`
  richiamavano `dutching.yml`, `pnl-engine.yml` e `live-gate.yml`, che non dichiaravano
  `on: workflow_call`; GitHub rifiutava l'intero workflow all'avvio (0 job, 100 run su 100 falliti).
  Aggiunto `workflow_call` senza togliere i trigger diretti; test statico sul grafo delle chiamate e
  sullo smart skip delle lane di modulo.
- **#525 — `mutation-guardrails` muta davvero il codice:** il runner precedente scriveva metadati nei
  JSON di `guardrails/mutations/` che nessuno leggeva, rilanciava l'intera suite per 120 voci e andava
  in timeout dopo 45 minuti. Il nuovo `scripts/run_mutation_guardrails.py` applica le 36 mutazioni
  sorgente di `guardrails/mutation_catalog.json` in una copia isolata, con test mirati, ripristino
  verificato e nessun PASS su esiti inconcludenti, non applicabili o non compilabili. La soglia resta
  all'80% e le 11 aree (finanziarie e gate LIVE) sono obbligatorie. `guardrail_runner --run-mutations` non
  tollera più un gate fallito; il catalogo e i file che muta attivano la lane nel dynamic; nome dell'artifact
  unico per job; percorsi del catalogo confinati alla sandbox. Le 7 mutazioni sopravvissute al primo run (timeout,
  errore ambiguo, INFLIGHT al riavvio, chiavi in volo, FAILED dell'exchange, lato non valido,
  esposizione già aperta), più la quota del cashout ≤ 1 nel safety layer, ora sono uccise dai test di
  `tests/mutation/test_mutation_killers_525.py`.
- **#524 — un ordine forse inviato non diventa mai FAILED senza prova del rifiuto (BLOCKS_LIVE):** con esito
  ignoto e transizione DB `INFLIGHT→AMBIGUOUS` fallita, il controllo di finalizzazione sollevava
  `FINALIZE_ON_NON_TERMINAL_DB_STATE` e l'`except` esterno dell'engine marcava l'ordine FAILED, liberando tavolo,
  chiavi anti-duplicato e fence su un ordine che poteva essere vivo su Betfair. Lo stesso accadeva a qualunque
  eccezione arrivata dopo l'avvio dell'invio (transizione `SUBMITTED` fallita, crash nella costruzione
  dell'esito). Ora l'esito resta AMBIGUOUS non terminale (`AMBIGUOUS_NOT_PERSISTED` / `ENGINE_FATAL_AFTER_SEND`,
  `is_terminal=False`, `lifecycle_stage="degraded"`): nessun rilascio, `QUICK_BET_AMBIGUOUS` (tavolo trattenuto,
  cashout nell'ambito incerto), riconciliazione accodata; la riga `INFLIGHT` su DB la ritrovano dedupe e
  riconciliazione al riavvio. Un rifiuto certo resta FAILED. Test RED→GREEN in
  `tests/acceptance/test_issue524_ambiguous_never_failed.py` (catena reale runtime + engine + sqlite + broker
  SIM, fault injection DB/timeout/crash, riavvio, multi-tavolo concorrente).

- **#505 ripresa sopra la #512 (PF-505-RECOVERY-INTEGRATION-01, fase D di PF-PR-SEQUENCE-RECOVERY-01):** il
  lavoro della #505 (chiusa temporaneamente, conservato in `archive/pf-505-pre-512` `618884e` e
  `archive/pf-505-cap-reconciliation-prep` `022ee9d`) è integrato su `main` `f8bbbe9`. Restano in vigore la
  rettifica owner del 18/10/2026, cioè i cap monetari assoluti (`max_order_exposure`, `max_market_exposure`,
  `max_event_exposure_abs`, `max_drawdown_abs`) **opzionali e non attivi di default**, e tutte le protezioni della
  #512 (fondi verificati, liability LAY, riserve atomiche, dedupe, riconciliazione). Due test sono stati
  allineati, senza allentare nessun assert:
  - il test del cap mercato/evento ora misura una posizione su un'altra chiave evento, perché la stessa chiave
    è fermata prima dalla #512 (`evento_gia_in_posizione`);
  - F033 (cap evento aggregato su più mercati dello stesso incontro) non è più xfail, perché lo consegna la #505.

  Il riesame indipendente ha trovato due difetti P1 nati solo dalla combinazione #505 + #512, entrambi corretti
  (RED→GREEN, `tests/acceptance/test_issue437_pr28d_integration_512.py`):
  - **lock di ammissione durante la rete:** il fetch del book di PF-ALL-F030 (attivo di default) girava con
    `_risk_admission_lock` tenuto, quindi un CASHOUT/CASHOUT_ALL, `risk_stop` o il salvataggio della config
    aspettavano la rete dell'ingresso. Ora il fetch gira fuori dal lock, insieme al best price
    (`_call_outside_admission_lock`), sia sul segnale sia sull'auto-next. Un cashout arrivato nel frattempo
    invalida l'ingresso prima di ogni ordine (`cashout_invalidation_stale`) e il tavolo viene liberato. In
    un'ammissione rientrante il lock resta tenuto;
  - **identità dell'evento dopo il riavvio:** l'ordine ora salva `event_id` ed `event_identity` e la
    ricostruzione F018 li rimette nel meta del tavolo. Con `max_event_exposure_abs` configurato, due segnali
    dello stesso `event_id` con nomi scritti diversamente vengono riconosciuti come lo stesso evento anche dopo
    un riavvio. Gli ordini salvati prima di questa correzione restano riconosciuti per nome, come prima.

  Correzioni dalla review Codex della #522, ognuna con test RED→GREEN:
  - **cashout per partita:** il `CashoutRouter` chiude per `event_name` tutti i mercati della partita, quindi
    la barriera d'ammissione ora copre anche la partita (chiave `("event", nome normalizzato)`), non solo
    mercato e selezione. Prima un CASHOUT con il solo nome evento non fermava nessun ingresso;
  - **config ricaricata durante la finestra di rete:** se `reload_config` sostituisce la config mentre un
    ingresso è fuori dal lock, l'ingresso approvato con la config precedente non parte
    (`config_cambiata_durante_ammissione`). Il tavolo viene liberato e il segnale si può ripetere;
  - **alias `match`:** vale come nome evento anche per il cap evento. Prima, con il cap configurato, un
    segnale con `match` veniva rifiutato come `max_event_exposure_abs_non_verificabile`;
  - **barriera viva fino all'esito dell'hedge** (PF-522-FINAL-HARDENING-01): il bus di produzione è
    asincrono, quindi il routing ritorna dopo aver pubblicato `REQ_EXECUTE_CASHOUT`, ma l'hedge viene
    piazzato più tardi. La barriera ora resta attiva finché ogni gamba pubblicata dal router non riceve un
    esito `CASHOUT_SUCCESS`/`CASHOUT_FAILED` (qualunque status). Un esito che non arriva lascia la barriera
    attiva (fail-closed) fino al riavvio, visibile in `get_status()["cashout_barriers_active"]`. Prima, un
    ingresso nello stesso ambito poteva essere piazzato mentre l'hedge era ancora in coda.
  - Il `RuntimeController` usa gli helper della barriera tramite la classe: i test con stub (`_RC`) che
    chiamano i metodi non legati continuano a funzionare.
  - **Barriera, correzioni dalla verifica indipendente:**
    - **identificativo per richiesta:** ogni `REQ_EXECUTE_CASHOUT` del router porta un `cashout_request_id`,
      che bridge ed executor riportano in ogni esito. La barriera si chiude solo con gli esiti delle proprie
      richieste: l'esito di un altro hedge sulla stessa gamba (auto-green, UI) non la rilascia;
    - **duplicati:** quando il bridge scarta una REQ duplicata pubblica `CASHOUT_REQUEST_SUPPRESSED`, che chiude
      la richiesta. Prima la barriera restava appesa fino al riavvio, in modo globale con CASHOUT_ALL (P1);
    - **esito ignoto:** con esito `AMBIGUOUS` o `ERROR` l'hedge è ignoto e la barriera resta attiva
      (`cashout_barriers_uncertain`);
    - **grace:** il cancel del grace chiude la barriera solo se il Timer non è ancora partito;
    - **`validators.signal_side`:** con un segnale non-dict dà `ValueError` o il default, mai `AttributeError`.

- **#522, review Codex su `c4ac69e`: quattro P1, ciascuno con test RED→GREEN.**
  - **UNMATCHED:** un hedge UNMATCHED resta a riposo e può abbinarsi. La barriera ora si chiude solo quando il
    `CashoutResidualHandler` conferma il cancel (`CASHOUT_RESIDUAL_HANDLED`, `cancel_ok=True`, con il
    `cashout_request_id`). Se il cancel non è confermato, la richiesta resta incerta e la barriera attiva.
  - **Alias evento in conflitto:** `event`, `match` ed `event_name` che indicano eventi diversi vengono rifiutati
    (`evento_ambiguo`). Un unico nome canonico (`_signal_event_name`) alimenta cap evento, barriera, payload
    d'ordine e meta del tavolo.
  - **Ingresso accodato prima di un cashout:** con il bus non bloccante, `CMD_QUICK_BET` è solo accodato. Il fence
    d'ammissione viaggia fino al `TradingEngine`, che lo ricontrolla prima del piazzamento
    (`admission_fence_verdict`). Se nel frattempo è partito un cashout nel suo ambito, l'ordine viene negato
    (`QUICK_BET_FAILED` DENIED, tavolo e dedupe liberati). Il cashout non viene ritardato (PF-EXEC-03).
  - **Cap assoluti su bet manuale e Dutching:** `max_order_exposure`, `max_market_exposure`,
    `max_event_exposure_abs` e `max_drawdown_abs`, se configurati, valgono anche per `manual_bet` e per il
    precheck Dutching, prima di ogni side-effect (`absolute_cap_reason_for_orders`, stessa formula dei segnali).
    I lotti Dutching occupano un tavolo con mercato, evento ed esposizione (`submit_dutching`), quindi entrano
    nelle misure dei controlli successivi. Limite preesistente: il bet manuale non occupa un tavolo, quindi la
    sua esposizione non entra in quelle misure (`manual_bet` oggi non ha chiamanti di produzione). Restano da
    fare prima di attivare le corsie Dutching/manuale (oggi dormienti), registrati in #523 con
    `BLOCKS_DUTCHING_ACTIVATION`: lato obbligatorio per ogni selezione e passaggio dalla barriera cashout e dal
    fence d'ammissione.
  - **Verifica indipendente del delta (#522):** la registrazione del fence ora si consuma per ogni ordine che
    arriva all'engine, anche se lo negano safe mode o RiskGate (prima restava in memoria). Un controllo del
    fence o dei cap diretti che risponde un tipo inatteso blocca l'ordine (`ADMISSION_FENCE_BAD_RESULT`,
    `cap_assoluti_non_verificabili`). Un CASHOUT che nomina la partita con `event` o `match` apre la barriera su
    quella partita. Aggiunti i test per LAY = liability, errore del controllo cap, cap evento senza identità,
    drawdown assoluto e registrazione dell'auto-next. Documentazione in `docs/cashout_overview.md` §7.1.
  - **Verifica owner pre-merge (#522), due P1 corretti con RED→GREEN:**
    - **Ingresso già in invio durante un cashout:** con il bus di produzione l'engine piazza fuori dal lock
      d'ammissione. Un cashout partito fra il controllo del fence e l'invio fotografava l'exchange senza
      l'ordine, che veniva poi piazzato fuori dalla chiusura (riprodotto 3 volte su 3). Ora l'ingresso resta
      «in volo» fino all'esito dell'invio e il cashout attende prima della fotografia. Attesa scaduta
      (35 s) o lock trattenuto: cashout comunque eseguito, barriera incerta fino al riavvio.
    - **Hedge abbinato in parte:** `CASHOUT_SUCCESS` con match parziale chiudeva la barriera e lasciava il
      residuo a riposo senza cancel né record. Ora l'executor dichiara `requested`/`residual`/`partial`, il
      residual handler esegue un solo cancel del residuo e persiste un record `PARTIAL`, e la barriera si
      chiude solo con il cancel confermato. Un successo senza `partial=False` non chiude nulla (fail-closed).
    - Test adeguati al nuovo contratto, senza allentare gli assert: gli esiti `CASHOUT_SUCCESS` sintetici dei
      test dichiarano `partial=False`; due test sulle sottoscrizioni includono `CASHOUT_SUCCESS` del residual
      handler.
    - **Review indipendente della correzione:** un invio con esito AMBIGUOUS (attesa dell'executor scaduta, il
      task può ancora inviare) chiudeva comunque la registrazione «in volo»; ora l'ingresso passa fra gli
      ambigui e ogni cashout nel suo ambito diventa subito incerto, finché un esito noto dell'ordine non lo
      risolve. La registrazione appartiene alla richiesta che l'ha creata: un duplicato con lo stesso
      `customer_ref` negato prima dell'invio non la chiude più. Docstring di `cashout_wiring.py` e del
      residual handler allineati.
    - **Seconda review indipendente:** l'ingresso ambiguo si risolve solo con un esito dello stesso ordine
      (stesso `correlation_id`, non un rifiuto pre-invio DENIED/DUPLICATE_BLOCKED di un duplicato); un secondo
      ingresso con lo stesso `customer_ref` mentre il primo è in invio o ambiguo è negato
      (`ingresso_gia_in_invio`) invece di sovrascrivere la registrazione.
    - **Codex su `fcbaad7` (deroga owner sul fix loop della #522, fino al ciclo 10):** due ingressi in coda con
      lo stesso `customer_ref` si sovrascrivevano il fence, quindi il primo poteva partire con il fence
      dell'altro evento e il secondo senza fence. Ora c'è un fence per richiesta e, finché quel ref ha più
      ingressi in coda, sono negati tutti (`customer_ref_in_conflitto`). Un ingresso rifiutato dall'engine
      prima del controllo del fence (lockdown, normalizzazione fallita, engine non pronto) scarta il proprio
      fence (`admission_fence_discard`, legato all'identità della richiesta): nessun fence orfano lascia le
      riconsegne dello stesso intento in conflitto fino al riavvio (review indipendente su `7a715c6`).
    - **Corsie Dutching e `manual_bet`:** il guardrail `tests/guardrails/test_dutching_lane_dormant_523.py`
      fallisce se il codice di produzione pubblica `REQ_PLACE_DUTCHING`/`CMD_PLACE_DUTCHING` (fuori
      dall'inoltro del RiskMiddleware) o chiama `manual_bet`/`submit_dutching`/`precheck`, e rimanda alla
      #523.

- **Rettifica owner 18/10/2026, chiarimento definitivo del 19/10: tetti monetari preesistenti.**
  `max_stake_abs` (RSR-15, tetto allo stake per ordine) e `max_win` (RSR-26, tetto alla vincita/esposizione
  per gamba sul RiskGate, sul precheck Dutching e sul bet manuale) sono **opzionali e disattivati di
  default**. Il default numerico 10000.0 è stato rimosso da `RoserpinaConfig`, dal loader, dalla GUI e da
  `trading_config.MAX_WIN`. Nella GUI un campo vuoto significa "non attivo". Un valore presente ma illeggibile
  o `<= 0` blocca gli ordini (fail-closed), non disattiva il tetto. Un `10000.0` salvato prima della rettifica,
  cioè il vecchio default scritto a ogni salvataggio della tab, vale "non attivo"; dopo il primo salvataggio la
  marcatura `roserpina.monetary_caps_rectified_20261018` rende esplicito ogni valore successivo. Restano
  invariati le formule e le percentuali del Money Management, i fondi, la liability LAY, le riserve, i vincoli
  Betfair e le protezioni di integrità. Anche un valore `<= 0` salvato prima della rettifica vale "non attivo":
  prima significava "nessun tetto" per `max_stake_abs` e ricadeva sul default per `max_win`. Dopo la marcatura,
  invece, un valore `<= 0` è una configurazione non valida e blocca.

  **Residui dichiarati** (verifica indipendente, P2):
  - un CASHOUT_ALL con una gamba AMBIGUOUS/ERROR tiene una barriera globale fino al riavvio, senza sblocco
    manuale (fail-closed, visibile in `cashout_barriers_uncertain`);
  - un `10000` scelto esplicitamente prima della rettifica non è distinguibile dal default storico e diventa
    "non attivo" (log info);
  - il set dei grace annullati cresce di un intero per cancel riuscito, senza effetti sulla correttezza;
  - se in futuro si aggiunge un secondo publisher di `REQ_EXECUTE_CASHOUT` (UI, auto-green), una sua REQ in
    volo non è coperta dalla barriera (commento in `core/runtime_controller.py`).

- **PR #512 ciclo 17 (PF-QA-AUTONOMOUS-CONTINUATION-01):** bloccante GPT-6.1 Sol su `9f0d6ec`. La riconciliazione
  sull'exchange vale ora per ogni settlement non SIM per mercato (anche chiusura di posizione e payload senza
  `event_key`), non solo per `cleared:`; senza verifica possibile nessun rilascio per mercato. Test esistenti che
  rilasciavano con payload senza chiave e senza exchange (`tests/f026/test_f026_units.py`, due test c14) usano ora la
  chiave SIM per mercato che intendevano. Test `test_c17_*`.
- **PR #512 ciclo 16 (PF-QA-AUTONOMOUS-CONTINUATION-01):** bloccante GPT-6.1 Sol su `23bdbfe` (filesystem in sola
  lettura: falliscono sia il registro hedge sia la sentinella). `_market_has_foreign_cleared_legs`: prima di un
  rilascio per mercato da settlement cleared per bet si interroga l'exchange (`listClearedOrders` per mercato); bet
  regolate non del bot, lettura fallita o risposta vuota/malformata => nessun rilascio (fail-closed, nessuna
  dipendenza da scritture locali). Test `test_c16_*` in `tests/test_pr512_codex_034af59.py`.
- **PR #512 ciclo 15 (PF-QA-AUTONOMOUS-CONTINUATION-01):** bloccante GPT-6.1 Sol su `180ffa3`. Se la scrittura del
  registro `roserpina.hedged_markets` falliva, il fail-closed restava in RAM e dopo un riavvio il registro leggibile
  (senza quel mercato) era trattato come affidabile. Ora si scrive anche una sentinella durevole accanto al DB
  (`<db_path>.hedged_markets.unreliable`, fsync); se presente all'avvio il registro e' inaffidabile finche'
  l'operatore non la rimuove dopo la riconciliazione. Test `test_c15_*` in `tests/test_pr512_codex_034af59.py`.
- **PR #512 ciclo 14 (PF-QA-AUTONOMOUS-CONTINUATION-01 / PF-PR-SEQUENCE-RECOVERY-01, owner 19/10/2026):** la verifica
  indipendente dei rinvii ha classificato BLOCKING il residuo di r4303297587. Con posizione singola + hedge di cashout
  (che passa da OrderRouter senza riga in `orders`) il settlement per bet senza `table_id` di una sola gamba liberava
  il tavolo con il P&L di quella gamba, falsando la perdita di recovery e lo stake successivo. Ora ogni esito di
  cashout (`CASHOUT_SUCCESS`/`CASHOUT_FAILED` con mercato) marca il mercato nel registro durevole
  `roserpina.hedged_markets` (lock, 365 giorni) e `_single_bet_position` rifiuta il rilascio per un settlement PER
  BET su quel mercato, o con registro illeggibile/non persistito (fail-closed); il settlement SIM per mercato (netto
  di tutte le gambe) continua a rilasciare. Corretta
  anche la nota del ciclo 10 qui sotto («limite invariato rispetto a main»: falsa per l'effetto sulla recovery). Test
  RED->GREEN in `tests/test_pr512_codex_034af59.py` (`test_c14_*`). Rilievi Codex su `6138254` corretti nello stesso
  ciclo: (a) `core/liquidity.single_entry_liquidity` richiede stato esplicito OPEN/ACTIVE, una risposta senza stato
  non e' liquidita' osservata (r4312719713); (b) `MicrostakeCoordinator.execute`: ordine gia' presente con lo stesso
  `customer_ref` => AMBIGUOUS con riserva trattenuta, mai FAILED, e `recover()` non adotta mai quell'ordine
  (r4312719704); (c) `pfckpt.capture_code_patch` non dereferenzia mai i symlink non tracciati: registra solo il testo
  del link (`skipped_symlinks`), il ripristino lo ricrea (r4312719746, sicurezza). Test in
  `tests/agent_checkpoint/test_pfckpt_codex_034af59.py::test_c14_*`.
- **PR #512 ciclo 13 (PF-QA-AUTONOMOUS-CONTINUATION-01, owner 19/10/2026, #426):** rilievi Codex su `4231de9`.
  (1) `TableManager.restore_recovery_state`: una perdita di recovery persistita negativa e' stato illeggibile
  (avvio in PAUSED), non piu' un debito cancellato in silenzio; lo zero resta ammesso (r4312360615). (2)
  `TradingEngine._resolve_executor_future`: un `CancelledError` sollevato dalla callable gia' partita e' AMBIGUOUS,
  non pre-invio; solo un Future davvero cancellato resta pre-invio (r4312360608). (3) `BetfairClient.get_current_orders`
  e `BetfairService._well_formed_current_orders`: `currentOrders` assente/null e' `CURRENT_ORDERS_MALFORMED`, mai
  "nessun ordine" per la ricostruzione F018; la lista vuota esplicita resta il caso legittimo (r4312360620). Il test
  legacy che codificava il fail-open (`test_get_current_orders_empty_result_returns_empty_list`) usa ora una lista
  vuota esplicita. Test RED->GREEN in `tests/test_pr512_codex_034af59.py` (`test_c13_*`).
- **PR #512 ciclo 12 (decisioni owner del 19/10/2026: autorizzazione straordinaria del ciclo 12 e
  PF-QA-AUTONOMOUS-CONTINUATION-01, #426):** P1 Codex su `3d3f5cc`. (1) `NativeSimMicrostakeExecutor.execute`: un
  errore ordinario del DB (non `SagaStoreError`) dopo un'operazione exchange diventa AMBIGUOUS
  (`saga_store_write_failed_after_exchange_call`), con riserva trattenuta e dedupe consumato; prima finiva FAILED
  (PERMANENT) con lo stub vivo sul broker e il tavolo libero (r4311806874). (2) Emergency stop: la provenienza dei
  settlement si persiste insieme al flag e si ripristina al riavvio; assente o non valida => `UNKNOWN`, trattata come
  LIVE (r4311806886). (3) Riesame indipendente: in `SimulationExchangePort.place` un errore di lettura DOPO
  l'accettazione dell'ordine e' una risposta persa (`LostResponse`, riconciliazione per `customer_ref`), non un errore
  generico che rilasciava la riserva chiudendo FAILED con lo stub vivo. Test RED->GREEN in
  `tests/test_pr512_codex_034af59.py` (`test_c12_*`, compresa la dedupe durevole dopo un nuovo engine).
- **PR #512 ciclo 10 (deroga owner PF-PR512-CYCLES-08-10-AUTHORIZATION, ultimo ciclo):** rilievi Codex su `b918a6d`.
  (1) il reset trattiene OGNI tavolo con esito ignoto dal registro per tavolo, anche a mercato regolato (r4308704060);
  (2) stato di recovery non persistito => nessun nuovo ingresso su ogni percorso (`recovery_state_not_persisted`, via
  `_loss_block_reason`) con nuovo tentativo di scrittura a ogni ammissione, che scioglie il blocco appena il disco
  torna (r4308704062); (3) all'avvio le riserve transitorie delle saghe pendenti (`reservation_held`, assente =
  tenuta) sono ripristinate prima di snapshot e recovery, cosi' lo stub pendente occupa il cap (r4308704071);
  (4) `pf_trace`: solo un'evidenza PASS copre un test dichiarato (r4308704068); (5) `pfckpt`: `STATE.json` JSON
  valido ma non oggetto e' un checkpoint invalido strutturato (`STATE_JSON_INVALID`), saltato da
  `last_valid_checkpoint` (r4308704074). Test RED->GREEN in `tests/test_pr512_codex_034af59.py`,
  `tests/traceability/test_pf_trace_codex_034af59.py`, `tests/agent_checkpoint/test_pfckpt_codex_034af59.py`.
  Verifica indipendente dei thread rinviati (decisione owner opzione 2): due non separabili corretti qui. (a)
  Settlement per bet senza `table_id`: il tavolo si libera per mercato solo se la posizione e' a selezione singola con
  UN solo ordine del bot sul mercato/evento che possa essere stato piazzato (`_single_bet_position`; esclusi solo
  FAILED/DENIED/DUPLICATE_BLOCKED); Dutching, piu' ordini persistiti o DB illeggibile => nessun rilascio, tavolo
  trattenuto, mercato marcato regolato. Limite noto (corretto nel ciclo 14, vedi sopra): un hedge di cashout passa da
  OrderRouter senza riga in `orders` e non era visto (lotti D-D/D-F) (prima il P&L della prima gamba diventava la perdita di
  recovery del tavolo; aggregazione per `bet_id` = lotto D-F, r4303297587). (b) Dossier dei 41 parametri: nessun
  clamp dello stake minimo, rifiuto esplicito (r4306544234). Test CI robusto al clone shallow; `pfckpt` rifiuta
  CURRENT/TASK_STATE non oggetto; cinque database finti di test con impostazioni in memoria (estensione di scope
  dichiarata). Rollback microstake con stub abbinato anche in parte prima della cancellazione => AMBIGUOUS
  `rollback_matched` con riserva tenuta, mai ROLLED_BACK/FAILED (riesame indipendente).
- **PR #512 ciclo 11 (autorizzazione owner straordinaria del 19/10/2026, #426; ultimo ciclo, il 12 non e' autorizzato):** bloccanti di Grok su `9ab753a`. (1) Settlement per bet
  senza `table_id`: conta ogni ordine del bot sullo stesso mercato, qualunque selezione; il tavolo si libera solo se
  l'unico ordine del mercato e' il suo (prima il P&L di un'altra selezione poteva diventare la perdita di recovery del
  tavolo, confermato su sqlite reale). (2) Recovery delle riserve microstake: importo transitorio ignoto => liability
  dello stub ricalcolata da lato e prezzo persistiti, o riserva illimitata; mai il solo stub da 2 EUR. Rilievi Codex su `9ab753a`
  confermati dalla verifica indipendente: (3) rollback deciso con stub gia' abbinato in parte => il recover cancella
  comunque il residuo, poi AMBIGUOUS `rollback_matched` (r4309039350); (4) F028 libera il tavolo dell'ORDINE
  (`table_id` persistito con verifica dell'evento), mai quello di un altro ordine vivo sullo stesso evento; senza
  `table_id` e con piu' tavoli nessun rilascio (r4309039372); (5) Dutching: book assente con guard in blocco =>
  `LIQUIDITY_UNKNOWN` per ogni gamba (r4309039358). Trasferiti: r4309039345 -> #521, r4309039363 -> #516,
  r4309039377 -> #518. Test di integrazione `test_fail_open_when_book_missing_block_mode` (codificava il fail-open)
  sostituito da `test_unknown_liquidity_when_book_missing_blocks_in_block_mode`; dossier, docstring e
  `docs/roserpina_bankroll_equity.md` (F028 per `table_id`) allineati.
- **PR #512 ciclo 9 (deroga owner PF-PR512-CYCLES-08-10-AUTHORIZATION):** (1) readback del checkpoint con tutti i branch
  remoti; evidenza su albero non pubblicato ammessa solo se dichiarata (`unpublished_tree` + `sha_note`); (2) bloccante Sol:
  cancellazione del rollback fallita => risultato AMBIGUOUS con riserva tenuta ma fase persistita ROLLBACK_PENDING, il
  recover successivo ritenta solo la cancellazione (mai replace); (3) nessun secondo ordine su un evento con tavolo gia'
  occupato (`evento_gia_in_posizione`) e chiavi anti-duplicato trattenute/ricostruite esenti dal TTL (Codex r4307328207,
  difetto preesistente su main ristretto); (4) baseline F032, stato di recovery e marker del reset in una sola
  transazione (r4307328199); (5) saga ROLLED_BACK riconciliata all'avvio, non pendente (r4307328204); (6) `pf_trace`:
  un test dichiarato e' coperto solo da un'evidenza che lo nomina, mai per implicito (r4307328211); aggiunta l'evidenza
  reale mancante per `tests/test_pr512_codex_findings.py` (PF-MS-01/02/05, append-only). Test RED->GREEN in
  `tests/test_pr512_codex_034af59.py`, `tests/traceability/test_pf_trace_codex_034af59.py`,
  `tests/agent_checkpoint/test_pfckpt_codex_034af59.py`.
  Riesame indipendente del ciclo 9 (prima del push): (a) l'intento di rollback della saga e' un flag durevole
  `rollback_intent`, che un recover senza ordine visibile non declassa ad AMBIGUOUS (prima il recover successivo
  riduceva e riprezzava l'ordine da cancellare); un residuo gia' cancellato con snapshot SIM non persistito si chiude
  ROLLED_BACK al giro dopo, senza seconda cancellazione; (b) `DuplicationGuard.unpin`: una chiave fissata al riavvio
  torna alla scadenza per TTL quando F028 libera il tavolo (prima bloccava l'evento fino al riavvio); (c) un reset di
  ciclo non persistito resta in sospeso e viaggia con la scrittura successiva dello stato tavoli; (d) `pf_trace`: gli
  argomenti delle opzioni non sono bersagli, i comandi che filtrano i test (-k, -m, --deselect, --ignore, --lf,
  --collect-only) o che non invocano pytest non coprono nulla senza `tests` esplicito; (e) `unpublished_tree` esige
  comunque uno SHA ben formato; (f) una saga pendente nello store ma assente dai risultati del recover resta pendente.
  Secondo riesame indipendente (prima del push): un reset non persistito e' abbandonato e dichiarato
  (`CYCLE_RESET_ABANDONED`) quando lo stato si ricarica dal disco (START nello stesso processo), mai applicato alle
  perdite del ciclo vecchio; sul broker SIM reale, dove l'ordine cancellato sparisce dagli ordini correnti, il rollback
  deciso si chiude leggendo per bet id la riga terminale senza residuo (un ordine ancora vivo resta pendente);
  `pf_trace` riconosce i filtri nei gruppi di opzioni corte (`-qk`, `-vm`), le opzioni che non eseguono test
  (`--setup-plan`, `--fixtures`, ...) e `pytest` solo in posizione di comando; ogni SHA di test del checkpoint deve
  essere esadecimale (mai `HEAD`/`main`); con store illeggibile dopo il recover si riportano tutti i ref pendenti.
  Terzo riesame: fixture di `tests/test_pr512_review_cycle6.py` allineata a `acquire(..., pin=True)`; test sulle righe
  terminali con abbinato trovate per bet id (sempre AMBIGUOUS); `pf_trace` non deriva copertura da `|| true`,
  `PYTEST_ADDOPTS`, `-o addopts` e `--cache-show`.
  Nota di comportamento: il rifiuto `evento_gia_in_posizione` (un solo ordine per evento in posizione) vale con
  `anti_duplication_enabled=true` (default), come estensione del dedupe oltre il TTL. Con l'anti-duplicazione
  disattivata dall'utente restano OBBLIGATORI il rifiuto su una posizione con esito incerto (AMBIGUOUS, registrata
  per tavolo: l'esito certo di un altro ordine sullo stesso evento non la cancella) o con chiave fissata
  (`DuplicationGuard.is_pinned`: ricostruita al riavvio da F018, o trattenuta dal reset se la chiave era attiva) e il
  dedupe dell'engine per customer_ref sulla riconsegna dello stesso messaggio (mandato owner di chiusura #512). Un
  nuovo segnale su una posizione regolare e nota, nello stesso processo, segue il comportamento precedente; dopo un
  riavvio la posizione ricostruita e' fissata e blocca (prudenziale).
- **PR #512 — patch preparate per i 10 rilievi Codex su 034af59 (in attesa di deroga ciclo 8).**
  (1) `services/betfair_service.py`: riga di `currentOrders` senza `betId`/`marketId` ⇒
  `CURRENT_ORDERS_MALFORMED` ⇒ F018 in PAUSED, mai "posizione chiusa"; (2) `core/microstake_sim_port.py`:
  snapshot finale del broker SIM non persistito ⇒ saga AMBIGUOUS con riserva tenuta, mai SUCCESS/ACTIVE,
  `recover_pending()` ritenta lo snapshot prima di promuovere; (3) `controllers/dutching_controller.py`:
  liquidità ignota con guard armato ⇒ shortfall `LIQUIDITY_UNKNOWN` (blocco); (4) `core/microstake_safe.py`:
  `recover()` su ROLLBACK_PENDING cancella il residuo (ROLLED_BACK / AMBIGUOUS), mai replace;
  (5) `core/runtime_controller.py`: `reset_cycle`/auto-reset con persistenza fallita ⇒
  `_recovery_persist_failed`, evento `RECOVERY_STATE_PERSIST_FAILED`, `persisted: false`; (6) stato di
  recovery illeggibile al riavvio ⇒ PAUSED (`RECOVERY_STATE_UNREADABLE`), `resume()` non lo scavalca,
  nessuna sovrascrittura; (7) docstring di `core/microstake_safe.py` veritiera; (8) `pf_trace`: l'ultima
  evidenza deve essere PASS; (9) `pfckpt.verify`: ogni `tests_executed[].sha` risolto come `code.sha`;
  (10) `core/money_management.py`: crescita ignota ⇒ NORMAL anche con soglia ≤ 0. Test:
  `tests/test_pr512_codex_034af59.py`, `tests/traceability/test_pf_trace_codex_034af59.py`,
  `tests/agent_checkpoint/test_pfckpt_codex_034af59.py` (tutti RED prima della patch).
- **PR #512 ciclo 7 (deroga owner PF-PR512-CYCLE7-AUTHORIZATION, tre P1 Codex su f62f0e3):** (1) riga pendente senza
  lato persistito => non attribuita, mai ricostruita come BACK (la liability di un LAY sarebbe sottostimata;
  r4306544229); (2) `reset_cycle()` non azzera il broker SIM finche' un tavolo resta trattenuto (esito
  `sim_reset_skipped`; r4306544236); (3) baseline di espansione piu' vecchia dell'ultimo reset di ciclo durevole
  (marker `roserpina.cycle_reset_state` scritto nella stessa transazione dello stato tavoli RSR-12) rifiutata al
  riavvio: nessuna espansione da una baseline mai ribasata (r4306544231). Riesame indipendente del ciclo: baseline e
  marker dello stesso reset portano lo stesso istante (la baseline fissata da `reset_cycle`/auto-reset sopravvive al
  riavvio), un marker illeggibile vale come reset ignoto e un `set_at_ts` non finito (NaN/inf) nella baseline non
  scavalca mai il confronto (nessuna espansione, fail-closed). Test RED->GREEN in
  `tests/test_pr512_codex_f62f0e3.py`; doc `docs/roserpina_bankroll_equity.md` (F032).
- **PR #512 ciclo 6 (deroga owner PF-DECISIONS-DA-DJ-20261018, D-A=A1):** (1) `MicrostakeCoordinator.execute` conserva
  la riserva transitoria **preesistente** quando `store.load` fallisce su un retry dello stesso `customer_ref`
  (rilascio solo di una riserva sicuramente nuova; Sol c.6242912622); (2) costruzione e rebinding dell'esecutore
  Microstake serializzati da un lock dedicato in `TradingEngine` (due chiamate concorrenti => un solo esecutore,
  una sola mappa di riserve, un solo store; Codex r4304867227); (3) decisione D-C: `BetfairClient.get_current_orders`
  e `BetfairService.list_current_orders` sollevano `CURRENT_ORDERS_MALFORMED` su contenitore non-lista o riga non-dict
  invece di filtrare a vuoto, cosi' la ricostruzione all'avvio finisce in `EXCHANGE_ORDERS_UNAVAILABLE` e non chiude
  posizioni SUBMITTED vive (Codex r4304867229). Test RED→GREEN in `tests/test_pr512_review_cycle6.py`.
- **PF-PR-RECOVERY-DECISIONS-02 (owner 17/10, lotto unico sui sei finding triagiati di PR #512):** `resume()` ritenta
  anche la recovery delle saghe Microstake (non solo F018) e resta PAUSED finché entrambe non riescono; stake con più
  di due decimali rifiutato prima dell'invio (`invalid_size_precision`) e residuo del broker diverso dallo stake
  richiesto ⇒ AMBIGUOUS `unexpected_residual_size`; ladder del book malformata ⇒ liquidità ignota (RiskGate nega),
  mai un'eccezione dopo tavolo e dedupe acquisiti; `persist_snapshot` del broker SIM scritto sotto lock e fallimento
  dichiarato nel risultato (`snapshot_persisted`); `pre_send` calcolato per `customer_ref`; scan segreti del
  checkpoint esteso ai sorgenti Python (esente solo `pfckpt.py`); `pf_trace` verifica che ogni SHA di evidenza sia un
  commit reale (clone shallow ⇒ warning, `--strict` lo rende errore). Test: `tests/test_pr512_recovery_fixes.py`.
- **PR #512 ciclo 4 (rilievi Codex su cad07e1/21aab95):** esecutore Microstake in cache legato al broker SIM e alla
  modalità correnti (ricostruito dopo stop/start, mai l'adapter SIM in LIVE); fondi disponibili non leggibili o
  incompleti ⇒ cap transitorio non disponibile; riserva transitoria rilasciata se lo store della saga fallisce prima
  dell'invio; cashout AMBIGUOUS conserva il `matched` noto; nessun nuovo hedge nativo su una posizione con saga
  pendente (`MICROSTAKE_PENDING_SAME_POSITION`); valori non finiti nella ladder ⇒ liquidità ignota; perdita di
  recovery persistita non finita ⇒ stato illeggibile; `pfckpt` cattura i percorsi con spazi (`--porcelain -z`).
  Test: `tests/test_pr512_review_cycle4.py`.
- **Decisione owner 2 (PF-PR-RECOVERY-DECISIONS-02):** `RoserpinaMoneyManagement.calculate` non ridimensiona più lo
  stake al tetto del singolo ordine: sopra `max_single_bet` rifiuto esplicito `supera_max_single_bet` con lo stake
  richiesto (anche per i passi di recovery). Riserva transitoria di una saga Microstake già avviata conservata anche se
  `recover()` fallisce (rilievi Sol/Grok su 464f347). Test: `tests/core/test_money_management_decision2_20261017.py`.

### Added
- **Microstake nativo automatico (PF-MICROSTAKE-NATIVE-INTEGRATION-02, decisioni owner PF-MICROSTAKE-NATIVE-AUTO-01 e
  PF-MICROSTAKE-OWNER-RECTIFICATION-20261017):** ogni importo sotto il minimo exchange (2 EUR) passa dal Microstake
  nativo — saga PLACE stub 2 EUR a prezzo passivo → `sizeReduction` al residuo esatto → REPLACE solo se serve (nuovo
  betId persistito) — sul `SimulationBroker` reale (`core/microstake_sim_port.py`), senza flag; LIVE senza adapter
  resta fail-closed. Vale per Quick Bet, Dutching, Roserpina e per l'hedge del cashout (`OrderRouter`). Esposizione
  temporanea dello stub entro fondi disponibili e headroom di esposizione; saga nel DB (`microstake.sagas`),
  recovery all'avvio prima di F018 (PAUSED se pendente). `SimulationBroker.cancel_orders` supporta `sizeReduction`.
  Docs: `docs/microstake_native.md`.

- **Continuità multi-agente (PF-AGENT-CONTINUITY-01):** `pfckpt` aggiunge `CURRENT.json`, `TASK_STATE.json`
  (puntatori verificati contro STATE.json), `AGENT_LEASE.json` (lease con scadenza e fencing token letto dal
  checkpoint remoto: `save --agent` obbligatorio, `LEASE_HELD` se tenuto da altri, `--release` per l'handoff), cattura
  e ripristino della patch del codice non committato (`--code-repo`, `restore-code`), comando `recover` (ultimo
  checkpoint valido, remoto intatto) e rilevazione della divergenza del branch di codice in `bootstrap`.

- **Riconciliazione delle issue aperte (PF-ISSUE-RECONCILIATION-01):** `docs/issue_reconciliation/` (csv/json/md):
  60 issue aperte al 17/10/2026 classificate con motivazione ed evidenza, deduplicate per scheda #461.

### Fixed
- **Rilievi Codex su PR #512 (commit 8ffb4f3):** riserva transitoria del Microstake rilasciata all'attivazione; blocco
  pre-invio del Microstake = errore pre-invio tipizzato (intento dedupe rilasciato, retry non duplicato); snapshot del
  broker SIM persistito dopo ogni operazione della saga; ricostruzione F018: righe pendenti senza attribuzione =>
  avvio non ok, chiavi dedupe dei tavoli gia' occupati riacquisite, betId annidato letto; RSR-12: tavolo surplus con
  perdita ricreato al riavvio, write failure dello stato di recovery fail-closed.

### Changed
- **Money Management (rettifica owner 17/10/2026):** lo stake calcolato non viene più alzato in silenzio al
  `min_stake`: sotto il minimo configurato la decisione è rifiutata con `stake_sotto_min_stake` e lo stake richiesto
  in chiaro. Test F001/side-parity del trasporto diretto portati sopra il minimo exchange (il percorso diretto non
  riceve più importi sotto 2 EUR).
- **Checkpoint multi-agente `pf-checkpoint/3`** (`tools/agent_checkpoint/pfckpt.py`, `docs/agent_checkpoint_protocol.md`):
  verifica sul tree committato, salvataggio atomico con concorrenza ottimistica, ripresa idempotente dopo crash,
  readback da clone pulito, bootstrap da zero; ripara l'incidente INC-b45f528 (artefatti `*.log` esclusi dal
  `.gitignore`). Regole per tutti gli agenti in AGENTS.md/CLAUDE.md. Nessun effetto sul runtime di trading.

### Fixed
- **F026 (v2):** Roserpina distingue saldo disponibile, fondi impegnati ed equità di cassa del conto
  (`RuntimeController._account_from_funds`, soli campi exchange). L'auto-reset, che ribasa il picco e libera
  risorse, si decide sul regolato e non sul solo disponibile: le puntate aperte non sono più perdite. Il reset
  trattiene tavoli e chiavi delle posizioni non regolate. La base di stake e dei limiti percentuali **non cambia**
  (resta il disponibile). Vedi `docs/roserpina_bankroll_equity.md`.
- **F2:** un settlement senza `table_id` (poller SIM/cleared) libera il tavolo del suo mercato, solo se univoco e
  con esito certo.
- **F018:** al riavvio le posizioni aperte vengono ricostruite dal DB e dagli ordini correnti dell'exchange (tavoli,
  esposizione, chiavi anti-duplicato, riserve incerte). Con exchange illeggibile o tavoli insufficienti il runtime
  parte in PAUSED e `resume()` rifiuta finché la riconciliazione non è completa.
- **F032:** l'espansione dello stake Roserpina si raggiunge davvero: la crescita si misura sull'equità di cassa
  regolata rispetto a una baseline persistente del ciclo (non più sul picco che segue il saldo); dati incerti ⇒
  nessuna espansione.
- **RSR-12:** lo stato dei tavoli di recovery è persistente e sopravvive al riavvio; il limite conta anche i tavoli
  in recovery senza ordini.
- **F027:** i settlement confermati si contabilizzano anche in PAUSED, LOCKDOWN ed emergency stop (nessun nuovo
  ordine dalla riconciliazione; dopo un emergency stop in LIVE il registro SIM non viene usato e l'incertezza si
  segnala).
- **F028:** un ordine d'ingresso con stato finale confermato dal broker e nessun abbinato libera tavolo e riserva;
  gli esiti incerti non vengono mai liberati.
- **F031:** nel broker SIM un ordine parzialmente abbinato con residuo annullato resta fra gli ordini correnti
  (parità con Betfair): F018 lo ricostruisce al riavvio.
- **F030:** il Liquidity Guard protegge anche gli ingressi singoli BACK/LAY (liquidità eseguibile al prezzo, lato
  corretto del book, `core/liquidity.py`); un book assente o non attendibile nega l'ingresso anche in solo-avviso.
  Il Book Block resta sul dutching: per una singola selezione non esiste un book percentuale significativo.
- **Stop di esposizione:** un ordine candidato che raggiungerebbe `max_exposure_stop` si rifiuta senza armare un
  RISK_STOP persistente; lo stop scatta quando è l'esposizione reale a raggiungere il limite.
- **GUI Roserpina:** etichette Max Win e Liquidity riallineate al comportamento (Max Win resta un blocco hard sul
  percorso ordine); checkbox su quattro righe, prima gli ultimi uscivano dalla finestra.
- **F029:** `TableManager.allocate` non riassegna più un tavolo in recovery già occupato da una posizione aperta,
  né un tavolo oltre `table_count`. Prima, con recovery attivo e dopo una perdita, un secondo ingresso riceveva
  lo stesso tavolo e ne sovrascriveva la posizione: esposizione persa dal totale dei cap e due posizioni su un
  tavolo. Trovato dalla campagna di simulazione Roserpina (con un solo tavolo), reso raggiungibile da F2.

### PR28-d — cap assoluti Roserpina (opzionali)
- Aggiunti i limiti assoluti in euro configurabili per singolo ordine,
  mercato, evento (identita' canonica) e drawdown assoluto, applicati sul
  rischio BACK/LAY e insieme ai limiti percentuali/totali esistenti.
- Rettifica owner 18/10/2026: nessun cap monetario fisso di default. I
  quattro limiti sono DISATTIVI finche' l'utente non li configura (chiave
  assente, `None` o campo vuoto = non attivo, mai un numero); un valore
  presente ma invalido (0, negativo, NaN, bool, testo) non e' un opt-out e
  nega i nuovi ordini con `<nome>_non_valido`; un valore valido e' applicato
  esattamente; le chiavi legacy senza prefisso `roserpina.` sono lette e
  loggate. `max_drawdown_abs` configurato mantiene il LOCKDOWN; assente non
  produce alcun lockdown. Rimosso il loader con default numerico
  (`_required_absolute_cap_value`); i campi GUI partono vuoti con etichetta
  «(€, vuoto = non attivo)». Salvare uno di questi quattro campi vuoto
  disattiva anche un valore gia' persistito (scritto come vuoto, letto come
  `None`, oscura la chiave legacy); gli hard-stop di perdita/esposizione
  conservano la semantica «vuoto preserva il valore salvato» (PF-CAP-10b).
- Il lato dell'ordine e' ora obbligatorio e coerente fra alias: assente,
  ambiguo o invalido viene rifiutato senza default BACK e senza invio.
- P06 per-strategy resta esplicitamente rinviato alla futura gestione MCP;
  questa slice non introduce strategie, bucket o un secondo money manager.
- Gli ingressi catturano una generation scoped market/selection prima del
  best-price; il cashout alza una barriera prima del routing e invalida gli
  ingressi concorrenti senza mantenere il lock durante lookup o network. Gli
  ordini stale rilasciano dedupe e tavolo prima del rifiuto.

### Security
- Secrets (`password`, `api_hash`, `session_string`, etc.) now encrypted at
  rest in SQLite using a stdlib-only XOF stream cipher (`hashlib.shake_256`).
  Wire format: `enc:v1:<base64(nonce_16 + ciphertext)>`.
- Sensitive fields are redacted in all Telegram alert text and log output via
  `observability/sanitizers.py` with dot-notation key support
  (`telegram.api_hash`, `betfair.password`, …).
- `.gitignore` now covers `*.pem`, `*.key`, `.pickfair/`, `.env.*` and common
  secret file patterns.
- AI review workflows (the five `pr-review-*.yml`): when a secret key starts
  the line (`.env`, YAML, INI, properties, `export`/`set`, list items, comments
  with `#`, `//`, `;`, `!`, `--`, `/*` or `<!--`), an
  unquoted value is now redacted up to the end of the line, spaces included,
  before the diff reaches the provider and before the review is published. It
  used to stop at the first space, so the rest of the value leaked. Mid-line
  matches (arguments, comparisons, signatures) and code-shaped values at the
  start of a statement (calls, subscripts, built-in type annotations,
  arguments) keep the previous rule, so the reviewer still sees the code.
  Arithmetic operators do not count as code, since encoded secrets and
  passphrases contain them; `==`, `:=` and `=>` are not separators, so
  comparisons stay visible.
  Across the whole repo, 18 of 219,588 lines are now redacted more than before
  (comments, examples, test paths joined with `/`), none of them production
  code. Quoted values and GitHub Actions expressions are unchanged.
  (DECISIONE-426 P14, #479)

### Added
- `core/duplication_guard.py`: concrete `is_duplicate()` / `register()` two-phase
  interface (non-atomic check + register) alongside existing atomic `acquire()`.
- `core/runtime_controller.py`: `emergency_stop()` fail-closed implementation —
  cancels all live orders grouped by market, blocks further signals, publishes
  `EMERGENCY_STOP_TRIGGERED`; `reset_emergency()` unblocks.
- `services/betfair_service.py`: `handle_session_expiry()` with bounded re-auth
  (max 1 attempt); `is_live_usable()` gate; `LIVE_BLOCKED_SESSION_INVALID`
  error when session invalid and live requested.
- `core/reconciliation_engine.py`: concrete fencing token implementation —
  `_next_fencing_token()`, `get_active_fencing_token()`,
  `assert_fencing_ownership()`.
- `circuit_breaker.py`: `record_success()` public method.
- `betfair_client.py`: `_api_breaker` (CircuitBreaker, 5 failures / 60 s)
  protecting all JSON-RPC calls; SESSION_EXPIRED does not trip the breaker.
- `core/trading_engine.py`: `_order_submission_breaker` (CircuitBreaker,
  3 failures / 120 s) protecting the live order submission path.
- `core/type_helpers.py`: canonical `safe_float()`, `safe_int()`, `safe_side()`
  — replaces copy-pasted helpers across 7 modules.
- `core/reconciliation_types.py`: extracted public types/enums from
  `reconciliation_engine.py` for focused module concerns.
- `database_schema.py`: extracted DDL from `Database._init_db()`.
- `requirements-dev.txt`, expanded `requirements-test.txt`.
- `pyproject.toml` packaging metadata.

### Changed
- AI review workflow for Sol (`pr-review-openrouter-gpt56-sol.yml`, file name
  kept): the per-push reviewer moves from `openai/gpt-5.6-sol` to
  `openai/gpt-6.1-sol` on OpenRouter, with reasoning effort `max` instead of
  `high` — the highest level the model offers, checked against its OpenRouter
  metadata. The effort whitelist of this workflow now admits `xhigh` and `max`
  (without it `max` would silently fall back to `high`); the output ceiling
  rises from 25,000 to 64,000 tokens (model maximum 128,000), each request may
  wait 1800 s instead of 100 s and the job timeout rises from 12 to 105 minutes.
  The cost estimate now uses the OpenRouter list price, $2/M input and $10/M
  output: the previous 5.00/30.00 overstated the reported cost about threefold.
  New tests pin the model id, tie the widened whitelist to that id, require a
  higher ceiling and a longer wait at `xhigh`/`max`, and check that the
  failed-call heading is recognised by the done-marker guard. (Owner decision,
  07-10-2026)
- Merge readiness gate (`pr-merge-readiness.yml`): the wait for the head's
  checks rises from 900 s to 6600 s and the job timeout from 25 to 120 minutes.
  The budget must cover the slowest review job (Sol, 105 minutes) plus its
  start-up delay: with 900 s a valid but slow review left the gate red, which
  could already happen with Grok's 20-minute job. The gate still exits as soon
  as the checks are settled, and still fails when the budget runs out. A test
  ties the budget to the five review workflows' timeouts. The worst-case test
  of the review workflows now also counts a GitHub call written over several
  lines: there are seven calls per workflow, not six, and every call must be
  recognised or the test fails.
- AI review workflow for Grok 4.7 (`pr-review-xai-grok46.yml`): follow-up owner
  decision on 07-10-2026 raises the per-attempt wait from 240 s to **400 s**
  and the job timeout from 20 to **30 minutes**, giving large diffs more time
  to finish instead of losing paid attempts. The request is not streamed, so
  the answer arrives only after the reasoning. The earlier 100→240 s change
  remains historical context; waiting longer adds no provider cost because
  billing is per generated token.
  A new test checks that, for all five reviewers, exactly three attempts plus
  backoff, the GitHub calls at their timeout and runner start-up fit in the
  job timeout. (DECISIONE-426 P24)
- `requirements.txt`: removed `pytest` / `pytest-asyncio` (test deps only).
- CI `unit.yml`, `failure.yml`, `net.yml`: install via `requirements-test.txt`
  instead of `requirements.txt + pip install pytest`.
- `pytest.ini`: registered missing markers (`reconciliation`, `safety`,
  `observability`, `mutation`).
- GitHub Actions: removed `|| true` from `pip install` steps (fail-closed CI).
- `core/reconciliation_engine.py`: -325 lines (-15%) after type extraction.
- `database.py`: -262 lines (-19%) after schema extraction.

### Fixed
- Order outcome with the production `ExecutorManager` (PF-ALL-F001, #453): the
  `TradingEngine` now waits for and classifies the `Future` instead of treating
  it as the broker response. Rejections are `FAILED`; timeouts after the task
  started, lost responses, `order_unknown`, mixed or malformed responses are
  `AMBIGUOUS` with reconciliation; a task still queued when the wait expires is
  cancelled and reported as pre-send `FAILED`. A top-level `SUCCESS` whose
  instruction reports are all `FAILURE` is a rejection. No order is recorded as
  `SUBMITTED` without the real broker response.
- Table release on terminal events (PF-ALL-F023): engine bus events carry
  `table_id` and `market_id`; a confirmed failure frees the table only if it still
  holds that order's reservation (`TableManager.force_unlock_if_event`,
  idempotent, lock-protected).
- `QUICK_BET_AMBIGUOUS` no longer releases the table, the exposure or the
  anti-duplicate key (PF-ALL-F024): the risk is held until reconciliation, also
  across `ROSERPINA_AUTO_RESET` and `reset_cycle` (`TableManager.reset_all` and
  `DuplicationGuard.clear` keep the uncertain reservations).
  See `docs/trading_engine_executor_contract.md`.
- RISK_STOP on owner stop limits, separate from EMERGENCY (#461 PR28-c,
  PKG-P24-B: P38 and the P37 exposure stop).
  - Before, reaching the session-loss limit only blocked new entries: the bot's
    unmatched orders stayed in the market and its open positions were not
    closed. There was no total-exposure stop at all.
  - **RISK_STOP** (`RuntimeController.risk_stop`, rules in `core/risk_stop.py`):
    blocks new entries (signals, auto-next, resume) but not cashout; cancels the
    bot's unmatched orders and attempts to close its positions by routing the
    existing `CASHOUT_ALL` through the CashoutRouter (bot orders only, the
    cashout's own gates, blocked or uncertain outcomes reported by the router).
    It publishes `RISK_STOP_TRIGGERED` and is idempotent. If EMERGENCY is
    already active (the strongest barrier, cancel-all + LOCKDOWN) no cashout is
    routed. PAUSE is unchanged (blocks new entries only).
  - The stop is persisted (`risk_stop_state` setting) together with the state
    change, under the same lock, as a versioned record `{active, reason, seq}`
    (the PR28-b model). If the db cannot be written, the same version goes to a
    marker file next to the db (`<db>.risk_stop_pending.json`). At restart the
    readable copy with the higher `seq` wins; an equal or unknown `seq`, or an
    unreadable copy, gives the conservative result (stop active).
  - Reset is a fail-closed two-phase commit. An independent
    `risk_stop_reset_guard` plus `<db>.risk_stop_reset_guard.json` is armed
    BEFORE any `active=false` write and remains active through persistence and
    readback. Only after the inactive state is verified is the DB guard cleared
    and the guard marker removed last. Any failure before that commit leaves a
    durable guard, so a rejected/incomplete reset cannot reopen after restart.
    Only if neither normal stop store can be written during activation does the
    stop hold for the running process alone (KNOWN_LIMITATION, as in PR28-b).
  - Neither a restart nor `start()` clears the stop: tables live in memory
    (exposure reads 0 after a restart) and the session loss restarts, so
    neither can prove the positions are closed. Only an explicit
    `reset_risk_stop()` clears it, after a full recheck: no emergency, daily
    loss not breached, limits valid, session loss below its limit and exposure
    below the exposure stop. While the stop stays on, `start()` re-attempts the
    `CASHOUT_ALL` (for example after a restart).
  - **Triggers:** session loss reached (`max_session_loss`, PR28-b) and the new
    `max_exposure_stop` (EUR, owner limit with no default, GUI/loader/save). It
    is checked before submission on the total exposure including the order; a
    misconfigured value (unreadable or <= 0) blocks entries without closing
    anything. No fixed amount belongs to STOP/cashout (#426, 06/10).
- Durable daily loss and session-loss stop (#461 PR28-b, PKG-P24-B: F4, P37).
  - **F4:** the day's realized loss is saved (`daily_loss_state` setting) and
    rebuilt at restart on the same UTC day, before any new risk. Before, a
    restart zeroed it: EUR 8 lost, restart, another EUR 8 = 16 against
    `max_daily_loss` 10 with no stop. An unreadable saved state, or an invalid
    or future saved day, counts as a breach (fail-closed); a past UTC day
    starts from zero. If a save fails, new risk is refused until a save
    succeeds and the state goes to a marker file next to the db
    (`<db>.daily_loss_pending.json`). Every record (db and marker) carries
    the UTC day and a monotonic sequence number `seq`. At restart both are
    read: the higher `seq` wins; with an equal or unknown `seq` the larger
    loss wins, and a breach in either always counts. A leftover marker
    (failed remove, crash between save and remove) has a lower `seq` and can
    never lower the loss. Known limitation (declared): if neither the db nor
    the marker can be written, the block holds for the session only; a
    restart in that condition cannot rebuild the unsaved loss.
    Persistence lives in the new `core/daily_loss_store.py` (`db_path` may
    be a str or an `os.PathLike`).
  - `reset_cycle()` and the drawdown auto-reset no longer wipe the day's or the
    session's loss (they zero the desk `realized_pnl`; the baselines now shift
    with it).
  - **P37:** new owner limit `max_session_loss` (EUR, realized loss since the
    last `start()`), in GUI, loader and save. No default: empty = not set.
    Reached (with float tolerance) => new entries, auto-next and `resume()`
    are refused and `SESSION_LOSS_BREACH_TRIGGERED` is published once.
    Unreadable or <= 0 => entries refused. It is a risk limit, not a cashout
    parameter: exits/cashout keep their own gates (#426 decision 06/10).
- Owner limits measured on the order's risk (#461 PR28, PKG-P24-B, first slice).
  - **Single formula:** `core/validators.py` `order_exposure` gives BACK = stake
    and LAY = liability `stake * (price - 1)`. `exceeds_cap` holds at the exact
    threshold with float tolerance.
  - **LAY consumers:** money management (single, total and event caps), the A2
    `max_open_exposure` cap, the table exposure and auto-next all used the stake.
    A LAY of 1.5 at 21 (liability 30) counted as 1.5, and a LAY of 10 at 21
    (liability 200) passed the single cap of 180. A LAY over a cap is now
    rejected, with no resizing. RiskGate uses the same function.
  - **Exact threshold:** 0.1 + 0.2 against a cap of 0.3, or a LAY of 0.1 at 4
    against MAX_WIN 0.3, are no longer denied. A one-cent excess still is.
  - **`reload_config`** (GUI save while the bot is running): it no longer
    rebuilds the TableManager and the ReconciliationEngine. Before, tables,
    exposure and recovery memory went back to zero, so the aggregate caps
    restarted from 0. The table count adapts without dropping busy tables.
    `start()` keeps its fresh session.
  - **Auto-next:** it now applies the A2 cap and the real event exposure, and
    activates the table with the liability.
  - **Best price DIRECT:** if the price changes after the caps, a LAY whose
    liability grows is rejected.
- Fail-closed numeric validation for orders and risk (#461 PR27, PKG-P24-A).
  - A new shared source, `core/validators.py` (`order_market_id`,
    `order_selection_id`, `order_price`, `order_stake`, `finite_number`), uses
    the same `INVALID_*` codes as the client. The #488 client check is unchanged.
  - Engine: a present bool, NaN/inf, non-numeric value or non-integer selection
    id is now rejected at normalization (FAILED/INVALID_REQUEST). There is no
    `orders` row, no intent mark, no transport, and the key of an in-flight
    original is not released. Before, `selection_id=True` was placed on
    runner 1, `stake=True` was placed as 1 EUR, and 5678.9 was truncated.
  - Runtime signal: market/selection/price are checked before anti-duplication,
    tables and money management. A non-numeric selection used to raise with the
    event key still locked.
  - A2 cap: `max_open_exposure` that is configured but unreadable
    (NaN/inf/bool/text) now denies. NaN used to disable the cap.
  - Money management:
    - an invalid fixed stake from a signal, including `False`, is rejected
      (`stake_segnale_non_valido`). `0`/`None`/`""` stay the producer's
      "let MM decide" sentinel;
    - a non-finite exposure (`total_exposure()` = inf on a corrupted state,
      #449) is rejected instead of being counted as 0;
    - an unreadable `max_stake_abs` fails closed;
    - a bool recovery cap counts as a corrupted config (chase 0, #450).
  - OrderManager: bool and truncated ids or amounts are rejected before
    conversion.
- Single order authority and durable intent de-dup (#461 PR26, gate
  `LIVE_BLOCKED_UNTIL_PR26_DURABLE_DEDUPE`): the engine marks a
  `customer_ref` as consumed in the new additive SQLite table
  `consumed_order_intents`, committed right before every transport call
  (LIVE branch, declared SIM broker, `OrderManager`, fallback). The mark does
  not follow the order status: the same intent is `DUPLICATE_BLOCKED` after
  COMPLETED, FAILED after send (broker rejection), AMBIGUOUS, and after a
  restart, and two processes on the same file cannot both send (PRIMARY KEY).
  Before, de-dup only saw INFLIGHT/SUBMITTED rows and in-memory keys released
  on terminal outcomes. The mark is removed only with proof that nothing was
  sent: an engine pre-send error, or an argument error raised by the real
  `BetfairClient` or by the identity-proven `SimulationBroker`. Timeout and
  unknown outcomes stay AMBIGUOUS and go to reconciliation, never to a replay.
  STOP and every gate before the transport consume nothing. In declared LIVE a
  DB without the capability sends nothing (`DURABLE_DEDUPE_UNAVAILABLE`). A
  `DUPLICATE_BLOCKED` no longer releases the original's in-memory key.
- `TradingEngine` with `simulation_broker` set and a getter that does not
  return it: places through the broker's `place_bet` with the same side and
  signature adapter instead of failing with `NO_VALID_EXECUTION_PATH`
  (`SimulationBroker` has no `execute()`).
- `SimulationBroker` (F8, F10 of #453): an accepted unmatched order reports
  `SUCCESS` with `orderStatus` `EXECUTABLE`, not `FAILURE`; an invalid market,
  selection, price or size (NaN/Inf included) raises the same
  `INVALID_*` code as `BetfairClient.place_bet` before matching, storage or
  writes, and `place_orders` reports that leg as `FAILURE` with nothing stored.
- `OrderManager` (F14 of #453 and adapter signatures): the client pre-send
  codes are certain rejections (PERMANENT), not AMBIGUOUS; non-finite price or
  stake and an invalid or conflicting side are rejected before saga and client
  (a typo no longer becomes BACK); the `BetfairClient` envelope is read
  (`ok`/`result`/`order_unknown`), so a placed order is no longer FAILED and a
  lost response is AMBIGUOUS; cancel uses each broker's signature for one bet
  id and refuses an empty bet id (never cancel-all) and `size_reduction`.
- `SafetyLayer.validate_cashout_request` (F15 of #453) rejects NaN/Inf price
  and stake.
- Order identity for existing producers (#461 PR26-a): the runtime signal
  path, the post-settlement auto-trade, every dutching leg and
  `DutchingController.manual_bet` now publish `CMD_QUICK_BET` with a
  `customer_ref`. Before, the engine rejected all of them with
  `CUSTOMER_REF_REQUIRED`. `core/order_identity.py` gives every ref the
  Betfair-compliant shape `<prefix>-<28 hex>` (32 chars). Where each ref
  comes from:
  - an upstream ref is preserved, also when the mini-GUI nests it in
    `raw_signal`; a non-compliant one is mapped
    deterministically instead of being silently dropped by `BetfairClient`;
  - signal: derived from the message and its chat, without `received_at`
    or `event_key` (at any depth) and without the MM stake. The message has
    the same canonical form on the headless and mini-GUI paths (the innermost
    `raw_signal`), so the resolved price and the GUI stake are not part of the
    identity, while the effective SIM/LIVE mode is. A redelivery after
    reconnect or restart, even on the other entrypoint, keeps the same ref,
    and the engine blocks it;
  - auto-trade: derived from the triggering settlement;
  - dutching leg: derived from the request inputs (market, total stake,
    sorted selections) plus the leg itself, not from the computed stakes, so
    neither the order of the selections nor the order-sensitive rounding of
    equal odds matters. The ref is also stored in
    `dutching_batch_legs`;
  - `manual_bet` without an upstream ref: a fresh operation ref per call.
  The `RiskMiddleware` REQ_QUICK_BET forward keeps the caller's ref and never
  invents one. The engine gate is unchanged, and the Telegram REQ_QUICK_BET
  compat fallback (no runtime gate) stays rejected.
- Betfair credentials (#461 PR04-quater, DEC-426-P28–P32): separate encrypted
  Delayed and Live App Keys; the legacy key migrates only to Delayed. LIVE
  requires the explicit Live key, and the real client never uses Delayed.
  Settings include certificate/private-key file pickers and validate readable
  PEM files, permissions and the matching TLS pair before an atomic save.
  An empty password preserves the saved password; a replacement clears the
  input after saving. App Keys are masked in the registry and redacted from
  client errors and diagnostics. SIM remains offline; its Delayed feed belongs
  to PR11, and the authenticated encryption upgrade remains in PR35.
  LIVE readiness reads canonical settings without triggering migration and
  reports actionable missing/unavailable-key remedies in registry/headless.
  The legacy credentials writer updates Delayed after migration while keeping
  Live/password unchanged. Colliding redaction masks use a neutral ASCII
  character so strict Windows CP1252 log sinks retain login diagnostics.
  A read-only Live-key status distinguishes absent values from undecodable or
  unsupported ciphertext, retaining the stored value and directing master-key
  recovery. The existing unauthenticated enc:v1 format is unchanged; integrity
  verification remains in PR35.
  Empty GUI saves preserve unreadable Delayed/Live or unmigrated legacy App
  Keys; explicit replacements and clearing readable keys remain supported.
  Registry configuration reports never migrate, including on read-only legacy
  storage, and unsupported legacy enc formats remain retryable after recovery.
  Volatile unreadable-key provenance also protects an old empty form when
  another writer restores or replaces the credential before that form saves.
  Registry entries and presence checklist direct unreadable credentials to
  recovery; expired matching certificates are rejected before persistence.
  Load values and unreadable provenance share a transaction snapshot;
  certificate validation also rejects a future notBefore date.
  The client also blocks a future certificate swapped in after save with the
  trusted CERT_NOT_YET_VALID diagnostic before any login HTTP request.
  Every credential echo is redacted, including short/concatenated values and
  login passwords in client diagnostics. Local login codes are separate from
  provider text; session expiry is classified before redaction.
  Certificate diagnostics remain actionable; unreadable legacy credentials
  do not finalize migration, and original ciphertext remains recoverable.
  LIVE readiness/deploy also require the Live key; SIM remains independent.
  Short session tokens and colliding redaction markers are covered.
- Live client, non-finite price or size (F11 in #453, DECISIONE-426 P27):
  `BetfairClient.place_bet` now rejects a price or a size that is NaN or
  infinite with `INVALID_PRICE` / `INVALID_SIZE` before building the request,
  as `replace_orders` already did. Before, `float()` accepted them and
  `nan <= 1.0` is false, so the `placeOrders` body went out with
  `"price": NaN` or `"size": Infinity`; Betfair would have refused a body
  that is not valid JSON, but the check belongs to the client. In production
  the cashout chain reaches the client through `OrderRouter.place`, where the
  SafetyLayer only checks `price <= 1` and `stake <= 0`: the client was the
  only barrier. `BetfairService.place_order` and `OrderManager.place_order`
  (which only checks `price <= 1.0`) had the same hole, but today nothing in
  production calls them. For the `TradingEngine` the rejection is a certain
  pre-send failure, not an ambiguous outcome.
- Book sides (F6 in #453, DECISIONE-426 P25): the simulation broker, the
  cashout router and the Telegram resolver now read the two ladders as Betfair
  defines them, the convention the owner chose for dutching in #383:
  `availableToBack` holds the prices you can back at now, `availableToLay` the
  prices you can lay at now.
  - SIM: a BACK matches on the best `availableToBack` when its price is at or
    below it, a LAY on the best `availableToLay` when its price is at or above
    it, both at the book price. Before, each side matched against the opposite
    ladder: paper results earned the spread instead of paying it, and a BACK at
    the really executable price stayed unmatched.
  - Cashout: a BACK position is closed with a LAY at the best `availableToLay`
    and a LAY position with a BACK at the best `availableToBack`; the L95 depth
    gate reads that same level. Before, in LIVE the hedge would have rested
    unmatched and the residual handler would have cancelled it.
  - Cashout in LIVE: the router now asks for the market book with prices
    (`include_prices=True`, `EX_BEST_OFFERS`). Without them Betfair returns no
    ladders, so every position was skipped with only a log warning and a
    `CASHOUT` from the chat closed nothing.
  - The "aggressive" BACK of the Telegram resolver (old Telegram tab of the
    GUI, reachable in SIM only) now takes the best `availableToBack`, the
    price that matches at once.
  - The SIM no longer matches an order with an invalid price (malformed,
    non-finite or at most 1.0), as the live client refuses it. Before F6 a LAY
    with such a price matched; the side fix alone would have moved the hole to
    the BACK side.
  - `SimulationMatchingEngine` and `SimulationOrderBook` (used by tests only)
    follow the same rules; `direct_best_price` docstrings no longer call the
    executable price "defensive" (its code was already right).
  May look like a regression: SIM results are less optimistic, since the
  spread is now paid, and a SIM BACK priced above the best back now rests
  unmatched instead of matching.
- Cashout in the GUI (#461 PR04): the GUI process now builds the same cashout
  execution chain as the headless app (request bridge with duplicate filter,
  executor with the real SafetyLayer, OrderRouter, residual handler), through
  one shared builder, `cashout_wiring.cabla_catena_cashout`, used by both
  entrypoints. Before, a `CASHOUT` / `CASHOUT ALL` message received by the
  GUI was rejected with `cashout_chain_not_wired`, and the cashout request of
  the auto-close fell on a bus with no listener. Now it goes through the same
  gates as headless (emergency stop, runtime active, session and deploy gate
  in LIVE) and, in LIVE, places a real hedge. The GUI `Log` shows
  `CASHOUT_SUCCESS` and `CASHOUT_FAILED`, and `Storico Bet` and `Risk Desk`
  refresh on both. The GUI still has no manual cashout control. The Telegram
  notification of a cashout residual moved from `headless_main` to
  `cashout_wiring` unchanged, and both entrypoints use it.
  Closing the GUI window now stops the runtime first, as the headless app
  does, and `RuntimeController.stop` marks the runtime stopped before tearing
  down Telegram and Betfair, in both entrypoints. A command or a delayed
  cashout (auto-green grace) arriving during that teardown is rejected
  instead of being placed while the Betfair service is still connected.
  If stopping Telegram fails, `stop` now still disconnects Betfair before
  raising the error; before, the Betfair disconnect was skipped.
- Simulation settlement (#461 PR03): a SIM market is now settled when its
  CLOSED market book reaches the simulation broker. Before, nothing settled
  a SIM market: stakes and liabilities stayed locked forever and the runtime
  never saw a SIM result. Each matched bet is settled with exchange
  semantics (`core.pnl_engine.exchange_settled_gross_pnl`), selections are
  aggregated per market before the 4.5% policy commission, which applies
  once to the market net. The balance is credited with the net minus what
  the fills already moved (stakes, liabilities, realized close-outs), so a
  stake is no longer counted twice, and the market's position ledgers are
  closed. Duplicate CLOSED books, replays and restarts never settle twice,
  and an order placed on an already settled SIM market lapses unmatched.
  With `settlement.poll_enabled` on (default still off) the settlement
  poller now runs in SIM too and delivers each settled market once to the
  runtime cycle (realized PnL, daily loss, bankroll sync, checkpoints),
  also after a restart between the SIM state save and the cycle update.
  Runners without a terminal status, removed runners with bets on other
  runners, possible dead heats (several winners without a declared number
  of winners, or more than declared) and ledgers inconsistent with the
  orders leave the positions open. If handing the settlement to the event
  bus fails, the next poll round delivers it again without re-applying it;
  a delivery in progress is never repeated by a concurrent call. In production SIM still receives no market books until
  the SIM feed arrives (#461 PR11).
- `SimulationBroker.place_bet` now requires BACK or LAY and normalizes case
  and surrounding spaces before recording a PAPER order. Missing, malformed,
  or non-string sides raise `INVALID_SIDE` without changing simulated orders
  or funds. `place_orders` rejects just the invalid instruction without an
  implicit BACK; empty `side` permits a valid `bet_type`, while conflicting
  non-empty aliases and non-string aliases fail. Independent valid batch
  instructions still run. A regression test exercises the actual PAPER
  OrderRouter, OrderManager and CashoutExecutor against the broker.
  This aligns the simulation boundary with the live client (#426 P15).
  The engine's PAPER path now checks the two side aliases (`bet_type` and
  `side`) the same way as the LIVE branch before calling the simulation
  broker: discordant aliases, such as `bet_type=BACK` with `side=LAY`, fail
  with `INVALID_SIDE` instead of recording a `bet_type` order (#426 P17).
  A non-string side alias now fails with `INVALID_SIDE` on both paths, even
  when `str()` would turn it into BACK or LAY; on the LIVE branch such a
  value had been converted and sent since #478 (#426 P20).
- `betfair_client.py`: `BetfairClient.place_bet` now rejects a side outside
  BACK/LAY (checked after strip/upper; empty, `None` and non-string values
  included) with `RuntimeError("INVALID_SIDE")` before building the request,
  so nothing is sent. It used to turn any such value into BACK and report
  `ok=True`: a typo or a missing side became a real BACK bet. Callers that
  relied on the implicit BACK now get the error instead. `TradingEngine`
  treats `INVALID_SIDE` from the real client as a pre-send error (FAILED, not
  AMBIGUOUS). `_safe_side` stays permissive for `calculate_cashout`.
  (DECISIONE-426 P13, #480)
