# Dispatcher trusted della lane Buildkite

Issue #562, mandato owner PF-TRUSTED-DISPATCHER-IMPLEMENTATION-01. Codice: `scripts/pr_trusted_dispatcher.py`
(solo libreria standard, Python ≥ 3.10). Test: `tests/scripts/test_pr_trusted_dispatcher.py`. Mutazioni: area
`trusted_dispatcher` di `guardrails/mutation_catalog.json`.

**Stato: NOT CERTIFIED.** Il codice e i test offline sono completi. Il percorso reale (VPS, webhook GitHub,
Buildkite, GitHub App, helper `pf-publish-status`) è **NOT VERIFIED** e lo collauda Grok sulla PR di collaudo
(checklist in fondo). Il file è a merge owner (pattern `scripts/pr_*.py` di `MANUAL_AUTHORITY_PATTERNS`).

## Cosa fa

Riceve i webhook GitHub `pull_request`, avvia le due pipeline trusted su `main` e pubblica l'esito sull'head della PR.
Il codice della PR non viene mai scaricato né eseguito: il dispatcher legge solo il payload firmato, l'API Buildkite e
il risultato JSON del guard.

| Passo | Regola (contratto Grok, #562 c.6438092317 §5) | Dove |
|---|---|---|
| Firma | `X-Hub-Signature-256` = HMAC SHA-256 del corpo grezzo, `hmac.compare_digest`, verificata **prima** di leggere il JSON. Assente, malformata o diversa ⇒ 401 | `verify_signature` |
| Evento | solo `pull_request` con azione `opened`, `synchronize`, `reopened` o `ready_for_review`; `ping` ⇒ 200 | `parse_event` |
| PR | repository e repository base `zarbopiero963-droid/Pickfair-nogui`; numero intero `[1-9][0-9]{0,6}` (un booleano o una stringa sono rifiutati); head SHA di 40 esadecimali minuscoli; `updated_at` ISO; PR aperta, non in bozza, base `main`, head nello stesso repository | `parse_event` |
| Replay | `X-GitHub-Delivery` (`[A-Za-z0-9-]{8,64}`, lo stesso formato del gate pre-command) unico nell'archivio SQLite; conservato `delivery_retention_hours` (≥ 24) | `Store.record_event` |
| Ordine | evento con `updated_at` più vecchio dell'ultimo visto per la PR ⇒ scartato. Un evento **strettamente** più recente con un head nuovo segna come sostituiti i job degli altri head: le loro build vengono annullate (anche quelle di una creazione ambigua, cercate per env) e non pubblicano mai `success`. Con lo stesso `updated_at` l'ordine non è noto: restano in corsa entrambi gli head e decide il ricontrollo dell'helper | `Store.record_event`, `_supersede` |
| Duplicati | indice unico parziale su (PR, head, guard) per i job attivi **non sostituiti**: un secondo evento per lo stesso head non avvia altre build; un head che torna con un force-push (A → B → A) riparte, perché i vecchi job sostituiti di A non occupano il posto | schema `jobs_active_current` |
| Build | solo `pr-overlap-guard` e `pr-merge-readiness` (tabella `GUARDS`, la configurazione non può aggiungerne), `branch=main`, `commit=HEAD`, env **esattamente** `PF_PR_NUMBER`, `PF_PR_HEAD_SHA`, `PF_PR_BASE_REF=main`, `PF_EVENT`, `PF_DELIVERY_ID` | `build_env`, `Buildkite.create_build` |
| Verifica build | la build letta deve avere branch `main`, lo stesso env inviato e l'URL della pipeline giusta; altrimenti viene annullata ed è `error` | `build_matches_job` |
| Esito | `success` **solo** con build `passed` **e** risultato del guard `PASS` con stesso repository, PR, guard e head. Build `failed` con verdetto `FAIL` coerente ⇒ `failure`. Tutto il resto (canceled, skipped, not_run, blocked, stato sconosciuto, timeout, artifact assente o illeggibile, head diverso = «stale SHA», build e verdetto in contraddizione) ⇒ `error` | `decide_outcome`, `_poll` |
| Pubblicazione | solo con `publisher.command` + `PR SHA STATE CONTEXT URL DESCRIZIONE` (sul VPS l'helper root `pf-publish-status`, che ricontrolla PR aperta, base, niente fork e head corrente prima di scrivere). Un job sostituito non pubblica. `pending` all'avvio della build, poi l'esito. Contesti `overlap-check` e `pr-merge-readiness` | `_adopt`, `_publish_final` |

### Creazione ambigua e ripresa dopo un'interruzione

Lo stato `creating` si scrive **prima** della chiamata a Buildkite. Se la risposta non arriva (timeout, rete, HTTP
5xx, crash del processo), la creazione è ambigua e non viene mai ripetuta alla cieca. Al giro successivo, anche dopo
un riavvio, il dispatcher elenca le build di `main` e cerca quella con lo stesso env:

- una sola ⇒ la adotta;
- nessuna (elenco letto con successo) ⇒ `error`, mai una seconda creazione;
- più di una ⇒ le annulla tutte, `error`;
- elenco illeggibile ⇒ riprova fino al timeout della build, poi `error`.

Un job `dispatched` riprende il polling dopo il riavvio; un job `publishing` ripubblica lo stesso esito (lo status
GitHub è idempotente per contesto e SHA).

**Esiti ambigui e liveness (audit indipendente su #569).** Regole valide per ogni chiamata esterna:

- creazione: solo un rifiuto 4xx esplicito è un esito definito; 5xx, 201 con corpo troncato, non JSON o senza
  numero di build, 2xx inattesi sono **ambigui** e vanno in riconciliazione;
- riconciliazione: un elenco vuoto non prova l'assenza finché non scade la finestra della build (la build può
  comparire in ritardo); una pagina piena (100 build) non prova nulla e si riprova;
- risultato del guard: un errore di download (rete, 5xx, `Location` malformata, corpo diverso da `file_size`) si
  ritenta entro la finestra, poi è `error`;
- `state` della build non stringa o sconosciuto ⇒ `error`; JSON annidato all'estremo (`RecursionError`) è una
  risposta illeggibile;
- annullamenti non confermati: le build restano in `pending_cancels` e il job, pubblicato l'esito, resta
  `canceling` finché l'annullamento non è confermato o scade la finestra (poi log esplicito «da verificare a mano»);
- fallback strutturale: un job che solleva un'eccezione inattesa a ogni giro, scaduto il timeout della build, viene
  annullato e chiuso con `error` («errore interno del dispatcher»), mai attivo per sempre.

**Pending ritentato.** Se la pubblicazione del `pending` all'avvio della build fallisce, si ritenta a ogni giro
finché la build gira (rilievo Codex P1 su #569). Rivalutando un head già `success` (per esempio dopo un reopen), il
vecchio `success` non resta l'esito visibile mentre la nuova build è in corso.

**Annullamento non confermato.** Se l'annullamento della build di un head sostituito fallisce (rete, API), il job
resta attivo e l'annullamento si ritenta a ogni giro fino alla finestra del timeout della build; solo dopo il job
viene ritirato, e il log lo dichiara (rilievo Codex P2 su #569).

**Risposte troncate.** Un errore di framing HTTP (`http.client.HTTPException`, per esempio `IncompleteRead` di una
risposta chunked troncata) è un errore di trasporto: il polling continua e il timeout della build resta efficace
(rilievo Codex P2 su #569).

**Risposte 5xx.** Alla creazione ogni HTTP 5xx di Buildkite, con o senza JSON, è un esito **ignoto** (rilievo Codex P2
su #569): un 502 HTML del gateway dopo una creazione arrivata a Buildkite porta alla riconciliazione, non a un
`error` che lascerebbe la build non seguita. Per le letture un 5xx è un errore temporaneo che si ritenta.

**Pubblicazione fallita.** Se l'helper non pubblica, il job resta `publishing` e lo stesso esito si ritenta a ogni
giro finché non va a buon fine, finché il job non viene sostituito o finché non scade una finestra pari a
`build_timeout_seconds` del guard. Allo scadere il job diventa `publish_failed`, e il log lo dichiara. Il ritento
copre anche il caso in cui un `success` vecchio sullo stesso head resterebbe visibile dopo un nuovo FAIL.

**Migrazione dell'archivio.** All'apertura `Store._migrate` aggiunge le colonne introdotte dopo la prima versione
(`MIGRATION_COLUMNS`) e rimuove il vecchio indice `jobs_active`, in modo idempotente: un archivio creato da una
versione precedente non fallisce a runtime (rilievo Sol su #569).

**Una sola istanza.** L'archivio è protetto da un `flock` esclusivo su `<state_db>.lock`: una seconda istanza
(`serve` lanciato due volte, sovrapposizione di systemd) si ferma con `NOT CONFIGURED` prima di toccare un job. Il
server fa il bind della porta prima di avviare il worker. Le transizioni `queued → creating` sono condizionate allo
stato letto, quindi una copia vecchia del job non crea una seconda build. Un COMMIT fallito viene annullato
(ROLLBACK), così la connessione non resta dentro una transazione aperta. `jobs` apre l'archivio in sola lettura e
funziona anche mentre il servizio è attivo.

## Modello di sicurezza

| Minaccia | Difesa | Prova |
|---|---|---|
| Evento falsificato | firma HMAC in tempo costante prima del parsing | `test_forged_or_unsigned_event_is_rejected_before_parsing`, `test_signature_over_a_different_body_is_rejected`, mutazione `td_signature_bypass` |
| Codice di un fork | head repo ≠ repository ⇒ nessuna build | `td_fork_accepted` |
| Replay | delivery unica, persistente anche dopo il riavvio | `test_replay_is_remembered_across_restart`, `td_replay_accepted` |
| Race | `BEGIN IMMEDIATE` + lock; 8 consegne concorrenti identiche ⇒ un solo evento | `test_concurrent_identical_deliveries_record_one_event` |
| Esito su uno SHA vecchio | sostituzione per head; ricontrollo dell'archivio subito prima della pubblicazione; identità del risultato; ricontrollo dell'helper | `test_new_push_cancels_old_builds_and_old_sha_never_gets_success`, `td_publish_recheck_skipped`, `td_stale_sha_accepted` |
| Pipeline non autorizzate | solo le due di `GUARDS`; un nome diverso solleva `KeyError` prima di qualsiasi richiesta | `test_only_the_two_contract_pipelines_can_be_started` |
| Status diagnostico scambiato per richiesto | contesti fissi; `overlap-diagnostic` non è mai pubblicato; lo status della readiness è self-check (`targetUrl` della pipeline `pr-merge-readiness`), quello dell'overlap no | `test_readiness_status_is_a_self_check_and_overlap_status_is_not` |
| Segreti verso il codice della PR | le build ricevono solo le cinque variabili; il token Buildkite e il secret del webhook non escono dal processo; il publisher parte con un ambiente minimo (`PATH`, `LANG`) | `test_build_env_never_contains_secrets`, `test_default_publisher_gets_no_inherited_environment` |
| Segreti nei log | i messaggi non contengono mai i valori; i file dei segreti sono rifiutati se leggibili da gruppo o altri, se sono link simbolici o fuori formato | `test_tokens_never_logged`, `test_secret_file_readable_by_others_is_refused` |
| Redirect dell'artifact | `download_url` deve stare sotto l'API Buildkite della pipeline; un solo redirect, verso un host esatto di `artifact_download_hosts`, **senza** Authorization | `test_artifact_redirect_drops_authorization_and_enforces_host`, `td_redirect_host_ignored` |
| Configurazione incompleta | ogni chiave obbligatoria, nessun default; chiave sconosciuta ⇒ errore; `NOT CONFIGURED`, exit 3, il servizio non parte | `test_every_missing_mandatory_key_is_not_configured`, `td_missing_key_default` |

Le chiamate HTTP non seguono i redirect (`_NoRedirect`), usano solo `https://` e hanno il timeout
`http_timeout_seconds`. Il corpo del webhook ha un tetto di 2 MiB (413 oltre), la lettura un timeout di 30 s, e
`Content-Length` deve essere fatto di sole cifre ASCII (411 altrimenti). L'uscita dell'helper di pubblicazione non
viene copiata nei log del dispatcher: solo il codice di uscita.

## Configurazione (tutta obbligatoria)

```json
{
  "listen": {"host": "<host>", "port": 0, "path": "/<percorso>"},
  "webhook_secret_file": "/<percorso assoluto>",
  "buildkite": {
    "organization": "<slug>",
    "token_file": "/<percorso assoluto>",
    "result_artifacts": {"overlap": "<relativo>/overlap.json", "readiness": "<relativo>/readiness.json"},
    "artifact_download_hosts": ["<host esatto>"]
  },
  "publisher": {"command": ["/<eseguibile assoluto>", "..."], "timeout_seconds": 60},
  "state_db": "/<percorso assoluto>",
  "delivery_retention_hours": 24,
  "poll_interval_seconds": 15,
  "build_timeout_seconds": {"overlap": 600, "readiness": 900},
  "http_timeout_seconds": 30
}
```

I valori qui sopra sono **segnaposto**, non valori da usare: nessun endpoint, organizzazione, percorso o comando è
deciso dal codice. Comandi:

- `python3 -I scripts/pr_trusted_dispatcher.py check-config --config F [--check-secrets]` ⇒ `CONFIGURED`
  oppure `NOT CONFIGURED` con l'elenco delle chiavi mancanti o non valide (exit 3);
- `serve --config F`: avvia il server e il worker; con configurazione incompleta non parte;
- `jobs --config F`: elenco in sola lettura degli ultimi job (stato, build, esito), l'interfaccia di consultazione.

Vincoli di formato: percorsi assoluti senza `..`; `result_artifacts.<guard>` relativo e con nome `<guard>.json` (il
nome che scrive `scripts/ci/trusted_pr_guard.py`); `delivery_retention_hours` 24..2160; `poll_interval_seconds`
1..300; `publisher.timeout_seconds` 5..300; `build_timeout_seconds.*` 60..21600; `http_timeout_seconds` 5..120; host senza caratteri jolly.

## Punti aperti: NOT CONFIGURED e BLOCKED_CONTRACT

| # | Punto | Stato | Cosa serve |
|---|---|---|---|
| 1 | Esposizione dell'endpoint, TLS, chi crea il webhook | **BLOCKED_CONTRACT** (TLS) + NOT CONFIGURED (`listen.*`) | Il dispatcher parla HTTP sul `listen` configurato e non termina TLS. Grok decide se e quale reverse proxy termina TLS, l'URL pubblico e chi registra il webhook (eventi `pull_request`, content type JSON, secret) |
| 2 | Età della delivery | non implementata, per contratto | Il contratto definitivo non la chiede più e GitHub non firma un timestamp. L'ordine si protegge con `updated_at` |
| 3 | Upload dell'artifact | NOT CONFIGURED (`result_artifacts`, `artifact_download_hosts`) | Le pipeline devono caricare `PF_RESULT_DIR/<guard>.json` come artifact (es. `artifact_paths`); Grok indica il percorso e l'host di download reale (#562 cita `s3.amazonaws.com` path-style) |
| 4 | Annullamento e ripresa | implementati | Contratto §5.2; sezione «Creazione ambigua» qui sopra |
| 5 | Coda trusted in pausa | operativo | Con la coda in pausa le build restano `scheduled` e vanno in `error` al timeout: nessun PASS |
| 6 | Tempi della readiness | **BLOCKED_CONTRACT** (quando avviarla) + NOT CONFIGURED (`build_timeout_seconds.readiness`) | L'env è solo quello del contratto, quindi la readiness usa l'attesa di default del guard (300 s) e il timeout dev'essere ≥ 600 s. Le review Sol/Grok durano molto di più: avviata sul push, la readiness è quasi sempre FAIL. Serve una decisione di Grok sul momento dell'avvio (per esempio dopo la chiusura dei check) o su `PF_READINESS_WAIT_SECONDS`, che il gate ammette ma il §5.3 esclude |
| 7 | Interfaccia di Claude (run/rerun/query) e token Buildkite (B3) | **BLOCKED_CONTRACT** (rerun) | Implementata solo la consultazione (`jobs`). Un rerun richiede un nuovo evento GitHub (push o reopen). Un comando di rerun non è nel contratto |
| 8 | App ID, Installation ID, percorsi di token, secret e helper | NOT CONFIGURED | App e installazione stanno nell'helper root, non nel dispatcher. Al dispatcher servono `webhook_secret_file`, `buildkite.token_file`, `publisher.command` (come l'utente `pf-dispatch` invoca l'helper root, es. una regola sudo dedicata: decisione di Grok) |

Domande tecniche aperte per Grok:

- lo status `overlap-check` pubblicato dall'App convive con il check run `overlap-check` di Actions: va verificato
  come il ruleset 25459867 li tratta (e2e punto 5);
- il confronto `build.env == env inviato` presuppone che l'API Buildkite restituisca l'env della build così come è
  stato inviato: se aggiunge chiavi, ogni build risulta `error` (fail closed), e va verificato sul primo e2e.

## Smoke trusted (`pf-trusted-smoke`)

Su #562 (c.6440620221) lo step dello smoke lanciava `scripts/pr_merge_readiness.py` senza argomenti, quindi argparse
usciva con exit 2 (`--repo`, `--pr` obbligatori). Correzione:

- lo step deve lanciare il wrapper `scripts/ci/pr_merge_readiness.sh`, che passa `--repo
  zarbopiero963-droid/Pickfair-nogui --pr <PF_PR_NUMBER>`, ma solo dopo aver verificato sull'API che la PR sia aperta
  e che l'head sia `PF_PR_HEAD_SHA`;
- il contesto PR (`PF_PR_NUMBER`, `PF_PR_HEAD_SHA`) arriva solo dal dispatcher. Senza contesto il guard esce con
  exit 1 e il motivo esplicito «contesto PR verificato assente», senza un numero di PR di ripiego e senza invocare la
  readiness (`test_smoke_without_pr_context_fails_closed_with_explicit_message`, mutazione
  `tg_smoke_no_context_message`).

Il gate pre-command oggi vieta le variabili del dispatcher su `pf-trusted-smoke`. Uno smoke della readiness può quindi
dare solo questo FAIL esplicito. Il PASS reale si prova sulle pipeline `pr-merge-readiness` e `pr-overlap-guard`
avviate dal dispatcher. Claude non modifica la configurazione Buildkite: cambiare il comando dello step tocca a Grok.

## Checklist dei collaudi reali (Grok)

Da eseguire sulla PR del dispatcher, che è la PR di collaudo. Va distinto il **test tecnico positivo** (la pipeline
funziona) dalla **readiness reale** (la PR è davvero mergiabile).

1. `check-config --check-secrets` ⇒ `CONFIGURED` con i percorsi reali; con un segreto 0644 ⇒ `NOT CONFIGURED`.
2. Webhook senza firma o con firma sbagliata ⇒ 401 e nessuna build (log del dispatcher, elenco build Buildkite).
3. Redelivery della stessa consegna dalla UI GitHub ⇒ 200 «replay ignorato» e nessuna build nuova.
4. Push sulla PR ⇒ due build (`pr-overlap-guard`, `pr-merge-readiness`) su `main` con esattamente le cinque
   variabili; status `pending` sull'head con l'URL della build.
5. PASS reale di `pr-overlap-guard` quando la PR è l'unica aperta ⇒ status `success` su `overlap-check` sull'head
   corrente.
6. Stale SHA: secondo push mentre le build del primo girano ⇒ build del primo annullate, nessun `success` sul primo
   SHA, nuove build sul secondo.
7. Dati non validi (PR in bozza, base diversa, azione `closed`) ⇒ nessuna build.
8. Build annullata a mano o in timeout ⇒ `error`, mai `success`.
9. Isolamento: il token Buildkite e il secret del webhook non compaiono nei log del dispatcher, nei log o nell'env
   delle build, né negli artifact; l'App key resta solo nell'helper root.
10. Required check `overlap-check` (ruleset 25459867, invariato): con lo status `failure` la PR non è mergiabile.
11. Riavvio del servizio con una build in corso ⇒ al ripartire l'esito viene pubblicato una sola volta.
12. Readiness reale: `pr-merge-readiness` `success` solo quando la PR è davvero mergiabile (review Sol/Grok PASS,
    check verdi, nessun file a merge owner). Su questa PR, che tocca file a merge owner, il verdetto atteso è FAIL:
    dà la prova del FAIL, non del PASS.

## Limiti dichiarati

- **NOT VERIFIED end-to-end**: verificato solo offline (test con un finto Buildkite fedele all'API REST v2 e un
  server HTTP reale su loopback). Nessuna chiamata reale a GitHub o Buildkite.
- Il dispatcher non verifica di persona lo stato della PR al momento della pubblicazione: il contratto assegna il
  ricontrollo all'helper root, che è infrastruttura di Grok. Il dispatcher riduce la finestra con il proprio
  ricontrollo dei job sostituiti.
- `list_builds` legge solo la prima pagina: con 100 build l'assenza non è dimostrata e si riprova fino alla
  finestra, poi `error` (fail closed). L'elenco degli artifact chiede `per_page=100`.
- `ThreadingHTTPServer` non limita il numero dei thread: un limite alle connessioni va messo nel reverse proxy
  (punto 1).
- Un evento perso da GitHub (nessuna consegna) non produce status: il required check resta quello di Actions, e la
  PR non diventa mergiabile per questo (fail closed).
