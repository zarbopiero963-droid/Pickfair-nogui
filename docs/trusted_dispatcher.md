# Dispatcher trusted della lane Buildkite

Issue #562, mandato owner PF-TRUSTED-DISPATCHER-IMPLEMENTATION-01; intake a **polling** per decisione owner PF-562
GITHUB POLLING ARCHITECTURE (opzione B: nessun webhook, nessun endpoint pubblico, nessun web server). Codice:
`scripts/pr_trusted_dispatcher.py` (solo libreria standard, Python ≥ 3.10, solo POSIX: `fcntl.flock`). Test:
`tests/scripts/test_pr_trusted_dispatcher.py`. Mutazioni: area `trusted_dispatcher` di
`guardrails/mutation_catalog.json`.

**Stato: NOT CERTIFIED.** Il codice e i test offline sono completi. Il percorso reale (VPS, GitHub App e helper del
token, Buildkite, helper `pf-publish-status`) è **NOT VERIFIED** e lo collauda Grok sulla PR di collaudo (checklist
in fondo). Il file è a merge owner (pattern `scripts/pr_*.py` di `MANUAL_AUTHORITY_PATTERNS`).

## Contratto §5 aggiornato per il polling

Restano invariati i punti 3–7 del contratto di Grok (#562 c.6438092317 §5): build, esito, pubblicazione, segreti,
fail closed. Cambiano solo intake e replay:

| § | Prima (webhook) | Ora (polling) |
|---|---|---|
| 5.1 Intake | webhook `pull_request` firmato HMAC | ogni `poll_interval_seconds` (consigliato 60) `GET` autenticato dell'elenco **completo** delle PR aperte, con token di sola lettura a durata limitata |
| 5.2 Replay e ordine | `X-GitHub-Delivery` unico, ordine per `updated_at` | transizioni derivate dallo stato osservato, con identificatore stabile per transizione; nessun evento da rigiocare né da ordinare |
| 5.3 Build | invariato | `PF_DELIVERY_ID` = identificatore della transizione; `PF_EVENT` = `opened`, `synchronize`, `reopened`, `ready_for_review` o `manual`; `ready_for_review` e `manual` vanno ammessi dal gate pre-command di Grok (punto aperto 8) |

## Cosa fa

| Passo | Regola | Dove |
|---|---|---|
| Credenziali | token di **sola lettura** prodotto da `github.token_command` (l'helper della GitHub App: la chiave resta fuori da questo processo), uscita JSON con le chiavi `token` ed `expires_at` (`…Z`); rinnovato 5 minuti prima della scadenza; 401 ⇒ invalidato. Comando fallito, uscita fuori formato, token scaduto o in scadenza, token classico con scope (`X-OAuth-Scopes` non vuoto) ⇒ nessun giro | `TokenProvider`, `GitHub.get_all` |
| Lettura | solo `GET https://api.github.com/repos/zarbopiero963-droid/Pickfair-nogui/pulls`, `…/issues/comments` e `…/collaborators/{login}/permission` (solo per i login in allowlist dei rerun); qualsiasi altro endpoint (contenuti, archivi, file della PR, altri repository, `http`) è rifiutato prima della richiesta, anche se arriva da un `Link: rel="next"` | `GitHub.check_url` |
| Paginazione | si seguono **tutte** le pagine `rel="next"`; pagina non elenco, JSON illeggibile, HTTP ≠ 200, oltre `MAX_PAGES` (50) ⇒ giro annullato | `GitHub.get_all` |
| Link `next` (#562 F1) | il link vale solo come segnale: deve essere `https://api.github.com` senza credenziali, porta o frammento, con lo **stesso** path della richiesta, nella forma `/repos/<repo>/…` o in quella che GitHub usa davvero, `/repositories/1189713160/…` (`TRUSTED_REPO_ID`, fisso), gli stessi parametri una volta sola e `page` = pagina corrente + 1. Altrimenti ⇒ `ApiError`. La pagina si chiede poi all'URL canonico `/repos/<repo>/…` ricostruito dal client, mai a quello ricevuto; i redirect non si seguono | `GitHub.next_page_url` |
| Rate limit | 403/429 con `X-RateLimit-Remaining: 0` o `Retry-After` ⇒ nessuna richiesta fino al reset (massimo un'ora) | `Dispatcher.poll` |
| Voce PR | tipi esatti: numero `[1-9][0-9]{0,6}` (non booleano né stringa), `state=open`, `draft` booleano, head SHA di 40 esadecimali minuscoli, base del repository fidato; una voce malformata o una PR ripetuta con dati diversi annulla **tutto** il giro | `parse_pull`, `parse_pulls` |
| Valutabile | base `main`, head nello stesso repository (niente fork né repository cancellato), non in bozza | `parse_pull` |
| Transizioni | nuova PR ⇒ `opened`; head diverso dall'ultimo osservato ⇒ `synchronize` (anche il **ritorno a uno SHA già elaborato**); PR chiusa che ricompare ⇒ `reopened` (anche **sullo stesso SHA**); bozza che diventa pronta ⇒ `ready_for_review`. Un head nuovo segna come sostituiti i job degli altri head (build annullate, mai `success`); una PR che diventa non valutabile ferma i suoi job | `Store.apply_poll` |
| Chiusura | una PR assente da **due** elenchi completi consecutivi è chiusa e le sue build si annullano: un solo elenco può perdere una PR spostata fra due pagine mentre lo si legge | `CLOSE_AFTER_MISSES` |
| Identificatore | `p<PR>-t<seq>-<sha12>`: `seq` è un contatore persistente per PR incrementato a ogni transizione, quindi A → B → A dà tre identificatori diversi (`t1`, `t2`, `t3`) e una riapertura sullo stesso SHA uno nuovo. Rerun: `p<PR>-r<comment_id>-<sha12>`. Formato `[A-Za-z0-9-]{8,64}`, lo stesso del gate pre-command | `Store.apply_poll`, `Store.record_rerun` |
| Duplicati | indice unico parziale su (PR, head, guard) per i job attivi non sostituiti: lo stesso stato riletto non avvia altre build | schema `jobs_active_current` |
| Build | solo `pr-overlap-guard` e `pr-merge-readiness` (tabella `GUARDS`), `branch=main`, `commit=HEAD`, env **esattamente** `PF_PR_NUMBER`, `PF_PR_HEAD_SHA`, `PF_PR_BASE_REF=main`, `PF_EVENT`, `PF_DELIVERY_ID`: le pipeline trusted eseguono solo script di `main`, mai codice della PR | `build_env`, `Buildkite.create_build` |
| Verifica build | branch `main`, stesso env inviato, URL della pipeline giusta; altrimenti annullata ed `error` | `build_matches_job` |
| Esito | `success` **solo** con build `passed` **e** risultato del guard `PASS` con stesso repository, PR, guard e head. Build `failed` con `FAIL` coerente ⇒ `failure`. Tutto il resto ⇒ `error` | `decide_outcome`, `_poll` |
| Pubblicazione | solo con `publisher.command` (helper root `pf-publish-status`, credenziale separata dal token di lettura, che ricontrolla PR aperta, base, niente fork e head corrente). Un job sostituito non pubblica. `pending` all'avvio, poi l'esito. Contesti `overlap-check` e `pr-merge-readiness` | `_adopt`, `_publish_final` |

Un giro di polling fallito non tocca lo stato salvato: niente transizioni, niente chiusure, niente status. Il giro
successivo riparte dallo stato vero di GitHub, quindi anche un evento «perso» durante un fermo viene recuperato.

### Riavvio e riconciliazione

Lo stato delle PR (`pr_state`), le transizioni e i job sono in SQLite. Al riavvio il primo giro confronta GitHub con
l'archivio: uno stato invariato non avvia build; un push o una chiusura avvenuti durante il fermo diventano la
transizione corrispondente. I job in corso riprendono come prima:

- **creazione ambigua:** `creating` si scrive prima della chiamata e si riconcilia cercando la build per env, mai una
  seconda creazione;
- **job avviato:** un job `dispatched` riprende il polling della build;
- **pubblicazione:** un job `publishing` ripubblica lo stesso esito.

### Rerun manuale autenticato, autorizzato e auditabile

- **Comando:** un commento sulla PR il cui testo è esattamente `/pf-trusted-rerun <sha40>`.
- **Autenticazione:** l'autore è quello che GitHub restituisce per il commento, letto con il token autenticato.
- **Autorizzazione:** sono richiesti tutti questi requisiti:
  - il login è in `rerun.allowed_logins` (un elenco vuoto disabilita i rerun). Allowlist iniziale decisa dall'owner
    (OWNER GO PF-570): solo `zarbopiero963-droid`; Claude può chiedere un rerun solo commentando con quell'account;
  - l'autore è un account utente (`user.type = User`, mai bot o app) con permesso **attuale** di scrittura sul
    repository: `admin`, `maintain` o `write`, letto da `GET /repos/{repo}/collaborators/{login}/permission` per
    ogni comando nuovo. `author_association` non basta (`MEMBER` indica l'appartenenza all'organizzazione,
    `COLLABORATOR` anche la sola lettura; rilievo di Sol su `dab1e94`). Accesso revocato (404) ⇒ `none`, rifiutato.
    Permesso fuori formato, riferito a un altro utente o errore API ⇒ lettura dei comandi annullata, cursore
    invariato. Login e permessi vengono dalle risposte autenticate dell'API, mai dal testo del commento;
  - il commento non è stato modificato dopo la pubblicazione;
  - la PR è aperta e valutabile;
  - lo SHA indicato è l'**head corrente**.
- **Audit:** ogni comando, accettato o rifiutato con il suo motivo, resta nella tabella `reruns`, consultabile con
  `reruns --config F`; il commento è il record pubblico.
- **Unicità:** un comando si esegue una sola volta. La rilettura dei commenti si sovrappone fra un giro e l'altro,
  e i doppioni li scarta l'archivio.
- **Primo avvio:** non si eseguono comandi pubblicati prima dell'avvio.
- **Commento malformato:** un comando con dati fuori formato annulla la lettura dei commenti senza avanzare il
  cursore.
- **Nessun aggiramento:** un rerun **rivaluta soltanto**. Annulla le build in corso della PR e ne avvia di nuove sulle
  stesse due pipeline, con lo stesso `decide_outcome`; nessun check viene aggirato.

### Esiti ambigui e liveness

Le regole dell'automa delle build sono invariate rispetto all'intake webhook. Riassunto (dettagli e prove nei rilievi
Codex, Sol, Grok e nelle revisioni indipendenti su #569):

- creazione: solo un rifiuto 4xx esplicito è un esito definito; 5xx, 201 con corpo troncato, non JSON o senza numero
  di build, 2xx inattesi sono **ambigui** e vanno in riconciliazione;
- riconciliazione: un elenco vuoto non prova l'assenza prima della finestra della build; una pagina piena non prova
  nulla;
- risultato del guard: un errore di download (rete, 5xx, `Location` malformata, corpo diverso da `file_size`) si
  ritenta entro la finestra, poi `error`;
- `state` non stringa o sconosciuto ⇒ `error`; JSON annidato all'estremo (`RecursionError`) è illeggibile;
- **obbligo di annullamento persistente**: le build in `pending_cancels` si ritentano a ogni giro in qualunque stato;
  nessuno stato finale con build ancora da annullare (`Dispatcher._set` lo converte in `canceling`);
- `pending` ritentato finché la build gira e finché l'artifact non è scaricabile; pubblicazione fallita ritentata
  entro la finestra, poi `publish_failed`;
- **consegna persistente dell'esito** (Sol su #569): un job `publish_failed` si riconsegna ogni `REDELIVERY_SECONDS`
  (300 s) finché è l'esito più recente per quel guard sia della PR sia dello SHA (gli status GitHub sono per SHA e
  contesto: due PR interne sullo stesso commit li condividono, Sol su #569) e la PR è aperta, valutabile e con lo
  stesso head.
- **un solo esito per SHA e contesto** (Codex P1 su #569): quando nasce la valutazione di uno SHA, i job attivi
  più vecchi con lo stesso SHA e lo stesso guard, anche di un'altra PR, sono sostituiti (build annullata, nessuna
  pubblicazione). Due PR interne sullo stesso commit condividono lo status: vale l'ultima valutazione. Se la PR più
  recente viene chiusa, l'altra resta con quell'esito finché un push o `/pf-trusted-rerun` non la rivaluta
  (fail-closed: un FAIL rimasto blocca, non apre);
  Così un vecchio `success` sullo stesso SHA non resta valido perché il publisher era fermo; un esito vecchio non
  copre mai uno più nuovo. Ogni tentativo aggiorna `updated_at`: un esito ancora da consegnare non viene ripulito;
- la consegna è registrata a parte (`final_published`), non dedotta dallo stato: un job che passa da `canceling`
  quando le build sono annullate torna `publish_failed` se l'esito non è mai stato consegnato, `done` solo se lo è
  (Codex P2 su #569: annullamento e publisher entrambi in errore lasciavano il required check `pending`);
- fallback strutturale: un job che solleva a ogni giro, scaduta la sua finestra, chiude con `error` o
  `publish_failed`, mai attivo per sempre;
- una sola istanza (`flock` su `<state_db>.lock`), transizioni condizionate allo stato letto, ROLLBACK dopo un
  COMMIT fallito, migrazione idempotente dell'archivio.

## Modello di sicurezza

| Minaccia | Difesa | Prova |
|---|---|---|
| Dati PR falsificati | si leggono solo dall'API GitHub autenticata del repository fidato, mai da un mittente esterno; voce malformata ⇒ giro annullato | `test_malformed_pr_item_aborts_the_whole_poll`, `test_github_client_refuses_any_other_endpoint` |
| Credenziali assenti o scadute | nessun token di ripiego; scadenza controllata e rinnovo anticipato; 401 ⇒ token invalidato | `test_failing_token_command_means_no_poll`, `test_expired_or_expiring_token_is_refused`, `test_token_is_refreshed_before_expiry`, `td_token_expiry_unchecked` |
| Token con permessi di scrittura | token classico con scope rifiutato; la sola lettura del token dell'App è un requisito dell'helper (Grok) | `test_classic_token_with_scopes_is_refused`, `td_classic_scoped_token_accepted` |
| Elenco incompleto o ambiguo | paginazione completa; pagine malformate, link `next` non coerenti o oltre il limite ⇒ giro annullato; una sola assenza non chiude una PR | `test_pagination_beyond_the_limit_is_unverifiable_and_aborts`, `test_a_single_missing_listing_does_not_close_the_pr`, `td_pagination_ignored`, `td_close_after_one_miss` |
| Codice di un fork | head repo ≠ repository ⇒ non valutabile | `td_fork_accepted` |
| Codice della PR con privilegi trusted | build sempre su `main`/`HEAD` con le sole cinque variabili; il client GitHub non può leggere contenuti o archivi; nessun web server | `test_builds_always_run_main_never_the_pr_head`, `test_dispatcher_has_no_web_server_and_no_code_fetching_paths`, `td_github_endpoint_unrestricted` |
| Doppie build | identificatore per transizione, indice unico sui job attivi, una sola istanza | `test_unchanged_state_polled_again_starts_no_new_builds`, `test_rerun_command_is_executed_once`, `td_rerun_replayed` |
| Rerun abusivo | allowlist di login, account utente con permesso attuale di scrittura (`collaborators/{login}/permission`), commento non modificato, SHA = head corrente; permesso non verificabile ⇒ lettura annullata; audit di ogni comando | `test_unauthorized_or_stale_rerun_is_rejected_and_audited`, `test_rerun_command_without_verified_permissions_keeps_the_cursor`, `test_rerun_accepted_only_with_current_write_permission`, `td_rerun_login_unchecked`, `td_rerun_permission_unchecked`, `td_rerun_read_permission_accepted`, `td_rerun_permission_404_as_write`, `td_rerun_permission_other_user`, `td_rerun_bot_accepted`, `td_rerun_permissions_optional`, `td_rerun_stale_sha_accepted` |
| Esito su uno SHA vecchio | sostituzione per head; ricontrollo prima della pubblicazione; identità del risultato; ricontrollo dell'helper | `test_new_commit_is_synchronize_and_cancels_the_old_builds`, `td_publish_recheck_skipped`, `td_stale_sha_accepted` |
| Pipeline non autorizzate | solo le due di `GUARDS` | `test_only_the_two_contract_pipelines_can_be_started` |
| Status diagnostico scambiato per richiesto | contesti fissi; lo status della readiness è self-check, quello dell'overlap no | `test_readiness_status_is_a_self_check_and_overlap_status_is_not` |
| Descrizione dello status fuori dal contratto del socket | `clean_description`: ASCII stampabile, al massimo 140 caratteri, mai `` ` `` `$` `\`; accenti alla lettera base, altri caratteri vietati a `?` visibile, troncamento chiuso da `...`, originale nei log; il publisher rifiuta (exit 2, niente invio) una descrizione che non rispetta il formato | `test_status_description_matches_the_socket_contract`, `test_fail_reason_with_forbidden_characters_reaches_the_publisher_cleaned_and_logged`, `test_default_publisher_refuses_a_description_outside_the_contract`, `td_description_forbidden_kept`, `td_description_cut_silently`, `td_description_accents_lost`, `td_publisher_sends_invalid_description` |
| Comando di rerun anteriore all'attivazione | limite inferiore persistente `comments_floor` (istante del primo avvio): il cursore non torna mai prima, e un comando con `created_at` ≤ limite (stesso secondo compreso) è rifiutato e registrato; id di commento fuori misura ⇒ comando malformato | `test_overlap_never_moves_the_cursor_before_the_first_start`, `test_command_in_the_same_second_as_the_first_start_is_rejected`, `test_cursor_floor_survives_restart_and_old_commands_never_build`, `test_comment_id_out_of_range_is_a_malformed_command`, `td_rerun_cursor_before_start`, `td_rerun_floor_unchecked`, `td_rerun_floor_same_second`, `td_comment_id_unbounded` |
| Segreti verso build, publisher o log | token Buildkite da file 0600/0400, oppure credenziale di systemd 0440 solo nella directory `$CREDENTIALS_DIRECTORY` del servizio (directory di root o dell'utente del servizio e non scrivibile da gruppo o altri, file di root o dell'utente del servizio, gruppo root o del processo, ACL che non concede la lettura ad altri utenti o gruppi; `LoadCredential` di systemd 255 la crea `0440 root:root` e la rende leggibile al servizio con una voce ACL, D1 su #562); token GitHub solo in memoria; publisher e comando del token con ambiente minimo | `test_tokens_never_reach_builds_publisher_or_logs`, `test_default_publisher_gets_no_inherited_environment`, `test_d1_systemd_255_root_credential_readable_via_acl_is_accepted`, `test_d1_systemd_credential_with_foreign_access_is_refused` |
| Configurazione incompleta | ogni chiave obbligatoria, nessun default; chiavi del webhook (`listen`, `webhook_secret_file`) rifiutate come sconosciute | `test_every_missing_mandatory_key_is_not_configured`, `test_invalid_config_values_are_not_configured` |

Le chiamate HTTP non seguono i redirect (`_NoRedirect`), usano solo `https://` e hanno il timeout
`http_timeout_seconds`.

## Configurazione (tutta obbligatoria)

```json
{
  "github": {"token_command": ["/usr/local/bin/pf-dispatch-github-token"], "token_command_timeout_seconds": 20},
  "rerun": {"allowed_logins": ["zarbopiero963-droid"]},
  "buildkite": {
    "organization": "<slug>",
    "token_file": "/<percorso assoluto>",
    "result_artifacts": {"overlap": ".trusted-guard/overlap.json", "readiness": ".trusted-guard/readiness.json"},
    "artifact_download_hosts": ["<host esatto>"]
  },
  "publisher": {"command": ["/usr/local/bin/pf-status-publish-client"], "timeout_seconds": 120},
  "state_db": "/<percorso assoluto>",
  "retention_hours": 168,
  "poll_interval_seconds": 60,
  "build_timeout_seconds": {"overlap": 600, "readiness": 900},
  "http_timeout_seconds": 30,
  "readiness_wait": {"enabled": true, "max_seconds": 7200, "settle_polls": 2, "min_delay_seconds": 120}
}
```

Comandi del token e del publisher, percorsi degli artifact e allowlist sono quelli dell'infrastruttura di Grok (#562
c.6444963640) e della decisione owner PF-570; `organization`, `token_file`, `state_db` e `artifact_download_hosts`
restano **segnaposto** finché non sono fissati sulla VPS. Il codice non ha default: la configurazione la scrive chi
installa.

**Publisher a socket (Grok t215u).** `pf-status-publish-client` ha la stessa argv di prima
(`PR SHA STATE CONTEXT URL DESCRIPTION`) e parla col lato root su `/run/pf-status-publisher/publish.sock` (solo
`pf-dispatch`, peercred, schema JSON stretto, replay store, ricontrollo di PR aperta, base `main`, niente fork e head
corrente prima del POST). Exit 0 solo se pubblicato (o accettato in DRY-RUN); qualsiasi altro codice ⇒ il
dispatcher ritenta al giro successivo, con un nuovo `request_id` a ogni chiamata del client. Il dispatcher non
richiede privilegi: niente sudo, niente setuid, `NoNewPrivileges` resta attivo. Il timeout di 120 s copre il caso
peggiore indicato da Grok (lock 60 s + helper 45 s).

**Artifact.** Il guard scrive `PF_RESULT_DIR/<guard>.json` con default `.trusted-guard`
(`scripts/ci/trusted_pr_guard.py`); se le pipeline non cambiano `PF_RESULT_DIR`, sono i percorsi qui sopra.
Il dispatcher scarica solo l'artifact con quel percorso esatto dalla build che ha creato, e solo da un host
in `artifact_download_hosts`.
Vincoli:
- percorsi assoluti senza `..`;
- `token_command_timeout_seconds` 5..120;
- `allowed_logins` di login GitHub validi (vuoto = rerun disabilitati);
- `retention_hours` 24..2160;
- `poll_interval_seconds` 10..600;
- `publisher.timeout_seconds` 5..300;
- `build_timeout_seconds.*` 60..21600;
- `http_timeout_seconds` 5..120;
- `readiness_wait.enabled` booleano esplicito; `max_seconds` 600..21600; `settle_polls` 1..10;
  `min_delay_seconds` 0..3600. Senza `readiness_wait` la configurazione è `NOT CONFIGURED`;
- `result_artifacts.<guard>` relativo e con nome `<guard>.json`;
- host senza caratteri jolly.

Comandi:
- `check-config --config F [--check-secrets]` ⇒ `CONFIGURED` o `NOT CONFIGURED` (exit 3). Con `--check-secrets`
  legge il token Buildkite ed esegue una volta il comando del token GitHub;
- `serve --config F`: ciclo unico, ogni `poll_interval_seconds` un giro di polling GitHub e un avanzamento dei job;
  SIGTERM lo ferma;
- `serve --config F --dry-run` (collaudo, #562): stesso codice, stesse letture di GitHub (PR, commenti, permessi) e
  stesse transizioni, ma nessuna build creata o annullata e nessuno status pubblicato. Il trasporto passa solo GET (ogni
  altra richiesta è bloccata prima di uscire), il publisher rifiuta, i job chiudono `done` con esito `dry_run` (il
  ramo DRY-RUN precede ogni altro, annullamenti pendenti compresi, che si azzerano senza tentativi) e nel
  log compaiono pipeline, cinque variabili e status che sarebbero stati usati. Archivio separato `<state_db>.dry-run`:
  l'archivio reale non viene mai creato né modificato, quindi il servizio vero riparte da zero. `--dry-run` vale solo
  con `serve`. Limiti voluti (review owner su #571):
  - **copre** la parte del ciclo prima di ogni scrittura: polling GitHub, transizioni (bozza, chiusura, riapertura,
    nuovo head e sostituzione), dedupe fra giri e fra riavvii dello stesso archivio, autorizzazione e audit dei rerun,
    build e status calcolati;
  - **non copre** `creating → dispatched → publishing`: senza build reali non ci sono numero di build, esito da
    leggere, artifact o status da consegnare. Annullamento, polling degli esiti, riavvio con build in corso e
    riconsegna dei `publish_failed` restano provati offline (finto Buildkite e finto GitHub) e si certificano solo nel
    collaudo reale (checklist, passi 2–12), con la pubblicazione degli status che richiede un GO owner separato.
    Anche fallback e riconsegna, se raggiunti, in DRY-RUN non scrivono
    (`test_dry_run_fallback_and_redelivery_never_write`);
  - «nessuna scrittura» vale per Buildkite e per gli status: il comando del token GitHub (`github_token_command`)
    gira come nel servizio vero, quindi l'helper conia comunque il token di lettura;
  - l'archivio `<state_db>.dry-run` resta fra una sessione e l'altra: un secondo DRY-RUN sugli stessi head non
    ricrea i job (dedupe). Per ripetere la prova da zero, rimuoverlo prima dell'avvio;
- `jobs --config F` e `reruns --config F`: consultazione in sola lettura degli ultimi job e dei comandi di rerun.

## Readiness non bloccante (#562, OWNER GO opzione A)

Con `readiness_wait.enabled: true` il job `readiness` di una transizione o di un rerun **non crea subito la build**.
Resta `queued` nell'archivio e il dispatcher, a ogni giro, legge i check run (`filter=latest`) e gli status
dell'head. Crea la build della readiness solo quando valgono tutte queste condizioni:
- nessun check run è aperto (`status` diverso da `completed`) e nessuno status ha come ultimo valore `pending`;
- c'è almeno un check;
- il numero dei check è rimasto uguale per `settle_polls` giri consecutivi;
- sono passati almeno `min_delay_seconds` dalla transizione, così i workflow fanno in tempo a registrarsi.

L'attesa la tiene il dispatcher: nessun agent Buildkite resta occupato. Per questo la pipeline può restare a 300 s di
attesa interna e 10 minuti di timeout.

| Regola | Dove |
|---|---|
| Non si attendono i self-check della readiness: lo status `pr-merge-readiness` della lane e il check run Actions «Merge readiness», come `is_self_check` di `scripts/pr_flow_automation.py`. Niente dipendenza circolare | `checks_gate`, `READINESS_SELF_STATUS_CONTEXTS` |
| Si attendono tutti gli altri: workflow Actions, review Sol/Grok/Codex, `overlap-check` della lane, `full-suite` se presente. L'esito non conta: un check `failure` rilascia la readiness, che lo giudica e dà FAIL. Il gate non dichiara mai PASS | `checks_gate` |
| `pending` subito, «in attesa degli altri check»: un vecchio esito sullo stesso SHA non resta visibile | `_await_checks` |
| Oltre `max_seconds` (7200) senza via libera ⇒ `error` «attesa dei check oltre … s», nessuna build | `_await_checks` |
| Check o status illeggibili, `total_count` diverso dagli elementi, rate limit ⇒ nessun via libera, si ritenta; resta valido il limite di `max_seconds` | `GitHub.get_all(key=…)`, `checks_gate` |
| Head nuovo ⇒ il job in attesa è sostituito come gli altri: nessuna build per il vecchio head, nuova attesa sul nuovo | `Store.apply_poll` |
| Riavvio ⇒ stato dell'attesa persistente (`gate_seen`, `gate_stable`, `released_at`), nessun doppione | schema `jobs` |
| Dopo il via libera la finestra della build (riconciliazione, timeout) parte da `released_at`, non dall'inizio dell'attesa | `_started` |

**Permessi del token del dispatcher (Grok).** Le letture nuove sono `GET /repos/…/commits/<sha40>/check-runs` e
`…/statuses`: il client le ammette solo per uno SHA di 40 esadecimali del repository fidato. Il token del dispatcher
deve quindi avere, in sola lettura, anche **Checks** e **Commit statuses**. Senza, le letture falliscono, la
readiness resta in attesa e allo scadere dà `error`: è fail-closed.

**Paginazione dei check (#562 F1).** Una PR ordinaria ha più di 100 check run (107 su #577), quindi l'elenco si legge
su almeno due pagine. Nel collaudo T252 di #576 il link `next` di GitHub aveva la forma
`/repositories/1189713160/commits/<sha>/check-runs?…&page=2` e il client lo rifiutava. La readiness restava
in attesa senza mai partire (fail-closed, ma inutilizzabile) e Grok ha fatto rollback. Ora il link è validato da
`GitHub.next_page_url` (tabella sopra). L'attesa si chiude solo dopo la lettura di **tutte** le pagine, con
`total_count` uguale fra le pagine e pari agli elementi letti. Un link non coerente, una pagina mancante o un
conteggio diverso lasciano l'attesa invariata, mai un via libera. Lo stesso difetto, latente, c'era su `pulls` e
`issues/comments` oltre i 100 elementi: è corretto dalla stessa funzione.

## Recovery da snapshot: identità degli intenti e deduplicazione remota (#562 P0)

**Difetto (rollback T252, #562 c.6471475567).** La deduplicazione era solo locale. Il cursore `comments_since` e la
tabella `reruns` stanno nello stesso SQLite, quindi ripristinando uno snapshot più vecchio tornano indietro insieme.
Il dispatcher allora:
- ha riletto il commento di rerun su #575, già mergiata, e non trovandolo in `reruns` lo ha rieseguito;
- ha rigenerato le transizioni di #577 con gli stessi tid, perché anche `seq` era stato ripristinato;
- ha creato build doppie (overlap #29/#30, readiness #21/#22). Il publisher le ha rifiutate perché le PR non erano
  aperte.

**Principio.** L'archivio locale non prova mai che un'operazione remota non sia avvenuta. Riavvio normale (archivio
intatto) e ripristino di uno snapshot (archivio che ha perso eventi) sono casi distinti. Il dispatcher non li deve
riconoscere: verifica sempre su Buildkite.

| Passo | Regola | Dove |
|---|---|---|
| Identità dell'intento | il tid (`p<pr>-t<seq>-<sha12>` per una transizione, `p<pr>-r<comment_id>-<sha12>` per un rerun) è deterministico, viaggia nella build come `PF_DELIVERY_ID` e come metadato Buildkite `pf_intent` | `Buildkite.create_build`, `INTENT_META_KEY` |
| Prima di creare | si cercano le build della pipeline con `meta_data[pf_intent]=<tid>`, a qualunque data, più quelle con l'env identico nella finestra (build create prima del metadato) | `Dispatcher._builds_for`, `Buildkite.list_builds_by_intent` |
| Build trovata in corso | adottata e seguita fino all'esito: nessuna build nuova | `Dispatcher._resume_prior` |
| Build trovata conclusa | job chiuso `done` con esito `recovered`, **senza ripubblicare**: l'esito l'aveva già pubblicato chi l'aveva creata | `_resume_prior` |
| Più build dello stesso intento | si segue la più vecchia, le altre si annullano | `_resume_prior`, `_create` |
| Stato remoto non verificabile (elenco illeggibile o pagina piena) | **nessuna build**. Il job resta `queued` (log `RECOVERY_UNCERTAIN`) e pubblica subito `pending` «intento non verificabile su Buildkite», così un vecchio `success` sullo stesso SHA non resta visibile; oltre la finestra della build diventa `error`, mai una creazione | `_create` |
| Build ritrovata conclusa | l'esito si correla agli status del contesto sull'head tramite il numero di build del loro `target_url` (crescente per pipeline). Uno status di una build più recente, in qualunque stato ⇒ superato: **non si ripubblica**, e se lo status con build più recente è finale, è proprio della build più recente ed è coperto da un `pending` senza build (attesa, recupero), lo si ripristina com'era (mai un finale vecchio pubblicato dopo); con la build più recente ancora in corso non si ripristina nulla. Esito finale di questa stessa build ⇒ consegnato (ripristinato se coperto). Nessuno status con build, o solo di build precedenti (crash fra la fine della build e la pubblicazione) ⇒ la build si adotta e l'esito si ricava da lei. Così un PASS vecchio non copre mai un FAIL più recente dello stesso SHA. Status illeggibili ⇒ il job torna `queued`, pubblica subito `pending` senza build (un vecchio esito non resta visibile; senza numero di build non vale mai come consegna) e riprova; oltre la finestra, `error` (Sol e Codex su #583). Serve al token del dispatcher la lettura **Commit statuses**, la stessa della readiness non bloccante | `_resume_prior`, `_delivery` |
| Stesso intento, env diversa | mai prova di assenza. Se differisce solo l'evento di una transizione (ricostruita da un archivio ripristinato: per esempio `ready_for_review` che rinasce `synchronize`) è la stessa valutazione: l'evento si riallinea, una sola volta, e la build si ritrova. Ogni altra differenza, o due build discordi, è un conflitto ⇒ nessuna build, `RECOVERY_UNCERTAIN`, poi `error` (Codex P2 su #583) | `_builds_for` |
| Readiness in attesa ripristinata | l'attesa ripubblica `pending`; se la build ritrovata è conclusa, quel `pending` è l'ultimo status visibile e quindi l'esito si ricava dalla build e si ripubblica (Codex P1 su #583, coperto dalla verifica di consegna) | `_await_checks`, `_resume_prior` |
| `seq` riportato indietro dallo snapshot | ogni build porta anche `pf_pr = <numero PR>`. Prima di creare una transizione si leggono le build della PR dalla più recente, pagina dopo pagina, fino alla prima pagina con una transizione (i rerun, anche cento di fila, non la nascondono) e se ne ricava il `seq` più alto già usato; oltre `PR_PAGES_MAX` (10) pagine senza transizioni ⇒ `RECOVERY_UNCERTAIN`, mai un massimo indovinato (Sol su #583). Contatore locale oltre il massimo ⇒ nulla da fare. Altrimenti l'archivio è stato ripristinato: se l'ultima valutazione remota della PR (build più recente) è dello stesso head con la stessa env, è questa stessa valutazione e la transizione (job fratelli in coda compresi) ne prende il tid; se no è una transizione nuova avvenuta dopo il ripristino e prende un tid oltre il massimo remoto. In entrambi i casi il `seq` della PR risale oltre il massimo, così un ritorno legittimo a uno SHA già visto resta una valutazione nuova (Sol e Codex su #583) | `_adopt_later_transition`, `Store.rekey_transition`, `PR_META_KEY` |
| Esito finale di un job ripristinato ancora attivo | prima di pubblicare (o riconsegnare) l'esito della build N si leggono gli status del contesto: se uno punta a una build più recente M > N, l'esito è superato e il job si chiude `superseded` senza pubblicare. Status illeggibili: un `success` non si pubblica (si riprova, poi `publish_failed` e riconsegna), un esito negativo sì. In esercizio normale non scatta mai, perché i job vecchi sono già sostituiti (Codex su #583) | `_final_still_current`, `_newer_on_github` |
| Riallineamento della transizione | tocca solo i job ancora `queued` senza build: un job già avviato resta legato alla propria build | `Store.rekey_transition` |
| Due istanze insieme | dopo la creazione si ricontrolla: con build doppie dello stesso intento si tiene la più vecchia e le altre si annullano | `_create` |
| Rerun vecchio | rifiutato e registrato se il commento, per l'orario di **GitHub**, ha più di `RERUN_MAX_AGE_SECONDS` (6 h) | `record_rerun`, `_rerun_too_old` |
| Rerun su una PR chiusa o mergiata | rifiutato se la PR manca dall'ultimo elenco delle PR aperte (`missing`), anche prima della seconda assenza che la chiude | `record_rerun` |

**Limiti dichiarati (non è exactly-once).**
- Le build create da versioni precedenti non hanno il metadato. Si riconoscono solo nella finestra temporale del job.
  Per i rerun c'è in più il limite di 6 ore, sull'orario del commento.
- La garanzia vale finché Buildkite conserva le build (retention dell'organizzazione). Una build cancellata dalla
  retention non è più una prova. Per i rerun copre il limite di 6 ore; per le transizioni l'head deve essere ancora
  quello corrente.
- Due istanze sullo stesso archivio non possono girare: `Store` prende un `flock` esclusivo sul file. Due istanze
  con archivi diversi (per esempio una copia avviata altrove) che creano nello stesso istante possono produrre per
  pochi secondi due build dello stesso intento. Dopo la creazione si ricontrolla: resta la più vecchia e le altre
  si annullano. Prima dell'annullamento una build doppia non produce effetti irreversibili: i guard trusted leggono
  GitHub e scrivono solo il proprio artifact, gli status li pubblica soltanto il dispatcher per il job che segue la
  build tenuta, e prima di ogni pubblicazione finale `_final_still_current` scarta l'esito di una build superata.
  Il costo residuo è il tempo di agent della build annullata. Non è una garanzia exactly-once: va comunque evitato
  di far girare due dispatcher.
- Lo snapshot va preso con l'API di backup di SQLite, oppure a servizio fermo e con il WAL. Il ripristino non
  richiede altre azioni: è proprio il caso coperto qui.

## Readiness di SHA superati e coda trusted (#562 P1)

Verifica sul codice: una readiness di uno SHA superato **non** occupa la coda trusted oltre il giro di polling che
vede il nuovo head.
- **In attesa** (opzione A): non ha build, quindi non c'è nessun agent da liberare. È sostituita e non avvia più nulla.
- **Già avviata**: `Store.apply_poll` la marca sostituita e lo stesso `tick` ne annulla la build (`_supersede`),
  mentre crea l'overlap del nuovo head.

Il ritardo massimo per l'overlap del nuovo head è quindi `poll_interval_seconds` più il tempo di annullamento
dell'agent. Lo provano `test_obsolete_readiness_build_is_cancelled_in_the_same_round_that_starts_the_new_overlap` e
`test_waiting_obsolete_readiness_holds_no_agent_and_starts_nothing`.

Una readiness **valida** di un'altra PR non si interrompe: può precedere l'overlap al massimo per la durata di una
build della readiness (misurata 45–47 s sulle build #24–#26, Grok t267 su #562; limite `build_timeout_seconds.readiness`). Se serve ridurla, la leva è la priorità dello step di
overlap nella pipeline Buildkite (`priority`, configurazione della UI, Grok), non una modifica del dispatcher. Non
cambia l'ordine di nessun controllo e non introduce nessun PASS.

## Punti aperti: NOT CONFIGURED e da decidere

| # | Punto | Stato | Cosa serve |
|---|---|---|---|
| 1 | Endpoint, TLS, webhook | **superato** dal polling | Nessun endpoint pubblico né web server |
| 2 | Token di lettura | **pronto** (Grok t214u/t215u) | `pf-dispatch-github-token` stampa il JSON con `token` ed `expires_at` del token ridotto a `pull_requests:read` + `metadata:read`, rinnovato ogni 45 min da un timer root; `metadata:read` copre anche `collaborators/{login}/permission`. Da verificare nel collaudo: la lettura dei commenti issue con quel token (Grok: 200) |
| 3 | Artifact | percorsi noti, host NOT CONFIGURED | `result_artifacts` = default `.trusted-guard/<guard>.json`; `artifact_download_hosts` va fissato sull'host reale del download (redirect di Buildkite) al collaudo |
| 4 | Publisher | **pronto in DRY-RUN** (Grok t215u, 35/35 test) | `publisher.command` = `pf-status-publish-client`; pubblicazione reale solo nella finestra di collaudo autorizzata dall'owner |
| 9 | Token API Buildkite | **rischio residuo registrato** (decisione owner PF-562 final integration) | Token esistente dell'utente admin, scope `read_builds`/`write_builds`/`read_artifacts`, IP `45.14.184.188/32`, scadenza esplicita, root 0600 + `LoadCredential`. Buildkite non lo limita alle pipeline: se trapelasse potrebbe creare o annullare build e leggere artifact di tutta l'organizzazione. La difesa applicativa è `GUARDS`: nessun input (PR, commento, configurazione) sceglie pipeline o comandi |
| 5 | Tempi della readiness | **deciso** (OWNER GO opzione A) | Readiness non bloccante: il dispatcher la avvia solo a check chiusi (sezione «Readiness non bloccante»); serve `readiness_wait` in configurazione e il token del dispatcher con Checks e Commit statuses in lettura |
| 6 | Allowlist dei rerun | decisa (OWNER GO PF-570), da configurare sul VPS | `rerun.allowed_logins: ["zarbopiero963-droid"]`; serve anche il permesso attuale di scrittura letto dall'API |
| 7 | Coda trusted in pausa | operativo | Build `scheduled` fino al timeout, poi `error`: nessun PASS |
| 8 | Valori di `PF_EVENT` | da verificare (Grok) | Il gate pre-command deve ammettere `ready_for_review` e `manual`; se li rifiuta la build fallisce e lo status è `failure`/`error`, mai PASS |

Domande tecniche per l'e2e:
- lo status `overlap-check` dell'App convive con il check run `overlap-check` di Actions (ruleset 25459867);
- il confronto `build.env == env inviato` presuppone che Buildkite restituisca l'env così come è stato inviato.

## Smoke trusted (`pf-trusted-smoke`)

Decisione owner (PF-562 final integration): controllo **diagnostico**, non un required check e non avviato dal
dispatcher (`GUARDS` ne è fuori, il socket non ne pubblica lo status). Lo esegue Grok con PR e SHA autentici e
registra PASS/FAIL con le prove sulla #562.

Su #562 (c.6440620221) lo step dello smoke lanciava `scripts/pr_merge_readiness.py` senza argomenti, quindi argparse
usciva con exit 2. Lo step deve lanciare il wrapper `scripts/ci/pr_merge_readiness.sh`, che passa `--repo` e `--pr`
solo dopo aver verificato sull'API che la PR sia aperta e che l'head sia `PF_PR_HEAD_SHA`.

Senza il contesto PR fornito dal dispatcher, il guard esce con exit 1 e il motivo esplicito «contesto PR verificato
assente» (`test_smoke_without_pr_context_fails_closed_with_explicit_message`, mutazione
`tg_smoke_no_context_message`). Cambiare il comando dello step tocca a Grok.

## Checklist dei collaudi reali (Grok)

Da eseguire sulla PR del dispatcher. Va distinto il **test tecnico positivo** (la pipeline funziona) dalla **readiness
reale** (la PR è davvero mergiabile).

1. `check-config --check-secrets` ⇒ `CONFIGURED` con i percorsi reali (anche la credenziale 0440 di `LoadCredential`);
   segreto 0644 o helper del token fallito ⇒ `NOT CONFIGURED`. La credenziale 0440 è ammessa solo con
   `CREDENTIALS_DIRECTORY` impostata: il controllo va lanciato nel contesto dell'unit (per esempio `systemd-run` con
   le stesse `LoadCredential`, `User` e `Group`, oppure un `ExecStartPre`); da una shell qualunque esce
   `NOT CONFIGURED` ed è un falso FAIL. Poi `serve --dry-run`, con l'archivio `.dry-run` rimosso prima: transizioni
   e build attese nel log, nessuna richiesta di scrittura verso Buildkite o GitHub.
2. Avvio del servizio: entro un intervallo, due build (`pr-overlap-guard`, `pr-merge-readiness`) su `main` per l'head
   corrente, con le cinque variabili e `PF_DELIVERY_ID=p<PR>-t<n>-<sha12>`; status `pending` con l'URL della build.
3. Un secondo giro senza cambiamenti ⇒ nessuna build nuova.
4. PASS reale di `pr-overlap-guard` quando la PR è l'unica aperta ⇒ `success` su `overlap-check` sull'head corrente.
5. Push mentre le build girano ⇒ build del vecchio head annullate, nessun `success` sul vecchio SHA, nuove build.
6. PR in bozza, chiusa e riaperta, ritorno a uno SHA precedente ⇒ comportamento della tabella «Transizioni».
7. **Rerun autonomo di Claude:** commento `/pf-trusted-rerun <sha>` sulla PR ⇒ nuove build con
   `PF_EVENT=manual`, riga in `reruns`; commento di un login non autorizzato, di un account senza permessi di scrittura o su uno SHA vecchio ⇒ rifiutato e
   registrato. Dimostra che Claude avvia e rilancia i test senza assistenza manuale.
8. Helper del token fermo o token scaduto ⇒ nessun giro, nessuno status; ripristinato ⇒ il giro successivo
   recupera lo stato.
9. Riavvio del servizio con una build in corso ⇒ esito pubblicato una sola volta; push fatto durante il fermo ⇒
   rilevato al primo giro.
10. Isolamento: token di lettura e token Buildkite assenti da log, env delle build e artifact; l'App key solo negli
    helper.
11. Required check `overlap-check` (ruleset 25459867, invariato): con `failure` la PR non è mergiabile.
12. Readiness reale: `pr-merge-readiness` `success` solo con la PR davvero mergiabile. Su questa PR, che tocca file a
    merge owner, il verdetto atteso è FAIL.

## Limiti dichiarati

- **NOT VERIFIED end-to-end**: verificato solo offline (finto GitHub e finto Buildkite fedeli alle API REST).
- Un elenco che si interrompe senza `Link: rel="next"` non è riconoscibile come incompleto: la difesa è che una PR
  sparita da un solo elenco non è chiusa (servono due assenze consecutive).
- Eccezione 0440 di `LoadCredential`: il file si verifica sul descrittore aperto (`O_NOFOLLOW`, `fstat`), la
  directory sul percorso (`realpath`). Fra i due resta una finestra teorica, chiusa in pratica dai requisiti della
  directory (di root o dell'utente del servizio, non scrivibile da gruppo o altri): può sfruttarla solo chi può già
  leggere il token. La ACL del file (`system.posix_acl_access`) si legge sul descrittore: utenti nominativi solo root
  o il servizio, gruppi nominativi solo root o il gruppo del servizio, «altri» senza permessi; ACL malformata o
  illeggibile ⇒ rifiutata. La ACL della directory non si legge: i bit di gruppo, che non devono avere `w`, ne sono
  la mask e limitano anche le voci nominative. Che `$CREDENTIALS_DIRECTORY` sia davvero di systemd non è dimostrato
  dalla variabile: lo garantiscono proprietario, permessi e ACL, mai il percorso da solo.
- Il ricontrollo dello stato della PR al momento della pubblicazione è dell'helper root; il dispatcher riduce la
  finestra con il proprio ricontrollo dei job sostituiti. Fra i due resta una finestra senza lock: un `success` su uno
  SHA appena sostituito non tocca la PR, perché i check si valutano sull'head corrente.
- Una PR chiusa e riaperta fra due giri consecutivi (meno di un intervallo) non produce una transizione `reopened`:
  lo stato osservato non è cambiato.
- `list_builds` legge solo la prima pagina di Buildkite: con 100 build l'assenza non è dimostrata e si riprova fino
  alla finestra, poi `error`.
