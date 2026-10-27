# Dispatcher trusted della lane Buildkite

Issue #562, mandato owner PF-TRUSTED-DISPATCHER-IMPLEMENTATION-01; intake a **polling** per decisione owner PF-562
GITHUB POLLING ARCHITECTURE (opzione B: nessun webhook, nessun endpoint pubblico, nessun web server). Codice:
`scripts/pr_trusted_dispatcher.py` (solo libreria standard, Python ≥ 3.10). Test:
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
| Credenziali | token di **sola lettura** prodotto da `github.token_command` (l'helper della GitHub App: la chiave resta fuori da questo processo), uscita JSON `{"token": ..., "expires_at": "…Z"}`; rinnovato 5 minuti prima della scadenza; 401 ⇒ invalidato. Comando fallito, uscita fuori formato, token scaduto o in scadenza, token classico con scope (`X-OAuth-Scopes` non vuoto) ⇒ nessun giro | `TokenProvider`, `GitHub.get_all` |
| Lettura | solo `GET https://api.github.com/repos/zarbopiero963-droid/Pickfair-nogui/pulls` e `…/issues/comments`; qualsiasi altro endpoint (contenuti, archivi, file della PR, altri repository, `http`) è rifiutato prima della richiesta, anche se arriva da un `Link: rel="next"` | `GitHub.check_url` |
| Paginazione | si seguono **tutte** le pagine `rel="next"`; pagina non elenco, JSON illeggibile, HTTP ≠ 200, ciclo, oltre `MAX_PAGES` (50) ⇒ giro annullato | `GitHub.get_all` |
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
  - il login è in `rerun.allowed_logins` (un elenco vuoto disabilita i rerun);
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
- `pending` ritentato finché la build gira; pubblicazione fallita ritentata entro la finestra, poi `publish_failed`;
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
| Elenco incompleto o ambiguo | paginazione completa; pagine malformate, cicli o oltre il limite ⇒ giro annullato; una sola assenza non chiude una PR | `test_pagination_beyond_the_limit_is_unverifiable_and_aborts`, `test_a_single_missing_listing_does_not_close_the_pr`, `td_pagination_ignored`, `td_close_after_one_miss` |
| Codice di un fork | head repo ≠ repository ⇒ non valutabile | `td_fork_accepted` |
| Codice della PR con privilegi trusted | build sempre su `main`/`HEAD` con le sole cinque variabili; il client GitHub non può leggere contenuti o archivi; nessun web server | `test_builds_always_run_main_never_the_pr_head`, `test_dispatcher_has_no_web_server_and_no_code_fetching_paths`, `td_github_endpoint_unrestricted` |
| Doppie build | identificatore per transizione, indice unico sui job attivi, una sola istanza | `test_unchanged_state_polled_again_starts_no_new_builds`, `test_rerun_command_is_executed_once`, `td_rerun_replayed` |
| Rerun abusivo | allowlist di login, commento non modificato, SHA = head corrente; audit di ogni comando | `test_unauthorized_or_stale_rerun_is_rejected_and_audited`, `td_rerun_login_unchecked`, `td_rerun_stale_sha_accepted` |
| Esito su uno SHA vecchio | sostituzione per head; ricontrollo prima della pubblicazione; identità del risultato; ricontrollo dell'helper | `test_new_commit_is_synchronize_and_cancels_the_old_builds`, `td_publish_recheck_skipped`, `td_stale_sha_accepted` |
| Pipeline non autorizzate | solo le due di `GUARDS` | `test_only_the_two_contract_pipelines_can_be_started` |
| Status diagnostico scambiato per richiesto | contesti fissi; lo status della readiness è self-check, quello dell'overlap no | `test_readiness_status_is_a_self_check_and_overlap_status_is_not` |
| Segreti verso build, publisher o log | token Buildkite da file 0600/0400; token GitHub solo in memoria; publisher e comando del token con ambiente minimo | `test_tokens_never_reach_builds_publisher_or_logs`, `test_default_publisher_gets_no_inherited_environment` |
| Configurazione incompleta | ogni chiave obbligatoria, nessun default; chiavi del webhook (`listen`, `webhook_secret_file`) rifiutate come sconosciute | `test_every_missing_mandatory_key_is_not_configured`, `test_invalid_config_values_are_not_configured` |

Le chiamate HTTP non seguono i redirect (`_NoRedirect`), usano solo `https://` e hanno il timeout
`http_timeout_seconds`.

## Configurazione (tutta obbligatoria)

```json
{
  "github": {"token_command": ["/<eseguibile assoluto>", "..."], "token_command_timeout_seconds": 20},
  "rerun": {"allowed_logins": ["<login GitHub>"]},
  "buildkite": {
    "organization": "<slug>",
    "token_file": "/<percorso assoluto>",
    "result_artifacts": {"overlap": "<relativo>/overlap.json", "readiness": "<relativo>/readiness.json"},
    "artifact_download_hosts": ["<host esatto>"]
  },
  "publisher": {"command": ["/<eseguibile assoluto>", "..."], "timeout_seconds": 60},
  "state_db": "/<percorso assoluto>",
  "retention_hours": 168,
  "poll_interval_seconds": 60,
  "build_timeout_seconds": {"overlap": 600, "readiness": 900},
  "http_timeout_seconds": 30
}
```

I valori qui sopra sono **segnaposto**: nessun comando, organizzazione, percorso o login è deciso dal codice.
Vincoli:
- percorsi assoluti senza `..`;
- `token_command_timeout_seconds` 5..120;
- `allowed_logins` di login GitHub validi (vuoto = rerun disabilitati);
- `retention_hours` 24..2160;
- `poll_interval_seconds` 10..600;
- `publisher.timeout_seconds` 5..300;
- `build_timeout_seconds.*` 60..21600;
- `http_timeout_seconds` 5..120;
- `result_artifacts.<guard>` relativo e con nome `<guard>.json`;
- host senza caratteri jolly.

Comandi:
- `check-config --config F [--check-secrets]` ⇒ `CONFIGURED` o `NOT CONFIGURED` (exit 3). Con `--check-secrets`
  legge il token Buildkite ed esegue una volta il comando del token GitHub;
- `serve --config F`: ciclo unico, ogni `poll_interval_seconds` un giro di polling GitHub e un avanzamento dei job;
  SIGTERM lo ferma;
- `jobs --config F` e `reruns --config F`: consultazione in sola lettura degli ultimi job e dei comandi di rerun.

## Punti aperti: NOT CONFIGURED e da decidere

| # | Punto | Stato | Cosa serve |
|---|---|---|---|
| 1 | Endpoint, TLS, webhook | **superato** dal polling | Nessun endpoint pubblico né web server |
| 2 | Helper del token di lettura | NOT CONFIGURED (`github.token_command`) | Grok: helper che conia un installation token della GitHub App **solo lettura** (Pull requests, Issues, Metadata: read) e stampa `{"token", "expires_at"}`; eseguibile dall'utente del dispatcher senza esporre la chiave |
| 3 | Upload dell'artifact | NOT CONFIGURED (`result_artifacts`, `artifact_download_hosts`) | Le pipeline caricano `PF_RESULT_DIR/<guard>.json`; Grok indica percorso e host di download reale |
| 4 | Publisher | NOT CONFIGURED (`publisher.command`) | Come l'utente del dispatcher invoca l'helper root `pf-publish-status` (per esempio una regola sudo dedicata) |
| 5 | Tempi della readiness | da decidere (Grok) | Avviata sul push, prima che finiscano le review Sol/Grok, la readiness è quasi sempre FAIL; la si può rilanciare con il rerun quando i check sono chiusi |
| 6 | Allowlist dei rerun | NOT CONFIGURED (`rerun.allowed_logins`) | I login autorizzati: oggi Claude commenta come l'account owner, quindi l'allowlist reale è decisione dell'owner |
| 7 | Coda trusted in pausa | operativo | Build `scheduled` fino al timeout, poi `error`: nessun PASS |
| 8 | Valori di `PF_EVENT` | da verificare (Grok) | Il gate pre-command deve ammettere `ready_for_review` e `manual`; se li rifiuta la build fallisce e lo status è `failure`/`error`, mai PASS |

Domande tecniche per l'e2e:
- lo status `overlap-check` dell'App convive con il check run `overlap-check` di Actions (ruleset 25459867);
- il confronto `build.env == env inviato` presuppone che Buildkite restituisca l'env così come è stato inviato.

## Smoke trusted (`pf-trusted-smoke`)

Su #562 (c.6440620221) lo step dello smoke lanciava `scripts/pr_merge_readiness.py` senza argomenti, quindi argparse
usciva con exit 2. Lo step deve lanciare il wrapper `scripts/ci/pr_merge_readiness.sh`, che passa `--repo` e `--pr`
solo dopo aver verificato sull'API che la PR sia aperta e che l'head sia `PF_PR_HEAD_SHA`.

Senza il contesto PR fornito dal dispatcher, il guard esce con exit 1 e il motivo esplicito «contesto PR verificato
assente» (`test_smoke_without_pr_context_fails_closed_with_explicit_message`, mutazione
`tg_smoke_no_context_message`). Cambiare il comando dello step tocca a Grok.

## Checklist dei collaudi reali (Grok)

Da eseguire sulla PR del dispatcher. Va distinto il **test tecnico positivo** (la pipeline funziona) dalla **readiness
reale** (la PR è davvero mergiabile).

1. `check-config --check-secrets` ⇒ `CONFIGURED` con i percorsi reali; segreto 0644 o helper del token fallito ⇒
   `NOT CONFIGURED`.
2. Avvio del servizio: entro un intervallo, due build (`pr-overlap-guard`, `pr-merge-readiness`) su `main` per l'head
   corrente, con le cinque variabili e `PF_DELIVERY_ID=p<PR>-t<n>-<sha12>`; status `pending` con l'URL della build.
3. Un secondo giro senza cambiamenti ⇒ nessuna build nuova.
4. PASS reale di `pr-overlap-guard` quando la PR è l'unica aperta ⇒ `success` su `overlap-check` sull'head corrente.
5. Push mentre le build girano ⇒ build del vecchio head annullate, nessun `success` sul vecchio SHA, nuove build.
6. PR in bozza, chiusa e riaperta, ritorno a uno SHA precedente ⇒ comportamento della tabella «Transizioni».
7. **Rerun autonomo di Claude:** commento `/pf-trusted-rerun <sha>` sulla PR ⇒ nuove build con
   `PF_EVENT=manual`, riga in `reruns`; commento di un login non autorizzato o su uno SHA vecchio ⇒ rifiutato e
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
- Il ricontrollo dello stato della PR al momento della pubblicazione è dell'helper root; il dispatcher riduce la
  finestra con il proprio ricontrollo dei job sostituiti. Fra i due resta una finestra senza lock: un `success` su uno
  SHA appena sostituito non tocca la PR, perché i check si valutano sull'head corrente.
- Una PR chiusa e riaperta fra due giri consecutivi (meno di un intervallo) non produce una transizione `reopened`:
  lo stato osservato non è cambiato.
- `list_builds` legge solo la prima pagina di Buildkite: con 100 build l'assenza non è dimostrata e si riprova fino
  alla finestra, poi `error`.
