# Lane Buildkite `trusted`: overlap guard e merge readiness

Issue #562. Grok cura infrastruttura e routing Buildkite; Claude cura questi script e i loro test. Le pipeline
`pr-overlap-guard` e `pr-merge-readiness` del cluster `pickfair-trusted` girano su un checkout di **`main`** (il gate
pre-command impone HEAD == origin/main == BUILDKITE_COMMIT). Il codice della PR non viene mai scaricato né eseguito.
La PR si legge solo dalla GitHub REST API.

## Percorsi

| Pipeline Buildkite | Comando (nessun argomento) |
|---|---|
| `pr-overlap-guard` | `scripts/ci/pr_overlap_guard.sh` |
| `pr-merge-readiness` | `scripts/ci/pr_merge_readiness.sh` |

Entrambi sono wrapper di `scripts/ci/trusted_pr_guard.py`, avviato con `python3 -I`: niente `PYTHONPATH`, niente
site utente, niente directory corrente in `sys.path`. Un job non può quindi iniettare moduli. Il guard usa solo la
libreria standard. La readiness delega a `scripts/pr_merge_readiness.py`, la stessa del workflow GitHub Actions, che
usa la CLI `gh`.

## Contratto d'ingresso

Variabili d'ambiente: **nessun argomento**. Un argomento qualunque produce FAIL.

| Variabile | Obbligatoria | Formato | Note |
|---|---|---|---|
| `PF_PR_NUMBER` | sì | `[1-9][0-9]{0,6}` ASCII | niente zeri iniziali, spazi, cifre Unicode |
| `PF_PR_HEAD_SHA` | sì | 40 esadecimali minuscoli | l'head per cui si chiede il verdetto; se la PR ha un head diverso, FAIL |
| `PF_PR_BASE_REF` | no | default `main` | branch base atteso |
| `PF_GITHUB_TOKEN_FILE` | una delle due fonti | percorso di un file regolare | permessi 0600/0400; leggibile da gruppo o altri ⇒ FAIL |
| `CREDENTIALS_DIRECTORY` | una delle due fonti | directory systemd `LoadCredential` | file `github-token` |
| `PF_RESULT_DIR` | no | default `.trusted-guard` | qui va `<overlap\|readiness>.json` |
| `PF_READINESS_WAIT_SECONDS` | no | 31..6600, default 300 | attesa dei check pendenti; vale solo per la readiness. Il minimo garantisce almeno due letture stabili: con 0 la readiness giudicherebbe una sola istantanea, prima che i check tardivi si registrino |

Il guard verifica anche le variabili Buildkite, quando sono presenti:
- `BUILDKITE_REPO` deve essere esattamente questo repository (https o ssh);
- `BUILDKITE_PULL_REQUEST` deve essere `false`;
- `BUILDKITE_BRANCH` deve essere `main`.

**Token.** Il token GitHub deve essere **in sola lettura** e limitato a questo repository. Sulla lane trusted è un
**token di installazione** della GitHub App `pickfair-trusted-status` (D3 della #562, c.6465081812), a durata
limitata, con questi permessi e nessuno in scrittura:
- Actions: read;
- Checks: read;
- Contents: read;
- Issues: read (per i commenti delle review AI);
- Metadata: read;
- Pull requests: read.

Il PAT con permessi di scrittura usato prima è stato tolto dalla VPS. Le prove di scrittura col token App (label,
commento, status, ref) danno 403. Il percorso reale di readiness, che legge anche gli status (`overlap-check`), è
stato collaudato con questi permessi in P0-A (build `pr-merge-readiness` #14 su #573).

Il token è opaco: si accetta qualunque valore di ASCII stampabile senza spazi, da 20 a 8192 caratteri (PAT
fine-grained, `ghs_` di Actions, formati futuri). Vuoto, con spazi, a capo o caratteri di controllo ⇒ FAIL prima di
qualunque richiesta, così nell'header Authorization non entra nulla di iniettabile. Il primo collaudo reale su #564
aveva trovato il `GITHUB_TOKEN` di Actions rifiutato dalla vecchia regola `[A-Za-z0-9_]{20,255}`.

Un token classico con scope dichiarati (`X-OAuth-Scopes` non vuoto: `repo`, `public_repo`, … scrivono) ⇒ FAIL. Il
token non viene mai stampato: l'output del processo figlio e i motivi passano da una redazione che lo sostituisce con
`***`. I redirect HTTP non vengono seguiti, perché `urllib` ripeterebbe l'header Authorization verso un altro host.
Le URL di paginazione fuori da `api.github.com` vengono rifiutate.

Limite del controllo sui permessi: il codice riconosce solo i token classici con scope. Un token fine-grained o di
una GitHub App (`ghs_`) con permessi di scrittura non dichiara scope e non si distingue da uno in sola lettura. Il
permesso minimo è quindi un **requisito dell'infrastruttura**, non verificabile dagli script, che comunque fanno solo GET.

## Verifiche di provenienza (entrambi i controlli)

1. Token: deve rispettare i permessi minimi.
2. PR: il numero restituito dall'API deve essere quello richiesto, con `state=open`.
3. Base: repository di base uguale a questo, branch base uguale a `PF_PR_BASE_REF`, base SHA di 40 esadecimali.
   `compare/<base_sha>...main` deve dare `ahead` o `identical`, cioè la base deve stare nella storia di main.
4. Head: deve stare nello **stesso repository** (fork o repository cancellato ⇒ FAIL) e coincidere con
   `PF_PR_HEAD_SHA`.
5. A fine valutazione la PR viene riletta: head, stato, repository base o branch base cambiati ⇒ FAIL. Una PR
   ridiretta su un altro branch durante l'attesa non resta certificata. Per la readiness si rilegge anche la label `manual-review-required`:
   aggiunta durante l'attesa ⇒ FAIL.

## `pr_overlap_guard.sh`

Porta in uno script la logica che stava inline in `.github/workflows/pr-overlap-guard.yml`, resa fail-closed:
- **una sola PR aperta alla volta** (CLAUDE.md): con più PR aperte il controllo è FAIL;
- **sovrapposizione critica**: con un'altra PR aperta che tocca uno dei cinque file critici del workflow
  (`core/trading_engine.py`, `order_manager.py`, `core/reconciliation_engine.py`, `core/state_recovery.py`,
  `database.py`), il controllo è FAIL.

Differenze volute dallo step inline:
- i file di una PR si leggono su **tutte le pagine**;
- un elenco più corto di `changed_files`, o un elenco che raggiunge il tetto di 3000 file dell'API, ⇒ FAIL;
- per un file rinominato contano sia il nuovo percorso sia quello di origine (`previous_filename`); un rename
  senza percorso di origine leggibile ⇒ FAIL. Vale anche per il controllo dei file a merge owner della readiness;
- una PR i cui file non si leggono ⇒ FAIL. Lo step inline la saltava (`except Exception: continue`).

## `pr_merge_readiness.sh`

Esegue `scripts/pr_merge_readiness.py --repo … --pr … --ignore-safe-autofix --wait-pending-seconds <W>
--poll-seconds 15`. Lo lancia dal checkout di main con un ambiente minimo: `GH_TOKEN`, `GH_CONFIG_DIR` temporanea,
nessun altro token ereditato. Esito **PASS solo se tutte** queste condizioni valgono:
- nessuna decisione owner aperta: niente label `manual-review-required` e nessun file a merge owner nel diff
  completo (tutte le pagine), riconosciuto con `scripts/pr_autonomy_policy.is_owner_manual_path` dal checkout di main;
- exit 0;
- `headRefOid` uguale all'head atteso;
- `can_merge` è il booleano `true`;
- la PR è aperta e non in bozza;
- `blockers` e `pending` sono liste vuote;
- nessun `reasons`;
- **`ai_review_gate_status == "PASS"`**.

Il gate Sol/Grok è **informativo** nel workflow GitHub Actions, qui invece **blocca**. Sono FAIL:
- review assente;
- `UNKNOWN`;
- `RERUN_REVIEW_ONCE`;
- `STOP_OWNER` (anche `VALID_INCONCLUSIVE` o crediti esauriti).

Il verdetto PASS significa «mergiabile senza decisioni owner aperte», non «approvato dall'owner». Una PR che la
governance lascia alla decisione dell'owner resta FAIL qui, e decide l'owner.

**Tempo.** Ogni check ancora pendente allo scadere dell'attesa dà FAIL, mai PASS. Il processo figlio viene fermato
ad attesa + 120 s, e anche un timeout dà FAIL. Il guard fa inoltre quattro chiamate API, ciascuna con un timeout di
30 s (token, PR, compare, rilettura finale), e c'è il checkout. La regola è quindi **timeout della pipeline ≥
`PF_READINESS_WAIT_SECONDS` + 120 + 120 + 60 s**. Con il timeout attuale di 10 minuti, il default 300 s la rispetta
(300 + 120 + 120 + 60 = 600). Le review AI possono durare molto più di 5 minuti (Sol a effort `max`:
fino a 105 minuti). Con questo budget, una readiness lanciata con le review ancora in corso è FAIL e va rilanciata
quando sono finite. Per non sprecare build, la lane dovrebbe lanciarla **dopo** che i check della PR si sono
chiusi, oppure con un timeout ≥ 115 minuti e `PF_READINESS_WAIT_SECONDS=6600`, come Actions.

**Nessuna dipendenza circolare.**
- Le review Sol/Grok/Codex partono a ogni push e non leggono la readiness.
- L'overlap guard non legge la readiness.
- La readiness richiede verdi tutti i check reali, overlap guard compreso, e il gate review.
- Il solo rischio è che la readiness attenda o blocchi **sé stessa**. Lo status che la lane pubblica sulla PR deve
  quindi essere riconosciuto come self-check da `scripts/pr_flow_automation.py` (`is_self_check`): **`target_url`
  = URL della build della pipeline `pr-merge-readiness`** (contiene `pr-merge-readiness`), oppure contesto
  «merge readiness».
- Lo status dell'overlap guard invece **non** deve essere un self-check: la readiness lo attende e lo richiede verde.
- I test `test_trusted_readiness_status_published_on_the_pr_is_its_own_self_check` legano le due regole al codice.

## Uscita

- Exit **0 solo con PASS**, **1** in ogni altro caso: dato mancante, formato errato, token non conforme, errore di
  rete o HTTP non 200, JSON non valido, provenienza incoerente, check rosso, pendente o sconosciuto, review non PASS,
  eccezione inattesa.
- `PF_RESULT_DIR/<guard>.json` contiene: `guard`, `repo`, `pr`, `head_sha`, `base_ref`, `base_sha` (se letto),
  `verdict` (`PASS`/`FAIL`), `reasons`. Il file di un giro precedente viene cancellato **prima** della valutazione;
  se il file non si scrive, l'esito è FAIL. Così un PASS vecchio non può essere pubblicato al posto di un giro
  fallito o interrotto. Gli script **non pubblicano nulla**: solo richieste GET, e un test lo
  verifica. Lo status GitHub lo pubblica l'infrastruttura con una credenziale separata, sullo **`head_sha` della PR**
  preso da questo file, non sul commit di main che Buildkite ha costruito.
- **Su Buildkite** (`BUILDKITE` impostata) il wrapper carica `PF_RESULT_DIR/<guard>.json` come artifact con
  `buildkite-agent artifact upload`, anche con verdetto FAIL, e conserva il codice d'uscita del guard. Il dispatcher
  scarica proprio quell'artifact (`result_artifacts`, default `.trusted-guard/<guard>.json`). Senza, lo status era
  `error` «risultato del guard assente»: difetto D2, trovato nella campagna E2E reale della #562 su #573. Upload non
  riuscito ⇒ exit 1. Nel workflow Actions `BUILDKITE` non c'è e il wrapper non carica nulla.

## Requisiti infrastrutturali (Grok)

- `python3` ≥ 3.10, `gh` ≥ 2.40 e `buildkite-agent` (upload dell'artifact del risultato) nel `PATH` dell'agent trusted. Uscita di rete verso `api.github.com` (REST e
  GraphQL).
- Mappatura dal trigger nel gate pre-command: `PF_PR_NUMBER` e `PF_PR_HEAD_SHA` dello stesso evento.
- Token GitHub (D3 della #562): non c'è più un file statico in `LoadCredential`.
  - `pf-lane-token-mint.service` crea all'avvio dell'agent un token di installazione dell'App `pickfair-trusted-status`
    e lo scrive in tmpfs, in `/run/pf-lane/github-token`. Il file non resta su disco persistente.
  - `pf-lane-token-refresh.timer` lo rinnova ogni 5 minuti mentre l'agent è fermo, così nessun job parte con un token
    in scadenza.
  - Il gate pre-command passa al guard un percorso fisso, che il job non può scegliere: `PF_GITHUB_TOKEN_FILE` e
    `CREDENTIALS_DIRECTORY` impostati dalla build vengono rifiutati.
  - Il mint rifiuta un token i cui permessi non sono esattamente quelli elencati sopra.
  - Il dispatcher ha un proprio token, separato (`github.token_command`, solo `pull_requests` e `metadata` in lettura).
- Il job trusted non deve esportare il token in variabili d'ambiente: lo legge il guard dal file.
- Il contesto PR (`PF_PR_NUMBER`, `PF_PR_HEAD_SHA`) lo imposta solo il dispatcher trusted
  (`scripts/pr_trusted_dispatcher.py`, contratto e configurazione in [trusted_dispatcher.md](trusted_dispatcher.md)),
  che pubblica anche lo status sull'head della PR. Senza contesto il guard esce con exit 1 e il motivo esplicito
  «contesto PR verificato assente». Uno step Buildkite che lanciasse `scripts/pr_merge_readiness.py` senza
  argomenti (lo smoke di #562, exit 2 di argparse) va portato sul wrapper `scripts/ci/pr_merge_readiness.sh`.

## Emergenza senza VPS (runbook, §5 della #562)

Quando la VPS 45.14.184.188, la lane trusted o `pf-dispatch` non sono disponibili, si ripiega su **GitHub Actions**.
Il ripiego **non è automatico**: nessun componente sposta da solo i controlli da una CI all'altra, e un failover
automatico non è mai stato dimostrato, quindi non va dichiarato.

1. **Riconoscere il guasto.**
   - Sulla PR gli status `overlap-check` / `pr-merge-readiness` della lane restano `pending`, mancano sull'head nuovo
     oppure finiscono in `error` («timeout della build», «lettura build fallita»).
   - Sulla VPS `/usr/local/sbin/pf-dispatch-health` esce con codice 1 (servizio fermo, dry-run attivo o errori negli
     ultimi 15 minuti).
   - Un `pending` o un `error` non è mai un PASS: la PR **non** si mergia sulla base di quegli status.
2. **Cosa resta valido senza la VPS.** I workflow Actions girano a ogni push sulle PR, indipendenti dalla VPS:
   - `overlap-check` di `.github/workflows/pr-overlap-guard.yml`, oggi l'unico required check del ruleset (legato
     all'app GitHub Actions, #562 c.6437684386: lo status omonimo pubblicato dalla lane non lo soddisfa);
   - le review Sol/Grok/Codex;
   - `Merge readiness`;
   - le suite di test.

   Gli status della lane trusted non sono required checks finché l'owner non decide altrimenti (D-3 della #562).
3. **Lo status della lane già pubblicato blocca comunque.** `Merge readiness` (Actions) esegue
   `scripts/pr_merge_readiness.py`, che legge **tutti** i check e gli status dell'head (`split_checks`), anche quelli
   non richiesti dal ruleset. Contano due casi, e nessuno dei due si risolve passando ad Actions:
   - **Guasto prima che la lane pubblichi qualcosa su quell'head.** Lo status della lane non esiste, `Merge readiness`
     non lo vede, e il gate di merge vigente (spec §0.9) si valuta sui controlli Actions qui sopra.
   - **Guasto che lascia un `overlap-check` della lane `pending` o `error` sull'head.** `Merge readiness` lo conta fra
     i `pending` o i `blockers`, e un `overlap-check` verde di Actions con lo stesso nome **non** lo annulla. Il merge
     resta bloccato. Lo stesso vale per qualsiasi altro status non self-check della lane (per esempio `full-suite`
     dopo la PR-A). Lo status `pr-merge-readiness` della lane è invece riconosciuto come self-check (vedi
     «Nessuna dipendenza circolare») e non blocca la readiness di Actions.

   In entrambi i casi un `pending` o un `error` non diventa PASS, e c'è un solo modo per sbloccare: il ripristino
   della lane, con un esito nuovo pubblicato dal dispatcher sullo stesso head (punto 4). In alternativa serve una
   decisione esplicita dell'owner registrata sulla issue, che non viene mai dalla sola urgenza. Lo stesso vale, a
   maggior ragione, se gli status della lane diventano required (D-3). Non si pubblicano status a mano per sbloccare
   una PR.
4. **Ripristino.** Grok riavvia i servizi. Il dispatcher riprende dallo stato salvato: nessuna build duplicata,
   esiti pubblicati una volta sola, un head cambiato durante il fermo viene rilevato al primo giro (P0-B, certificato).
   Se una build è stata persa si usa un rerun (`/pf-trusted-rerun <sha>`, vedi
   [trusted_dispatcher.md](trusted_dispatcher.md)), mai una ripubblicazione manuale dello status.
5. **Annotare** sulla issue #562 l'inizio e la fine del guasto, le PR coinvolte e gli status rimasti stantii.

## Limiti dichiarati

- **Collaudato end-to-end (#562 CERTIFIED, c.6465081812).** Percorso reale con token App, `gh` 2.45 e status
  GitHub sull'head della PR di collaudo #573:
  - P0-A: `overlap-check` `success`, `pr-merge-readiness` `failure` con verdetto reale, un solo `pending` e un solo
    finale per contesto;
  - P0-B, P0-C, P0-D: PASS (riavvio, errori 403/429/5xx/timeout, cambio di head).
  Restano i residui dichiarati su #562 (R1, R3, R4).
- Il workflow Actions `.github/workflows/pr-overlap-guard.yml` non ha più la copia inline: esegue lo script canonico
  (Fase 2 di PF-GOV-DOC-WORKFLOW-ALIGN-01, sezione «Riserva GitHub Actions» qui sotto). Le due fasi hanno evitato una
  dipendenza circolare: su `pull_request` il workflow esegue lo script del commit base, e finché `main` aveva la
  vecchia regola sul token l'`overlap-check` della PR che introduceva il workflow era rosso per costruzione. La Fase 1
  (#564) ha corretto lo script senza toccare il workflow; questa Fase 2 lo introduce con lo script già corretto su
  `main`.
- I tre file degli script sono **a merge owner**: sono in `OWNER_MANUAL_MERGE_FILES` e nel pattern
  `^scripts/ci/(pr_[a-z0-9_]+\.sh|trusted_pr_guard\.py)$` di `MANUAL_AUTHORITY_PATTERNS`, copiato nei workflow di Sol
  e Grok, e sono elencati anche in `CLAUDE.md`, `AGENTS.md` e `docs/auto_pr_flow_spec.md` §0.9
  (PF-GOV-DOC-WORKFLOW-ALIGN-01). `tests/guardrails/test_governance_docs_aligned.py` rende rossa ogni divergenza fra
  classificatore e prosa.

## Riserva GitHub Actions (`pr-overlap-guard.yml`)

Dal mandato PF-GOV-DOC-WORKFLOW-ALIGN-01 il workflow Actions `pr-overlap-guard.yml` non ha più una copia inline della
logica: il job `overlap-check` (stesso nome del check di prima) esegue lo script canonico
`scripts/ci/pr_overlap_guard.sh`.
- **Codice non fidato:** il checkout è il commit **base** della PR (`github.event.pull_request.base.sha`), con
  `persist-credentials: false`; il codice della PR non viene scaricato né eseguito. I dati arrivano solo dalla
  GitHub API.
- **Token:** `github.token` del job, permessi `contents: read` e `pull-requests: read`. Il workflow lo scrive in un
  file 0600 temporaneo (`umask 077`), toglie la variabile dall'ambiente prima di lanciare lo script e cancella il
  file all'uscita (`trap`).
- **Ingresso:** `PF_PR_NUMBER` e `PF_PR_HEAD_SHA` solo dall'evento `pull_request` (nessun `workflow_dispatch`).
  Il risultato `overlap.json` è caricato come artifact `pr-overlap-report`.
- **Differenze dal vecchio step inline:**
  - legge tutte le pagine;
  - una PR illeggibile dà FAIL;
  - verifica la provenienza: una PR da fork o con head diverso dall'evento dà FAIL;
  - senza PR aperte dà FAIL invece che verde;
  - una PR con base diversa da `main` dà FAIL (il workflow non imposta `PF_PR_BASE_REF`, quindi vale il default
    `main`): prima il controllo la valutava comunque. Il check gira anche su `edited`, quindi un cambio di base lo
    ricalcola: una PR aperta su un branch con uno script compiacente e poi spostata su `main` non tiene il verde
    calcolato sul vecchio base (review indipendente su #567). Limite: lo script è quello del commit base
    dell'evento, che può essere un `main` meno recente; resta codice trusted e sbaglia verso il FAIL.
- **Collaudo manuale separato (`pr-overlap-guard-diagnostic.yml`).** Il check run prende il nome dal job, non dal
  workflow. Su #564 un `workflow_dispatch` dal branch di `pr-overlap-guard.yml` ha pubblicato sull'head `b4e6604` un
  secondo `overlap-check` (success, check suite 109551036228) accanto a quello del `pull_request` (failure, suite
  109551022632): stessa app `github-actions`, stesso SHA, stesso nome. Un PASS così può soddisfare il required check.
  Per questo:
  - `pr-overlap-guard.yml` gira **solo** su `pull_request` ed è l'unico workflow che pubblica `overlap-check` (un
    test scorre tutti i workflow);
  - la diagnostica manuale è un workflow a parte, solo `workflow_dispatch`, con job e check `overlap-diagnostic`:
    non può soddisfare il required check `overlap-check`;
  - la diagnostica accetta solo `refs/heads/main`: da un altro ref il primo step fallisce, prima del checkout e del
    token. Gira quindi solo codice di `main`; il run da `main` si attacca al commit di `main`, non all'head della PR.
    Un dispatch rifiutato da un branch lascia invece un `overlap-diagnostic` **rosso** sull'head di quel branch:
    merge readiness lo conta come bloccante fino al push successivo (fail-closed, mai un verde);
  - nessun `defaults:`, nessun `${{ }}` nei `run`, nessun `BASH_ENV`/`ENV`, e ogni step usa solo le chiavi `name`,
    `uses`, `with`, `run`, `env`;
  - nessuno step dei due workflow ha `if:` o `shell:` (salvo `if: always()` sull'upload): uno step saltato o
    eseguito da un'altra shell darebbe un verde senza guard. Nessun `env:` ridefinisce `GITHUB_*`/`RUNNER_*`, e lo
    step del gate non ha `env`: il gate legge `$GITHUB_REF` del runner (non `${{ github.ref }}` nel testo dello
    script, che con un nome di branch contenente `$(` diventerebbe un'iniezione di comandi);
  - stesso isolamento del token del check obbligatorio.
  Merge readiness (`scripts/pr_flow_automation.split_checks`) legge ogni check run dell'head: un FAIL resta un
  bloccante anche se accanto c'è un PASS, diagnostico o con lo stesso nome; un test lo lega al codice.
  Il controller (`scripts/pr_automation_controller.dedupe_github_check_runs`, #566) non scarta più nessun check run
  dell'head corrente, salvo la copia esatta della stessa check run (stesso `id`/`databaseId`, stesso stato e stessi
  tempi, per esempio da pagine sovrapposte; stesso id con esito diverso non è una copia). Continua a ignorare i run di uno SHA vecchio o senza SHA. Così un FAIL non diventa mai PASS per
  effetto della deduplica. Motivo, verificato su dati reali:
  - run diversi, anche dello stesso workflow sullo stesso SHA, stanno in check suite diverse (`b4e6604` su #564);
  - dentro la stessa suite possono esserci job fratelli con lo stesso nome, anche in sequenza (`needs:`);
  - `run_attempt` identifica il tentativo del workflow, non il job: rilanciando un solo fratello sembrerebbe il rerun
    dell'altro; `external_id` cambia a ogni tentativo (run 40406913995 su `49518cc`).

  Dai dati dei check run non si può quindi dimostrare che due run omonimi siano lo stesso job. Lo sa solo GitHub:
  - **fonte prevista:** `GET commits/{sha}/check-runs` con `filter=latest` (il default) restituisce solo l'ultimo
    tentativo di ogni job. Sul run 40406913995 dà soltanto il tentativo 2. Con questi dati un rerun legittimo non
    lascia blocchi, e un FAIL concorrente si supera rilanciando il suo workflow run;
  - **con `filter=all`** la storia resta visibile: il tentativo fallito blocca e uno cancellato accanto a un PASS
    produce un piano di rerun (`workflow_cancelled`), mai `ready`. È il comportamento voluto: chi legge la storia
    completa la deve interpretare;
  - una conclusione fuori dagli stati noti (es. `STARTUP_FAILURE`, `EXPECTED` di GraphQL, un completato senza
    conclusione) dà `current_head_inconclusive` e `needs_manual`, mai `ready`. Contano come esito positivo solo
    `SUCCESS`, `NEUTRAL` e `SKIPPED`;
  - oggi le tre funzioni sono usate solo dai test e dal controller stesso; un futuro chiamante deve leggere i check
    run con `filter=latest`.
  Limite: GitHub esegue `workflow_dispatch` col file del workflow del ref scelto, quindi il blocco sul ref vale per
  il file di `main`. Una PR che modifica i workflow può cambiarli, ed è per questo che ogni workflow è a merge owner.
  Il required check si riconosce solo dal nome: un altro workflow aggiunto da una PR, uno status di commit o un'altra
  app con nome `overlap-check` lo soddisferebbero. Il repository non può impedirlo; lo chiuderebbe solo legare il
  required check all'app sorgente nel ruleset (decisione owner, fuori da questa PR).
- **Bootstrap (storico):** su `pull_request` lo script viene dal commit base. Su #564, prima della separazione in due
  fasi, l'`overlap-check` era rosso perché `main` aveva ancora la vecchia regola sul token; un collaudo dal branch
  (run 40418758994) aveva prodotto proprio il PASS duplicato descritto sopra. Dalla Fase 2 il check usa lo script
  corretto di `main`.
- **Permessi del token di Actions:** il guard riconosce solo i token classici con scope dichiarati; il token del job
  non ne dichiara. La sola lettura dipende quindi dal blocco `permissions:` dei due workflow (`contents: read`,
  `pull-requests: read`), che un test vincola.
- Il workflow che gira su `pull_request` è quello della PR stessa: una PR che modifica questo workflow lo esegue
  modificato. È per questo che tutti i workflow sono a merge owner.
- `tests/guardrails/test_una_sola_pr_aperta.py` esegue la regola sullo script vero e lega il workflow allo script:
  checkout del base, nessuna copia inline, token solo come file, permessi in sola lettura, solo `pull_request`,
  unico workflow col check `overlap-check`, diagnostica con nome distinto e solo da `main`, FAIL mai mascherato da
  un PASS in merge readiness.
