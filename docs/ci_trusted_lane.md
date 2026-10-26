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

**Token.** Il token GitHub deve essere **fine-grained, in sola lettura**, limitato a questo repository:
- Contents: read;
- Pull requests: read;
- Commit statuses: read;
- Checks: read;
- Actions: read;
- Issues: read (per i commenti delle review AI).

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

## Requisiti infrastrutturali (Grok)

- `python3` ≥ 3.10 e `gh` ≥ 2.40 nel `PATH` dell'agent trusted. Uscita di rete verso `api.github.com` (REST e
  GraphQL).
- Mappatura dal trigger nel gate pre-command: `PF_PR_NUMBER` e `PF_PR_HEAD_SHA` dello stesso evento.
  Il token arriva via `LoadCredential=github-token:/etc/buildkite-agent-trusted/secrets/github-token`, quindi
  dentro `CREDENTIALS_DIRECTORY`.
- Il job trusted non deve esportare il token in variabili d'ambiente: lo legge il guard dal file.

## Limiti dichiarati

- **NOT VERIFIED end-to-end.** Il percorso reale con token, `gh` e status GitHub resta da collaudare sulla lane
  dopo che l'owner fornisce il token. Verificato offline: 151 test e mutazioni. Smoke dell'overlap guard sull'API
  pubblica reale su #561: PASS sull'head corrente, FAIL su un head stantio.
- I tre file degli script sono **a merge owner**: sono in `OWNER_MANUAL_MERGE_FILES` e nel pattern
  `^scripts/ci/(pr_[a-z0-9_]+\.sh|trusted_pr_guard\.py)$` di `MANUAL_AUTHORITY_PATTERNS`, copiato nei workflow di Sol
  e Grok, e sono elencati anche in `CLAUDE.md` e `AGENTS.md` (PF-GOV-DOC-WORKFLOW-ALIGN-01).
  `tests/guardrails/test_governance_docs_aligned.py` rende rossa ogni divergenza fra classificatore e prosa.

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
    `main`): prima il controllo la valutava comunque.
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
    token. Gira quindi solo codice di `main`, e il check run si attacca al commit di `main`, non all'head della PR;
  - stesso isolamento del token del check obbligatorio.
  Merge readiness (`scripts/pr_flow_automation.split_checks`) legge ogni check run dell'head: un FAIL resta un
  bloccante anche se accanto c'è un PASS, diagnostico o con lo stesso nome; un test lo lega al codice.
  Limite: GitHub esegue `workflow_dispatch` col file del workflow del ref scelto, quindi il blocco sul ref vale per
  il file di `main`. Una PR che modifica i workflow può cambiarli, ed è per questo che ogni workflow è a merge owner.
- **Bootstrap:** su `pull_request` lo script viene dal commit base, quindi una PR che corregge lo script viene
  valutata con la versione di `main`. Su #564 l'`overlap-check` resta rosso finché la correzione del token non è
  su `main`. La correzione è stata collaudata una volta dal branch (run 40418758994, prima della separazione), e
  quel run ha prodotto proprio il PASS duplicato descritto sopra.
- Il workflow che gira su `pull_request` è quello della PR stessa: una PR che modifica questo workflow lo esegue
  modificato. È per questo che tutti i workflow sono a merge owner.
- `tests/guardrails/test_una_sola_pr_aperta.py` esegue la regola sullo script vero e lega il workflow allo script:
  checkout del base, nessuna copia inline, token solo come file, permessi in sola lettura, solo `pull_request`,
  unico workflow col check `overlap-check`, diagnostica con nome distinto e solo da `main`, FAIL mai mascherato da
  un PASS in merge readiness.
