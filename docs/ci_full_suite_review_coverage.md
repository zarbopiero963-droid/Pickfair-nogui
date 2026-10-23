# CI senza duplicati, check `full-suite` e copertura attestata delle review (PFC-002)

Roadmap #461, scheda **PFC-002**: M156 (deduplica CI), M130 (raccolta/workflow e
policy reviewer), M159 (copertura review completa e attestata). La PR modifica
workflow e `scripts/pr_*`: **merge manuale dell'owner**. Non tocca codice di
trading, money management, Betfair o configurazione runtime.

## 1. Cosa non andava (misurato su #535, #537 e #538)

- **La suite completa girava 17 volte per ogni push**, sempre sullo stesso albero
  `refs/pull/N/merge`:
  - 10 volte come backstop di `_module-ultra-check` nelle lane chiamate da
    `ci-master-gate`;
  - 3 volte con `ci-dynamic-intelligent` (annidato nel master e da solo);
  - 3 volte con `merge-simulation-hard` (da solo e nei due orchestratori);
  - 1 volta in `pr-check`.
- **Una ventina di workflow partivano due volte**, una su `push` e una su
  `pull_request`: il `push` non aveva filtro di branch e scattava anche sul
  branch della PR.
- **I run degli SHA superati non si cancellavano**: 162 minuti-job persi solo
  su #535.
- **Nessun check verificava le proprie condizioni.**
  - `pytest -q` verde significava solo exit code 0: non distingueva una raccolta
    parziale, test deselezionati o test raccolti e mai eseguiti.
  - Merge Readiness accettava `SKIPPED` come OK per ogni check, inclusi quelli
    che contano. Un job saltato da `if:` diventa `SKIPPED`, e GitHub lo conta
    come required soddisfatto.
- **Le review di Sol e Grok troncavano il diff in silenzio.**
  - Ogni file oltre 7000 caratteri (Sol) o 5000 (Grok) veniva tagliato.
  - Oltre 24 000 o 18 000 caratteri di prompt i file venivano saltati per intero.
  - Il job finiva comunque verde. Un segreto mancante o un errore del provider
    davano a loro volta un job verde.
  - Su #538 tre file sono rimasti parzialmente senza review indipendente. È
    servita l'eccezione owner PF-PFC001-REVIEW-EXCEPTION-01.

## 2. Topologia dopo la PFC-002

| Esecuzione | Prima (per push) | Dopo |
|---|---|---|
| Suite completa (`pytest` dell'intera raccolta) | 17 | **1**, nel check `full-suite` |
| `mutation-guardrails` | fino a 3 (master, master→dynamic, dynamic) | **1** (`ci-master-gate`) |
| `ci-dynamic-intelligent` | 2 (da solo e annidato) | **1** (da solo, solo `detect` + `dutching` + `pnl-engine`) |
| `merge-simulation-hard` | 3, ognuno con la suite completa | **1**, senza pytest: merge con il main più recente, marker, JSON dei guardrail |
| Workflow con `push` + `pull_request` | 2 run per push sul branch della PR | 1 (`push` solo su `main`) |
| Run di SHA superati su una PR | proseguivano fino alla fine | cancellati (`full-suite`, `ci-master-gate`, `ci-dynamic-intelligent`, `merge-simulation-hard`) |

Le lane di modulo (`_module-ultra-check`) mantengono:
- esistenza del modulo e dei file guardrail;
- validazione JSON;
- `guardrail_runner.py`;
- test mirati.

Perde solo il backstop. I test mirati non si perdono: girano anche dentro
`full-suite`.

**Su `main` non si cancella mai nulla.** Ogni merge ha la sua suite completa:
`full-suite` gira su `push` a `main` con un gruppo di concorrenza per SHA, perché
GitHub scarterebbe un run in attesa nello stesso gruppo anche con
`cancel-in-progress: false`.

**Equivalenza della copertura.** Su `pull_request` tutti i workflow fanno
checkout di `refs/pull/N/merge`, cioè del merge della PR con la base. Le 17
esecuzioni erano quindi la stessa suite sullo stesso albero, e una basta.

L'unica differenza era `merge-simulation-hard`, che rifà il merge con il main
più recente. Quel controllo resta, senza pytest: con una sola PR attiva alla
volta, il main non si muove fra l'evento e il job.

**Effetto collaterale dichiarato.** Alcuni workflow avevano filtri `paths` sul
`push` più larghi di quelli sul `pull_request`. Per esempio
`reconciliation-engine` scattava anche su `tests/reconciliation/*`. Una PR che
tocca solo quei test non avvia più quella lane. I test girano comunque, e una
volta sola, in `full-suite`.

## 3. Il check `full-suite`

File: `.github/workflows/full-suite.yml`. Job unico, nome stabile
`full-suite`, adatto a un required status check:

- parte su ogni `pull_request` (opened, synchronize, reopened,
  ready_for_review) **senza filtri di path o branch**, e su `push` a `main`;
- **nessun `if:`** sul job e **nessun `workflow_dispatch`**: un dispatch sul
  branch della PR farebbe girare la versione del file presente nella PR e
  attaccherebbe un verde al suo head;
- esegue `python scripts/ci/full_suite_gate.py run`, che lancia pytest con il
  plugin `scripts/ci/full_suite_plugin.py` e **giudica il report**, non
  l'exit code.

Il gate fallisce se una qualsiasi di queste condizioni non regge:

| Condizione | Contratto |
|---|---|
| exit code di pytest | 0 |
| errori di raccolta | 0 |
| test deselezionati | 0, e nessun argomento fra `-k`, `-m`, `--deselect`, `-x`, `--lf`, `--sw`… |
| test raccolti | almeno `min_collected` (8916, baseline CI di #538) |
| test raccolti senza esito | 0 |
| `failed` / `error` / `xpassed` | 0 |
| `skipped` | al massimo `max_skipped` (15) |
| `xfailed` | al massimo `max_xfailed` (5) |

Il contratto sta in `scripts/ci/full_suite_contract.json`. Abbassare una soglia
è una modifica visibile nel diff e, come tutto `scripts/ci`, passa dall'owner.
Prima di lanciare pytest il gate cancella il report precedente, così un report
vecchio non può valere per il run nuovo.

L'evidenza `full-suite-evidence.json` contiene:
- SHA del run e head della PR;
- evento, run e tentativo;
- conteggi, durata ed esito.

Si carica come artifact `full-suite-evidence-<sha>-a<attempt>` ed è riportata
nel riepilogo del job.

La suite gira ancora in serie: xdist entra con PFC-003. Il marker `serial` di
PFC-001 resta pronto per quel passo.

## 4. Copertura attestata delle review (M159)

Modulo: `scripts/ci/review_coverage.py`, senza dipendenze esterne. Lo usano i
workflow di Sol e Grok.

### 4.1 Segmentazione deterministica, niente troncamento

- `build_patch_payload` non taglia più nulla: redige i segreti, neutralizza i
  fence e passa i file a `plan_review`.
- Ogni patch si divide ai confini degli hunk (`@@`). Un hunk più lungo del tetto
  si divide per righe, una riga più lunga per caratteri. La concatenazione dei
  segmenti è il patch originale, e ogni segmento ha id (`F003S02`) e sha256.
- I segmenti si impacchettano in **lotti** entro `MAX_TOTAL_PATCH_CHARS`, nel
  testo esatto che finisce nel prompt. Un lotto corrisponde a una chiamata al
  modello.
- I lotti girano in parallelo, quindi il job dura quanto una chiamata e il
  timeout del job resta valido.
- `MAX_PATCH_PER_FILE_CHARS` (7000 Sol / 5000 Grok) diventa la dimensione del
  segmento. `MAX_TOTAL_PATCH_CHARS` (24 000 / 18 000) diventa quella del lotto.
- **Tetto di spesa:** `MAX_REVIEW_CALLS` (8). Se un range richiede più lotti, non
  si chiama nessun modello e l'attestazione è incompleta: non si paga una review
  che resterebbe incompleta. Le strade sono dividere la PR o alzare il tetto, ed
  è una decisione owner.

### 4.2 Riscontro del modello

Il prompt di ogni lotto elenca i propri segmenti e chiede come ultima riga
`SEGMENTI ESAMINATI: <id>, …`. Un lotto conta come revisionato solo se:
- la risposta è completa, cioè non troncata (`finish_reason=length`), non vuota,
  non un errore del provider;
- l'ultima riga di riscontro cita **tutti** i suoi segmenti.

Un riscontro scritto dentro il diff, che non è attendibile, non vale: conta
solo l'ultima riga.

### 4.3 Attestazione

Per ogni range il workflow scrive un JSON, schema `pickfair-review-coverage/v1`,
con:
- revisore, modello, PR e range;
- run, tentativo e file workflow;
- segmenti con sha256, lotti ed esito per lotto;
- segmenti revisionati e non revisionati;
- file binari, file rinominati senza modifiche, file di testo senza patch;
- `complete` e i motivi di incompletezza.

Il JSON si carica come artifact del run
(`review-coverage-<reviewer>-<base12>-<head12>-a<attempt>`). Un artifact non si
modifica: si può solo cancellare, e allora manca, quindi fail-closed.

Rendono l'attestazione **incompleta**:
- un lotto non revisionato;
- un file di testo senza patch (GitHub non lo fornisce oltre una certa
  dimensione);
- la Compare API al tetto di 300 file;
- il tetto di lotti superato.

I binari non bloccano, ma sono elencati. Il `done_marker` del commento, che
evita di ripagare lo stesso range, si scrive **solo** con attestazione completa.

### 4.4 Verifica dell'intera PR: il verdetto del check

L'ultimo step del job (`Verify review coverage of the whole PR`, `if: always()`)
esegue `review_coverage.py verify`. Il check del revisore è verde **solo se**
le attestazioni complete formano una **catena di range** che parte da un commit
del branch base e arriva al head corrente.

Ogni riga del diff finale è stata scritta da una push, e quella push è un
anello della catena.

Contano solo le attestazioni prodotte dai **run ufficiali**:
- il file workflow del revisore;
- evento `pull_request_target`, quindi codice del branch base e mai della PR;
- head del run fra i commit della PR;
- artifact che nomina lo stesso run e lo stesso head.

Sono quindi scartati:
- un workflow aggiunto dalla PR;
- un evento `pull_request`;
- un artifact con lo stesso nome caricato da un altro run;
- un commento forgiato.

Il modulo del verificatore arriva da un checkout **sparse del branch base**
(`persist-credentials: false`): la PR non può cambiare né come si segmenta né
come si verifica.

Il job è rosso, senza alcuna chiamata al modello, nei casi seguenti:
- PR in draft;
- PR da fork;
- segreto mancante;
- errore delle API GitHub.

Il job non ha più un `if:` a livello di job, perché uno skip varrebbe come
required soddisfatto.

**Rimedi quando la catena è rotta.**
- *Push senza review completa* (per esempio provider in timeout): si rilancia
  quel run (re-run del job). Il nuovo tentativo produce l'attestazione per lo
  stesso range, e la dedup evita di ripagare i range già completi.
- *Force-push*: il range passa per un head che non è più della PR. Chiudere e
  riaprire la PR produce una review `base...head` dell'intera PR.
- *Artifact scaduti*: la conservazione è di 90 giorni. Riaprire la PR produce
  una review completa nuova.

**Su questa stessa PR** i workflow `pull_request_target` girano dal branch base,
cioè con la versione **precedente**. La nuova copertura si applica dalle PR
successive al merge. La PFC-002 stessa è revisionata con la vecchia logica, e
il gate ne dichiara la copertura effettiva.

## 5. Merge Readiness: check obbligatori

`scripts/ci/required_checks.json` elenca i check che devono essere
**SUCCESS sull'head**:

- `full-suite`
- `mutation-guardrails / mutation`
- `GPT-6.1 Sol push-range review`
- `Grok 4.7 push-range review`

Come li giudica `scripts/pr_flow_automation.py`:
- **assente** → pending, quindi il gate aspetta, e a budget scaduto è rosso;
- `SKIPPED`/`NEUTRAL` → bloccante;
- `FAILURE`/`CANCELLED`/`TIMED_OUT` → bloccante, come prima;
- SHA diverso dall'head (quando il rollup lo riporta) → bloccante;
- due esecuzioni con lo stesso nome valgono solo se sono **entrambe** SUCCESS;
- manifest illeggibile o vuoto → gate rosso.

Per i check non obbligatori `SKIPPED` resta accettabile: le lane di modulo si
saltano da sole quando il diff non le riguarda.

Restano invariati: thread irrisolti, `mergeable`, `mergeStateStatus`, draft,
nessun check osservato e stabilità del rollup.

## 6. Enforcement GitHub: stato e azioni owner

**Stato verificato il 23/10/2026** (API pubblica, ruleset 25459867
`PICKFAIR-MAIN-PROTECTION`):
- `deletion`, `non_fast_forward`, PR obbligatoria con 0 approvazioni, thread
  risolti e solo squash;
- **nessuna** regola `required_status_checks`;
- `bypass_actors` vuoto.

Finché il ruleset non richiede i check, **nessun check blocca davvero il
merge**: lo blocca solo la disciplina owner/agente. Quindi:

> **IMPLEMENTATION READY / ENFORCEMENT PENDING OWNER.** Il merge automatico resta
> vietato.

**Azione owner, dopo il merge della PFC-002** (prima i check nuovi non esistono
su `main`):

1. *Settings → Rules → Rulesets → PICKFAIR-MAIN-PROTECTION → Add rule →
   Require status checks to pass*.
2. Aggiungi questi check, ognuno con sorgente **GitHub Actions**: così un
   commit status scritto con un altro token non lo soddisfa.
   - `full-suite`
   - `mutation-guardrails / mutation`
   - `GPT-6.1 Sol push-range review`
   - `Grok 4.7 push-range review`
   - `Merge readiness`
3. Spunta **Require branches to be up to date before merging**: i check devono
   riguardare il merge con il main attuale. Con una PR alla volta il costo è
   nullo.
4. Lascia vuoti i **bypass actors**.

Equivalente via API (token owner con permesso Administration):
`PUT /repos/zarbopiero963-droid/Pickfair-nogui/rulesets/25459867` con le regole
attuali più:

```json
{"type": "required_status_checks", "parameters": {
  "strict_required_status_checks_policy": true,
  "do_not_enforce_on_create": false,
  "required_status_checks": [
    {"context": "full-suite", "integration_id": 15368},
    {"context": "mutation-guardrails / mutation", "integration_id": 15368},
    {"context": "GPT-6.1 Sol push-range review", "integration_id": 15368},
    {"context": "Grok 4.7 push-range review", "integration_id": 15368},
    {"context": "Merge readiness", "integration_id": 15368}]}}
```

`15368` è l'id della GitHub App «GitHub Actions».

**Limiti che il ruleset da solo NON chiude.** Vanno dichiarati: a causa loro il
merge automatico resta vietato anche dopo l'azione qui sopra.

1. **Una PR può modificare i propri check `pull_request`.** `full-suite`,
   `ci-master-gate` (mutation) e Merge Readiness girano dalla versione della
   PR. Una PR che li indebolisce fa girare su di sé il check già indebolito.
   - Un ruleset di repository personale non può fissare il file del workflow:
     *Require workflows* esiste solo a livello di organizzazione.
   - Mitigazione in vigore: ogni modifica a `.github/workflows/`,
     `scripts/pr_*` e (da questa PR) tutto `scripts/ci/` è
     `MANUAL_AUTHORITY_PATTERNS`, quindi `manual-review-required` e merge
     manuale owner.
   - Su main solo `scripts/ci/check_ci_quarantine.py` lo era.
     `full_suite_gate.py`, `review_coverage.py`, il contratto e il manifest
     sono entrati in `OWNER_MANUAL_MERGE_FILES` e nei pattern, nella copia
     identica di Sol e Grok.
   - L'elenco in prosa di CLAUDE.md (file dell'owner) non è toccato:
     aggiornarlo è un'azione owner.
   - I check dei reviewer invece girano da `pull_request_target`, cioè dalla
     base, e la PR non li può cambiare.
2. **Il nome di un check non identifica il workflow.** Un altro job chiamato
   `full-suite` in un workflow aggiunto dalla PR soddisferebbe il nome. Merge
   Readiness esige che **tutte** le esecuzioni con quel nome siano SUCCESS, ma
   non può impedire una copia verde se l'originale viene rimosso nello stesso
   diff. Anche questo caso è coperto solo dal merge owner sui workflow.
3. **Identità dell'agente.** L'agente lavora con l'account dell'owner. GitHub
   non permette di approvare la propria PR, quindi «review del code owner
   obbligatoria» bloccherebbe ogni PR, senza aggiungere un controllo
   indipendente. Un enforcement indipendente completo richiede un'identità
   separata per l'agente (GitHub App o utente macchina). Allora l'owner può
   diventare reviewer obbligatorio per `.github/workflows/**`, `scripts/pr_*`,
   `scripts/ci/**` e i file di policy. È una decisione owner (scheda
   «identità dell'agente» della roadmap).

## 7. Tempi CI

| Misura | Prima | Dopo | Tipo |
|---|---|---|---|
| Suite completa in CI | 711–753 s per esecuzione (#538 `pr-check` 711 s; main `99250c5` 752,65 s), ×17 per push | 1 esecuzione per push | prima [M]; dopo [M] sul head della PFC-002, nel report della PR |
| Minuti-job per push | 342–391 (#535/#537) | ~70 | prima [M]; dopo [S], da confermare con la PR |
| Dal push all'ultimo check | 25–30 minuti | ~14–16 minuti, limitato da `full-suite` più Merge Readiness | prima [M]; dopo [S] |
| Review AI | 1 chiamata per reviewer e push, troncata | 1–8 chiamate parallele per reviewer e push, solo se il range supera il budget di un lotto | regola |
