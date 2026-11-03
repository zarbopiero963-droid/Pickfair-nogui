# Merge readiness: tempi, scadenze e riattivazione (PF-MERGE-READINESS-FAST-01 / PF-CI-PERFORMANCE-FINAL-01)

Riferimenti: #586, diagnosi c.6590416700. Codice:
- `scripts/pr_flow_automation.py` (`cmd_readiness`, `sh`, `gh_json`);
- `scripts/pr_readiness_retrigger.py`;
- workflow `pr-merge-readiness.yml` e `pr-readiness-retrigger.yml`.

Test:
- `tests/scripts/test_merge_readiness_fast.py`;
- `tests/scripts/test_pr_readiness_retrigger.py`.

**Principio.** La VPS esegue la certificazione, GitHub ne verifica il risultato, la Merge readiness decide. Mai veloce
a costo di un falso PASS.

## 1. Causa della lentezza osservata (#602, `d990fa6`)

Il passo «Validate merge readiness» restava in polling per tutta la durata del PRE-LIVE (`pf-betfair-certification`,
oltre 55 minuti). Pesavano quattro cose:
- **budget di attesa:** 6600 s, con un giro ogni 15 s;
- **lettura completa a ogni giro:** rollup GraphQL più la paginazione di tutti i thread di review;
- **nessun timeout per singola chiamata `gh`;**
- **nessun retry sugli errori transitori:** un `i/o timeout` GraphQL faceva cadere il passo senza un giudizio scritto.

Non c'erano deadlock: il self-check è escluso dai pending.

## 2. Limiti configurati

| Limite | Valore | Dove |
|---|---|---|
| Singola chiamata `gh` | 60 s (`PF_GH_CALL_TIMEOUT_SECONDS`) | `sh()`; altri comandi 300 s (`PF_CALL_TIMEOUT_SECONDS`) |
| Retry | 3 tentativi (`PF_GH_RETRIES`), backoff 2 s / 4 s (`PF_GH_RETRY_BACKOFF_SECONDS`) | `gh_json()`, solo errori transitori |
| Polling | 15 s, che diventano 60 s dopo 300 s con check pending; la stabilità si conta sempre a 15 s | `--poll-seconds`, `--max-poll-seconds`, `--backoff-after-seconds` |
| Attesa dei check | 6600 s (invariata: copre il check più lento) | `--wait-pending-seconds` |
| Uscita anticipata | dopo 300 s, se restano pending **solo** check esterni (PRE-LIVE): nessun runner occupato durante la certificazione (owner PF-POST-602-MASTER-EXECUTION-01 §4) | `--pending-external-exit-after` |
| Scadenza complessiva | 6900 s, sotto il timeout del job (7200 s); il sonno di polling non la supera e, scaduta, le letture informative (LIVE, gate AI) si saltano (UNKNOWN) | `--deadline-seconds` |
| Riattivazione | 10 min di job, attesa fino a 420 s di una readiness ancora in corso, 30 s per richiesta, al massimo 3 tentativi per run | `pr-readiness-retrigger.yml` (`--wait-running-seconds`) |

**Errori transitori** (timeout, 5xx, rete interrotta, rate limit, `i/o timeout`): vengono ritentati in modo limitato.
Durante l'attesa si tiene l'ultima lettura buona, ma una lettura tenuta **non è una verifica**: non conta come
intervallo stabile e non può essere il giudizio finale. Se all'inizio o all'ultima lettura GitHub non risponde, il
risultato è `github_api_unavailable` (TIMEOUT) e la PR non è pronta (rilievo di Sol su #603: prima due errori
transitori dopo una lettura verde bastavano per READY).

**Errori non transitori** (404, autenticazione, permessi, schema): non vengono ritentati.

## 3. Stati (`readiness_status`)

Solo READY corrisponde a `can_merge = true` ed exit 0. Tutti gli altri stati danno exit 1.

| Stato | Significato |
|---|---|
| READY | tutti i gate sono validi sull'ultima lettura completa |
| PENDING_EXTERNAL | restano solo certificazioni esterne in corso; la readiness viene rieseguita al loro completamento |
| FAILED | un check è rosso |
| BLOCKED | bloccanti di review, thread, label manuale, head cambiato, altri check pending |
| TIMEOUT | scadenza complessiva superata, oppure GitHub non disponibile; non è un fallimento del codice, ma non autorizza il merge |

La semantica dei check GitHub non cambia: il check «Merge readiness» resta success solo con READY. Nessun nuovo check
obbligatorio.

## 4. Riattivazione event-driven

Il flusso è questo:
1. Pickfair Dedicated conclude `pf-betfair-certification` su uno SHA.
2. GitHub emette `check_run: completed`.
3. Il workflow `pr-readiness-retrigger.yml` parte dal branch di default, senza codice della PR, e chiama
   `scripts/pr_readiness_retrigger.py`.
4. Lo script **rilancia la run di Merge readiness già esistente** per quell'head, quindi il check resta attaccato
   all'head della PR.

**Protezioni:**
- conta solo il check `pf-betfair-certification` dell'app `pickfair-dedicated`;
- conta solo una PR aperta, dello stesso repository, con head uguale allo SHA del check (`STALE_SHA` altrimenti), così
  un risultato vecchio non viene mai attribuito a un head nuovo;
- una run in corso non si tocca (niente doppia pubblicazione) e una run già verde non si rilancia;
- una run in corso partita **prima** della conclusione del PRE-LIVE (per esempio mentre esce PENDING_EXTERNAL) può non
  averne visto l'esito: si attende che termini, fino a 420 s, rileggendo PR e run a ogni giro, poi si decide; una run
  partita dopo legge già l'esito e non si attende; ancora in corso al limite ⇒ nessuna scrittura (rilievo di Sol su
  #603: prima l'unico evento andava perso);
- al massimo 3 tentativi per run, contro loop e replay;
- l'unica scrittura è `POST …/actions/runs/<id>/rerun`.

Se `check_run.pull_requests` arriva vuoto, le PR si cercano con `GET /commits/<sha>/pulls`, con gli stessi filtri.

**Non verificato:** un rerun richiesto con `GITHUB_TOKEN` da un workflow `check_run` è NOT VERIFIED fino alla prova su
una PR reale (il workflow gira solo dal branch di default, quindi la prova è possibile solo dopo il merge). Se il
rerun non scattasse, la readiness resta rossa in PENDING_EXTERNAL, mai un falso PASS, e va rilanciata a mano:
è un rischio di disponibilità, non di sicurezza (rilievo di Grok su #603).

## 5. LIVE (PF-DUAL-CERTIFICATION-01, solo informativo)

La decisione riporta `live_certification` e `live_readiness`, letti dal check `pf-betfair-live-certification`
di Pickfair Dedicated sull'head esatto.

| Campo | Valori |
|---|---|
| `live_certification` | NOT_RUN = neutral; CERTIFIED = success; FAILED; PENDING; MISSING |
| `live_readiness` | PASS o BLOCKED dal titolo/riepilogo `LIVE_READINESS_PASS|BLOCKED`; altrimenti UNKNOWN |

- Il LIVE non cambia mai `can_merge` e non viene mai atteso.
- LIVE_READINESS_PASS non equivale a LIVE_CERTIFIED.
- Un errore di lettura dà UNKNOWN, mai PASS.

## 6. Cosa resta da misurare

- Tempi prima/dopo su più esecuzioni reali e almeno un timeout controllato: NOT_RUN fino alla PR dedicata, dopo la #602.
- Tempi della VPS per fase: lato Grok (#586 c.6590416700, richieste 1–4).
