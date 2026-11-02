# Certificazione Betfair delle PR: protocollo `/pf-cert v2` (PF-BETFAIR-VPS-GITHUB-CERT-01)

Riferimenti: roadmap #586; mandati owner PF-BETFAIR-VPS-GITHUB-CERT-01 e PF-BETFAIR-GITHUB-APP-ACTIVATION-01;
infrastruttura VPS (pf-bfgw, recorder, pf-orch, sandbox `pf-sbx`) descritta da Grok in #586 c.6575246644.

Obiettivo: ogni head di una PR dello stesso repository viene certificato in modo automatico, senza un `/pf-cert`
scritto a mano. Il codice di Pickfair gira sulla VPS contro dataset Betfair Delayed autentici, solo nel SIM. Il
verdetto arriva come check `pf-betfair-certification` della GitHub App «Pickfair Dedicated».

## 1. Perché v2: un commento non prova chi lo ha scritto

In v1 la VPS accettava un `/pf-cert` scritto da un autore in allowlist. I commenti dell'agente compaiono però con
l'utenza dell'owner, quindi l'autore del commento non prova nulla (mandato: «una semplice richiesta scritta nei
commenti della PR non è sufficiente come prova di autorizzazione»).

In v2 la richiesta porta un **token OIDC di GitHub Actions**:
- lo emette GitHub, solo al workflow `pf-cert-request.yml` in esecuzione dal branch `main`;
- è firmato RS256 con le chiavi pubblicate su `https://token.actions.githubusercontent.com/.well-known/jwks`.

Né l'agente né una PR possono produrne uno valido per quel workflow su `main`.

## 2. Lato GitHub: `.github/workflows/pf-cert-request.yml`

- **Trigger:** `pull_request_target` (`opened`, `synchronize`, `reopened`, `ready_for_review`). Gira dal **branch
  base**, quindi una PR non può modificare il workflow che la certifica (a merge owner come ogni workflow).
- **Condizioni:** PR dello stesso repository (le fork sono escluse) e non in bozza.
- **Permessi:** `contents: read`, `pull-requests: write` (per il commento) e `id-token: write` (per l'OIDC).
- **Codice della PR:** mai fatto uscire né eseguito. Il checkout è `github.sha`, cioè il base, con
  `persist-credentials: false`. I dati della PR passano solo da `env`: nessuna espressione `${{ }}` dentro i `run`.
- **Passi:**
  1. elenco dei file cambiati (`filename` e `previous_filename`) dall'API;
  2. `python3 -E -s -m tools.pf_cert.classify` ricava gli scenari obbligatori e li riporta nel riepilogo del job;
  3. token OIDC con audience `pf-cert:<owner>/<repo>`;
  4. `python3 -E -s -m tools.pf_cert.request` compone la riga;
  5. pubblicazione della riga come commento della PR.

Formato della riga, una sola riga senza spazi interni ai campi:

```
/pf-cert v2 pr=<n> sha=<40 hex> run=<run_id>.<run_attempt> catalog=<x.y.z> scenarios=<s1,s2,...|-> oidc=<jwt>
```

## 3. Lato VPS: verifica della richiesta (`tools/pf_cert/request.py`, `verify_request`)

La VPS (pf-orch, Grok) applica **tutti** i controlli seguenti. Ogni controllo fallito rifiuta la richiesta con un codice
stabile, senza dati riservati, e la richiesta rifiutata non consuma il registro anti-replay.

| Controllo | Codice se fallisce |
|---|---|
| riga conforme alla regex | `MALFORMED` |
| JWT con `alg=RS256`, `kid` presente nel JWKS, firma PKCS#1 v1.5 SHA-256 valida, modulo di almeno 2048 bit | `TOKEN_ALG_NOT_ALLOWED`, `TOKEN_KEY_UNKNOWN`, `TOKEN_BAD_SIGNATURE` |
| `iss` = `https://token.actions.githubusercontent.com` | `TOKEN_WRONG_ISSUER` |
| `aud` = `pf-cert:<repo>` | `TOKEN_WRONG_AUDIENCE` |
| `exp`, `nbf`, `iat`: tolleranza 60 s, età massima 15 min | `TOKEN_EXPIRED` |
| `repository`, `repository_id` | `TOKEN_CLAIM_MISMATCH:<claim>` |
| `job_workflow_ref` = `<repo>/.github/workflows/pf-cert-request.yml@refs/heads/main` | `TOKEN_CLAIM_MISMATCH:job_workflow_ref` |
| `event_name` = `pull_request_target`, `ref` = `refs/heads/main` | `TOKEN_CLAIM_MISMATCH:…` |
| `run_id`, `run_attempt` uguali a quelli della riga | `TOKEN_CLAIM_MISMATCH:…` |
| `jti` e `run_id.run_attempt` mai visti (registro persistente) | `REPLAY` |
| API con l'installation token della app: run con `path` del workflow ed `event` trusted | `RUN_NOT_TRUSTED_WORKFLOW` |
| PR aperta | `PR_NOT_OPEN` |
| head della PR nello stesso repository | `FORK_PR_REFUSED` |
| `sha` = head attuale della PR | `SHA_NOT_PR_HEAD` |
| PR fra le `pull_requests` del run | `RUN_NOT_FOR_THIS_PR` |
| API non leggibile | `GITHUB_API_UNAVAILABLE` |

Gli **scenari si ricalcolano sulla VPS** con `tools/pf_cert/classify.py` preso da `main`, sui file cambiati letti
dall'API. La lista `scenarios=` della riga è informativa e non viene mai usata per ridurre il set.

Primitive crittografiche in Python puro, senza dipendenze (`tools/pf_cert/crypto_verify.py`): RS256 e ed25519
(RFC 8032). Test di interoperabilità con openssl in `tests/pf_cert`.

## 4. Selezione degli scenari (`tools/pf_cert/classify.py`)

Catalogo v`1.0.0` (catalogo v1 di pf-orch):

| Scenario | Obbligatorio |
|---|---|
| `sim-replay-microstake` | sì |
| `pytest-microstake-recovery` | sì |
| `pytest-reconciliation` | sì |
| `live-real-order` | no: sempre NOT_RUN, nessun ordine reale senza una campagna LIVE autorizzata dall'owner |

Regole fail-closed:
- **file neutri** (docs, markdown, test, `tools/`, guardrail, `.github/`, `scripts/`): nessuno scenario;
- **file di prodotto:** gli scenari della regola che li riguarda (ordini, SIM, microstake, PnL, money management,
  dutching, cashout, rischio, riconciliazione, recovery, database, client Betfair);
- **file di configurazione** (`config*.json`, `*trading_config*`, `core/config_registry*`): intero set obbligatorio;
- **file di prodotto senza regola**, percorso illeggibile o elenco assente: intero set obbligatorio.

`classifier_digest()` dà l'impronta del classificatore, da riportare nel check.

## 5. Risultati firmati e verdetto (`tools/pf_cert/result.py`)

Per ogni scenario l'orchestratore firma con ed25519 un manifest JSON. La chiave privata sta solo sulla VPS; la chiave
pubblica è pinnata in `ORCHESTRATOR_RESULT_KEY_SPKI_B64`. Campi obbligatori del manifest:

```json
{"repo": "<owner>/<repo>", "sha": "<40 hex>", "scenario": "<id del catalogo>", "catalog_version": "1.0.0",
 "result": "PASS|FAIL|BLOCKED|NOT_RUN", "issued_at": "<ISO 8601 con fuso>", "expires_at": "<ISO 8601 con fuso>",
 "dataset": {"signature": "VERIFIED", "...": "..."}}
```

`dataset.signature = VERIFIED` è richiesto per un PASS degli scenari su dati autentici (`sim-replay-microstake`).

L'output del check riporta una riga per scenario, con i **byte esatti firmati**, così la verifica non dipende da una
ri-serializzazione:

```
pf-cert-evidence <scenario> <base64 dei byte del manifest> <base64 della firma di 64 byte>
```

`certification_verdict` rende `CERTIFIED` **solo** se ogni scenario obbligatorio ha un manifest autentico, dello stesso
repository e head, nella finestra di validità, con `result == "PASS"`. Negli altri casi rende `NOT_CERTIFIED`, con il
motivo per scenario:
- prova mancante (`NOT_RUN`);
- firma errata;
- altro SHA o repository;
- versione del catalogo diversa;
- scaduto;
- dataset non verificato;
- FAIL, BLOCKED o NOT_RUN;
- etichetta della riga diversa dallo scenario firmato.

Se per lo stesso scenario ci sono due prove, prevale la peggiore.

Ogni nuovo push cambia l'head: le prove del vecchio SHA non valgono più (`SHA_MISMATCH`) e la certificazione riparte.

## 6. GitHub App «Pickfair Dedicated»

- **Permessi:** Checks read/write, Contents read, Pull requests read, Metadata read. Nessun permesso di scrittura sul
  codice, sulle PR o sulle protezioni.
- **Chiave privata:** solo sulla VPS, in `/etc/pf-orch/github_app/`. Non passa da agenti, Actions o commit e non
  viene mai stampata.
- **Check `pf-betfair-certification`:** pubblicato sull'head della PR. `success` solo con il verdetto `CERTIFIED`,
  altrimenti `failure` con i motivi. Un nome nuovo, che non riusa nessun check richiesto esistente.

Inserire il check in Merge readiness e nei required checks è una PR di governance separata, a merge owner. Le
protezioni GitHub non si modificano direttamente.

## 7. Cosa non fa

- Nessun ordine con denaro reale: `live-real-order` resta NOT_RUN.
- Nessuna credenziale Betfair su GitHub o sui runner. `getAccountFunds` passa solo dal gateway trusted della VPS.
- Nessun dato Betfair autentico su GitHub.
- Nessun runner pubblico privilegiato sulla VPS.
- Nessun codice arbitrario della PR eseguito con credenziali: gira nella sandbox `pf-sbx`, senza segreti.
- `/pf-cert` manuale dell'owner resta come fallback.
