# PF-DISPATCHER-01 — coda di lavoro della Roadmap V2 e worker di Claude B

Riferimenti:
- mandato owner PF-CLAUDE-AUTO-DEV-01;
- scheda Grok PF-DISPATCHER-01 (#586 c.6583997783);
- registrar VPS PF-REGISTRAR-ACTIVATION (#586 c.6589579350).

Codice:
- `scripts/roadmap_dispatcher.py` (coda e decisione);
- `scripts/claude_worker.py` (worker, **solo dry-run**).

Test:
- `tests/scripts/test_roadmap_dispatcher.py`;
- `tests/scripts/test_claude_worker.py`.

**Stato.** Il codice e i test offline sono completi, e c'è un dry-run contro GitHub reale. Non è installato nessun
workflow o timer, e il worker non può eseguire nulla. L'attivazione operativa richiede un GO separato dell'owner
(§6).

## 1. Cosa integra (nessuna seconda infrastruttura)

| Parte | Chi la gestisce | Uso nel dispatcher |
|---|---|---|
| Schede e label di stato (BACKLOG … VERIFIED, `roadmap-v2`) | registrar VPS (Grok) | fonte primaria delle schede |
| Registro su #586 (`<!-- pf-roadmap-registry:v1 -->` + JSON) | Claude B / owner | schede che non hanno ancora un'issue (F-xx della Roadmap V2) |
| Eventi di avanzamento (`<!-- pf-dispatch:v1 {...} -->` su #586) | dispatcher / worker | stato persistente delle lavorazioni |
| Certificazione | `pf-betfair-certification` (Pickfair Dedicated) | gate della lavorazione; LIVE neutral = LIVE_NOT_RUN, mai PASS |
| Merge | `pr_autonomy_policy.py merge-decision` oppure owner | il dispatcher non mergia; CI verde non equivale a merge autorizzato |
| Checkpoint multi-agente | `tools/agent_checkpoint/pfckpt.py` | invariato; il worker non lo sostituisce |

## 2. Formati

**Scheda in un'issue `roadmap-v2`.**
- L'id si prende da `<!-- pf-card:id=CB-586-FI-08B priority=P1 deps=F-05,F-07 order=3 -->` nel corpo, oppure dal titolo
  `[CB-586-FI-08B] …`.
- Lo stato è la label di stato.
- La priorità è la label `P0..P3` o il campo `priority` della marcatura.
- Due label di stato sulla stessa issue, o un id malformato, fanno fallire tutto in modo chiuso.

**Registro su #586.** È il commento più recente di un login fidato che contiene la marcatura, seguita da
` ```json {"cards": [{"id", "title", "priority", "status", "deps", "order", "issue"}]} ``` `. La label di un'issue
prevale sul registro per stato e priorità.

**Evento.** `<!-- pf-dispatch:v1 {"assignment":"F-13b#1","card":"F-13b","state":"PR_OPEN","pr":610,"sha":"…"} -->`.
Contano solo i login fidati, cioè l'owner per default, perché gli agenti pubblicano come owner (#472). Una marcatura
malformata da un login fidato fa fallire tutto in modo chiuso; quella di un altro login viene ignorata.

## 3. Decisione (`plan`)

Le regole si applicano in quest'ordine:

1. **Lavorazione attiva:** se l'ultimo evento non è VERIFIED né RELEASED, la decisione è `RESUME` di quella
   lavorazione. Una seconda assegnazione non è possibile. Due lavorazioni attive insieme fanno fallire tutto in modo
   chiuso.
2. **Una PR alla volta:** una PR `[TASK:` aperta, o una PR in `--blocking-pr` (oggi #602), che non è la PR registrata
   della lavorazione, dà `NONE ONE_PR_AT_A_TIME`.
3. **Dipendenze:** una scheda è candidata solo se è `READY_FOR_CLAUDE` e ha tutte le dipendenze MERGED o VERIFIED,
   sulla label o per un evento. Una dipendenza sconosciuta non è mai soddisfatta.
4. **Ordine:** P0 > P1 > P2 > P3, poi `order`, poi id. Il nuovo identificativo è `<card>#<n+1>`.

`MERGED` non chiude la lavorazione: si attende VERIFIED, cioè la verifica di main dopo il merge, prima della scheda
successiva. `fingerprint` rende confrontabili due giri: stessa impronta, stessa decisione.

**Transizioni ammesse:**
- ASSIGNED → PR_OPEN → CERTIFICATION_RUNNING → MERGED → VERIFIED;
- da ogni stato attivo si può andare a BLOCKED o OWNER_REQUIRED, e da questi si riprende;
- RELEASED vale solo prima del merge.

Qualsiasi altra transizione dà `TRANSITION_INVALID`, che fa fallire tutto in modo chiuso.

## 4. Lock e recovery

- **Lease globale:** è l'evento ASSIGNED su #586. Dopo un riavvio lo stato si ricostruisce solo da GitHub, quindi una
  scheda già assegnata viene ripresa e non riassegnata.
- **Lease locale del worker** (`LocalLease`):
  - file con `fcntl.flock`, campi `{assignment, holder, fencing, expires}`, permessi 0600;
  - un altro holder, o un'altra lavorazione, con lease valido produce `LeaseHeld`;
  - lo stesso holder che riparte rinnova il lease con lo stesso fencing;
  - un lease scaduto viene ripreso con fencing+1, e lo scrittore vecchio perde (`check`);
  - un file corrotto fa fallire tutto in modo chiuso.
- **Ripresa:** con `RESUME` il worker riusa il worktree (`git status`) e il prompt dice di ripartire dallo stato
  registrato, senza aprire una seconda PR.

## 5. Classificazione degli esiti (nessun errore diventa PASS)

`classify_check` e `pr_gate` distinguono questi esiti:

| Esito | Quando |
|---|---|
| PASS | il check è `success` |
| PENDING | il check non è concluso |
| LIVE_NOT_RUN | il check LIVE è neutral |
| OWNER_APPROVAL | `action_required`, oppure un gate owner |
| REVIEW_BLOCKERS | bloccanti di review aperti |
| INFRA_FAILURE | timeout, runner perso, rete, GraphQL i/o timeout |
| APP_FAILURE | gli altri fallimenti |
| MISSING_EVIDENCE | check richiesto assente (PRE-LIVE compreso), `skipped` non ammesso, stato sconosciuto |

L'esito complessivo è PASS solo se tutti i check sono PASS, ammettendo LIVE_NOT_RUN solo per il LIVE, e se ogni check
richiesto è presente.

## 6. Contratto con Grok (VPS)

```
Grok rileva → registrar deduplica (label/schede, digest su #586) → scheda READY_FOR_CLAUDE
→ dispatcher SELECT (evento ASSIGNED) → worker/Claude B sviluppa → PR (evento PR_OPEN)
→ CI + review (Sol/Grok/Codex) → pf-betfair-certification (evento CERTIFICATION_RUNNING)
→ merge consentito dai gate o dall'owner (evento MERGED) → verifica main + rerun VPS (evento VERIFIED) → prossima
```

Ogni transizione è un evento idempotente su #586.

**Ruoli:**
- Grok resta responsabile di:
  - label e schede;
  - certificazione;
  - prove full-cycle indipendenti;
  - VERIFIED, che richiede l'evidenza di main, della CI post-merge e del rerun VPS;
- il dispatcher non promuove mai da solo a VERIFIED.

**Formato delle review:** l'attestazione firmata è il formato canonico; vedi CB-602-REVIEW-FORMAT, #586
c.6589740443.

## 7. Worker di Claude B e piano di attivazione

`claude_worker.py` prende la decisione e produce un piano. In dry-run fa queste cose:
- acquisisce il lease;
- calcola worktree `<workdir>/<card>` e branch `auto/<card>`;
- prepara il prompt, che rimanda a CLAUDE.md, AGENTS.md e alla spec: RED prima, una PR, budget di 10 cicli, condizioni
  di arresto;
- prepara i comandi `git worktree add …` e `claude -p <prompt> --output-format json`;
- scrive `plan.json` e `audit.jsonl`.

In dry-run **non esegue nessun comando**. Piano e audit con un segreto riconoscibile fanno fallire tutto in modo
chiuso. `--activate` è sempre rifiutato (`OWNER_GO_REQUIRED`).

**Prima del GO di attivazione servono:**
1. **Host e utente:** VPS esistente, utente di servizio dedicato senza accesso a `/etc/pf-orch` né ai segreti Betfair.
   Worktree sotto una directory sua.
2. **Credenziali Claude:** login di `claude` fatto dall'owner sull'host. Niente token nel repository, nei log o nelle
   issue. Il worker non le legge.
3. **Credenziali GitHub:** token a privilegi minimi per Claude B, con push solo sui branch `auto/*`. Merge solo dai
   gate.
4. **Consumo:** limite di sessioni al giorno e di turni per sessione (`--max-turns`); arresto su crediti esauriti.
5. **Isolamento e audit:** audit append-only verificato e un kill switch, che è un file di stop.
6. **Collaudo:** una scheda P2 a basso rischio eseguita in modalità presidiata.
7. **Attivazione:** un timer (cron o systemd) che chiama `roadmap_dispatcher.py plan --live` e poi il worker.
   `PF_DISPATCHER_LIVE=1` va impostato dall'owner.

## 8. Cosa non fa

- Non apre, modifica o mergia PR.
- Non scrive label, contenuti o protezioni. L'unica scrittura ammessa, con `--live` e `PF_DISPATCHER_LIVE=1`, è un
  commento evento su #586, e il client rifiuta il resto prima della richiesta.
- Non avvia lavori fuori dalle schede READY_FOR_CLAUDE.
- Non esegue ordini con denaro reale.
- Non tocca MatchPilot.
- Non cambia la visibilità del repository.
