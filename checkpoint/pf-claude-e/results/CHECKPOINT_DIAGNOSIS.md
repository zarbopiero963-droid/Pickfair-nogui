# Diagnosi catene di checkpoint B e D — Claude E, 08/11/2026

Strumento: `tools/agent_checkpoint/pfckpt.py` di main `a09d8d6`, da clone pulito. Correzione candidata: `pfckpt-squash-durable` @ `5c52cd9f` (Claude B).
Nessuna scrittura sui branch di stato altrui; prove solo in locale (clone e mirror nello scratchpad).

## Rettifica della segnalazione precedente
La prima segnalazione (`LATEST_CHECKPOINT_CORRUPT` su B e D) nasceva da un **errore di invocazione di Claude E**: `bootstrap`
senza `--dir`, quindi con la directory di default `checkpoint/pf-full-delivery`, che non è quella di B né di D.
- Su `agent-state/pf-claude-d` quella directory non esiste → `FILE_REQUIRED_MISSING` (artefatto).
- Su `agent-state/pf-claude-b` esiste una copia ereditata di `pf-full-delivery` con seq 99 ripetuto → errore di una
  directory che non è la catena di B (artefatto).

## Claude D — catena SANA
`bootstrap --branch agent-state/pf-claude-d --dir checkpoint/pf-claude-d` → `READY_READ_ONLY`, seq 7, commit `73b2d69c`,
lease di claude-d. Nessun problema.

## Claude B — dati INTATTI, difetto dello STRUMENTO su main
Catena `agent-state/pf-claude-b`, dir `checkpoint/pf-claude-b-f33`, 23 checkpoint (genesi `2bde7fd6` seq 1 … `0e329b8a` seq 23).

| Condizione del clone | Esito di `verify` per commit |
|---|---|
| solo `refs/heads/*` (come il readback del main) | VALID: seq 23 e seq 20; ERROR "SHA del codice inesistente" / "test sha unresolved": seq 1–19, 21, 22 |
| più `refs/pull/*/head` | **VALID 23/23** (seq 1 → 23) |

- **Prima sequenza valida:** seq 1 (`2bde7fd6`). Con i ref completi, tutte le sequenze sono valide; nessuna sequenza è
  realmente invalida.
- **Prima sequenza non verificabile con lo strumento di main:** seq 1 (code.sha `3873eaac…`, ora solo in una PR).
- **Errore riproducibile:** `pfckpt.py bootstrap --branch agent-state/pf-claude-b --dir checkpoint/pf-claude-b-f33` quando
  l'ultimo checkpoint punta a uno SHA di una PR mergiata con squash e branch cancellato. Esempi: seq 22 → `6fdb6398`
  (= `refs/pull/610/head`), seq 21 → `7fae18d4`, seq 19 → `1b2b5024` (= `refs/pull/609/head`), seq 17 → `aa184a28`
  (= `refs/pull/596/head`), seq 16 → `84fd4f7b` (= `refs/pull/593/head`).
- **Causa:** `verify`/`readback`/`bootstrap` di main risolvono `code.sha` e gli SHA dei test solo in `refs/heads/*`.
  Dopo il merge squash e la cancellazione del branch GitHub conserva il commit solo in `refs/pull/<N>/head`.
- **Stato attuale:** seq 23 (`0e329b8a`, code.sha `34bbf01f` su `claude/d-g7-r06-settled-ambiguous` e PR #614 aperta)
  → `READY_READ_ONLY`. **Tornerà `LATEST_CHECKPOINT_CORRUPT` appena #614 sarà mergiata con squash e il branch cancellato.**

### Impatto sul recovery
- `bootstrap` di main su una catena in quello stato → `LATEST_CHECKPOINT_CORRUPT` falso: l'agente si ferma (fail-closed, nessuna perdita).
- `recover` di main indicherebbe come `last_valid_checkpoint` un checkpoint VECCHIO (es. seq 20 `d85cf314` invece di
  seq 22): se qualcuno ripartisse da lì senza leggere questa diagnosi, perderebbe il lavoro di seq 21–22. È il rischio vero.
- I dati e la storia del branch non sono alterati: nessuna perdita.

### Prova della correzione (solo locale)
Mirror locale di GitHub (con `refs/pull/*`), `agent-state/pf-claude-b` spostato SOLO nel mirror su seq 22 `be16bbbd`:
- `pfckpt.py` di main → `LATEST_CHECKPOINT_CORRUPT` (SHA `6fdb6398` inesistente);
- `pfckpt.py` di `pfckpt-squash-durable` @ `5c52cd9f` → `READY_READ_ONLY`, seq 22, `code_reference.original_reachable: true`
  (fallback `fetch_pull_heads`).
Il remoto non è stato toccato.

### Procedura di riparazione NON distruttiva (proposta)
1. Nessun force-push, nessuna riscrittura, nessuna eliminazione di checkpoint: la catena è valida, non va "riparata".
2. Integrare la correzione `pfckpt-squash-durable` (già nella coda di Claude B: F-R06/#614 → pfckpt → BB-1). È un file
   `tools/` (non `scripts/pr_*`): merge secondo i gate vigenti.
3. **Prima del merge squash di #614**: annotare nel checkpoint successivo di B che code.sha `34bbf01f` sarà
   raggiungibile solo via `refs/pull/614/head`, oppure aspettare la correzione pfckpt prima di cancellare il branch.
4. Fino ad allora, per riprendere B: `git fetch origin '+refs/pull/*/head:refs/remotes/pull/*'` nel clone e `verify`
   commit per commit; NON usare `recover` di main per scegliere il punto di ripartenza.
5. Dopo l'integrazione: rieseguire `bootstrap` da clone pulito su B, D, E e pf-full-delivery e registrare l'esito.

## Altre catene
- `agent-state/pf-full-delivery` (default dir): `READY`, seq 102.
- `agent-state/pf-claude-e`: `READY` seq 1 (code.sha su main, quindi durevole).
