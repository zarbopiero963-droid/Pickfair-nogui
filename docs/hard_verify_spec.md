# Hard Verify — Specifica di verifica implementazione

> Processo obbligatorio per dichiarare che un task/PR è tecnicamente implementato.
> Vale per qualsiasi agente (Claude, Codex, o altri) che lavora su questo repo.
> Complementare a `docs/auto_pr_flow_spec.md` (che governa il flusso PR);
> questa spec governa il giudizio finale di implementazione.

## La formula

```
IMPLEMENTED = codice presente
            + test presenti
            + test passano
            + comportamento coperto (PASS e BLOCK)
            + current-head corretto
            + nessuno scope vietato
            + integrazione reale nel flusso
            + docs allineate al cambiamento (§12-bis)
```

Se manca uno di questi, NON è fully implemented.
"La PR è merged" o "vedo qualche grep" NON sono prove.

---

## 1. Prima definisci il contratto della PR

Prima devi sapere cosa doveva implementare. Per ogni PR scrivi:

```
TASK name:        [TASK: ...]
Obiettivo:        cosa deve fare
File autoritativi: dove deve vivere la logica
File test attesi:  dove devono vivere i test
Comportamenti PASS: casi validi che devono funzionare
Comportamenti BLOCK: casi invalidi che devono bloccare
Scope vietato:     file che non dovevano essere toccati
```

Esempio (PR8E):

```
TASK: [TASK: claude_bug_pr8e_required_check_evidence_integration]

Obiettivo:
Merge readiness deve capire se DeepSource Python è required o advisory.

PASS:
branch protection absent + DeepSource non-required + Codacy success
+ review attive 0 => non blocca

BLOCK:
required-check evidence mancante/malformata
DeepSource required failing
Codacy failure
review attive > 0
head mismatch
=> blocca
```

Senza contratto non puoi dire "implemented". Puoi solo dire "qualcosa esiste".

---

## 2. Verifica current-head e repo pulito

Prima di ogni audit:

```bash
git fetch origin main --quiet
echo "origin/main=$(git rev-parse origin/main)"
echo "local_branch=$(git rev-parse --abbrev-ref HEAD)"
echo "local_head=$(git rev-parse HEAD)"
git status --short
```

Devi vedere: origin/main = SHA aggiornato, worktree = vuoto.
Se il worktree è sporco, l'audit non è pulito.

---

## 3. Verifica stato PR GitHub

```bash
gh pr view <PR> --json number,state,mergedAt,mergeCommit,headRefName,baseRefName,title,url
```

Attenzione: **PR merged ≠ feature fully implemented**.
Il merge dice solo che è entrata in main. Non prova che il comportamento sia completo.

---

## 4. Hard static audit: codice + test

Cerca nomi tecnici precisi, non parole generiche.

Esempio buono:

```bash
git grep -nEi "def (automation_ledger_path|append_automation_ledger_event|read_automation_ledger_events)|automation-ledger\.jsonl|latest\.json|summary\.md" origin/main -- scripts tests
```

Esempio debole (vietato come unica prova):

```bash
git grep -n "ledger" origin/main
```

Criterio minimo: `code_hits > 0` E `test_hits > 0` — ma non basta il numero:
devi anche LEGGERE gli hit.

---

## 5. Verifica che gli hit siano nei file giusti

Classifica ogni hit:

- **AUTHORITATIVE** = file che il flusso usa davvero
- **NON AUTHORITATIVE** = docs, commenti, vecchi script, test fake, helper scollegati

Una PR è fully implemented solo se la logica è nei moduli autoritativi.

---

## 6. Verifica test positivi e negativi

Per dire "matematicamente coperto", non basta un test happy path. Servono:

- **PASS tests**: caso valido funziona
- **BLOCK tests**: caso invalido blocca
- **MALFORMED tests**: input malformato blocca fail-closed
- **EDGE tests**: provider mancante, stato sconosciuto, head mismatch,
  file vietato, branch protection ambiguo

Per i task safety-critical, i test BLOCK sono i più importanti.
Se ci sono solo test PASS, la PR è PARTIAL, non fully implemented.

---

## 7. Py compile

Sempre:

```bash
python3 -m py_compile <file_autoritativi_toccati>
```

Se fallisce, la PR non è implementata tecnicamente.

---

## 8. Pytest mirato

Test mirato sul task con selettori precisi:

```bash
python3 -m pytest -q <test_files> -k "<selettori_del_task>"
```

Criterio: targeted pytest = PASS. Se i test non passano, non è implemented.

---

## 9. Verifica integrazione reale nel flusso (wiring)

Il punto che frega di più (caso storico: PR256 — policy DeepSource advisory
merged, ma readiness/guardrails non ricevevano ancora l'evidence corretta).

Per ogni feature del flusso devi verificare la catena completa:

1. input letto
2. evidence prodotta
3. evidence passata alla decisione
4. decisione finale che cambia correttamente
5. test di wiring presente

Domanda obbligatoria: *la logica è solo implementata, o è anche collegata
al punto decisionale finale?*

Se manca il wiring: `STATUS = PARTIAL` oppure `IMPLEMENTED_WITH_MISSING_INTEGRATION`.

---

## 10. Verifica check PR current-head

Per PR aperte o appena mergiate, leggi su current head:

- bad / pending / skipped
- headRefOid
- Codacy conclusion + annotations_count
- active review comments / unresolved_active
- DeepSource status
- Merge readiness / PR flow guardrails

Regole:

- Codacy success + annotations 0 = bene
- unresolved_active 0 = bene
- DeepSource failure = blocca solo se required/current-head/blocking
- Merge readiness / guardrails failure = wiring o gate non verde

Se una PR è merged ma i guardrail sono failure, distingui:
codice merged = sì; flusso fully automatic = no.

---

## 11. Verifica scope e file vietati

```bash
git diff --name-only origin/main...HEAD
# oppure: gh pr diff <PR> --name-only
```

Controlli: solo file allowed? workflow toccati? secrets? runtime trading?
core business non previsto? live automation introdotta? auto-merge introdotto?

Se una PR tocca file vietati senza autorizzazione:
`STATUS = NEEDS_MANUAL / NOT ACCEPTABLE` — anche se i test passano.

---

## 12. Verifica comportamento fail-closed

Non basta che "funzioni quando tutto è bello". Deve bloccare quando manca evidenza:

- env mancante => disabled/block
- mode sconosciuto => disabled/block
- flag malformed => false/block
- head SHA mancante => block
- branch protection ambiguous => block
- required checks missing => block
- DeepSource required failing => block
- Codacy annotations current-head > 0 => block
- review active > 0 => block
- file forbidden touched => block
- workflow edit non autorizzato => block

Se una feature "passa" anche quando l'evidenza manca, è pericolosa
e non fully implemented.

---

## 12-bis. Verifica docs allineate al cambiamento

Ogni aggiunta/modifica/rimozione di codice (funzione, classe, modulo,
comportamento, chiave config, gate, contratto, voce di roadmap) deve
avere la documentazione corrispondente aggiornata nello STESSO PR
(README, docs/ di dominio, ops/, docstring). Il report finale DEVE
includere il campo:

```
docs aggiornate per il cambiamento: PASS / FAIL / N/A
```

Regole di classificazione:

- **PASS**: le docs pertinenti al cambiamento sono aggiornate entro lo
  scope della PR.
- **N/A**: cambiamento interno senza impatto su comportamento/API/contratto
  documentato — vale solo se scritto come nota esplicita.
- **FAIL**: codice cambiato, doc pertinente esistente e dentro
  files_allowed NON aggiornata => la PR è al più `PARTIAL` o
  `IMPLEMENTED_WITH_NOTE`, mai `FULLY_IMPLEMENTED`.

Vincolo di scope (coerente con §11 e con files_allowed): se la doc da
aggiornare è FUORI da files_allowed, NON allargare lo scope. In quel
caso il campo è `FAIL` con causa "doc fuori allowlist" e lo stato è
`NEEDS_MANUAL` (serve estensione esplicita dell'allowlist), non un commit
fuori scope. La doc-update obbligatoria vale solo entro files_allowed.

---

## 13. Classificazione finale

Etichette obbligatorie nel report finale:

| Etichetta | Significato |
|---|---|
| `MISSING` | nessun codice autoritativo e nessun test |
| `PARTIAL` | codice presente ma test mancanti, o test che falliscono, o wiring assente |
| `IMPLEMENTED_WITH_NOTE` | codice + test passano, con nota di layout/integrazione/scope futuro |
| `FULLY_IMPLEMENTED` | codice autoritativo + test PASS e BLOCK + validation pass + wiring finale + current-head evidence coerente |
| `MERGED_BUT_NOT_FULLY_AUTOMATED` | PR merged, ma readiness/guardrails/flusso non replicano ancora il comportamento automaticamente |

Esempi storici:

- PR256 = MERGED; policy DeepSource advisory = implemented;
  flusso automatico readiness/guardrails = not fully implemented
- PR5A ledger = FULLY_IMPLEMENTED (code_hits=32, test_hits=225,
  pytest=43 passed, layout canonico provato)

---

## 14. Template comando hard audit riutilizzabile

```bash
set -euo pipefail

echo "=== HARD VERIFY ==="
git fetch origin main --quiet

echo "== HEADS =="
echo "origin/main=$(git rev-parse origin/main)"
echo "branch=$(git rev-parse --abbrev-ref HEAD)"
echo "head=$(git rev-parse HEAD)"

echo "== WORKTREE =="
git status --short

CODE="<REGEX_CODICE_PRECISA>"
TEST="<REGEX_TEST_PRECISA>"

echo "== STATIC AUDIT =="
C=$(git grep -nEi "$CODE" origin/main -- scripts .github tests 2>/dev/null | wc -l | tr -d " ")
T=$(git grep -nEi "$TEST" origin/main -- tests scripts 2>/dev/null | wc -l | tr -d " ")

if [ "$C" -gt 0 ] && [ "$T" -gt 0 ]; then S=IMPLEMENTED_STATIC
elif [ "$C" -gt 0 ]; then S=PARTIAL_CODE_ONLY
else S=MISSING
fi
echo "code_hits=$C test_hits=$T static_status=$S"

echo "--- code hits ---"
git grep -nEi "$CODE" origin/main -- scripts .github tests 2>/dev/null | sed -n "1,120p" || true
echo "--- test hits ---"
git grep -nEi "$TEST" origin/main -- tests scripts 2>/dev/null | sed -n "1,160p" || true

echo "== PY_COMPILE =="
python3 -m py_compile <file_autoritativi>

echo "== TARGETED PYTEST =="
python3 -m pytest -q <test_files> -k "<selettori>"

echo "== FINAL =="
echo "STATIC_STATUS=$S"
```

---

## 15. Regola d'oro

Una cosa è tecnicamente implementata solo quando puoi mostrare:

1. SHA esatto di origin/main
2. file autoritativi
3. funzioni/contratti presenti
4. test mirati presenti
5. test PASS
6. casi negativi fail-closed coperti
7. wiring nel flusso finale
8. nessun file vietato
9. check/review current-head coerenti

- Solo 1–3: **non basta**
- 1–6 ma manca 7: **implementata nel codice, non nel flusso finale**
- 1–9: **fully implemented**
