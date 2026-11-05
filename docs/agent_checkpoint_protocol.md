# Protocollo di checkpoint multi-agente — `pf-checkpoint/3`

Mandato owner PF-UNIVERSAL-CHECKPOINT-V1 (15/10/2026). Strumento: `tools/agent_checkpoint/pfckpt.py`
(solo Python standard library + git). Test: `tests/agent_checkpoint/` (git reale, offline; ref keep e squash merge in `test_pfckpt_squash_durable.py`).

## 1. Scopo e fonte di verità

Un lavoro lungo deve poter riprendere da **qualunque** agente (Claude, ChatGPT Work, Grok, ...) in un ambiente
nuovo, senza la cronologia della chat e senza file locali. L'unica fonte di verità è un **branch di stato su
GitHub** (per Pickfair: `agent-state/pf-full-delivery`), mai la memoria di una sessione, mai un Artifact privato,
mai un file nello scratchpad. Il protocollo non dipende dal fornitore dell'agente: serve solo git e Python 3.

Un checkpoint è **un commit** (atomico) che contiene la directory di stato (`checkpoint/pf-full-delivery/`):

| File | Contenuto |
| --- | --- |
| `STATE.json` | stato strutturato, schema versionato (§2) |
| `MANIFEST.sha256` | SHA-256 di ogni altro file della directory (`<hash>  ./<percorso>`) |
| `HANDOFF.md` | sintesi umana per chi riprende |
| `RUNBOOK.md` | comandi di ripresa, ambiente, test |
| `results/`, `tools/`, ... | artefatti e prove |

## 2. Schema `STATE.json`

Campi obbligatori (lo strumento rifiuta il checkpoint se manca uno o il tipo è errato; campi in più sono ammessi):

`schema` (`"pf-checkpoint/3"`), `repository`, `seq` (intero, +1 a ogni checkpoint), `mandate_id`,
`previous_checkpoint_sha` (= primo parent del commit), `code` (`{branch, sha}` con SHA completo di 40 caratteri,
che deve esistere nel repository), `lotto_corrente`, `lotti_completati`, `next_atomic_action`, `dependencies`,
`blocks`, `owner_authorizations`, `tests_executed`, `tests_missing`, `missing_historical_evidence`, `gate_a`,
`pr_505`, `certification_verdict` (`PASS` / `PARTIAL` / `NO-GO` / `BLOCKED` / `NOT VERIFIED`).

Ogni voce di `tests_executed` ha `status` ∈ `PASS` / `FAIL` / `BLOCKED` / `NOT VERIFIED` / `MISSING`. `PASS` e `FAIL`
richiedono `sha` (il commit di codice su cui il test è stato eseguito): un risultato senza versione non vale, e
`verify` lo risolve con la stessa lookup di `code.sha` (`git cat-file -e <sha>^{commit}`; irrisolto ⇒ problema
`test sha unresolved`, checkpoint invalido — ciclo 8, rilievo Codex su 034af59). Se
una voce dichiara `artifact`, il file deve essere nel commit.

## 3. Verifica (`verify`)

`pfckpt.py verify --repo R --ref REF` legge **dal tree committato** (mai dal disco) e controlla: STATE e MANIFEST
presenti; JSON valido e schema rispettato; manifest ↔ commit completo in entrambe le direzioni; hash di ogni file;
assenza di pattern di segreti; `previous_checkpoint_sha` = parent; `seq` = seq del parent + 1; SHA del codice
esistente (o, se non più raggiungibile, transizione `integration` TREE_EQUAL provata, §6-ter); artefatti dichiarati
presenti. Qualsiasi problema → exit 2 con l'elenco dei problemi.

## 4. Salvataggio atomico (`save`)

```
pfckpt.py save --repo WORKTREE --branch agent-state/pf-full-delivery --expected <SHA remoto di partenza> \
    --message "..." --code-branch <branch di codice>
```

Ordine: 1) legge il remoto (`ls-remote`); 2) confronta con `--expected` (concorrenza ottimistica); 3) il worktree
deve essere su `expected`; 4) rigenera il MANIFEST; 5) `git add -A -f` (un `.gitignore` non può più escludere un
artefatto in silenzio); 6) `previous_checkpoint_sha` deve essere `expected`; 7) un solo commit; 8) `verify` sul
commit; 9) nessun file solo-locale nella directory (`LOCAL_ONLY_FILES`); 10) push **senza force**; 11) readback:
clone pulito dal remoto e `verify` completo lì; 12) SHA remoto = SHA locale. Solo allora stampa
`"status": "DURABLE"`. Prima di quel momento il checkpoint **non** è salvato.

Ripresa dopo un crash: si riesegue lo stesso comando con lo stesso `--expected`.

| Crash | Comportamento alla ripresa |
| --- | --- |
| prima del commit | salvataggio normale |
| dopo il commit, prima del push | riusa il commit (`resumed: after_commit`), nessun duplicato |
| durante il push | se il push non è arrivato: come sopra; se è arrivato: solo readback |
| dopo il push, prima del readback | solo readback (`resumed: after_push`) |

Exit: 0 DURABLE, 2 checkpoint non valido (nulla pubblicato), 3 conflitto di concorrenza, 4 readback fallito,
5 errore git/ambiente.

## 5. Concorrenza

Nessun lock, nessun force-push, nessun merge automatico di stato. Se il remoto non è più `expected`
(`CONCURRENT_UPDATE`) o il push non è fast-forward (`PUSH_REJECTED`), l'agente si ferma, legge il nuovo
checkpoint, **riconcilia esplicitamente** (riparte dal nuovo SHA e riapplica i propri risultati senza perdere quelli
dell'altro agente) e risalva con il nuovo `expected`. Un lotto non si chiude con risultati ottenuti su un `code.sha`
diverso da quello dichiarato.

## 6. Ripresa da zero (`bootstrap`)

```
pfckpt.py bootstrap --url <repo> --branch agent-state/pf-full-delivery --dest <dir> --code-branch <branch>
```

Clona, verifica e restituisce `READY` con `next_atomic_action`, `owner_authorizations`, `blocks`, `code_sha`.
Se l'ultimo checkpoint è corrotto: `LATEST_CHECKPOINT_CORRUPT` con `last_valid_checkpoint` (ultimo commit della
catena che passa `verify`). **Nessun rollback automatico**: il remoto non viene toccato e decide l'owner o una
riconciliazione esplicita.

## 6-bis. Continuità multi-agente (PF-AGENT-CONTINUITY-01, #461 c.6225689722)

Ogni agente (Claude Code, Codex, ChatGPT Work, Grok, futuri) è un implementatore intercambiabile: stesso ruolo, stesse
procedure, nessuna dipendenza dalla cronologia della chat. Lo strumento aggiunge al checkpoint:

| File | Contenuto | Verifica |
| --- | --- | --- |
| `CURRENT.json` | puntatore minimo: seq, SHA precedente, `code.{branch,sha}`, mandato, prossima azione atomica, verdetto | deve coincidere con STATE.json, altrimenti checkpoint non valido |
| `TASK_STATE.json` | mandato, lotto corrente, lotti completati, prossima azione, blocchi, dipendenze, test mancanti, Gate A, PR #505 | idem |
| `AGENT_LEASE.json` | `owner`, `fencing_token` (= seq del checkpoint che lo ha preso/rinnovato), `acquired_at`, `expires_at` | token = seq, timestamp validi |
| `code_patch/` | `BASELINE.json` (head/tree del codice, file modificati e nuovi), `tracked.patch`, `untracked/<file>` (un symlink non tracciato NON viene mai seguito: se ne registra solo il testo del link in `BASELINE.json` `skipped_symlinks`, e il ripristino ricrea il link; nessun contenuto di file fuori dal repo finisce nel checkpoint pubblicato) | ogni file dichiarato presente nel commit |

**Lease e fencing.** `save --agent <nome>` è obbligatorio: il lease si legge dal checkpoint **remoto atteso**
(`--expected`), mai dal disco. Tenuto da un altro agente e non scaduto ⇒ `LEASE_HELD` (exit 6), nessuna scrittura.
Scaduto o rilasciato (`--release`) ⇒ chiunque può subentrare; ogni salvataggio rinnova il lease del suo autore con
`--ttl-min` (default 120). Un agente stale (base vecchia) è fermato prima dal controllo ottimistico
(`CONCURRENT_UPDATE`), e sulla base nuova dal lease del nuovo proprietario. Un solo agente scrive per volta; gli altri
leggono (`bootstrap` senza il proprio agente o con lease altrui ⇒ `READY_READ_ONLY`).

**Patch del codice.** `save --code-repo <repo>` cattura il lavoro non committato (diff tracciato + file nuovi, ignorati
esclusi) e lo registra in `STATE.code.patch`; `restore-code --code-repo <clone pulito su code.sha>` lo riapplica e
verifica che lo stato risultante coincida con quello dichiarato (repo sporco o baseline diversa ⇒ rifiuto).

**Divergenza di baseline.** `bootstrap --code-branch` confronta il head del branch di codice con `code.sha`:
`ALIGNED` oppure `DIVERGED` con ahead/behind — un lotto non si completa con risultati di un'altra versione.

**Procedure universali**
- **START / RESUME**: `bootstrap --agent <nome> --code-branch <branch>` ⇒ `READY` (lease libero o proprio) con
  `next_atomic_action`, autorizzazioni, blocchi, divergenza; `READY_READ_ONLY` ⇒ non scrivere;
  `LATEST_CHECKPOINT_CORRUPT` ⇒ fermarsi e decidere (indica `last_valid_checkpoint`).
- **HANDOFF**: ultimo `save --agent <nome> --release` con `next_atomic_action`, test eseguiti/mancanti, finding e,
  se c'è lavoro non committato, `--code-repo`. Il subentrante fa START.
- **RECOVER**: `recover --dest <dir>` materializza l'ultimo checkpoint valido della catena senza toccare il remoto;
  la decisione di ripartire da lì è esplicita (nuovo checkpoint con nuova identità, mai rollback del branch).
- **Crash / limite d'uso**: rieseguire `save` con lo stesso `--expected` e `--agent`: riprende da commit o push già
  avvenuti; un lease scaduto permette a un altro agente di subentrare senza perdere nulla.

Sincronizzazione reale: solo dopo pubblicazione sul branch condiviso autorizzato e ripresa verificata da un secondo
ambiente/agente (`bootstrap` in un clone nuovo). Un archivio locale non è un handoff sincronizzato.

## 6-ter. SHA durevoli dopo squash merge (`pfckpt_squash_durable_refs`)

**Incidente.** `agent-state/pf-claude-b` seq 17 citava in `code.sha` e `tests_executed[].sha` l'head di una PR
integrata con squash merge. Cancellato il branch, quello SHA non era più raggiungibile da nessun ref remoto: un clone
nuovo non aveva l'oggetto, quindi `verify` lo dava inesistente e `bootstrap` rispondeva `LATEST_CHECKPOINT_CORRUPT`,
pur con il checkpoint integro.

**1. Ref keep.** A ogni `save`, per ogni SHA di codice citato e risolvibile (`code.sha`, gli `sha` dei
`tests_executed` PASS/FAIL non dichiarati `unpublished_tree`, i commit di integrazione), lo strumento pubblica
`refs/agent-state/keep/<sha completo>` → `<sha>`. Avviene **nello stesso push atomico** del checkpoint
(`git push --atomic`), **senza force**: un ref keep già presente sullo stesso SHA è un no-op, uno che punta altrove
fa rifiutare tutto il push, branch di stato compreso. Il namespace sta fuori da `refs/heads` e `refs/tags`, quindi
non fa partire workflow `on: push` e non dipende dal branch della PR. Il readback scarica `refs/agent-state/keep/*`
e richiede, per ogni SHA citato, il ref keep sul remoto e puntato a quello SHA (`READBACK_KEEP_REF_MISSING`
altrimenti, mai DURABLE). `bootstrap` e `recover` scaricano i ref keep prima di verificare. Nella ripresa
`after_push` (crash fra il push del branch e quello dei ref, o un checkpoint pubblicato dallo strumento precedente)
i ref mancanti vengono pubblicati senza duplicare il commit. Il `save` registra anche `code.tree` (il tree di
`code.sha`) finché l'oggetto esiste.

**2. Transizione verso main.** Campo opzionale `integration` in `code` e in ogni voce di `tests_executed`:

```json
{"original_sha": "<40 hex>", "original_tree": "<40 hex>", "integration_sha": "<40 hex>",
 "integration_tree": "<40 hex>", "equivalence": "TREE_EQUAL" | "NOT_PROVEN"}
```

Regole di `verify`, confrontando solo tree SHA e mai file per file:
- `original_sha` deve iniziare con lo SHA citato; tutti gli SHA sono id completi, mai riferimenti mobili. Il commit
  di integrazione è quello specifico, mai «l'ultimo main».
- Se l'originale si risolve: tree(`original_sha`) == `original_tree`.
- Se `integration_sha` si risolve: tree(`integration_sha`) == `integration_tree`.
- Originale **non** risolvibile: accettato solo con `equivalence: "TREE_EQUAL"`, `integration_sha` risolto e
  tree(`integration_sha`) == `integration_tree` == `original_tree`. Con `NOT_PROVEN`, o con un tree diverso anche
  per un solo file, il riferimento resta non provato.
- `original_tree` deve coincidere con i tree già registrati per quello SHA (`code.tree`, `code.patch.tree`).
- La dichiarazione non è mai creduta: `verify` ricalcola sempre i tree.

`pfckpt.py integrate --repo R --original <sha> --integration <sha> [--original-tree <tree registrato>]` calcola il
record dagli oggetti git (exit 0 = `TREE_EQUAL`, 2 = `NOT_PROVEN`). Se l'oggetto originale manca e non si passa un
tree registrato, l'esito è `ORIGINAL_TREE_UNKNOWN` (BLOCKED). Lo STATE conserva gli SHA originali: le autorizzazioni
owner e i risultati legati a quegli SHA restano invariati. La transizione entra con un **nuovo** checkpoint
(seq + 1), mai riscrivendo quello vecchio.

**3. Fallback `refs/pull/<N>/head`.** GitHub conserva `refs/pull/<N>/head` anche dopo la cancellazione del branch.
Se, dopo i ref keep, uno SHA citato resta irrisolto, `bootstrap`, `recover` e il readback scaricano i
`refs/pull/<N>/head` dichiarati nello STATE (`code.pr` / `tests_executed[].pr`, interi), oppure tutti i
`refs/pull/*/head` se non ne è dichiarato nessuno. Non serve configurazione per agente o per catena (B, C, D). Il
fallback fa solo tornare risolvibile l'oggetto e non sostituisce alcun controllo.

**4. Esiti.** In `verify`, i problemi di riferimento (SHA irraggiungibile, transizione non provata o malformata,
tree in contrasto) sono separati da quelli di integrità. Se un checkpoint ha **solo** problemi di riferimento,
`bootstrap` dà `NEEDS_RECONCILIATION` (exit 2, `last_valid_checkpoint`, nessun rollback); con qualunque problema di
integrità dà `LATEST_CHECKPOINT_CORRUPT`. Mai `READY` in nessuno dei due casi. `bootstrap` riporta
`code_reference` (`via`: `ORIGINAL` | `INTEGRATION_TREE_EQUAL`, `effective_sha`), `keep_refs` (presenti/mancanti)
e `pull_refs_fetched`.

**Riparazione di seq 17 (procedura).** Dopo `bootstrap` si ottiene `NEEDS_RECONCILIATION`. Si scarica
`refs/pull/<N>/head` della PR, poi `integrate --original <aa184a2…> --integration <SHA dello squash su main>` (dall'oggetto:
`original_tree_source: OBJECT`). Quindi un nuovo checkpoint seq 18 con `code.integration` e
`tests_executed[].integration` e `save`: il `save` pubblica i ref keep dell'originale e del commit di integrazione.

**Operazioni e duplicati.** Lo strumento registra solo checkpoint (un commit per `save`) e ref keep (uno per SHA, per
nome). Ripetere lo stesso `save` dopo DURABLE è idempotente (`resumed: after_push`, nessun commit e nessun ref
nuovo). Uno scrittore su base vecchia riceve `CONCURRENT_UPDATE`. PR, merge, ordini e altre operazioni sensibili
**non sono modellati** dallo strumento: `lotti_completati` è testo dichiarato, senza registro di operazioni né
deduplica. Il ripristino non può quindi garantire «nessuna PR o merge duplicato»: è un limite dichiarato (§8).

## 7. Incidente storico INC-b45f528

Il checkpoint seq 7 (`b45f528`) elencava nel MANIFEST tre log mai committati
(`results/mutation_c03_b33b54d.log`, `results/mutation_d04_180f405.log`, `results/mutation_d04_180f405_partial.log`).
Causa: `.gitignore` (`*.log`) + manifest calcolato dal disco + `git add` senza `-f` + readback limitato a tre file
(vecchio `save_checkpoint.sh`, ritirato). Gli originali sono stati ritrovati nella stessa sessione con SHA-256
identici al manifest e committati nel checkpoint seq 8 (`1297f7f`); `b45f528` resta nella storia invariato.
`pfckpt.py verify` su `b45f528` rileva il difetto (prova in `results/incident_b45f528_verify.json`).

## 8. Limiti dichiarati

- Provato con git reale offline (repository bare locali: 81 test, inclusi lease/fencing, patch del codice, recover, ref keep e transizione squash) e su GitHub per Claude Code. L'esecuzione da ChatGPT Work
  o Grok **non è stata provata**: `NOT VERIFIED`. Il protocollo non usa nulla di specifico del fornitore.
- Il controllo dei segreti è per pattern: riduce il rischio, non lo azzera. È esente **solo** `pfckpt.py` (contiene i
  pattern); ogni altro file del checkpoint, sorgenti Python del `code_patch` compresi, viene scansionato (rilievo
  Codex #512, PF-PR-RECOVERY-DECISIONS-02).
- La concorrenza è ottimistica: due agenti possono lavorare in parallelo, ma solo uno pubblica per volta.
- Ref keep (§6-ter): provati con git reale offline su repository bare (push atomico, gc del remoto, clone nuovo).
  L'accettazione di push `--atomic` e di ref fuori da `refs/heads`/`refs/tags` su GitHub **non è stata provata**
  (`NOT VERIFIED`); se GitHub rifiutasse il push, il `save` fallisce senza pubblicare nulla, mai in modo silenzioso.
  Il fallback `refs/pull/*/head` senza PR dichiarata può scaricare molti ref su repository grandi.
- `original_tree` di un oggetto ormai irraggiungibile è verificabile solo contro i tree registrati (`code.tree`,
  `code.patch.tree`): senza registrazione resta il valore scritto da chi ha creato la transizione, che `integrate`
  calcola dall'oggetto quando c'è.
- `restore-code` richiede ancora HEAD == `code_patch.head`: dopo uno squash si ripristina sull'originale (risolvibile
  via ref keep), non sul commit di integrazione.
- Lo strumento non modella PR, merge od ordini: nessuna deduplica di operazioni sensibili oltre ai checkpoint.

### SHA dei test eseguiti (ciclo 8/9 #512)

Ogni voce di `tests_executed` con esito `PASS`/`FAIL` porta uno `sha` che DEVE risolversi a un commit reale con la
stessa lookup di `code.sha`; il readback di `save` scarica tutti i branch remoti (non solo quello di codice), cosi'
un'evidenza pubblicata su un altro branch resta verificabile. Un'evidenza su un albero scratch mai pubblicato e'
ammessa solo se dichiarata esplicitamente (`"unpublished_tree": true` piu' una `sha_note` motivata): la
dichiarazione resta visibile nel checkpoint e non e' mai inferita da uno SHA che semplicemente non si risolve. La
dichiarazione esonera solo dalla risoluzione, mai dalla forma: lo `sha` resta un id di commit esadecimale minuscolo
di 7-40 caratteri, altrimenti `test sha unresolved` (riesame indipendente del ciclo 9). La forma vale per OGNI voce,
dichiarata o no: un riferimento mobile (`HEAD`, `main`, `HEAD~1`) si risolverebbe a commit diversi nel tempo ed e'
rifiutato (secondo riesame del ciclo 9).
