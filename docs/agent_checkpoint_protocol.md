# Protocollo di checkpoint multi-agente — `pf-checkpoint/3`

Mandato owner PF-UNIVERSAL-CHECKPOINT-V1 (15/10/2026). Strumento: `tools/agent_checkpoint/pfckpt.py`
(solo Python standard library + git). Test: `tests/agent_checkpoint/test_pfckpt.py` (git reale, offline).

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
richiedono `sha` (il commit di codice su cui il test è stato eseguito): un risultato senza versione non vale. Se
una voce dichiara `artifact`, il file deve essere nel commit.

## 3. Verifica (`verify`)

`pfckpt.py verify --repo R --ref REF` legge **dal tree committato** (mai dal disco) e controlla: STATE e MANIFEST
presenti; JSON valido e schema rispettato; manifest ↔ commit completo in entrambe le direzioni; hash di ogni file;
assenza di pattern di segreti; `previous_checkpoint_sha` = parent; `seq` = seq del parent + 1; SHA del codice
esistente; artefatti dichiarati presenti. Qualsiasi problema → exit 2 con l'elenco dei problemi.

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
| `code_patch/` | `BASELINE.json` (head/tree del codice, file modificati e nuovi), `tracked.patch`, `untracked/<file>` | ogni file dichiarato presente nel commit |

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

## 7. Incidente storico INC-b45f528

Il checkpoint seq 7 (`b45f528`) elencava nel MANIFEST tre log mai committati
(`results/mutation_c03_b33b54d.log`, `results/mutation_d04_180f405.log`, `results/mutation_d04_180f405_partial.log`).
Causa: `.gitignore` (`*.log`) + manifest calcolato dal disco + `git add` senza `-f` + readback limitato a tre file
(vecchio `save_checkpoint.sh`, ritirato). Gli originali sono stati ritrovati nella stessa sessione con SHA-256
identici al manifest e committati nel checkpoint seq 8 (`1297f7f`); `b45f528` resta nella storia invariato.
`pfckpt.py verify` su `b45f528` rileva il difetto (prova in `results/incident_b45f528_verify.json`).

## 8. Limiti dichiarati

- Provato con git reale offline (repository bare locali: 39 test, inclusi lease/fencing, patch del codice, recover) e su GitHub per Claude Code. L'esecuzione da ChatGPT Work
  o Grok **non è stata provata**: `NOT VERIFIED`. Il protocollo non usa nulla di specifico del fornitore.
- Il controllo dei segreti è per pattern: riduce il rischio, non lo azzera. È esente **solo** `pfckpt.py` (contiene i
  pattern); ogni altro file del checkpoint, sorgenti Python del `code_patch` compresi, viene scansionato (rilievo
  Codex #512, PF-PR-RECOVERY-DECISIONS-02).
- La concorrenza è ottimistica: due agenti possono lavorare in parallelo, ma solo uno pubblica per volta.
