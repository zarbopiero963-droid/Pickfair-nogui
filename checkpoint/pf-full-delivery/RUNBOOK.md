# RUNBOOK — riprendere Pickfair da qualsiasi sessione

Istruzione di ripresa: «Riprendi Pickfair dal checkpoint persistente più recente su `agent-state/pf-full-delivery`.
Verifica stato remoto, manifest, SHA, lotti completati e test ancora necessari. Prosegui dal primo passo non
completato, rispettando decisioni owner e gate. Non ripartire da zero.»

## 1. Ripresa da zero (protocollo pf-checkpoint/3, qualunque agente)

Serve solo git + Python 3 (stdlib). Nessuna cronologia di chat, nessun file locale.

```bash
git clone https://github.com/zarbopiero963-droid/Pickfair-nogui.git /tmp/pf && cd /tmp/pf
git fetch origin agent-state/pf-full-delivery ccr-bcc81683-ud0hgc
git show origin/agent-state/pf-full-delivery:checkpoint/pf-full-delivery/tools/pfckpt.py > /tmp/pfckpt.py
python3 /tmp/pfckpt.py bootstrap --url https://github.com/zarbopiero963-droid/Pickfair-nogui.git \
    --branch agent-state/pf-full-delivery --dest /tmp/state --code-branch ccr-bcc81683-ud0hgc
```

`READY` (exit 0) => leggere `next_atomic_action`, `owner_authorizations`, `blocks` e lavorare su `code.sha`.
`LATEST_CHECKPOINT_CORRUPT` (exit 2) => FERMARSI: il report indica `last_valid_checkpoint`; nessun rollback
automatico, decide l'owner o una riconciliazione esplicita. Verifica puntuale: `pfckpt.py verify --repo /tmp/state --ref HEAD`.

## 2. Ambiente (verificato: Ubuntu 24.04, Python 3.12.3, pytest 9.1.1, pytest-xdist 3.8.0, requests 2.32.5)

```bash
python3.12 -m venv /tmp/venv && /tmp/venv/bin/pip install -r requirements.txt pytest pytest-xdist pytest-asyncio
mkdir -p /tmp/netguard && cp checkpoint/pf-full-delivery/tools/sitecustomize.py /tmp/netguard/   # blocco rete nei test
export PF_PY=/tmp/venv/bin/python PF_NETGUARD=/tmp/netguard
Xvfb :99 -screen 0 1400x900x24 &   # per i test GUI
```

## 3. Test (dalla radice del repo sul commit `STATE.work.head_sha`)

```bash
# regressione completa
DISPLAY=:99 PYTHONPATH=$PF_NETGUARD HOME=$(mktemp -d) $PF_PY -m pytest -q -p no:cacheprovider -o log_level=CRITICAL -n 4
# suite finanziarie Roserpina
PYTHONPATH=$PF_NETGUARD HOME=$(mktemp -d) $PF_PY -m pytest -q -p no:cacheprovider -n 4 tests/f026 tests/f001 tests/f029 tests/roserpina41
# mutation (ogni mutante copia l'albero; non modificare il repo mentre gira)
$PF_PY checkpoint/pf-full-delivery/mutation/mutate_c03.py <repo> $PF_PY $PF_NETGUARD /tmp/mutation.json
# campagna (scenari generati, oracolo indipendente, verdetti)
cd checkpoint/pf-full-delivery/campaign
python3 generate.py base.jsonl && python3 generate.py frontier.jsonl --frontier && python3 generate_stress.py stress.jsonl
python3 gen_durability.py durability
python3 run.py base.jsonl out_base --tree <albero del commit> --workers 5 && python3 judge_all.py out_base
# scenario critico multi-tavolo
$PF_PY ../critical/child_replay2.py <albero> ../critical/scoretrend_20261013.json out/critical_pre_X.json critical <home> pre mixed
$PF_PY ../critical/child_replay2.py <albero> ../critical/scoretrend_20261013.json out/critical_post_X.json critical <home> post mixed
python3 ../critical/critical_oracle.py out X
```

## 4. Guard prima di ogni push di codice

`scripts/guardrail_check.py` (con pr_meta.json, pr_files_raw.json, allowed_scope_base.json dalla base c92bb79) e
`scripts/pr_autonomy_policy.py full-diff`; scope `issue461-pr28-f026v2-f2-f018-positions` = file del diff.

## 5. Salvataggio del checkpoint (dopo ogni lotto)

1. `EXP=$(git -C /tmp/state ls-remote origin refs/heads/agent-state/pf-full-delivery | cut -f1)` e il worktree deve essere su `EXP`.
2. Aggiornare `STATE.json`: `seq` = precedente + 1, `previous_checkpoint_sha` = `EXP`, `code.sha` (40 caratteri), test con
   stato `PASS/FAIL/BLOCKED/NOT VERIFIED/MISSING` e SHA del codice, artefatti dichiarati presenti nella directory.
3. `python3 tools/pfckpt.py save --repo /tmp/state --branch agent-state/pf-full-delivery --expected $EXP --message "..." --code-branch ccr-bcc81683-ud0hgc`

Solo `"status": "DURABLE"` (readback da clone pulito superato) vale come salvato. Exit 3 = un altro agente ha avanzato
lo stato: leggere il nuovo checkpoint, riconciliare a mano, ripartire da quello SHA (mai force-push, mai merge automatico).
Dopo un crash si riesegue lo stesso comando: riprende da commit locale o push gia' avvenuto senza duplicare.
Specifica completa: `docs/agent_checkpoint_protocol.md` sul branch di codice.

## Vincoli

Nessun LIVE, nessun deploy, nessuna modifica a main o alla PR #505, nessun merge, una PR per repository, nessun bypass
di Gate A/Gate B, nessuna certificazione senza prove (#351 non avviata).
