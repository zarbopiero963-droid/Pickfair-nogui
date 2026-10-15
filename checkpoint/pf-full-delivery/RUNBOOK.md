# RUNBOOK — riprendere Pickfair da qualsiasi sessione

Istruzione di ripresa: «Riprendi Pickfair dal checkpoint persistente più recente su `agent-state/pf-full-delivery`.
Verifica stato remoto, manifest, SHA, lotti completati e test ancora necessari. Prosegui dal primo passo non
completato, rispettando decisioni owner e gate. Non ripartire da zero.»

## 1. Verifica dello stato

```bash
git fetch origin agent-state/pf-full-delivery ccr-bcc81683-ud0hgc main
git show origin/agent-state/pf-full-delivery:checkpoint/pf-full-delivery/STATE.json      # stato, SHA, prossima azione
git worktree add /tmp/state origin/agent-state/pf-full-delivery
(cd /tmp/state/checkpoint/pf-full-delivery && sha256sum -c MANIFEST.sha256)             # integrita' degli artefatti
git rev-parse origin/ccr-bcc81683-ud0hgc                                                  # deve essere STATE.work.head_sha
```

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

## 5. Salvataggio del checkpoint

`checkpoint/pf-full-delivery/tools/save_checkpoint.sh <worktree> "<messaggio>"`: aggiornamento ottimistico (nessun
force-push), MANIFEST.sha256, readback remoto. Solo con «DURABLE <sha>» il checkpoint è salvato.

## Vincoli

Nessun LIVE, nessun deploy, nessuna modifica a main o alla PR #505, nessun merge, una PR per repository, nessun bypass
di Gate A/Gate B, nessuna certificazione senza prove (#351 non avviata).
