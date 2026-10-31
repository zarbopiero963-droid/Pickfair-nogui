# RUNBOOK — ripresa Claude B

```bash
git clone https://github.com/zarbopiero963-droid/Pickfair-nogui && cd Pickfair-nogui
python3 tools/agent_checkpoint/pfckpt.py bootstrap --url https://github.com/zarbopiero963-droid/Pickfair-nogui \
  --branch agent-state/pf-claude-b --dir checkpoint/pf-claude-b-f33 --dest /tmp/ckpt-b \
  --code-branch f33-full-scope-harness --agent claude-b
git fetch origin f33-full-scope-harness main
git worktree add ../wt-f33 origin/f33-full-scope-harness
python3.12 -m venv ../venv && ../venv/bin/pip install -r requirements-test.txt
cd ../wt-f33 && ../venv/bin/python -m pytest tests/fullscope -q -n 3            # atteso: 0 failed, 56 xfailed
../venv/bin/python scripts/run_mutation_guardrails.py --module tools.combi.generator --fail-under 100 --jobs 2
```
F-05: `git worktree add -b f05-hedge-ledger ../wt-f05 origin/main`, poi i test RED di `results/F05_PHASE0.md` §7.5.
La directory `checkpoint/pf-full-delivery/` appartiene a Claude A (#562): non modificarla.
