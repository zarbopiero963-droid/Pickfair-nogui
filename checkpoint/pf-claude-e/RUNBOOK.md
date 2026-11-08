# RUNBOOK — ripresa Claude E (PF-ISSUE-CLEANUP-01)

```bash
git clone https://github.com/zarbopiero963-droid/Pickfair-nogui && cd Pickfair-nogui
python3 tools/agent_checkpoint/pfckpt.py bootstrap --url https://github.com/zarbopiero963-droid/Pickfair-nogui \
  --branch agent-state/pf-claude-e --dir checkpoint/pf-claude-e --dest /tmp/ckpt-e --code-branch main --agent claude-e
cat /tmp/ckpt-e/checkpoint/pf-claude-e/results/PLAN.md
```
Salvataggio: `pfckpt.py save --repo <worktree su SHA remoto> --branch agent-state/pf-claude-e --dir checkpoint/pf-claude-e \
  --expected <sha remoto> --agent claude-e --code-branch main --message "checkpoint claude-e seq N: ..."`.
Fase 2: prima di ogni chiusura rileggere la issue (stato/commenti nuovi) e il main; una chiusura = commento conclusivo + close
con state_reason; registrare numero, commento e URL in results/closures.json e salvare il checkpoint.
