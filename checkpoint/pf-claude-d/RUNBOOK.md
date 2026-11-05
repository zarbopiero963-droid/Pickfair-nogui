# RUNBOOK — ripresa Claude D

```bash
git clone https://github.com/zarbopiero963-droid/Pickfair-nogui && cd Pickfair-nogui
python3 tools/agent_checkpoint/pfckpt.py bootstrap --url https://github.com/zarbopiero963-droid/Pickfair-nogui \
  --branch agent-state/pf-claude-d --dir checkpoint/pf-claude-d --dest /tmp/ckpt-d \
  --code-branch claude/d-roserpina-100-sim-report --agent claude-d
git fetch origin 'refs/heads/claude/d-*:refs/remotes/origin/claude/d-*' main
git worktree add ../wt-sim origin/claude/d-roserpina-100-sim-report
cd ../wt-sim && python3 -m pytest -q tests/g7 tests/f026/test_roserpina_100_report.py
python3 tools/sim_report/run_g7.py --out /tmp/g7.json            # evidenza (mode development salvo SHA pubblicato)
```
Salvataggio: `pfckpt.py save --repo <wt> --branch agent-state/pf-claude-d --dir checkpoint/pf-claude-d \
  --expected <sha remoto> --agent claude-d --code-branch <branch> --message "checkpoint claude-d seq N: ..."`.
