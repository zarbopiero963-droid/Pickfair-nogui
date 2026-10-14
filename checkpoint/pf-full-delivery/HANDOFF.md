# HANDOFF — PF-FULL-DELIVERY-01 (stato persistente, leggibile da qualsiasi agente)

**Fonte di verità:** questo branch (`agent-state/pf-full-delivery`) + branch di lavoro `ccr-bcc81683-ud0hgc`.
La chat e gli Artifact NON sono la fonte di verità.

## Ripresa («Riprendi Pickfair»)
1. `git ls-remote origin refs/heads/main refs/pull/505/head refs/heads/ccr-bcc81683-ud0hgc`:
   main deve essere `028cee9`, #505 `618884e`, branch di lavoro = `work.head_sha` di STATE.json. Altrimenti STOP.
2. `git checkout ccr-bcc81683-ud0hgc`; venv Python 3.12 da `requirements-lock` + pytest/xdist; Xvfb `:99` per i test GUI.
3. Rieseguire almeno `pytest -n 4 tests/f001 tests/f026` (atteso: tutti PASS) prima di proseguire.
4. Eseguire solo `next_atomic_action`.

## Fatto (verificato)
- F001/F023/F024 consolidati (`c92bb79`).
- F026 v2 + F2 + F018 (`afebec7`): grandezze del conto separate, base dello stake invariata, auto-reset sul regolato,
  rilascio tavolo al settlement, ricostruzione posizioni al riavvio con PAUSED se la riconciliazione è incompleta.
- Regressione completa 7363/0.

## Non fare
- Nessuna PR (una PR per repository: #505 è aperta), nessun merge, nessuna modifica a main o #505, nessun LIVE.
- Non integrare la patch F026 v1 (`b766261`): sostituita da v2.
