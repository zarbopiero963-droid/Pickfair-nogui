# tests/fullscope — harness F-33

Vedi `docs/testing/full_scope_harness.md`. In breve:

- `combi/` test del generatore combinatorio (`tools/combi/`) e dell'inventario delle 190 chiavi;
- `requirements/` catalogo dei requisiti UNVERIFIED (#586 tracker) e tracciatore pytest (`pf_requirement`);
- `harness/` child runner (HeadlessApp reale in SIM), driver con SIGKILL esterno e riavvio, netguard, oracolo Decimal;
- `config/` livello 1 sulle configurazioni persistite (round-trip reale, invalidi);
- `scenarios/` scenari E2E con componenti reali (SIM-COMBO-001, SIM-CASH-002, SIM-DUP-005, SIM-RESTART-003) e il binding L2/L3 (`test_l2_l3_runtime_binding.py`, casi del generatore eseguiti nel runtime SIM con oracolo; `PF_COMBI_CASES`).

`python -m pytest tests/fullscope -q` — artifact in `art/fullscope/<sha>/` (ignorato da git).
Nessun file applicativo è toccato da questo harness; i difetti trovati sono xfail strict con riferimento alla scheda F-xx.
