# F-33 — Harness full-scope: combinatorio, E2E e tracciabilità dei requisiti UNVERIFIED

Scheda: #586 PICKFAIR_FUTURE_PR_EXECUTION_CARDS v2 blocco 3 (F-33). Mandato: owner 29/10/2026 «FULL-SCOPE
TESTING» e «CONTINUOUS UNVERIFIED CLOSURE». **Solo tooling di test**: nessun file applicativo di produzione è
toccato; nessuna logica finanziaria cambia. I test che rivelano difetti producono finding tracciabili (xfail
`strict=True` con il riferimento alla scheda F-xx che li corregge), mai skip silenziosi né asserzioni indebolite.

## Componenti

| Componente | Percorso | Funzione |
|---|---|---|
| Inventario | `tools/combi/inventory.json` | 190 chiavi censite (139 ATTIVE, 51 LEGACY; 42 Roserpina), dominio, valori validi/di confine/invalidi, modalità L1 (`settings_roundtrip` 79, `load_only` 51, `static_or_manual` 60) |
| Generatore | `tools/combi/generator.py` | pairwise / three-way greedy (famiglia IPOG) con `random.Random(seed)`, vincoli, tuple non estendibili escluse, report di copertura verificato da un controllo indipendente (`verify`), CLI |
| Tracciatore requisiti | `tests/fullscope/requirements/tracker.py`, `catalog.json`, `tests/fullscope/conftest.py` | marker `@pytest.mark.pf_requirement("ID")`; report `art/fullscope/<sha>/requirements_report.json` (coperti / tentati non coperti / senza test / ID sconosciuti); riga pronta per il tracker #586 |
| Child runner | `tests/fullscope/harness/child_runner.py` | HeadlessApp REALE in SIM, SQLite su disco, passi di scenario, snapshot per passo, crash cooperativi (`os._exit`) e checkpoint per crash esterno |
| Driver | `tests/fullscope/harness/runner.py` | sottoprocesso isolato per HOME, **SIGKILL esterno al checkpoint**, riavvio sullo stesso HOME, netguard, artifact JSON per scenario |
| Netguard | `tests/fullscope/harness/netguard/sitecustomize.py` | ogni connessione non locale nel sottoprocesso è un errore (invariante 11: nessun ordine reale da SIM) |
| Oracolo | `tests/fullscope/harness/oracle.py` | Decimal, indipendente dal codice sotto test: conservazione, P&L BACK/LAY, duplicati, tavoli = posizioni aperte, settlement unico per mercato, gambe exchange assenti dal ledger |
| L1 configurazioni | `tests/fullscope/config/test_l1_settings_roundtrip.py` | round-trip reale save→SQLite→reload→RoserpinaConfig per le 79 chiavi persistite; invalidi: fail-closed atteso, fail-open documentato come xfail F-18 |
| Scenari E2E | `tests/fullscope/scenarios/` | SIM-COMBO-001, SIM-CASH-002, SIM-DUP-005, SIM-RESTART-003 (con sonda RST-03) |

Il child runner è un'evoluzione di `tests/f026/child_f026.py` (che resta intatto e in uso dai test F026/F027/F028):
stesso principio — nessun valore atteso nel runner, l'oracolo vive nei test — con vocabolario esteso (LAY, stake
fisso, book mobile, checkpoint, crash prima della contabilizzazione del settlement, settings a caldo).

## Marker pytest (registrati in `pytest.ini`)
`fullscope`, `pf_requirement(*ids)`, `stress` (solo VPS isolata), `requires_vps`, `requires_realdata` (F-31 +
GO-REALDATA), `requires_xvfb`.

## Comandi

```bash
# tutto l'harness (≈ 2 min; scrive art/fullscope/<sha>/requirements_report.json e scenarios/*.json)
PF_SEED=7 python -m pytest tests/fullscope -q
# solo configurazioni L1 / solo scenari / solo generatore
python -m pytest tests/fullscope/config -q
python -m pytest tests/fullscope/scenarios -q
python -m pytest tests/fullscope/combi -q
# report di copertura combinatoria (tutti i gruppi, seed 7)
python -m tools.combi.generator --group ALL --seed 7 --out art/fullscope/coverage_seed7.json
```

Variabili: `PF_SEED` (seed riproducibile, default 0), `PF_FULLSCOPE_ART` (directory artifact), `PF_TREE`, `PF_PY`.
`art/` è ignorato da git: gli artifact si allegano alla PR/#586, non si committano.

## Regole di copertura e di chiusura
- Un requisito è **coperto** solo da un test `pf_requirement(id)` con esito `passed` sullo SHA del report; xfail,
  skip, failed, error non coprono. Gli ID fuori catalogo sono segnalati, non scartati.
- Nessuna dichiarazione di copertura pairwise/three-way senza il `coverage_report` del generatore (`complete: true`
  e ricontrollo `verify`).
- Un test rosso su un difetto noto resta nel repository come `xfail(strict=True, reason="F-xx: ...")`: quando la
  PR correttiva mergia, l'XPASS strict fa fallire la suite e obbliga a rimuovere il marker (chiusura verificata).

## Finding prodotti dalla prima esecuzione su `main` 6fa29b2 (SIM, seed 0)
| Finding | Test | Stato | Scheda |
|---|---|---|---|
| Hedge del cashout eseguito sull'exchange ma assente dal ledger `orders` | `test_sim_combo_001.py::test_cashout_hedge_is_written_to_ledger` | xfail strict (CSH-06 confermato) | F-05 |
| Monitor SL/TP inerte: nessun trigger osservabile | `test_sim_combo_001.py::test_stop_loss_leg_triggers_on_adverse_price` | xfail strict | F-01/F-02 |
| Loader: valore illeggibile ⇒ default silenzioso (`_f`) o `inf` accettato | `test_l1_settings_roundtrip.py::test_invalid_numeric_value_is_not_silently_replaced_by_a_default` (51 casi) | xfail (NF-16) | F-18 |
| RST-03: perdita regolata dall'exchange prima del crash, contabilizzata una sola volta dopo il riavvio | `test_sim_restart_003.py::test_rst03_...` | **PASS** (prova positiva riproducibile) | F-10 (tracker: COVERED su 6fa29b2, SIM) |
| Secondo comando di cashout sulla stessa posizione: nessun secondo hedge | `test_sim_cash_002.py::test_second_cashout_command_...` | PASS (sonda; CSH-09 resta PARTIAL per il vincolo durevole) | F-05 |

## Limiti dichiarati (v1)
- Scenari non ancora scritti: SIM-TRAIL-004 (dipende da F-03), SIM-FAULT-006, SIM-CFG-007 (parte GUI), SIM-GUI-008
  (Xvfb), SIM-SEC-009, SIM-PKG-010 (VPS/GO-4), SIM-MICRO-011 (Microstake owner-reserved: sola osservazione),
  SIM-SETTLE-012, SIM-REALDATA-013 (F-31); C-01…C-14 solo in parte coperti dai quattro scenari presenti.
- Dutching + Microstake nello stesso run (MOD-05) non ancora eseguito: manca il passo `dutching` nel child runner.
- L2/L3: il generatore produce i casi con report completo, ma i casi NON sono ancora eseguiti contro il runtime
  (manca il binding «caso → avvio istanza con quelle settings → oracle»).
- Nessuna esecuzione su VPS, nessun stress/long-running, nessun dato reale: tutto SIM locale.
