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
| **Binding L2/L3 → runtime** | `tests/fullscope/harness/combi_runner.py`, `tests/fullscope/scenarios/test_l2_l3_runtime_binding.py` | ogni caso del generatore: istanza SIM isolata → settings del caso salvate prima dello START → snapshot `config`/`sim_config`/`table_count` prova l'applicazione → scenario deterministico (ingresso+settlement, LAY, parziale+cashout, tre tavoli) → oracolo Decimal + controlli coerenti con i limiti DEL CASO (stake entro `max_single_bet_pct`/`max_stake_abs`/`min_stake`, tavoli ≤ `table_count`, rifiuto esplicito se l'ingresso è negato) → record in `art/fullscope/<sha>/combi_runs.json` (seed, SHA, caso, scenario, esito, violazioni). `PF_COMBI_CASES` = casi per gruppo (default 3, `all` = tutti) |
| Test negativi dell'oracolo | `tests/fullscope/harness/test_oracle_negative.py` | ogni invariante fallisce su uno snapshot manomesso (nessun controllo vacuo) |
| Riconciliazione requisiti | `tests/fullscope/requirements/reconciliation.json`, `test_reconciliation.py` | 47 righe del tracker → 43 ID del catalogo (+K-6): 6 chiavi proxy aggregate in `R2-proxy`, L6 = SIG-02, 3 differiti (QA-04 infra, CTL-01 fase E, MCP-01 fase F), stati COVERED/PARTIAL/XFAIL/UNVERIFIED/DEFERRED/NOT_APPLICABLE |
| Mutation testing | `guardrails/mutation_catalog.json` (area `fullscope_tooling`, 13 mutazioni `cg_*`) | `scripts/run_mutation_guardrails.py --module tools.combi.generator --fail-under 100`: 12 uccise, 1 equivalente documentata (`cg_complete_ignores_constraints`: il controllo finale di `_complete` è ridondante perché ogni passo e ogni chiamante verificano già i vincoli) |

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
| Loader: valore illeggibile ⇒ default silenzioso (`_f`) o `inf` accettato | `test_l1_settings_roundtrip.py::test_invalid_numeric_value_is_not_silently_replaced_by_a_default` (51 casi) | xfail imperativo (NF-16): il ramo fail-open non sara' piu' raggiunto quando F-18 chiude | F-18 |
| RST-03: perdita regolata dall'exchange prima del crash, contabilizzata una sola volta dopo il riavvio | `test_sim_restart_003.py::test_rst03_...` | **PASS** (prova positiva riproducibile) | F-10 (tracker: COVERED su 6fa29b2, SIM) |
| Secondo comando di cashout sulla stessa posizione: nessun secondo hedge | `test_sim_cash_002.py::test_second_cashout_command_...` | PASS (sonda; CSH-09 resta PARTIAL per il vincolo durevole) | F-05 |
| `simulation.starting_balance = 0` ricade in silenzio sul default 1000 (`float(cfg or 1000.0)` in `BetfairService._connect_simulation`); stesso schema per `commission_pct = 0` (`or 4.5`) | `test_l2_l3_runtime_binding.py` (caso `c_esecuzione_sim` con saldo 0) | xfail imperativo con riferimento (configurazione ignorata) | F-18/F-17 |
| `simulation.commission_pct ≠ 4.5` ⇒ il SimulationBroker rifiuta il settlement (`COMMISSION_POLICY_VIOLATION`): le posizioni restano aperte senza contabilizzazione; fail-closed per policy, ma la configurazione lo permette senza avviso | binding, gruppo `c_esecuzione_sim` (chiave esclusa dal gruppo eseguito) | documentato | F-17 (commissione etichettata: una sola fonte) |
| `settlement.poll_sec = 0` ricade sul default 60 s (`float(x or 60.0)`): valore 0 trattato come «assente» | binding, inventario (0 = confine) | documentato, nessun test dedicato (confine) | F-18/F-17 |
| Con `settlement.poll_enabled = False` l'exchange regola ma il bot non contabilizza (nessun poller): comportamento atteso, verificato dall'oracolo lato exchange | binding, gruppo `c_esecuzione_sim` | PASS (dichiarato) | — |

## Revisione indipendente locale (A13, 30/10) e hardening
Prima revisione: nessun rilievo bloccante, nove non bloccanti. Tutti indirizzati: confronto per chiave nella fase
verticale del generatore; flag `vacuous`; attesa del cashout duplicato a bus fermo; dicitura xfail imperativo; codice
morto; `tree_dirty` nel report; **nessuna attesa fissa** nel child runner salvo `wait`/`checkpoint` espliciti (tutte le
altre sono attese a evento o a bus fermo con grazia breve); **`bus_idle` sull'API pubblica** dell'EventBus
(`queue_depth`, `published_total_count`, `delivered_total_count`); **netguard** su `connect`, `connect_ex`, `sendto`
(TCP e UDP) e `create_connection`. Limite dichiarato del netguard: il tipo C `_socket.socket` è immutabile e un uso
diretto di `_socket` (che Pickfair non fa) non è intercettato; i sottoprocessi Python ereditano il guard via
PYTHONPATH, i binari esterni no.

## Review su PR #589 (head `d46e6da`) e correzioni
Sol, Grok e Codex: bloccanti reali, tutti corretti nello stesso push.
- **Kill esterno su Windows:** `signal.SIGKILL` non esiste su Windows ⇒ `Popen.kill()` (SIGKILL su POSIX, TerminateProcess su Windows).
- **xfail che mascherava l'oracolo:** nel binding gli invarianti finanziari e le attese si asseriscono PRIMA dell'xfail del finding noto `starting_balance 0`.
- **Report sotto xdist:** ogni worker scrive solo i propri record (`requirements_records.<worker>.json`, `combi_runs.<worker>.json`); il controller aggrega in un unico `requirements_report.json` (i record «non eseguiti su quel worker» non sovrascrivono gli esiti reali; fra esiti reali vince il non coprente).
- **Eventi della sonda stop-loss:** i prefissi conservati negli artifact stanno in `tests/fullscope/harness/events.py` (testato) e includono `SL_`, `TP_`, `STOP_LOSS`, `TAKE_PROFIT`, `TRAILING`.
- **Hash dell'inventario:** con `--inventory` il report cita lo sha256 del file realmente usato.

## Limiti dichiarati (v1)
- Scenari non ancora scritti: SIM-TRAIL-004 (dipende da F-03), SIM-FAULT-006, SIM-CFG-007 (parte GUI), SIM-GUI-008
  (Xvfb), SIM-SEC-009, SIM-PKG-010 (VPS/GO-4), SIM-MICRO-011 (Microstake owner-reserved: sola osservazione),
  SIM-SETTLE-012, SIM-REALDATA-013 (F-31); C-01…C-14 solo in parte coperti dai quattro scenari presenti.
- Dutching + Microstake nello stesso run (MOD-05) non ancora eseguito: manca il passo `dutching` nel child runner.
- L2/L3: eseguiti i primi N casi per gruppo (N = `PF_COMBI_CASES`, default 3; la corsa completa con `all` richiede
  ~10 s per caso: 38+44+16+39+125+145 casi ≈ 70 min) con scenari deterministici; il gruppo SL/TP/trailing non è
  eseguibile (chiavi inesistenti: xfail strict F-01/F-02/F-03).
- Nessuna esecuzione su VPS, nessun stress/long-running, nessun dato reale: tutto SIM locale.
