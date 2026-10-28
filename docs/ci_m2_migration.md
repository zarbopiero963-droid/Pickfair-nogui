# Migrazione M2 dei workflow Linux su Buildkite (#562, PR-B)

Stato: **preparata, non certificata.** Questo documento dice cosa esegue la full-suite per i workflow Linux che
finora restavano solo su GitHub Actions, con quali limiti, e cosa serve sulla lane. Un workflow si dichiara
equivalente solo dopo un run reale su Buildkite con esito verificato, mai dalla sola presenza di uno step.

## Copertura: ogni workflow ha un posto

`scripts/pr_full_suite_pipeline.py` classifica **tutti** i file di `.github/workflows/` in quattro gruppi disgiunti.
Lo verifica `test_every_workflow_file_is_mapped_extra_excluded_or_not_migrated_with_a_reason`.

| Gruppo | Significato |
|---|---|
| `WORKFLOW_MAP` | workflow che su Actions gira sulle PR ⇒ step equivalenti della full-suite |
| `WORKFLOW_EXTRA` | workflow che su Actions **non** gira sulle PR (push/dispatch/schedule), ma che la full-suite esegue comunque |
| `WORKFLOW_EXCLUDED` | gira sulle PR ma resta su Actions o sulla lane trusted, con il motivo |
| `WORKFLOW_NOT_MIGRATED` | fuori dalle PR e non migrato, con il motivo; mai dichiarato equivalente |

`WORKFLOW_LIMITS` elenca, workflow per workflow, cosa la migrazione **non** certifica.

## Step M2

| Workflow Actions | Step Buildkite | Cosa fa | Limite dichiarato |
|---|---|---|---|
| `generate-linux-lockfile.yaml` (PR) | `w5-lockfile-fresh` | `pip==24.3.1` + `pip-tools==7.4.1`, `pip-compile --generate-hashes --allow-unsafe --no-annotate`. Fallisce se il lock committato è vecchio (diff con la rigenerazione) o se è stato rimosso mentre su `main` esiste. La rigenerazione va come artifact | la Job Summary di Actions diventa l'artifact `requirements-build-linux.fresh.lock` |
| `build-linux.yml` (push) | `w4-build-linux-tests` (PR-A) + `w5-build-linux-appimage` | tkinter obbligatorio (fail-closed). Installa il lock con `--require-hashes` in un venv nuovo, **senza** aggiornare pip, poi esegue `tests/packaging`, PyInstaller (`pickfair`, `pickfair-headless`), il tar.gz e l'AppImage con `packaging/build_appimage.sh`. Controlla che tutti e tre siano file ELF. Release in **DRY-RUN**: servono entrambi gli asset (uno solo ⇒ FAIL), poi `SHA256SUMS`; tutto va come artifact | la GitHub Release sui tag resta su Actions; l'avvio grafico dell'AppImage non viene eseguito |
| `telegram-live-integration.yml` (dispatch/schedule) | `w5-telegram-live-sim` | si ferma se nella lane c'è una variabile `TG_*`. Esegue `tests/integration/test_telegram_live_sim.py`: i 7 scenari del test live con le **stesse asserzioni**, il trasporto sostituito da un'eco locale, ogni socket e POST bloccato | trasporto Telethon/Bot API, consegna e chat reale **non certificati** (servono credenziali e GO owner) |

## Non migrati

| Workflow | Motivo |
|---|---|
| `build-windows-exe.yml` | esclusa dal mandato #562: solo workflow Linux |
| `telegram-test.yml` | invia un messaggio Telegram reale: vietato |
| `merge-simulation.yml` | variante manuale di `merge-simulation-hard`, già coperta da `w4-merge-simulation` |
| `queue-on-pr-closed.yml` | recupero manuale della coda dei task: scrive sul repository, non è un controllo |
| `pr-overlap-guard-diagnostic.yml` | diagnostica manuale della lane trusted |
| `_module-ultra-check.yml` | workflow riusabile, eseguito tramite i moduli (`MODULES`) |

## Rilievo trovato dalla simulazione Telegram

Lo scenario 5 di `test_telegram_live_e2e.py` (`test_legacy_signal_emoji_format`) si aspettava che il formato emoji
legacy producesse un segnale con evento, quota e stake. Dal motore neutro (#448), invece:
- il parsing legacy hardcoded non esiste più (`TelegramListener._parse_legacy_signal` restituisce sempre `None`);
- i segnali passano solo dai pattern dell'utente;
- il parser non inventa dati.

Il test non lo segnalava perché senza segreti non girava mai. La simulazione lo ha fatto fallire. L'aspettativa ora
è **nessun segnale**: è il comportamento sicuro, e non c'è codice applicativo da cambiare.

## Requisiti di infrastruttura (Grok)

Sulla coda `pickfair-dedicated` (lane non fidata, nessun segreto):

1. **tkinter** nel Python della lane (`/var/lib/buildkite/tools/py311/bin/python3.11`): senza, `w5-build-linux-appimage`
   si ferma, come su Actions con `python3-tk`.
2. **appimagetool** in `/var/lib/buildkite/tools/appimagetool/appimagetool`, eseguibile e non scrivibile dal job,
   con lo sha256 in `appimagetool.sha256` accanto. Il job non scarica mai lo strumento (Actions prende la release
   `continuous` senza pin): senza file si ferma.
3. Uscita di rete verso **PyPI** (lock e build) e verso GitHub (`git fetch origin main`), come per gli altri step.
4. **Artifact:** un'AppImage e un tar.gz per push, nell'ordine delle decine di MB. Va fissata una retention.
5. Nessuna variabile `TG_*`, Betfair o token nell'ambiente dei job: `w5-telegram-live-sim` lo verifica e si ferma.
6. Il dispatcher installato deve conoscere i tre step nuovi (`FULL_SUITE_STEP_KEYS`): finché non è aggiornato, la
   full-suite dà `error` (step attesi mancanti o in più: fail-closed).

## Prove locali (non sostituiscono il run su Buildkite)

- `tests/scripts/test_pr_full_suite_pipeline.py`:
  - mappatura completa;
  - pin e argomenti uguali ai workflow;
  - nessuna pubblicazione e nessun download dello strumento;
  - lock installato senza pip aggiornato.
- Prove con bash reale:
  - lock rimosso nella PR, provato contro un repository git di prova;
  - credenziali `TG_*` rifiutate;
  - release in dry-run con 0, 1, 2 o 3 asset.
- `tests/integration/test_telegram_live_sim.py`: 7 scenari, il blocco della rete, e un caso in cui un parser rotto
  fa fallire lo scenario.
- Mutazioni `fsp_m2_*`: 6 su 6 rilevate.
- Build reale dello step `w5-build-linux-appimage` in locale: esito riportato nella PR, con Python 3.12 al posto del
  3.11 della lane, che in questo ambiente non ha tkinter.
