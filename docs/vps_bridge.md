# VPS Autonomy Bridge (preparazione, NON installato)

Stato: **codice preparato offline** (GO-PARALLEL-PREP-01, WP4–WP5). Nessuna installazione, nessuna attivazione:
richiedono GO-INSTALL e poi GO-ACTIVATE dell'owner. Esecutore designato dell'installazione iniziale: Grok.

## Architettura
- **Solo uscita.** Il controllore interroga GitHub con un timer; non ascolta su nessuna porta. Nessuna porta nuova
  in ingresso.
- **Richieste.** `workflow_dispatch` su `main` con attestazione Sigstore. Il contenuto segue il contratto unico
  `pf-test-request/2` (`scripts/vps_bridge/contract.py`):
  - campi chiusi;
  - nonce di 32 hex;
  - TTL di 1800 s;
  - `catalog_sha256` del catalogo di una revisione fidata di `main`;
  - suite solo da catalogo;
  - corsie `development` e `certification`; `certification` solo con GO dell'owner;
  - **nessuna corsia LIVE**;
  - richieste legacy rifiutate.
- **Idempotenza e nomi.** Una sola chiave di idempotenza e un solo schema di nomi dei check (`pf-test/<suite>`,
  `pf-dev-test/<suite>`). Sostituiscono le tre convenzioni divergenti di certd e della #606.
- **Pacchetti.** Il pacchetto di rilascio si verifica con `verify_artifact.py`:
  - sha256 atteso pubblicato su GitHub;
  - nessun symlink, device o percorso pericoloso;
  - `SHA256SUMS` completo;
  - nessun contenuto simile a un segreto.

  La firma di provenienza la verifica `gh attestation verify` prima di questo controllo.

## Cinque ruoli separati (BRIDGE-01)

| Ruolo | Utente | Rete | Scrive in | Privilegi |
|---|---|---|---|---|
| 1. Controllore delle richieste | `pf-bridge-ctl` | solo uscita verso GitHub | `/var/lib/pf-bridge` | nessuno; accoda soltanto |
| 2. Runner sandbox | pool T186 `pf-sbxj0..7` (invariato) | nessuna | run dir del job | quelli di certd/T186 |
| 3. Verificatore delle evidenze | `pf-bridge-verify` | nessuna | audit | chiave di firma **propria** |
| 4. Operatore di deploy | `pf-bridge-deploy` | nessuna | `releases/` | solo artefatti verificati del catalogo |
| 5. Operatore di rollback | `pf-bridge-rollback` | nessuna | `current` | solo rollback predefiniti, autorizzazione owner distinta |

Le unità (`scripts/vps_bridge/systemd/`) hanno:
- `NoNewPrivileges`, `ProtectSystem=strict`, `CapabilityBoundingSet=` vuoto;
- `PrivateNetwork`, tranne il controllore;
- `InaccessiblePaths` sul vault della chiave LIVE e su `/root/.ssh`.

Non c'è nessuna shell e nessuna chiave unica per tutte le operazioni. **I kill switch restano fuori dal controllo
remoto** e il preflight li richiede presenti.

## T186 (BRIDGE-03)
- Il ponte **non sostituisce** l'orchestratore.
- Il preflight richiede `orch.py` 1.4.3 T186 (`c45ba1d8…`, oppure lo sha della correzione GO-FIX-T186 quando sarà
  applicata) e `certd.py` `80886a45…`. Se non coincidono: STOP.
- Il runner resta il pool per job di T186, con tutte le sue protezioni.

## Installazione (dopo GO-INSTALL, eseguita da Grok o dall'owner via console/ConnectBot)
1. `bash scripts/vps_bridge/preflight.sh`: sola lettura; deve dare exit 0.
2. `bash scripts/vps_bridge/install.sh PACCHETTO.tgz SHA256`: **dry-run**, rivedere l'output.
3. Scrivere `GO-INSTALL <sha256>` in `/etc/pf-bridge/GO-INSTALL` (owner), poi `install.sh … --apply`:
   - è idempotente;
   - la release va in `releases/<sha>`;
   - lo switch di `current` è atomico;
   - il timer NON viene abilitato.
4. Smoke test senza operazioni reali: una richiesta `development` di una suite offline, con l'evidenza verificata.
5. Il timer si abilita solo con GO-ACTIVATE.

## Rollback
`bash scripts/vps_bridge/rollback.sh` (dry-run), poi `--apply`:
- disabilita il timer e ferma solo le unità del ponte;
- riporta `current` a `previous` dopo aver verificato `SHA256SUMS`, oppure disinstalla;
- non tocca mai T186, i kill switch, pf-live o le credenziali.

## Limiti dichiarati (NOT_VERIFIED finché non c'è una prova sulla VPS)
- Comportamento reale delle unità con systemd 255.
- Interazione con il timer di pf-orch.
- Smoke test end-to-end.
- Il controllore (`controller.py`) e i ruoli 3–5 sono **specificati, non ancora implementati**: arriveranno nella
  PR del ponte, dopo #607, #606 e l'import Grok.
