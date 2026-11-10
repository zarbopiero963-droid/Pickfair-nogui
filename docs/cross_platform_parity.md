# Parità Windows/Linux — decisione owner vincolante

**Codice:** `PICKFAIR-CROSS-PLATFORM-PARITY-20261110`.
**Issue permanente:** [#625](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/625). Contiene la matrice
completa, i requisiti di accettazione, la sequenza delle PR e i responsabili.
**Stato:** vincolante e permanente finché l'owner non la modifica esplicitamente. Ogni deroga richiede una nuova
autorizzazione owner esplicita e tracciata (issue o commento owner che la nomina).

## Regola

Pickfair deve funzionare su **Windows e Linux con le medesime funzionalità, garanzie di sicurezza e criteri di
certificazione**: stesse strategie, Money Management, Roserpina, Control API e MCP, stesse regole di rischio, scatola
nera obbligatoria su entrambe, test E2E reali e certificazione indipendente per ciascuna piattaforma.

- È **vietato** considerare Windows una piattaforma secondaria o permanentemente non supportata. La decisione C12-A
  di #617 («Windows: rifiuto di avviarsi») resta valida **solo** come fail-closed temporaneo, non come obiettivo.
- **Fail-closed obbligatorio** finché una piattaforma non offre garanzie equivalenti: una funzionalità non ancora sicura
  su una piattaforma rifiuta di partire su quella piattaforma.
- **Vietati:** fallback insicuri, disabilitazione della scatola nera, riduzione dei controlli per far funzionare una
  piattaforma.
- **Differenze tecniche ammesse solo per ottenere garanzie equivalenti.** Esempi su Windows: handle senza
  `FILE_SHARE_DELETE`, reparse point, ACL, `LockFileEx` al posto di `openat`/`O_NOFOLLOW`, `0600`, `fcntl`. Si
  riusa la logica comune e si isolano solo le primitive di sistema operativo; il codice Linux già verificato non si
  riscrive senza motivo.
- **Nessuna piattaforma è dichiarata supportata o certificata senza le proprie evidenze**, prodotte su quella
  piattaforma reale: una simulazione su un altro sistema operativo è NOT_RUN, mai PASS. Nessuna release può
  dichiarare Windows supportato prima della sua certificazione. `LIVE_CERTIFIED` vale per piattaforma, mai ereditato.

## Stato al 10/11/2026 (sintesi; dettaglio in #625)

| Area | Linux | Windows |
|---|---|---|
| Scatola nera BB-1, percorso headless | in #617 | fail-closed (avvio rifiutato) |
| Scatola nera, percorso GUI (`main.py` → `mini_gui`) | assente | assente |
| Protezione percorsi, permessi, lock, crash/recovery | verificati (#617) | da progettare e collaudare |
| CI 3.11/3.12 | presente | assente |
| Build eseguibile | binario OK, AppImage FAIL | workflow presente, pubblicazione da sospendere (#623) |
| Certificazione | PRE-LIVE su VPS Linux | nessun ambiente: NOT_CERTIFIED |

## Gate di rilascio (decisione owner `PF-CROSS-PLATFORM-EXECUTION-20261110`, D02)

**Vietato distribuire o attivare operativamente** un eseguibile o launcher che possa avviare il trading senza
scatola nera obbligatoria: EXE Windows, GUI Linux, AppImage e ogni altro launcher che aggiri BB-1. La lacuna GUI
(`main.py` / `mini_gui.py` → `RuntimeController` senza `blackbox_runtime.install`) è **P0 per la distribuzione** su
entrambe le piattaforme, finché **BB-1G** non la chiude in modo fail-closed.

Inventario dei launcher al 10/11/2026:

| Launcher | Percorso | Scatola nera |
|---|---|---|
| `main.py --headless`, `pickfair-headless` | `headless_main.main()` | installata (Linux); Windows: avvio rifiutato |
| `main.py` senza `--headless`, `pickfair` / `pickfair.exe`, AppImage (`AppRun` → `pickfair`) | `mini_gui.main()` | **assente** |
| `python mini_gui.py` | `mini_gui.main()` | **assente** |
| `AVVIA_API.bat` → `betfair_market_api.py` | dati di mercato in sola lettura, nessun ordine | assente (non opera; in ritiro, #491) |

**Enforcement materiale oggi: assente.** I job `release` di `build-linux.yml` e `build-windows-exe.yml` pubblicano
sui tag `v*` senza alcun controllo sulla scatola nera: una nota documentale non impedisce un rilascio. Meccanismi da
introdurre e verificare, ognuno con PR e review propri (workflow e impostazioni a merge o azione dell'owner):
1. **BB-1G nel codice** (enforcement principale): ogni entrypoint che può avviare il trading installa la scatola nera
   prima di costruire il runtime, oppure rifiuta l'avvio; test che falliscono se un nuovo launcher la aggira.
2. **Gate nei workflow di rilascio**: il job `release` parte solo se un controllo versionato (entrypoint, test BB-1G)
   è verde sullo SHA del tag.
3. **Protezione dei tag `v*`** nelle impostazioni del repository (azione owner).

## Requisiti di accettazione per piattaforma (A1–A9, testo completo in #625)

A1 suite completa 3.11/3.12 in CI sulla piattaforma; A2 `tests/blackbox` senza skip sui test di garanzia e mutazioni
pertinenti tutte uccise; A3 sicurezza dei percorsi contro un altro utente (link, junction, hardlink, sostituzioni
concorrenti); A4 un solo runtime per registro con processi reali; A5 crash forzato e recovery sullo stesso registro;
A6 credenziali assenti da registro, log e argv persistiti; A7 E2E sul binario impacchettato (headless e GUI);
A8 parità decisionale SIM sugli stessi dati di replay; A9 verifica indipendente e certificazione PRE-LIVE della
piattaforma.

## Per gli agenti

- Prima di qualsiasi lavoro che tocca piattaforma, filesystem, lock, packaging, scatola nera o certificazione:
  leggere #625 e questo documento.
- Questa decisione autorizza **progettazione e pianificazione**. Lo sviluppo di ogni passo richiede un GO owner
  separato; nessun merge automatico, deploy o attivazione LIVE ne deriva.
- Ogni PR che introduce una funzionalità deve dichiarare lo stato per Linux e per Windows (PASS / NOT_RUN /
  fail-closed) con le evidenze; «funziona su Linux» non è «funziona».
