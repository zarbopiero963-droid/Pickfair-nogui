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
