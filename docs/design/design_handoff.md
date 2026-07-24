# Design Handoff — Pickfair Mini GUI

> Fonte unica di verità sull'aspetto UI/UX della mini GUI per chi lavora sul
> design. Deve restare SEMPRE allineata al codice reale (label verbatim,
> stati e flussi corretti): ogni PR che tocca l'aspetto design la aggiorna
> nello stesso PR (vedi «GATE DESIGN HANDOFF» in CLAUDE.md).
> Stato: prima stesura, generata leggendo `mini_gui.py`,
> `telegram_tab_ui.py`, `theme.py`, `main.py`, `headless_main.py`,
> `ui_panels/*` sul main corrente.

## 1. Entry point e rapporto con la modalità headless

- `main.py` sceglie la modalità dagli argomenti: `--headless` (o `headless`)
  => `run_headless()`; **default = GUI** (`run_gui()` importa `mini_gui`).
- `mini_gui.main()` costruisce `MiniPickfairGUI(force_simulation=True)`:
  all'avvio la GUI **forza sempre SIMULAZIONE** (uno stato LIVE persistito
  non viene mai riapplicato in automatico — fail-closed).
- `headless_main.py` non importa la GUI: è un runtime CLI separato con flag
  propri (`--live`, `--simulation`, `--live-enabled`, `--kill-switch-on/off`,
  `--emergency-stop`, `--preflight`, `--telegram-login`, ...).
- Senza display X11 la GUI non parte (errore Tk «no display»): su host
  headless si usa `--headless`.

## 2. Framework, finestra, aspetto

- Framework: **customtkinter** (wrapper di Tkinter); tabelle `ttk.Treeview`,
  lista Provider `tk.Listbox`, log `CTkTextbox`.
- Titolo finestra: `Pickfair Mini GUI` — geometria `1420x900`.
- Aspetto: `set_appearance_mode("dark")`, tema `blue`.
- Heading della top bar: `Pickfair Mini Control Panel`.
- Lo stato runtime è pollato ogni 1000 ms.

## 3. Elenco tab (ordine e nomi esatti)

1. `Dashboard`
2. `Impostazioni`
3. `Telegram`
4. `Storico Bet`
5. `Roserpina`
6. `Simulazione`
7. `Watchdog`
8. `Alert`
9. `Storico Risk Desk`
10. `Risk Desk`
11. `Provider` (assente in test_mode)
12. `Log`

## 4. Top bar — controlli di esecuzione globali

Cluster `Modalità:`:

- Switch `SIMULAZIONE` (default ON) → `_toggle_simulation_mode`; label di
  modalità `SIMULAZIONE` / `LIVE`.
- Label `Requested:` + valore; label `Execution:` + combobox
  `SIMULATION | LIVE`.
- Switch `LIVE Gate` (default OFF).
- Switch `KILL Switch` (default OFF, progress color rosso `#d9534f`).
- Bottoni `Applica Cambiamenti` e `Refresh`.

⚠ Nota UX (stato reale): **nessun dialogo di conferma** al passaggio
SIMULAZIONE→LIVE, all'attivazione del LIVE Gate o all'armamento del KILL
Switch.

## 5. Tab Dashboard

Colonna sinistra `Runtime Controls`:

| Bottone | Azione | Stile |
| --- | --- | --- |
| `AVVIA BOT` | `_runtime_start` | verde `#2fa26b`, h40 |
| `STOP BOT` | `_runtime_stop` | rosso `#d9534f`, h40 |
| `PAUSA` | `_runtime_pause` | default |
| `RIPRENDI` | `_runtime_resume` | default |
| `RESET CICLO` | `_runtime_reset` | default |
| `EMERGENCY STOP` | `_runtime_emergency_stop` | nero, testo rosso, h50 |

⚠ Nessuno di questi bottoni ha un dialogo di conferma (l'unico `askyesno`
di tutta la GUI è l'eliminazione di una regola parser).

Colonna destra `System Status` (label sola lettura): `Status:` (`STOPPED`),
`Broker:` (`SIMULATION`), `Betfair:` (`DISCONNECTED`/`CONNECTED`),
`Telegram:` (`STOPPED`/`LISTENING`), `Bankroll:`, `Drawdown:`, `Exposure:`,
`Active Tables:`, `Last Signal:`.

Pannello `Live Control Plane`:

- readiness: `READY` / `DEGRADED` / `NOT_READY` / `UNKNOWN`;
- effective status: `LIVE_BLOCKED` (kill switch) / `SAFE_MODE` (non LIVE) /
  `LIVE_REQUESTED_BLOCKED` (gate OFF o blocker) / `LIVE_ACTIVE` / `UNKNOWN`;
- control state (stringhe verbatim): `LIVE blocked by kill switch`,
  `SIMULATION`, `LIVE requested but blocked (gate OFF)`,
  `LIVE requested but blocked`, `LIVE active`,
  `LIVE requested (readiness unknown)`, `Status unavailable`;
- `Last decision:` `GO`/`NO-GO`/`N/A`; `Last reason:` con reason code
  (`KILL_SWITCH_ACTIVE`, `SIMULATION_MODE`, `LIVE_DISABLED`, `READY`,
  `READINESS_UNKNOWN`, ...).

In basso: `Last Error/Info:` in rosso `#d9534f`.

## 6. Tab Impostazioni (Betfair Credentials)

Entry: `Username`, `Password` (mascherata `*`), `App Key`,
`Certificate Path`, `Key Path`. Bottone `Salva Betfair` → dialog
`OK / Impostazioni Betfair salvate.` o errore.

## 7. Tab Roserpina (Roserpina Cycle Configuration)

Entry (label verbatim): `Target Profit Cycle %`, `Max Single Bet %`,
`Max Total Exposure %`, `Max Event Exposure %`, `Auto Reset Drawdown %`,
`Defense Drawdown %`, `Lockdown Drawdown %`, `Expansion Profit %`,
`Expansion Multiplier`, `Defense Multiplier`, `Table Count`,
`Max Recovery Tables`, `Commission %`, `Min Stake`, `Max Stake Assoluto`,
`Hard-stop: Perdita Giornaliera Max (€, vuoto=non impostato)`,
`Hard-stop: Esposizione Aperta Max (€, vuoto=non impostato)`,
`Hard-stop: Drawdown Max % (0-100, vuoto=non impostato)`,
`Book Warning % (avviso over-round)`,
`Book Block % (blocca submit se book >= soglia)`,
`Liquidity avviso: Moltiplicatore (richiesta = stake x N)`,
`Liquidity avviso: Floor assoluto € (0 = nessun floor)`,
`Quota minima / Min Price (>= 1.02)`,
`Max Win € (cap vincita/payout per gamba)`,
`Max Stake % (avviso se esposizione operazione > % balance)`,
`Profit epsilon € (avviso se varianza profitto tra esiti > tolleranza)`,
`Auto-green delay (s) — grace prima del cashout (0.1-30)`.

Combobox `Risk Profile`: `CONSERVATIVE | BALANCED | AGGRESSIVE`
(default `BALANCED`).

Checkbox: `Allow Recovery` (ON), `Anti Duplication Enabled` (ON),
`Liquidity Guard attivo`,
`Liquidity: solo avviso (⚠ togliere = BLOCCA il submit)`,
`Max Win: solo avviso (⚠ togliere = BLOCCA il submit)` (ON),
`Auto-green: attiva grace prima del cashout (ritardo)` (OFF),
`Profit epsilon: mostra avviso varianza profitto` (ON).

Bottone `Salva Roserpina`. I campi hard-stop sono fail-closed: vuoto = non
impostato (il gate resta bloccante); valore non numerico o ≤0 (o % >100)
annulla il salvataggio.

## 8. Tab Simulazione

Testo descrittivo: parametri attivi solo in SIMULAZIONE; «La commissione
simulata e' bloccata al 4.5% (policy Betfair Italia) e non e' editabile
qui.» Entry `Bankroll iniziale simulazione (€)` (default `1000.0`);
checkbox `Partial fill abilitato` (ON), `Consuma liquidita' del book` (ON),
`Persisti stato simulazione` (ON); bottone `Salva Simulazione` (fail-closed
sui campi protetti `commission_pct`, `enabled`).

## 9. Tab Watchdog

Checkbox: `Watchdog anomalie ATTIVO  (⚠ disattivarlo spegne una guardia di
safety)` (default ON, tri-state: non configurato = ON), `Notifiche Telegram
delle anomalie` (OFF), `Azioni automatiche su anomalie  (⚠ consente al bot
azioni automatiche)` (OFF). Bottone `Salva Watchdog` → dialog «effetto entro
~5s».

## 10. Tab Alert

Severità: `INFO, WARNING, ERROR, HIGH, CRITICAL`. Checkbox: `Alert Telegram
ATTIVI  (⚠ disattivarli sopprime le notifiche di safety)` (OFF), `Deduplica
alert ripetuti` (ON), `Formato ricco (dettagli renderizzati)` (ON). Entry:
`Chat ID destinazione (obbligatorio se alert attivi)`, `Nome chat
(facoltativo)`, `Cooldown anti-spam (secondi, >= 0)` (default `300`).
Combobox `Severita' minima` (default `WARNING`). Bottone `Salva Alert`
(fail-closed: alert attivi senza Chat ID => errore «Chat ID obbligatorio
quando gli alert sono attivi.»).

## 11. Tab tabellari

- `Storico Bet`: colonne `ID Bet, Evento, Mercato, Selezione, Stake, Quota,
  Stato, Esito, Profitto, Data/Ora`; bottone `Forza refresh Storico Bet`.
- `Risk Desk`: colonne `Tavolo, Stato, Loss, Exposure, Event Key, Market ID,
  Selection ID`; bottone `Forza refresh Risk Desk`.
- `Storico Risk Desk`: colonne `ID, Data/Ora, Stato, Loss, Exposure, Evento,
  Mercato, Selezione`; bottone `Forza refresh Storico Risk Desk`.
- `Log`: solo `CTkTextbox` in append.

## 12. Tab Provider (solo GUI reale)

Label `Catalogo Betfair: Sincronizzazione Automatica Attiva` + info
`Ultimo Sync: {…} ({n} ev)`. Pannello sinistro `Provider` (Listbox scura
`#2b2b2b`). Pannello destro con sub-tab: `Nomi Eventi` (colonne `Alias,
Betfair Name, Country`), `Mercati` (`Frase, Tipo Mercato, Betfair Name,
Selezione`), `Parser Avanzati` (`Nome, Stato, Tipo`).

⚠ Nota tecnica (stato reale): `_on_sync_catalog` passa `on_success=` a
`_run_runtime_command_async` che non accetta quel parametro (bug latente);
nessun bottone risulta collegato a `_on_sync_catalog` in `mini_gui.py`.

## 13. Tab Telegram (`telegram_tab_ui.py`)

Unico modulo che usa `theme.COLORS`/`theme.FONTS`. Due colonne (sinistra
scrollabile).

- **Configurazione Telegram**: info «Ottieni API ID e Hash su
  my.telegram.org»; entry `API ID:`, `API Hash:`, `Numero di Telefono
  (+39...)`, `Stake Automatico (EUR)` (default `1.0`); checkbox `Piazza
  automaticamente` (OFF), `Richiedi conferma (solo se auto OFF)` (ON); riga
  auth `Codice:` + `2FA:` (mascherato) con bottoni `Invia Codice`,
  `Verifica`, `Reset Sessione` (rosso); label `Stato: {telegram_status}`
  (default `STOPPED`, `LISTENING` se connesso); bottoni `Salva`, `Avvia
  Listener` (verde), `Ferma` (rosso).
- **Chat Monitorate**: bottone `Rimuovi`; treeview `Nome Chat, Attivo`.
- **Bot (Bot API)**: descrizione (token cifrato/mascherato, «in modifica
  lascialo VUOTO», «non ancora attiva a runtime»); entry `Etichetta:`,
  `Bot Token (BotFather):` (mascherato); checkbox `Attivo`; bottoni `Nuovo`,
  `Salva/Aggiorna Bot` (verde), `Rimuovi Bot` (rosso); treeview `Etichetta,
  Token, Attivo`. Sotto-sezione `Chat del bot selezionato:` con `Chat ID:`,
  `Titolo (opzionale):`, bottoni `Aggiungi Chat al Bot` (verde), `Rimuovi
  Chat dal Bot` (rosso), treeview `Chat, Attivo`.
- **Chat Disponibili da Telegram**: bottoni `Carica/Aggiorna Chat`,
  `Aggiungi Selezionate` (verde); treeview con prima colonna senza titolo,
  poi `Tipo`, `Nome` (multi-select).
- **Regole di Parsing**: sub-label «Regex + market/side/template + filtri
  minuto/score/live/priority»; bottoni `Aggiungi` (verde), `Modifica`,
  `Elimina` (rosso), `Attiva/Disattiva`; treeview `ON, Nome, Mercato, Side,
  Template, Minuti, Score, Live, Prio, Pattern` (ON reso `✅`/`❌`, Live
  `YES`/`NO`).
- **Segnali Ricevuti** (colonna destra): treeview `Data, Selezione, Tipo,
  Quota, Stake, Stato` con tag `success` (verde) / `failed` (rosso);
  bottone `Aggiorna Segnali`.
- **Dialog editor regola**: titolo `Modifica Regola Parser` / `Nuova Regola
  Parser`, `600x700`, modale (`grab_set`). Entry `Nome Regola`, `Regex
  Pattern`, `Selection Template`, `Min Minuto`, `Max Minuto`, `Min Score
  (Totale)`, `Max Score (Totale)`, `Priorità`; combobox `Mercato`
  (`MATCH_ODDS, OVER_UNDER, BOTH_TEAMS_TO_SCORE`) e `Side` (`BACK, LAY`);
  checkbox `Solo Live` (ON); bottone `Salva Regola`.

## 14. Palette (`theme.py`) e semantica di sicurezza

`COLORS`: sfondi `bg_dark #111827`, `bg_panel #1f2937`, `bg_card #374151`,
`bg_hover #4b5563`; bordo `#6b7280`; testo `#f9fafb` / `#d1d5db` /
`#9ca3af`; stati `success #22c55e`, `error/loss #ef4444`, `warning #f59e0b`;
betting `back #2563eb` (blu), `lay #f43f5e` (rosso/rosa); bottoni
`button_primary #2563eb`, `button_secondary #475569`,
`button_success #16a34a`, `button_danger #dc2626`; telegram `#229ED9` /
`#1b8cc9`.
`FONTS`: heading `Segoe UI 14 bold`, subheading `12 bold`, body `11`,
small `9`, mono `Consolas 10`.

Semantica: `status_color()` mappa `OK/CONNECTED/ACTIVE/MATCHED` → verde,
`WARNING/PARTIAL/BORDERLINE` → ambra, `ERROR/FAILED/DISCONNECTED` → rosso.
Convenzione: **rosso = errore/perdita/pericolo/azioni distruttive; verde =
successo/avvio; ambra = warning**.

⚠ Incoerenza nota (stato reale): Dashboard e top bar NON usano `theme.py` ma
hex hardcoded paralleli (`#2fa26b` verde, `#cc9a06` ambra, `#d9534f` rosso,
`#9aa0a6` grigio, `#000000`/`#ff0000` per EMERGENCY STOP). Solo il tab
Telegram e il dialog regole leggono `theme.COLORS`.

## 15. Dialoghi e flussi di conferma (esaustivo)

- START rifiutato (fail-closed): `Avvio bloccato` — «La sincronizzazione
  della modalita' col runtime e' fallita: ripeti il toggle SIM/LIVE con
  successo prima di avviare.»
- Split-brain modalità: `EMERGENCY STOP` — «Sync modalita' fallita con LIVE
  attivo: trading fermato (LOCKDOWN). Per riprendere serve
  reset_emergency.»; `EMERGENCY STOP FALLITO` e `KILL-SWITCH NON
  DISPONIBILE` con istruzione di fermare manualmente il trading.
- Dialog di salvataggio per tab (`OK` / `Errore ...`).
- Regole parser: `showwarning("Attenzione", ...)` su selezione mancante;
  **unico yes/no dell'intera GUI**: `askyesno("Conferma", "Vuoi davvero
  eliminare questa regola?")`.

**Gap UX documentato (per il futuro lavoro design)**: nessuna conferma su
`AVVIA BOT`/`STOP BOT`/`EMERGENCY STOP`, sul passaggio a LIVE, sul LIVE
Gate né sul KILL Switch. Qualsiasi introduzione di flussi di conferma è una
modifica design/safety e deve aggiornare questo handoff.

## 16. Invarianti di sicurezza lato UI (dal codice)

- Default fail-closed: SIMULAZIONE ON, LIVE Gate OFF, KILL Switch OFF;
  avvio GUI sempre con `force_simulation=True`.
- LIVE effettivo solo con: execution `LIVE` + LIVE Gate ON + KILL Switch
  OFF; il kill switch forza `live_enabled=False`. La GUI passa
  `live_readiness_ok=False`: l'autorità sul deploy LIVE è del runtime.
- START rifiutato finché la sync di modalità non è confermata
  (`_mode_sync_failed`).
- Split-brain (ultima modalità confermata LIVE + sync fallita) => emergency
  stop automatico `mode_sync_failed_fail_closed`, con retry.
- Dedup comandi in-flight (nessun doppio comando runtime).
- Hard-stop Roserpina fail-closed; guard "solo avviso" con ⚠ che se
  disattivate BLOCCANO il submit.
- Watchdog default ON; Alert default OFF con Chat ID obbligatorio.
- Commissione simulata bloccata al 4.5%, non editabile.
- Segreti mascherati (password Betfair, 2FA, bot token); token mai
  ricaricato in chiaro.

## 17. `ui_panels/` — superficie separata NON collegata alla Mini GUI

Nessun file di `ui_panels/` è importato da `mini_gui.py`,
`telegram_tab_ui.py` o `headless_main.py`: sono widget ttk standalone di
observability (ObservabilityPanel con sub-tab `Health, Metrics, Alerts,
Incidents, Incident Timeline, Audit, Safe Mode, Diagnostics`; badge
`🟢 READY / 🟠 DEGRADED / 🔴 NOT_READY`; SafeModePanel con `Enable/Disable
Safe Mode` senza conferma yes/no; ExportPanel `Export Diagnostics ZIP`).
Se verranno integrati nella Mini GUI, l'integrazione è una modifica design
e va riportata qui.
