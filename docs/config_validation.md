# Validazione delle impostazioni Roserpina (F-18)

Riferimenti: roadmap #586, scheda F-18 (CFG-01, CFG-02, CFG-04, NF-16, NF-17/NEW-01). Decisioni owner del 18/10/2026
(nessun cap monetario attivo di default) e del 02/11/2026 (microstake 0,10 €, nessun minimo applicativo).

## 1. Un solo dominio, `core/config_validation.py`

`SPECS` descrive per ogni chiave Roserpina il tipo (numero, intero, booleano, enum) e il dominio che il runtime sa
trattare.

| Chiavi | Dominio |
|---|---|
| Percentuali di rischio (`target_profit_cycle_pct`, `max_single_bet_pct`, `max_total_exposure_pct`, `max_event_exposure_pct`, `max_drawdown_hard_stop_pct`, `max_stake_pct`) | (0, 100] |
| Soglie di drawdown ed espansione (`auto_reset_drawdown_pct`, `defense_drawdown_pct`, `lockdown_drawdown_pct`, `expansion_profit_pct`) | [0, 100] |
| `commission_pct` | [0, 100); default 4,5 (Betfair Italia) |
| Cap monetari opzionali (`max_daily_loss`, `max_open_exposure`, `max_order_exposure`, `max_market_exposure`, `max_event_exposure_abs`, `max_drawdown_abs`, `max_session_loss`, `max_exposure_stop`, `max_stake_abs`, `max_win`) | se presenti, > 0; assenti = **non attivi** |
| `max_recovery_chase_abs` | qualsiasi numero finito (≤ 0 = opt-out documentato) |
| Moltiplicatori, `min_stake`, `book_warning`, `book_block` | > 0 |
| `min_liquidity_absolute`, `profit_epsilon`, `auto_green_delay_sec` | ≥ 0 |
| `min_price` | [1.01, 1000] |
| `table_count`, `max_recovery_tables` | interi in [1, 50], [0, 50] |
| Interruttori | booleano esplicito: bool, 0/1, `true/false`, `yes/no`, `on/off` |
| `risk_profile` | `CONSERVATIVE`, `BALANCED`, `AGGRESSIVE` |

Regola di coerenza: `book_block` ≥ `book_warning`.

Non ci sono minimi monetari: `min_stake` deve solo essere > 0, quindi 0,10 € è valido e non viene mai alzato. Un numero
può usare la virgola decimale (`"0,10"`). NaN, inf, un bool al posto di un numero e il testo non sono mai numeri.

## 2. Al salvataggio

`SettingsService.save_settings` rifiuta l'intero salvataggio (`InvalidSettingsError`, con tutti i motivi) prima di
qualsiasi scrittura. Succede quando una chiave Roserpina è illeggibile o fuori dominio, oppure quando una coppia è
incoerente con quanto già salvato. La configurazione precedente resta intatta. La tab Roserpina della GUI mostra
l'errore («Errore salvataggio Roserpina»). Assente, `None` o `""` non sono errori.

## 3. Al caricamento (DB legacy o corrotto)

Un valore presente ma invalido non diventa mai il default di codice in silenzio:
- numeri con default di codice: NaN, e la chiave va in `RoserpinaConfig.invalid_settings`;
- interi: il default resta, così il conteggio dei tavoli rimane utilizzabile, ma la chiave è registrata;
- interruttori: il valore che protegge (`anti_duplication_enabled`, `liquidity_guard_enabled`,
  `profit_epsilon_enabled` attivi; `allow_recovery`, i `*_warning_only` e `auto_green_delay_enabled` disattivi), con la
  chiave registrata;
- `risk_profile`: `BALANCED`, con la chiave registrata.

Con `invalid_settings` non vuoto, `validators.risk_pcts_reason` nega ogni nuova entrata con
`configurazione_non_valida:<chiave>`. Vale per il Money Management, il Dutching e l'indicatore della GUI. Le uscite
protettive (hedge del cashout) non passano da questo controllo.

Le percentuali obbligatorie e i cap opzionali conservano il loro loader. Un valore illeggibile diventa NaN, e il
consumer nega ogni valore non finito o ≤ 0 con il proprio motivo (`<chiave>_non_valido`). Questi valori sono comunque
rifiutati al salvataggio.

Il caricamento di un DB corrotto non va mai in crash.

Test: `tests/f18/`; mutazioni `f18_*` in `guardrails/mutation_catalog.json`.
