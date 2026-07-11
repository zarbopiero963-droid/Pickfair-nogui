# Analisi Implementazione Issue #320

## 1. Allineamento MIN_STAKE = 0.10
- **trading_config.py**: 🔴 **NON IMPLEMENTATO**. `MIN_STAKE` è ancora impostato a `2.0`.
- **core/system_state.py**: ✅ **IMPLEMENTATO**. `RoserpinaConfig.min_stake` è `0.10`.
- **core/money_management.py**: ✅ **IMPLEMENTATO**. Il floor `0.10` viene applicato tramite `_clamp` nel calcolo dello stake.
- **telegram_listener.py**: 🔴 **PARZIALE**. Non c'è un riferimento esplicito a `MIN_STAKE` per il floor dello stake estratto, sebbene il MM lo applichi successivamente.
- **dutching.py**: 🔴 **NON IMPLEMENTATO**. Non esiste una logica di floor per-runner a `0.10`.

## 2. Hardening (A-D)
- **A1 (Drawdown Hard Stop)**: 🔴 **NON IMPLEMENTATO**. Viene validato in `_validate_live_hard_stop_config` ma non c'è logica di enforcement a runtime in `runtime_controller.py`.
- **A2 (Max Open Exposure)**: 🔴 **NON IMPLEMENTATO**. Validato ma non applicato come cap assoluto nel calcolo del MM (usa solo %).
- **A3 (Max Recovery Tables)**: 🔴 **NON IMPLEMENTATO**. Il `table_manager.py` non limita il numero di tavoli in recovery in base a `max_recovery_tables`.
- **B1 (Expansion Mode Bug)**: ✅ **IMPLEMENTATO**. La logica in `money_management.py` è stata corretta per calcolare `growth_pct` rispetto a `equity_peak`.
- **C1 (SL/TP/Trailing)**: 🔴 **NON IMPLEMENTATO**. I campi sono accettati nel payload (`risk_middleware.py`) ma non consumati da nessuna logica di chiusura.
- **D2 (Staking a recupero)**: ⚠️ **RISCHIO**. La logica di recupero è attiva ma senza i cap di sicurezza (A1-A3) implementati, il rischio di perdite accelerate è reale.

## Conclusione
L'implementazione della Issue #320 è **PARZIALE**. Mentre la correzione del bug della modalità Expansion e il floor del MM sono ok, mancano i gate di sicurezza critici (Hardening A1, A2, A3) e l'allineamento globale della costante `MIN_STAKE`.
