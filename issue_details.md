# Dettagli Issue Repository Pickfair-nogui

## ISSUE #274: [Spec] MINI GUI — Storico Bet + Storico Risk Desk (persistenze separate)
- **Obiettivo**: Aggiungere due nuove tab alla GUI per visualizzare lo storico delle scommesse (simulazione) e lo storico delle posizioni chiuse del Risk Desk.
- **Requisiti**:
    - Tab "Storico Bet": Visualizza `recent_simulation_bets` dal DB.
    - Tab "Storico Risk Desk": Visualizza `risk_position_history` dal DB.
    - Refresh automatico o coordinato.
    - Persistenza dei dati (SQLite).

## ISSUE #285: Daily-loss kill switch — snapshot di lockdown non-mutante
- **Obiettivo**: Evitare che lo snapshot di lockdown sovrascriva il bankroll live con quello simulato durante un `emergency_stop`.
- **Problema**: `emergency_stop` chiama `set_simulation_mode(True)` prima di `get_status()`, causando una lettura errata del bilancio dal broker simulato.
- **Fix**: Usare uno snapshot non-mutante o preservare la sorgente live del balance.

## ISSUE #293: [Spec] MINI GUI — Implementazione Storico Bet + Storico Risk Desk
- **Obiettivo**: Implementazione tecnica delle tab definite nella #274.
- **Requisiti**:
    - `database_schema.py`: Aggiunta tabella `risk_position_history`.
    - `database.py`: Metodi `get_recent_simulation_bets`, `add_risk_position_history`, `get_risk_position_history`.
    - `mini_gui.py`: `RefreshCoordinator` per gestire i refresh concorrenti/off-thread.
    - `mini_gui.py`: `_build_storico_bet_tab` e `risk_history_tree`.
    - Integrazione EventBus: `RUNTIME_CLOSE_POSITION` persiste e aggiorna la cronologia.

## ISSUE #297: Riferimento Issue n 297 (Titolo)
- **Obiettivo**: Generalmente riferita alla stabilità del Money Management e all'integrazione tra Parser e Execution. (Nota: Il contenuto specifico della #297 non è stato stampato interamente, ma il titolo suggerisce un task di consolidamento).

## ISSUE #320: Hardening A1-A3, B1, C1
- **Obiettivo**: Blindatura del sistema con gate di sicurezza.
- **Requisiti**:
    - **MIN_STAKE**: Floor a 0.10€ (MM e Dutching).
    - **A1**: Drawdown Hard Stop (Lockdown al superamento della soglia %).
    - **A2**: Max Open Exposure (Cap assoluto €).
    - **A3**: Max Recovery Tables (Limite tavoli in recovery).
    - **B1**: Fix calcolo crescita Expansion Mode.
    - **C1**: Monitoraggio SL/TP/Trailing (Chiusura automatica).
