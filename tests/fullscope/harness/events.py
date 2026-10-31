"""F-33 — prefissi degli eventi del bus conservati negli artifact del child runner.

Modulo separato (importato dal child e testato) perche' un prefisso mancante rende un test cieco: e' successo con
`SL_`/`STOP_LOSS` (rilievo Codex/Sol su d46e6da), per cui la sonda dello stop-loss non avrebbe mai visto il trigger.
"""
INTERESTING = ("QUICK_BET", "SETTLEMENT_", "EMERGENCY", "RUNTIME_LOCK", "SIGNAL_REJ", "RUNTIME_CLOSE", "ROSERPINA",
               "BANKROLL_SYNC", "RISK_STOP", "CASHOUT", "REQ_EXECUTE", "CMD_QUICK", "RUNTIME_START", "AUTO_TRADE",
               "SL_", "TP_", "STOP_LOSS", "TAKE_PROFIT", "TRAILING")


def keep(event_name: str) -> bool:
    return str(event_name).startswith(INTERESTING)
