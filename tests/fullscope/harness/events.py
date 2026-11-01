"""F-33 — prefissi degli eventi del bus conservati negli artifact del child runner.

Modulo separato (importato dal child e testato) perche' un prefisso mancante rende un test cieco: e' successo con
`SL_`/`STOP_LOSS` (rilievo Codex/Sol su d46e6da), per cui la sonda dello stop-loss non avrebbe mai visto il trigger.

``bus_drained``: condizione di quiete del bus usata dalle attese del child (coda vuota e ogni callback accodato
terminato), testata con l'EventBus reale.
"""
INTERESTING = ("QUICK_BET", "SETTLEMENT_", "EMERGENCY", "RUNTIME_LOCK", "SIGNAL_REJ", "RUNTIME_CLOSE", "ROSERPINA",
               "BANKROLL_SYNC", "RISK_STOP", "CASHOUT", "REQ_EXECUTE", "CMD_QUICK", "RUNTIME_START", "AUTO_TRADE",
               "SL_", "TP_", "STOP_LOSS", "TAKE_PROFIT", "TRAILING")


def keep(event_name: str) -> bool:
    return str(event_name).startswith(INTERESTING)


def bus_drained(bus) -> bool:
    """True se il bus non ha callback in coda ne' in esecuzione (vedi ``bus_idle``)."""
    try:
        p = bus.pressure_snapshot()
        return int(p["queue_depth"]) == 0 and int(p["dequeued_total"]) >= int(p["enqueued_total"])
    except Exception:  # noqa: BLE001 - snapshot non disponibile: fail-closed, il bus non si dichiara fermo
        return False
