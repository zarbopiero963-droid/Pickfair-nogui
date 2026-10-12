# Evidenze di codice per i finding D10-F01…F05 (citazione da main 028cee9, non modifica)

> Correzione richiesta implicitamente dalle review Sol/Grok del push 1: il codice citato non era nel diff. Qui le righe sono riportate testualmente con numero di riga. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO.

## D10-F01 (BF2-A-F01) — `betfair_client.py` @ 028cee9
[betfair_client.py#L1683-L1691](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/betfair_client.py#L1683-L1691)
```python
1683:             status = str(result.get("status") or "").upper()
1684:             reports = result.get("instructionReports") or []
1685: 
1686:             if status != "SUCCESS":
1687:                 # errorCode nel testo => la classificazione order_unknown sotto puo'
1688:                 # riconoscere DUPLICATE_TRANSACTION (dedup customerRef, #PR-C/#452):
1689:                 # la prima bet e' VIVA, non e' un fallimento definitivo.
1690:                 error_code = str(result.get("errorCode") or "").strip()
1691:                 raise RuntimeError(f"BET_FAILED: {status} {error_code}".strip())
```
[betfair_client.py#L1724-L1731](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/betfair_client.py#L1724-L1731)
```python
1724:                 "order_unknown": any(
1725:                     marker in error_upper
1726:                     for marker in (
1727:                         "TIMEOUT", "NETWORK_ERROR", "HTTP_5", "UNKNOWN_ERROR",
1728:                         "DUPLICATE_TRANSACTION",
1729:                     )
1730:                 ) or error_text in _RISPOSTA_ILLEGGIBILE_DOPO_L_INVIO,
1731:             }
```

## D10-F02 (BF2-R-F01) — `core/reconciliation_engine.py` @ 028cee9
[core/reconciliation_engine.py#L714-L719](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/core/reconciliation_engine.py#L714-L719)
```python
714:     def _extract_customer_ref(self, order: Dict[str, Any]) -> str:
715:         return self._first_of(
716:             order,
717:             "customerOrderRef", "customer_ref",
718:             "customerRef", "customerOrderReference",
719:         )
```
[core/reconciliation_engine.py#L984-L987](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/core/reconciliation_engine.py#L984-L987)
```python
984:                 "remote": [
985:                     {
986:                         "ref": o.get("customerOrderRef", o.get("customer_ref", "")),
987:                         "bid": o.get("betId", o.get("bet_id", "")),
```

## D10-F03 (BF2-C-F01) — `services/streaming_feed.py` @ 028cee9
[services/streaming_feed.py#L260-L270](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/services/streaming_feed.py#L260-L270)
```python
260:     def _process_message(self, message: Any) -> None:
261:         with self._lock:
262:             self._last_message_at = time.monotonic()
263: 
264:         if isinstance(message, dict):
265:             status_code = str(message.get("status") or "")
266:             if status_code == "503":
267:                 with self._lock:
268:                     self._degraded_503 = True
269:                 logger.warning("Betfair stream in degraded mode (503): continuing without disconnect")
270:                 return
```
