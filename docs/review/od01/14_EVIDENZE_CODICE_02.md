# Evidenze di codice per i finding D10-F01…F05 (citazione da main 028cee9, non modifica)

> Correzione richiesta implicitamente dalle review Sol/Grok del push 1: il codice citato non era nel diff. Qui le righe sono riportate testualmente con numero di riga. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO.

## D10-F04 (BF2-C-F02) — `services/streaming_feed.py` @ 028cee9
[services/streaming_feed.py#L341-L352](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/services/streaming_feed.py#L341-L352)
```python
341:     def _merge_market_change(self, change: Dict[str, Any]) -> Optional[Dict[str, Any]]:
342:         market_id = str(change.get("id") or change.get("marketId") or "").strip()
343:         if not market_id:
344:             return None
345: 
346:         state = self._get_or_init_market_state(market_id)
347:         self._merge_market_definition(state, change.get("marketDefinition"))
348:         for rc in change.get("rc") or []:
349:             if not isinstance(rc, dict):
350:                 continue
351:             self._merge_runner_change(state, rc)
352:         return self._state_to_market_book(state)
```

## D10-F05 (BF2-E-F04/E-F01) — `core/runtime_controller.py` @ 028cee9
[core/runtime_controller.py#L777-L784](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/core/runtime_controller.py#L777-L784)
```python
777:             for chunk_start in range(0, len(bot_bet_ids), 1000):
778:                 chunk = bot_bet_ids[chunk_start:chunk_start + 1000]
779:                 rows.extend(
780:                     self.betfair_service.list_cleared_orders(
781:                         bet_status="SETTLED",
782:                         settled_after=settled_after,
783:                         bet_ids=chunk,
784:                     )
```
[core/runtime_controller.py#L840-L841](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/core/runtime_controller.py#L840-L841)
```python
840:             settlement_key = f"cleared:{market_id}:{bet_id}"
841:             if settlement_key in self._settlement_emitted_keys:
```
