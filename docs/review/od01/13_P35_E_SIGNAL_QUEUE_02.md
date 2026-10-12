        `dedup_key` (#192): chiave di deduplica per-riga con cui la riga è piazzata, memorizzata
        sul segnale per il riconoscimento provenienza-esatta dei duplicati ancora attivi.

        `signal_id` assente → generato automaticamente. `timeout` assente →
        `default_timeout`."""
        now = self._resolve_now(now)
        timeout = self.default_timeout if timeout is None else self._validate_timeout(timeout)
        if signal_id is None:
            # Id auto-generato che NON collide con un id fornito dal chiamante:
            # altrimenti un "s1" esplicito verrebbe sovrascritto dal primo add()
            # senza id (stesso "s1") e una riga ancora attiva sparirebbe.
            existing = {a.signal_id for a in self._active}
            self._counter += 1
            while f"s{self._counter}" in existing:
                self._counter += 1
            signal_id = f"s{self._counter}"
        sig = ActiveSignal(signal_id, dict(row), now, timeout, str(dedup_key or ""))
        if self.mode == OVERWRITE_LAST:
            self._active = [sig]
            return signal_id
        is_update = any(a.signal_id == signal_id for a in self._active)
        # Tetto: blocca SOLO un nuovo segnale (non l'aggiornamento di uno già attivo) che
        # porterebbe il numero di righe attive oltre `max_active` (#136 punto 5). `force=True`
        # bypassa il tetto per NON spezzare il blocco di un singolo messaggio multi (#192).
        if not force and not is_update and self.max_active and len(self._active) >= self.max_active:
            return None
        self._active = [a for a in self._active if a.signal_id != signal_id]
        self._active.append(sig)
        return signal_id
```

Record D03 BLOCKED interessati: DIFF-PF-B1-QUEUE-OVERWRITE, -CAP, -FORCE, -EXPIRY, -RESTORE.
