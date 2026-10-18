# Riesame indipendente ciclo 6 (agente separato, non anticipato sulle conclusioni) — diff cycle6_code.diff su 5ef61da+patch

Esito: fix 1 (riserva preesistente) e fix 2 (lock esecutore) corretti, nessun difetto; nessun deadlock RLock/_lock
(ordine sempre _lock -> _microstake_executor_lock); fix 3 corretto sul comportamento.
Rilievi: (1) CURRENT_DEFECT di pacchetto: due test esistenti in tests/unit/test_betfair_service_list_current_orders.py
codificavano il filtro silenzioso => allineati a D-C nel commit 49f7ee8 (docstring compreso);
(2) GUARDRAIL_GAP: fallback `has_reservation` assente -> False (rilascio) non fail-closed => default True (riserva
conservata) + has_reservation su StatefulExchangeSimulator + test RED/GREEN
`test_adapter_without_has_reservation_keeps_the_reservation_on_unreadable_store`;
(3) THEORETICAL: finestra has_reservation->reserve non atomica sullo stesso ref: lo stesso ref non passa due volte in
concorrenza (dedupe durevole engine), non corrente; (4) NONE: cancel_order su payload malformato ora solleva invece di
NOT_CURRENT (fail-closed, dichiarato nel commit). Suite mirate con stub betfairlightweight: 231 passed, 2 failed
(= rilievo 1, corretto).
