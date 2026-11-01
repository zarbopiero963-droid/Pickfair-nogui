"""F-05 (CSH-06) — identita' delle righe `orders` che rappresentano l'hedge di un cashout.

L'hedge del cashout passa da ``OrderRouter`` e muove denaro reale, ma non e' un ingresso: non ha tavolo, evento,
riserva ne' batch. Da F-05 viene scritto nel ledger ``orders`` (write-ahead, poi esito) con:

- ``customer_ref`` = ``hedge:`` + 26 esadecimali (32 caratteri: il limite del ``customerOrderRef`` Betfair);
- payload con ``role = "HEDGE"``, ``order_origin = "CASHOUT_HEDGE"``, senza ``event_key``/``table_id``/``batch_id``;
- stati propri (``HEDGE_*``), mai ``INFLIGHT``/``SUBMITTED``/``AMBIGUOUS``: le letture degli ordini APERTI del bot
  (de-dup dell'engine, riconciliazione all'avvio, rebuild F018) non li vedono e non creano tavoli fantasma.

I consumer che attribuiscono un ordine a un tavolo (``_resolve_market_settlement``, ``_single_bet_position``) li
escludono con :func:`is_hedge_order_ref`. L'identita' I1 del bot (``Database.get_bot_active_orders``) li INCLUDE:
il daily-loss LIVE vede la bet dell'hedge e il router del cashout vede la posizione come mista (nessun secondo
hedge della stessa posizione).
"""
from __future__ import annotations

import uuid

HEDGE_REF_PREFIX = "hedge:"
HEDGE_ROLE = "HEDGE"
HEDGE_ORIGIN = "CASHOUT_HEDGE"

HEDGE_PENDING = "HEDGE_PENDING"          # write-ahead: scritto PRIMA dell'invio (crash dopo l'invio => traccia)
HEDGE_PLACED = "HEDGE_PLACED"            # abbinato (in tutto o in parte)
HEDGE_UNMATCHED = "HEDGE_UNMATCHED"      # accettato ma a riposo
HEDGE_FAILED = "HEDGE_FAILED"            # rifiutato dall'exchange / non piazzato
HEDGE_AMBIGUOUS = "HEDGE_AMBIGUOUS"      # esito ignoto dopo l'invio: mai retry, riconciliazione

HEDGE_STATUSES = frozenset({HEDGE_PENDING, HEDGE_PLACED, HEDGE_UNMATCHED, HEDGE_FAILED, HEDGE_AMBIGUOUS})


def new_hedge_ref() -> str:
    return HEDGE_REF_PREFIX + uuid.uuid4().hex[:26]


def is_hedge_order_ref(ref: object) -> bool:
    return str(ref or "").startswith(HEDGE_REF_PREFIX)
