"""F-13 (#586, ID-01, D-582-01): ``customerOrderRef`` dei place diretti.

``customerRef`` (la chiave di de-dup della richiesta, 60 s) e ``customerOrderRef`` (l'identita' DELL'ORDINE, restituita
da ``listCurrentOrders``) hanno gli stessi vincoli Betfair: al massimo 32 caratteri del charset
``[A-Za-z0-9-._+*:;~]``. Un ref conforme diventa anche ``customerOrderRef``; uno non conforme o assente viene OMESSO,
mai troncato o ripulito (un ref alterato potrebbe coincidere con quello di un altro ordine) e mai motivo di rifiuto
dell'ordine (``BetfairClient.place_bet`` rifiuterebbe un ``customer_order_ref`` non conforme: qui non arriva mai).
Il pattern e' lo stesso di ``betfair_client._CUSTOMER_REF_RE`` (test di parita' in ``tests/f13``).
"""
from __future__ import annotations

import re
from typing import Any

CUSTOMER_REF_RE = re.compile(r"[A-Za-z0-9\-._+*:;~]{1,32}")


def customer_order_ref(customer_ref: Any) -> str:
    """Il ``customerOrderRef`` da inviare con l'ordine: ``customer_ref`` se conforme ai vincoli Betfair, altrimenti ''."""
    return customer_ref if isinstance(customer_ref, str) and CUSTOMER_REF_RE.fullmatch(customer_ref) else ""
