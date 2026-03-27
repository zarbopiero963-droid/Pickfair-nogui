from __future__ import annotations


class OrderRouter:
    """
    Router unico:
    - simulation vs live
    """

    def __init__(self, betfair_service):
        self.service = betfair_service

    def place(self, payload: dict):
        client = self.service.get_client()

        return client.place_bet(
            market_id=payload["market_id"],
            selection_id=payload["selection_id"],
            side=payload["bet_type"],
            price=payload["price"],
            size=payload["stake"],
            customer_ref=payload.get("customer_ref", ""),
            event_key=payload.get("event_key", ""),
            table_id=payload.get("table_id"),
            batch_id=payload.get("batch_id", ""),
        )