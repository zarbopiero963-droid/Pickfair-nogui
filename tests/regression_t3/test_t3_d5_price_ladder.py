"""Decisione owner D5 (PF-OWNER-GATEA-FIVE-DECISIONS-20261111): validazione OBBLIGATORIA delle quote secondo la price
ladder del mercato. Per la scala CLASSIC Betfair (incrementi ufficiali: 1.01-2 0.01; 2-3 0.02; 3-4 0.05; 4-6 0.1;
6-10 0.2; 10-20 0.5; 20-30 1; 30-50 2; 50-100 5; 100-1000 10; massimo 1000) una quota fuori tick o oltre il massimo
non deve raggiungere il trasporto. ID: PF-I04T-03 (riga rettificata da D5).

Stato su main c535c1d5: la validazione NON esiste (CODE_MISSING): `validators.order_price` controlla solo finita > 1.0
e il client accetta 2.03 e 1000.5. I casi di rifiuto restano xfail(strict) finche' la correzione non arriva; i casi
validi passano gia' e restano verdi (nessuna normalizzazione silenziosa: la quota inviata e' quella richiesta)."""
import pytest

from betfair_client import BetfairClient


class _CountingSession:
    def __init__(self):
        self.posts = 0
        self.payloads = []

    def post(self, *args, **kwargs):
        self.posts += 1
        self.payloads.append(kwargs.get("json") or kwargs.get("data"))

        class _R:
            status_code = 200

            def json(self_inner):
                return {"jsonrpc": "2.0", "result": {"status": "SUCCESS", "instructionReports": [
                    {"status": "SUCCESS", "betId": "1", "sizeMatched": 0.0}]}, "id": 1}
        return _R()


def _client(session):
    c = BetfairClient(username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem", session=session)
    c.session_token = "TOK"
    return c


OFF_LADDER = [2.03, 3.01, 4.05, 6.1, 10.3, 20.5, 31.0, 52.0, 105.0, 1000.5, 1010.0]


@pytest.mark.xfail(strict=True, reason="PF-I04T-03 / D5: validazione price ladder CLASSIC assente su main (CODE_MISSING)")
@pytest.mark.parametrize("price", OFF_LADDER, ids=repr)
def test_pf_i04t_03_off_ladder_price_never_reaches_transport(price):
    """ID: PF-I04T-03"""
    session = _CountingSession()
    with pytest.raises(RuntimeError):
        _client(session).place_bet(market_id="1.234", selection_id=5678, side="BACK", price=price, size=5.0)
    assert session.posts == 0


@pytest.mark.parametrize("price", [1.01, 1.99, 2.02, 3.05, 4.1, 6.2, 10.5, 20.0, 30.0, 50.0, 100.0, 1000.0], ids=repr)
def test_pf_i04t_03_on_ladder_price_is_sent_unchanged(price):
    """ID: PF-I04T-03 — una quota valida passa e non viene normalizzata in silenzio."""
    session = _CountingSession()
    try:
        _client(session).place_bet(market_id="1.234", selection_id=5678, side="BACK", price=price, size=5.0)
    except Exception:
        pass  # l'esito del parsing della risposta finta non e' oggetto di questo test
    assert session.posts == 1
    assert repr(price) in str(session.payloads[0]) or str(price) in str(session.payloads[0])
