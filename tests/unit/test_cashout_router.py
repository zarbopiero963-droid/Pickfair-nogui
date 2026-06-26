"""Unit test per CashoutRouter (Fase 2.1-B, design A3 + identità DB I1)."""

from cashout_router import CASHOUT_FAILED, REQ_EXECUTE_CASHOUT, CashoutRouter


def _curr(bet_id="B1", market="1.1", sel=7, side="BACK", matched=10.0, remaining=0.0, avg=2.0):
    return {
        "betId": bet_id, "marketId": market, "selectionId": sel, "side": side,
        "sizeMatched": matched, "sizeRemaining": remaining, "averagePriceMatched": avg,
    }


def _bot(bet_id="B1", market="1.1", sel=7, event="inter vs milan"):
    return {"bet_id": bet_id, "market_id": market, "selection_id": sel, "event_name": event}


def _book(market="1.1", sel=7, status="OPEN", back=2.6, lay=1.5, size=100.0):
    return {
        "marketId": market, "status": status,
        "runners": [{
            "selectionId": sel,
            "availableToBack": [{"price": back, "size": size}],
            "availableToLay": [{"price": lay, "size": size}],
        }],
    }


class _Harness:
    def __init__(self, current, bot, books, raise_current=False,
                 raise_book_for=None, raise_cancel=False, cancel_result=True,
                 raise_bot=False):
        self.published = []
        self.cancelled = []
        self._current = current
        self._bot = bot
        self._books = books
        self._raise = raise_current
        self._raise_book_for = set(raise_book_for or ())
        self._raise_cancel = raise_cancel
        self._cancel_result = cancel_result
        self._raise_bot = raise_bot

    def _fetch_current(self):
        if self._raise:
            raise RuntimeError("LIVE_BLOCKED_SESSION_INVALID")
        return self._current

    def _fetch_bot(self):
        if self._raise_bot:
            raise RuntimeError("DB_LOCKED")
        return self._bot

    def _fetch_book(self, market):
        if market in self._raise_book_for:
            raise RuntimeError("BOOK_SNAPSHOT_FAILED")
        return self._books.get(market)

    def _cancel(self, market, ids):
        if self._raise_cancel:
            raise RuntimeError("CANCEL_FAILED")
        self.cancelled.append((market, ids))
        return self._cancel_result

    def router(self):
        return CashoutRouter(
            fetch_current_orders=self._fetch_current,
            fetch_bot_orders=self._fetch_bot,
            fetch_market_book=self._fetch_book,
            cancel_orders=self._cancel,
            publish=lambda t, p: self.published.append((t, p)),
            commission_pct=4.5,
        )

    def reqs(self):
        return [p for t, p in self.published if t == REQ_EXECUTE_CASHOUT]


def test_cashout_all_publishes_one_req_per_bot_position():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0, avg=2.0),
                 _curr("B2", "1.2", 9, "LAY", 8.0, avg=1.8)],
        bot=[_bot("B1", "1.1", 7, "inter vs milan"),
             _bot("B2", "1.2", 9, "roma vs lazio")],
        books={"1.1": _book("1.1", 7, lay=1.5), "1.2": _book("1.2", 9, back=2.2)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 2
    reqs = h.reqs()
    by_market = {r["market_id"]: r for r in reqs}
    assert by_market["1.1"]["side"] == "LAY"   # chiusura di un BACK
    assert by_market["1.2"]["side"] == "BACK"  # chiusura di un LAY
    assert all(r["stake"] > 0 and "green_up" in r for r in reqs)


def test_cashout_single_restricted_to_event_name():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B2", "1.2", 9, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7, "Inter vs Milan"),
             _bot("B2", "1.2", 9, "Roma vs Lazio")],
        books={"1.1": _book("1.1", 7), "1.2": _book("1.2", 9)},
    )
    out = h.router().route({"signal_type": "CASHOUT", "event_name": "inter vs milan"})
    assert out["published"] == 1
    assert h.reqs()[0]["market_id"] == "1.1"


def test_cashout_single_without_event_name_publishes_nothing():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7, "Inter vs Milan")],
        books={"1.1": _book("1.1", 7)},
    )
    out = h.router().route({"signal_type": "CASHOUT"})
    assert out["published"] == 0
    assert h.reqs() == []


def test_non_bot_orders_are_not_closed():
    # B9 non è negli ordini del bot => non deve diventare un cashout.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B9", "1.2", 9, "BACK", 50.0)],  # bet manuale
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7), "1.2": _book("1.2", 9)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1
    assert h.reqs()[0]["market_id"] == "1.1"


def test_suspended_market_is_skipped_best_effort():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B2", "1.2", 9, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7), _bot("B2", "1.2", 9)],
        books={"1.1": _book("1.1", 7, status="SUSPENDED"), "1.2": _book("1.2", 9)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1   # solo il mercato OPEN
    assert out["skipped"] == 1
    assert h.reqs()[0]["market_id"] == "1.2"


def test_missing_status_book_is_skipped_fail_closed():
    # CodeRabbit/Codacy: un book senza status (parziale/malformato) NON e'
    # tradabile => fail-closed, niente cashout pubblicato.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7, status="")},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert h.reqs() == []


def test_missing_book_is_skipped():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={},  # nessun book
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0


def test_resting_is_cancelled_before_publishing():
    # Ordine parzialmente abbinato (matched 6 + resting 4): il resting va
    # cancellato prima del cashout.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1
    assert h.cancelled == [("1.1", ["B1"])]


def test_unconfirmed_cancel_skips_position_fail_closed():
    # Codex P1: cancel_orders che ritorna un esito NON confermato (falsy) senza
    # sollevare => niente cashout (il resting vivo potrebbe abbinarsi dopo l'hedge).
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
        cancel_result=False,
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert h.reqs() == []


def test_cancel_live_failure_dict_skips_position():
    # Codex P1: BetfairClient ritorna {"ok": False} su errore SENZA sollevare;
    # un dict non vuoto e' truthy, quindi serve ispezionarne l'esito => skip.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
        cancel_result={"ok": False, "error": "CANCEL_FAILED: X"},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert h.reqs() == []


def test_cancel_sim_per_instruction_failure_skips_position():
    # Codex P1: SimulationBroker ritorna status top-level SUCCESS ma con un report
    # per-istruzione FAILURE => non confermato, skip (fail-closed).
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
        cancel_result={"status": "SUCCESS",
                       "instructionReports": [{"status": "FAILURE"}], "simulated": True},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert h.reqs() == []


def test_cancel_confirmed_success_dict_publishes():
    # Esito confermato (report SUCCESS) => si procede al cashout.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
        cancel_result={"status": "SUCCESS",
                       "instructionReports": [{"status": "SUCCESS"}], "simulated": True},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1
    assert h.cancelled == [("1.1", ["B1"])]


def test_empty_event_name_does_not_overwrite_valid_mapping():
    # Codex P2: una riga DB successiva con event_name vuoto NON deve cancellare la
    # mappa market->event valida di una riga precedente (stessa partita).
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7, "Inter vs Milan"),
             _bot("B2", "1.1", 7, "")],  # stesso mercato, event_name vuoto
        books={"1.1": _book("1.1", 7)},
    )
    out = h.router().route({"signal_type": "CASHOUT", "event_name": "inter vs milan"})
    assert out["published"] == 1
    assert h.reqs()[0]["market_id"] == "1.1"


def test_cancel_nested_instruction_failure_skips_position():
    # Codex P1: il wrapper live di BetfairClient ritorna ok/status SUCCESS al top
    # e annida i report grezzi sotto result["result"]["instructionReports"]; un
    # report annidato non-SUCCESS (es. TIMEOUT) deve comunque bloccare il cashout.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0, avg=2.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7, back=2.0, lay=2.1)},
        cancel_result={"ok": True, "status": "SUCCESS",
                       "result": {"status": "SUCCESS",
                                  "instructionReports": [{"status": "TIMEOUT"}]}},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert h.reqs() == []


def test_back_position_prices_hedge_from_available_to_back():
    # Codex P1: l'hedge LAY di una posizione BACK matcha (nel motore del repo)
    # contro availableToBack; su spread normale (back<lay) prezzare da
    # availableToLay lo lascerebbe non abbinato.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0, avg=2.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7, back=2.0, lay=2.1)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1
    req = h.reqs()[0]
    assert req["side"] == "LAY"
    assert req["price"] == 2.0  # availableToBack[0], non 2.1


def test_lay_position_prices_hedge_from_available_to_lay():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "LAY", 10.0, avg=2.1)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7, back=2.0, lay=2.1)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1
    req = h.reqs()[0]
    assert req["side"] == "BACK"
    assert req["price"] == 2.1  # availableToLay[0], non 2.0


def test_bot_orders_db_error_publishes_failure_fail_closed():
    # Codex P2: se la lettura DB degli ordini bot solleva, si pubblica
    # CASHOUT_FAILED e si aborta (nessuna chiusura alla cieca).
    h = _Harness(current=[], bot=[], books={}, raise_bot=True)
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert any(t == CASHOUT_FAILED for t, _ in h.published)


def test_current_orders_error_aborts_fail_closed():
    h = _Harness(current=[], bot=[_bot("B1", "1.1", 7)], books={}, raise_current=True)
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert any(t == CASHOUT_FAILED for t, _ in h.published)


def test_no_bot_orders_does_nothing():
    h = _Harness(current=[_curr("B9", "1.1", 7)], bot=[], books={"1.1": _book("1.1", 7)})
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert out["reason"] == "no_bot_orders"


def test_mixed_position_is_skipped_by_resolver():
    # Stessa selezione con matched BACK e LAY (mista) => il resolver la salta.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B2", "1.1", 7, "LAY", 4.0)],
        bot=[_bot("B1", "1.1", 7), _bot("B2", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0


def test_book_fetch_error_on_one_market_does_not_abort_batch():
    # Greptile P1: se fetch_market_book solleva per UN mercato, le altre
    # posizioni OPEN del batch CASHOUT_ALL devono comunque essere chiuse.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", 10.0),
                 _curr("B2", "1.2", 9, "BACK", 10.0)],
        bot=[_bot("B1", "1.1", 7), _bot("B2", "1.2", 9)],
        books={"1.1": _book("1.1", 7), "1.2": _book("1.2", 9)},
        raise_book_for={"1.1"},  # il book di 1.1 solleva
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1          # 1.2 chiuso nonostante l'errore su 1.1
    assert out["skipped"] == 1
    assert h.reqs()[0]["market_id"] == "1.2"


def test_failed_cancel_skips_position_fail_closed():
    # Greptile P1 / Codacy HIGH: se il cancel del resting fallisce, NON si
    # pubblica il cashout (il resting vivo potrebbe abbinarsi dopo l'hedge).
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
        raise_cancel=True,
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert out["skipped"] == 1
    assert h.reqs() == []


def test_insufficient_depth_rejects_cashout():
    # L95 (R1): lo stake dell'hedge supera la size disponibile al prezzo
    # eseguibile => skip (meglio non chiudere che chiudere a metà).
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=100.0, avg=2.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7, back=2.0, lay=2.1, size=1.0)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 0
    assert h.reqs() == []


def test_sufficient_depth_publishes_cashout():
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=10.0, avg=2.0)],
        bot=[_bot("B1", "1.1", 7)],
        books={"1.1": _book("1.1", 7, back=2.0, lay=2.1, size=1000.0)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1


def test_pure_resting_is_flattened_on_cashout_all():
    # L147: un ordine bot del tutto NON abbinato (matched 0, remaining 5) non forma
    # una posizione ma va cancellato, altrimenti si abbinerebbe dopo il cashout.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=10.0, remaining=0.0),
                 _curr("B2", "1.2", 9, "BACK", matched=0.0, remaining=5.0)],
        bot=[_bot("B1", "1.1", 7), _bot("B2", "1.2", 9)],
        books={"1.1": _book("1.1", 7), "1.2": _book("1.2", 9)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1                 # solo la posizione su 1.1
    assert ("1.2", ["B2"]) in h.cancelled        # il pure-resting su 1.2 è flattato


def test_pure_resting_flatten_restricted_to_event_for_single_cashout():
    # Il flatten del singolo CASHOUT è ristretto ai mercati della partita.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=0.0, remaining=5.0),
                 _curr("B2", "1.2", 9, "BACK", matched=0.0, remaining=5.0)],
        bot=[_bot("B1", "1.1", 7, "Inter vs Milan"),
             _bot("B2", "1.2", 9, "Roma vs Lazio")],
        books={"1.1": _book("1.1", 7), "1.2": _book("1.2", 9)},
    )
    h.router().route({"signal_type": "CASHOUT", "event_name": "inter vs milan"})
    assert ("1.1", ["B1"]) in h.cancelled            # partita target
    assert all(m != "1.2" for m, _ in h.cancelled)   # NON l'altra partita


def test_pure_resting_sharing_market_is_not_double_cancelled():
    # Greptile P2: un ordine puro che condivide market+selection con una posizione
    # è già cancellato dal close per-posizione => il flatten NON lo ricancella.
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=6.0, remaining=4.0),
                 _curr("B2", "1.1", 7, "BACK", matched=0.0, remaining=5.0)],
        bot=[_bot("B1", "1.1", 7), _bot("B2", "1.1", 7)],
        books={"1.1": _book("1.1", 7)},
    )
    out = h.router().route({"signal_type": "CASHOUT_ALL"})
    assert out["published"] == 1
    assert h.cancelled == [("1.1", ["B1", "B2"])]   # un solo cancel, niente doppione


def test_pure_resting_snake_case_keys_are_flattened():
    # Le chiavi size_matched/size_remaining (snake_case) sono gestite come le
    # camelCase: l'ordine puro viene comunque flattato.
    snake = {"bet_id": "B2", "market_id": "1.2", "selection_id": 9, "side": "BACK",
             "size_matched": 0.0, "size_remaining": 5.0}
    h = _Harness(
        current=[_curr("B1", "1.1", 7, "BACK", matched=10.0), snake],
        bot=[_bot("B1", "1.1", 7), _bot("B2", "1.2", 9)],
        books={"1.1": _book("1.1", 7), "1.2": _book("1.2", 9)},
    )
    h.router().route({"signal_type": "CASHOUT_ALL"})
    assert ("1.2", ["B2"]) in h.cancelled


def test_non_cashout_signal_is_ignored():
    h = _Harness(current=[], bot=[], books={})
    out = h.router().route({"signal_type": "QUICK_BET"})
    assert out["reason"] == "not_cashout"
    assert h.published == []
