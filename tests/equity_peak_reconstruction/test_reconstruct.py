"""F-11 — strumento di ricostruzione del picco dallo storico Betfair (solo anteprima, nessuna scrittura).

Dati d'ingresso: estratti conto SINTETICI nella forma di `getAccountStatement` (MOCK), non storici reali.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone

import pytest

from tools.equity_peak_reconstruction.reconstruct import (
    STATE_OK, STATE_REJECTED, pickfair_order_window, preview, reconstruct)

T = lambda s: datetime.fromisoformat(s).replace(tzinfo=timezone.utc)   # noqa: E731
PRIMO, ULTIMO = T("2026-09-10T12:00:00"), T("2026-10-20T18:00:00")


def _riga(ref, quando, importo, saldo):
    return {"refId": ref, "itemDate": quando, "amount": importo, "balance": saldo, "itemClass": "UNKNOWN"}


def _estratto():
    # saldo d'apertura 100,00 -> +40 (140, picco) -> -30 (110) -> -15,5 (94,50)
    return {"accountStatement": [
        _riga("r1", "2026-09-01T10:00:00Z", "0.00", "100.00"),
        _riga("r2", "2026-09-15T20:00:00Z", "40.00", "140.00"),
        _riga("r3", "2026-10-02T21:00:00Z", "-30.00", "110.00"),
        _riga("r4", "2026-10-25T21:00:00Z", "-15.50", "94.50"),
    ]}


def test_ricostruzione_coerente_propone_il_picco_con_evidenze() -> None:
    out = reconstruct(_estratto(), first_order_at=PRIMO, last_order_at=ULTIMO)

    assert out["state"] == STATE_OK, out
    assert out["cash_equity_peak"] == "140.00" and out["equity_peak"] == "140.00"
    assert out["equity_peak_basis"] == "CONSERVATIVE_UPPER_BOUND_FROM_CASH_EQUITY"
    assert out["evidence"]["balance_chain"] == "OK" and out["evidence"]["rows"] == 4
    assert out["proposed_value"] == {"equity_peak": 140.0, "cash_equity_peak": 140.0,
                                     "source": "RECONSTRUCTED_BETFAIR_STATEMENT"}


def test_righe_fuori_ordine_vengono_ordinate_per_data() -> None:
    est = _estratto()
    est["accountStatement"].reverse()
    assert reconstruct(est, first_order_at=PRIMO, last_order_at=ULTIMO)["cash_equity_peak"] == "140.00"


def test_il_saldo_d_apertura_puo_essere_il_picco() -> None:
    est = {"accountStatement": [_riga("a", "2026-09-01T10:00:00Z", "-50.00", "150.00"),
                                _riga("b", "2026-10-30T10:00:00Z", "10.00", "160.00")]}
    # apertura = 150 + 50 = 200 > ogni saldo successivo
    assert reconstruct(est, first_order_at=PRIMO, last_order_at=ULTIMO)["cash_equity_peak"] == "200.00"


@pytest.mark.parametrize("mutare,motivo", [
    (lambda e: e["accountStatement"].pop(1), "BALANCE_CHAIN_BROKEN"),                   # riga mancante
    (lambda e: e["accountStatement"][2].update(balance="111.00"), "BALANCE_CHAIN_BROKEN"),  # saldo alterato
    (lambda e: e["accountStatement"][1].update(refId="r1"), "DUPLICATE_REF"),
    (lambda e: e["accountStatement"][1].update(amount="nan"), "FIELD_INVALID:amount"),
    (lambda e: e["accountStatement"][1].update(balance=True), "FIELD_INVALID:balance"),
    (lambda e: e["accountStatement"][1].update(itemDate="2026-09-15T20:00:00"), "FIELD_INVALID:itemDate"),  # senza fuso
    (lambda e: e["accountStatement"][1].update(itemDate="ieri"), "FIELD_INVALID:itemDate"),
    (lambda e: e["accountStatement"].__setitem__(0, "x"), "ROW_MALFORMED"),
    (lambda e: e.update(accountStatement=[]), "STATEMENT_EMPTY"),
])
def test_block_dati_incompleti_o_incoerenti_rifiutati(mutare, motivo: str) -> None:
    est = _estratto()
    mutare(est)

    out = reconstruct(est, first_order_at=PRIMO, last_order_at=ULTIMO)

    assert out["state"] == STATE_REJECTED and out["reason"].startswith(motivo), out
    assert out["proposed_value"] is None


@pytest.mark.parametrize("primo,ultimo,motivo", [
    (T("2026-08-30T00:00:00"), ULTIMO, "STATEMENT_STARTS_AFTER_FIRST_ORDER"),   # storico che non copre l'inizio
    (PRIMO, T("2026-11-01T00:00:00"), "STATEMENT_ENDS_BEFORE_LAST_ORDER"),
    (None, None, "PICKFAIR_HISTORY_UNKNOWN"),
])
def test_block_storico_che_non_copre_l_attivita_rifiutato(primo, ultimo, motivo: str) -> None:
    out = reconstruct(_estratto(), first_order_at=primo, last_order_at=ultimo)
    assert out["state"] == STATE_REJECTED and out["reason"] == motivo, out


# ------------------------------------------------------------------ anteprima su file: sola lettura, provenienza
def _db_con_ordini(path, istanti):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_ref TEXT, created_at REAL NOT NULL)")
    conn.executemany("INSERT INTO orders (customer_ref, created_at) VALUES (?, ?)",
                     [(f"r{i}", t.timestamp()) for i, t in enumerate(istanti)])
    conn.commit()
    conn.close()


def test_anteprima_su_file_non_scrive_niente_e_riporta_gli_hash(tmp_path) -> None:
    db = str(tmp_path / "pickfair.db")
    _db_con_ordini(db, [PRIMO, ULTIMO])
    st = tmp_path / "statement.json"
    st.write_text(json.dumps(_estratto()), encoding="utf-8")
    prima = (os.path.getmtime(db), open(db, "rb").read())

    out = preview(str(st), db)

    assert out["state"] == STATE_OK and out["provenance"]["writes"] == "NONE"
    assert len(out["provenance"]["statement_sha256"]) == 64 and len(out["provenance"]["db_sha256"]) == 64
    assert (os.path.getmtime(db), open(db, "rb").read()) == prima, "DB modificato dall'anteprima"


def test_il_db_e_aperto_in_sola_lettura(tmp_path) -> None:
    db = str(tmp_path / "pickfair.db")
    _db_con_ordini(db, [PRIMO])
    os.chmod(db, 0o444)
    try:
        assert pickfair_order_window(db)[0] == PRIMO
    finally:
        os.chmod(db, 0o644)


def test_db_senza_ordini_non_e_ricostruibile(tmp_path) -> None:
    db = str(tmp_path / "pickfair.db")
    _db_con_ordini(db, [])
    assert pickfair_order_window(db) == (None, None)


# ------------------------------------------------------------------ integrazione con F-11: il valore ratificato sblocca
def test_valore_proposto_accettato_dal_ripristino_f11(tmp_path) -> None:
    """Il valore proposto ha la forma che `RuntimeController._restore_equity_peaks` accetta: una procedura approvata che
    lo scrivesse (qui simulata dal test) sbloccherebbe l'installazione; senza, resta PAUSED."""
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain

    c = _chain(tmp_path)
    c.db.save_settings({c.rc._EXPANSION_KEY: json.dumps({"baseline": 100.0, "set_at_ts": 1.0})})   # pre-v9
    assert c.rc._restore_equity_peaks() is False

    proposto = reconstruct(_estratto(), first_order_at=PRIMO, last_order_at=ULTIMO)["proposed_value"]
    c.db.save_settings({c.rc._EQUITY_PEAKS_KEY: json.dumps(proposto)})          # procedura approvata (simulata)

    assert c.rc._restore_equity_peaks() is True
    assert c.rc._cash_equity_peak == 140.0
