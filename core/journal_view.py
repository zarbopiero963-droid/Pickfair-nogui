"""CLI read-only di ispezione del diario eventi (#236, follow-up di #230/#234).

Rende il ledger `event_journal.jsonl` **consultabile** senza aprire il `.jsonl` a
mano: `python -m xtrader_bridge.journal_view [--type ...] [--last N] [--since TS]
[--until TS] [--json]`.

Invarianti (dalla issue #236):

- **Read-only**: riusa `event_journal.read_events`, non scrive né modifica MAI il
  ledger; tollerante alle righe malformate (una riga troncata da un crash è già
  saltata da `read_events`).
- **Niente segreti**: gli eventi sul ledger sono già redatti (token + `chat_id` come
  `chat:sha256:…`). Questa vista **non de-redige nulla**: mostra i valori esattamente
  come sono sul file.
- **Ordine forense**: gli eventi sono riordinati per `ts` (ricostruisce l'ordine reale
  anche se due append concorrenti finissero fuori ordine sul file).

La logica pura (`filter_events`/`format_table`/`format_json`/`render`) è separata
dall'entrypoint `main`, così è interamente testabile headless.
"""

import argparse
import json
from datetime import datetime

from . import config_store, event_journal, runtime_state


def default_path() -> str:
    """Percorso di default del ledger: `<config_dir>/event_journal.jsonl`."""
    return runtime_state.event_journal_path(config_store.config_dir())


def _ts_value(event) -> float:
    """`ts` di un evento come float ordinabile; mancante/non-numerico → `-inf` (finisce
    in testa, così una riga con ts rotto è visibile e non maschera il resto)."""
    try:
        return float(event.get("ts"))
    except (TypeError, ValueError):
        return float("-inf")


def _ts_label(ts) -> str:
    """`ts` epoch → etichetta leggibile locale `YYYY-MM-DD HH:MM:SS`. Un valore non
    convertibile (ts assente/rotto) è mostrato grezzo, senza crashare."""
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError, OverflowError):
        return str(ts)


def filter_events(events, *, types=None, last=None, since=None, until=None) -> list:
    """Eventi **ordinati per `ts`** e filtrati (tutti i filtri sono opzionali):

    - `types`: iterabile di tipi ammessi (gli altri sono esclusi);
    - `since`/`until`: intervallo epoch inclusivo su `ts`;
    - `last`: solo gli ultimi N (dopo l'ordinamento); `last=0` → nessun evento,
      `last<0` è ignorato (nessun taglio).

    Non muta la lista in ingresso (lavora su una copia)."""
    out = sorted(events, key=_ts_value)
    if types is not None:
        want = {str(t) for t in types}
        out = [e for e in out if e.get("type") in want]
    if since is not None:
        out = [e for e in out if _ts_value(e) >= since]
    if until is not None:
        out = [e for e in out if _ts_value(e) <= until]
    if last is not None and last >= 0:
        out = out[-last:] if last else []
    return out


def _row_cells(event) -> tuple:
    """Celle `(ts leggibile, TYPE, data JSON compatto)` di un evento. `data` è serializzato
    con chiavi ordinate (output deterministico) e mostrato così com'è sul ledger (già
    redatto: la vista non de-redige nulla)."""
    ts = _ts_label(event.get("ts"))
    typ = str(event.get("type", ""))
    data = event.get("data") or {}
    data_str = json.dumps(data, ensure_ascii=False, sort_keys=True) if data else ""
    return (ts, typ, data_str)


def table_rows(events) -> list:
    """Righe strutturate `(ts, type, data)` per il rendering tabellare, riusate sia dalla
    CLI (`format_table`) sia dalla scheda GUI «📒 Diario» (#236). Logica pura, testabile
    headless: la GUI resta solo widget/wiring."""
    return [_row_cells(e) for e in events]


def format_table(events) -> str:
    """Rendering tabellare: `ts leggibile · TYPE · data (JSON compatto)`, una riga per
    evento (riusa `table_rows`)."""
    lines = [f"{ts}  {typ:<26}  {data_str}".rstrip()
             for ts, typ, data_str in table_rows(events)]
    return "\n".join(lines)


def format_json(events) -> str:
    """Rendering JSON (lista di eventi già ordinata/filtrata), indentato e UTF-8."""
    return json.dumps(list(events), ensure_ascii=False, indent=2)


def render(path=None, *, types=None, last=None, since=None, until=None,
           as_json=False) -> str:
    """Legge il ledger (`path` o default), applica i filtri e formatta. Read-only:
    file assente/illeggibile → stringa vuota (via `read_events` → `[]`)."""
    path = path or default_path()
    events = filter_events(event_journal.read_events(path), types=types, last=last,
                           since=since, until=until)
    return format_json(events) if as_json else format_table(events)


def build_parser() -> argparse.ArgumentParser:
    """Parser degli argomenti (separato per testabilità)."""
    p = argparse.ArgumentParser(
        prog="python -m xtrader_bridge.journal_view",
        description="Ispeziona il diario eventi del bridge (read-only, mai segreti).")
    p.add_argument("--path", default=None,
                   help="Percorso del .jsonl (default: cartella di configurazione).")
    p.add_argument("--type", action="append", dest="types", metavar="TYPE",
                   help="Filtra per tipo evento (ripetibile). Tipi: "
                        + ", ".join(sorted(event_journal.EVENT_TYPES)) + ".")
    p.add_argument("--last", type=int, default=None, metavar="N",
                   help="Solo gli ultimi N eventi (dopo l'ordinamento per ts).")
    p.add_argument("--since", type=float, default=None, metavar="TS",
                   help="Solo eventi con ts >= TS (epoch).")
    p.add_argument("--until", type=float, default=None, metavar="TS",
                   help="Solo eventi con ts <= TS (epoch).")
    p.add_argument("--json", action="store_true", dest="as_json",
                   help="Output JSON invece che tabellare.")
    return p


def main(argv=None) -> int:
    """Entrypoint CLI: stampa la vista e ritorna 0. Non scrive mai il ledger."""
    args = build_parser().parse_args(argv)
    print(render(args.path, types=args.types, last=args.last, since=args.since,
                 until=args.until, as_json=args.as_json))
    return 0


if __name__ == "__main__":       # pragma: no cover — esercitato via `python -m`
    raise SystemExit(main())
