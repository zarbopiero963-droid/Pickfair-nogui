"""Resolver deterministico del catalogo Betfair locale (Fase A1, PR-20).

Risolve evento, mercato e runner partendo dal testo del provider. Contratto:

- l'esito e' UNO o NESSUNO: due candidati diversi sono un'ambiguita' e l'esito e'
  un rifiuto esplicito (``AMBIGUOUS``), mai «il primo che capita»;
- il catalogo deve essere di eta' nota e recente: senza metadati di sync, con
  metadati illeggibili o piu' vecchi di ``max_catalog_age_sec`` il resolver
  rifiuta (``CATALOG_STALE``);
- il testo del provider non entra mai in un pattern ``LIKE``: il confronto
  avviene su nomi normalizzati (``%`` e ``_`` sono caratteri qualsiasi);
- il resolver non inventa nulla: input mancante o malformato = ``INVALID_INPUT``.

I metodi ``*_strict`` restituiscono una :class:`Resolution` con il motivo; i
metodi storici (``resolve_event``/``resolve_market``/``resolve_runner``)
delegano e restituiscono ``None`` su qualunque rifiuto.
"""

import logging
import math
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

OK = "OK"
INVALID_INPUT = "INVALID_INPUT"
CATALOG_STALE = "CATALOG_STALE"
NOT_FOUND = "NOT_FOUND"
AMBIGUOUS = "AMBIGUOUS"

# Tolleranza per un orologio di sync leggermente avanti rispetto al resolver.
_FUTURE_TOLERANCE_SEC = 60.0


@dataclass(frozen=True)
class Resolution:
    """Esito di una risoluzione: ``value`` e' valorizzato solo con ``ok``."""

    ok: bool
    reason: str
    value: Any = None
    candidates: Tuple[str, ...] = ()


def _refuse(reason: str, candidates: Tuple[str, ...] = ()) -> Resolution:
    return Resolution(False, reason, None, tuple(candidates))


def normalize_name(value: Any) -> str:
    """Nome normalizzato per il confronto: senza accenti, minuscolo, spazi singoli.

    Restituisce ``""`` per qualunque valore che non sia una stringa non vuota.
    """
    if not isinstance(value, str):
        return ""
    decomposed = unicodedata.normalize("NFKD", value)
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return " ".join(stripped.casefold().split())


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DeterministicResolver:
    """Risolve Eventi, Mercati e Runner sul catalogo locale e sulle mappe alias."""

    def __init__(self, db, *, max_catalog_age_sec: float, now: Callable[[], datetime] = _utc_now):
        age = max_catalog_age_sec
        if isinstance(age, bool) or not isinstance(age, (int, float)) or not math.isfinite(age) or age <= 0:
            raise ValueError("max_catalog_age_sec deve essere un numero finito > 0")
        self.db = db
        self.max_catalog_age_sec = float(age)
        self._now = now

    # ── freschezza del catalogo ─────────────────────────────────────────
    def catalog_block_reason(self) -> str:
        """``""`` se il catalogo e' utilizzabile, altrimenti ``CATALOG_STALE``."""
        try:
            meta = self.db.get_sync_meta()
            synced = datetime.fromisoformat(str(meta["last_sync_at"]))
        except Exception:
            return CATALOG_STALE
        if synced.tzinfo is None:
            synced = synced.replace(tzinfo=timezone.utc)
        now = self._now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        age = (now - synced).total_seconds()
        if age > self.max_catalog_age_sec or age < -_FUTURE_TOLERANCE_SEC:
            return CATALOG_STALE
        return ""

    def _rows(self, sql: str, params: tuple = ()):
        return self.db._execute(sql, params, fetch=True, commit=False) or []

    # ── evento ──────────────────────────────────────────────────────────
    def resolve_event_strict(self, provider_id: int, event_name: str) -> Resolution:
        """Evento Betfair per il nome (squadra o «Home v Away») dato dal provider."""
        wanted = normalize_name(event_name)
        if not wanted or isinstance(provider_id, bool) or not isinstance(provider_id, int):
            return _refuse(INVALID_INPUT)
        blocked = self.catalog_block_reason()
        if blocked:
            return _refuse(blocked)

        targets = {
            normalize_name(a["betfair_name"])
            for a in self.db.get_name_aliases(provider_id)
            if normalize_name(a["alias_norm"]) == wanted
        }
        targets.discard("")
        if len(targets) > 1:
            return _refuse(AMBIGUOUS, tuple(sorted(targets)))

        events = [(str(r["event_id"]), normalize_name(r["name"])) for r in self._rows("SELECT event_id, name FROM bf_events")]
        # Prima il nome Betfair indicato dall'alias, poi il testo del provider cosi' com'e'.
        for target in list(targets) + [wanted]:
            found = sorted({
                event_id for event_id, name in events
                if name == target or name.startswith(target + " v ") or name.endswith(" v " + target)
            })
            if len(found) > 1:
                return _refuse(AMBIGUOUS, tuple(found))
            if found:
                return Resolution(True, OK, found[0])
        return _refuse(NOT_FOUND)

    # ── mercato ─────────────────────────────────────────────────────────
    def resolve_market_strict(self, provider_id: int, event_id: str, market_phrase: str) -> Resolution:
        """Mercato dell'evento indicato dalla frase alias del provider."""
        phrase = normalize_name(market_phrase)
        if (not phrase or not isinstance(event_id, str) or not event_id.strip()
                or isinstance(provider_id, bool) or not isinstance(provider_id, int)):
            return _refuse(INVALID_INPUT)
        blocked = self.catalog_block_reason()
        if blocked:
            return _refuse(blocked)

        aliases = {
            (str(a["market_type"] or ""), str(a["market_name"] or ""), str(a["selection_name"] or ""))
            for a in self.db.get_market_aliases(provider_id)
            if normalize_name(a["phrase_norm"]) == phrase
        }
        if not aliases:
            return _refuse(NOT_FOUND)
        if len(aliases) > 1:
            return _refuse(AMBIGUOUS, tuple(sorted(a[0] for a in aliases)))
        market_type, alias_market_name, selection_name = next(iter(aliases))
        if not market_type:
            return _refuse(NOT_FOUND)

        markets = self._rows(
            "SELECT market_id, market_name FROM bf_markets WHERE event_id = ? AND market_type = ?",
            (event_id, market_type),
        )
        if len(markets) > 1 and normalize_name(alias_market_name):
            named = [m for m in markets if normalize_name(m["market_name"]) == normalize_name(alias_market_name)]
            markets = named or markets
        if len(markets) > 1:
            return _refuse(AMBIGUOUS, tuple(sorted(str(m["market_id"]) for m in markets)))
        if not markets:
            return _refuse(NOT_FOUND)
        return Resolution(True, OK, {
            "market_id": markets[0]["market_id"],
            "market_type": market_type,
            "market_name": markets[0]["market_name"],
            "selection_name": selection_name,
        })

    # ── runner ──────────────────────────────────────────────────────────
    def resolve_runner_strict(self, market_id: str, selection_name: str) -> Resolution:
        """``selection_id`` Betfair del runner con quel nome nel mercato."""
        wanted = normalize_name(selection_name)
        if not wanted or not isinstance(market_id, str) or not market_id.strip():
            return _refuse(INVALID_INPUT)
        blocked = self.catalog_block_reason()
        if blocked:
            return _refuse(blocked)

        found = sorted({
            str(r["selection_id"])
            for r in self._rows("SELECT selection_id, runner_name FROM bf_runners WHERE market_id = ?", (market_id,))
            if normalize_name(r["runner_name"]) == wanted
        })
        if len(found) > 1:
            return _refuse(AMBIGUOUS, tuple(found))
        if not found:
            return _refuse(NOT_FOUND)
        return Resolution(True, OK, found[0])

    # ── API storica: valore o None ──────────────────────────────────────
    @staticmethod
    def _value(kind: str, result: Resolution):
        if not result.ok:
            logger.warning("[Resolver] %s non risolto: %s %s", kind, result.reason, list(result.candidates))
        return result.value

    def resolve_event(self, provider_id: int, event_name: str) -> Optional[str]:
        return self._value("evento", self.resolve_event_strict(provider_id, event_name))

    def resolve_market(self, provider_id: int, event_id: str, market_phrase: str) -> Optional[Dict[str, Any]]:
        return self._value("mercato", self.resolve_market_strict(provider_id, event_id, market_phrase))

    def resolve_runner(self, market_id: str, selection_name: str) -> Optional[str]:
        return self._value("runner", self.resolve_runner_strict(market_id, selection_name))
