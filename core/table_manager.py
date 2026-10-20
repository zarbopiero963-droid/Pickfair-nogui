from __future__ import annotations

import math

from dataclasses import dataclass, asdict
from datetime import datetime
from threading import RLock
from typing import Any, Dict, List, Optional


@dataclass
class TableState:
    table_id: int
    status: str = "FREE"  # FREE / ACTIVE / RECOVERY / LOCKED
    current_event_key: str = ""
    current_exposure: float = 0.0
    loss_amount: float = 0.0
    in_recovery: bool = False
    market_id: str = ""
    selection_id: str = ""
    meta: Dict[str, Any] = None
    updated_at: str = ""

    def __post_init__(self):
        if self.meta is None:
            self.meta = {}
        if not self.updated_at:
            self.updated_at = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TableManager:
    """
    Gestione tavoli Roserpina.

    Regole:
    - ogni tavolo può essere FREE / ACTIVE / RECOVERY / LOCKED
    - ogni tavolo mantiene la propria memoria perdite
    - l'allocazione preferisce:
        1) tavolo FREE
        2) tavolo già in recovery se allow_recovery=True
    """

    def __init__(self, table_count: int = 5):
        self._lock = RLock()
        self.table_count = max(1, int(table_count or 1))
        self._tables: Dict[int, TableState] = {
            idx: TableState(table_id=idx)
            for idx in range(1, self.table_count + 1)
        }

    # =========================================================
    # INTERNAL
    # =========================================================
    def _touch(self, table: TableState) -> None:
        table.updated_at = datetime.utcnow().isoformat()

    def _get(self, table_id: int) -> Optional[TableState]:
        return self._tables.get(int(table_id))

    @staticmethod
    def _is_clean_free(table: TableState) -> bool:
        return (
            table.status == "FREE"
            and not table.in_recovery
            and table.loss_amount <= 0.0
            and not table.current_event_key
            and not table.current_exposure
        )

    def _prune_if_surplus(self, table: TableState) -> None:
        # Un tavolo oltre `table_count` (dopo una riduzione) resta finche' ha
        # stato; tornato libero e pulito si toglie (#461 PR28).
        if table.table_id > self.table_count and self._is_clean_free(table):
            self._tables.pop(table.table_id, None)

    def resize(self, table_count: int) -> None:
        """Adegua il numero di tavoli SENZA perdere stato (#461 PR28).

        Patch di config a bot acceso: i tavoli esistenti restano con stato,
        evento, esposizione e memoria perdite; i mancanti si aggiungono liberi.
        Riducendo, si tolgono solo i tavoli liberi e puliti oltre il nuovo
        numero; gli altri restano (esposizione contata nei cap) e non vengono
        riassegnati come liberi. `int()` solleva su un valore illeggibile
        PRIMA di toccare lo stato."""
        count = max(1, int(table_count or 1))
        with self._lock:
            for idx in range(1, count + 1):
                if idx not in self._tables:
                    self._tables[idx] = TableState(table_id=idx)
            self.table_count = count
            for table in list(self._tables.values()):
                self._prune_if_surplus(table)

    # =========================================================
    # ALLOCATION
    # =========================================================
    def allocate(self, *, event_key: str, allow_recovery: bool = True) -> Optional[TableState]:
        with self._lock:
            # 1) tavolo libero (entro table_count: un tavolo oltre il numero
            #    configurato, rimasto dopo una riduzione, non si riassegna)
            for table in self._tables.values():
                if table.status == "FREE" and table.table_id <= self.table_count:
                    return TableState(**table.to_dict())

            # 2) recovery consentito. PF-ALL-F029: solo un tavolo in recovery
            #    NON occupato (activate sovrascriverebbe la posizione aperta:
            #    esposizione persa dai cap, due posizioni su un tavolo) ed entro
            #    table_count, come per i tavoli liberi.
            if allow_recovery:
                recovery_candidates = [
                    t for t in self._tables.values()
                    if (t.status in {"RECOVERY", "LOCKED"} or t.in_recovery)
                    and not t.current_event_key
                    and t.table_id <= self.table_count
                ]
                if recovery_candidates:
                    recovery_candidates.sort(key=lambda t: (t.loss_amount, t.table_id))
                    return TableState(**recovery_candidates[0].to_dict())

            return None

    def activate(
        self,
        *,
        table_id: int,
        event_key: str,
        exposure: float,
        market_id: str,
        selection_id: Any,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._lock:
            table = self._get(table_id)
            if not table:
                raise RuntimeError(f"Tavolo non trovato: {table_id}")

            table.current_event_key = str(event_key or "")
            table.current_exposure = float(exposure or 0.0)
            table.market_id = str(market_id or "")
            table.selection_id = str(selection_id or "")
            table.meta = dict(meta or {})

            if table.loss_amount > 0.0 or table.in_recovery:
                table.status = "RECOVERY"
                table.in_recovery = True
            else:
                table.status = "ACTIVE"
                table.in_recovery = False

            self._touch(table)

    # =========================================================
    # RELEASE / PNL
    # =========================================================
    def release(self, table_id: int, *, pnl: float = 0.0) -> None:
        with self._lock:
            table = self._get(table_id)
            if not table:
                return

            pnl = float(pnl or 0.0)

            if pnl < 0:
                table.loss_amount += abs(pnl)
                table.in_recovery = True
                table.status = "RECOVERY"
            else:
                # profitto usato per assorbire recovery
                if table.loss_amount > 0:
                    table.loss_amount = max(0.0, table.loss_amount - pnl)

                if table.loss_amount <= 0.0:
                    table.loss_amount = 0.0
                    table.in_recovery = False
                    table.status = "FREE"
                else:
                    table.in_recovery = True
                    table.status = "RECOVERY"

            table.current_event_key = ""
            table.current_exposure = 0.0
            table.market_id = ""
            table.selection_id = ""
            table.meta = {}
            self._touch(table)
            self._prune_if_surplus(table)

    def force_unlock(self, table_id: int) -> None:
        with self._lock:
            table = self._get(table_id)
            if not table:
                return

            table.current_event_key = ""
            table.current_exposure = 0.0
            table.market_id = ""
            table.selection_id = ""
            table.meta = {}

            if table.loss_amount > 0.0:
                table.status = "RECOVERY"
                table.in_recovery = True
            else:
                table.status = "FREE"
                table.in_recovery = False

            self._touch(table)
            self._prune_if_surplus(table)

    def force_unlock_if_event(self, table_id: int, event_key: str) -> bool:
        """Rilascia il tavolo SOLO se occupa ancora la riserva di ``event_key``.

        PF-ALL-F023: il rilascio da evento terminale deve colpire l'ordine giusto.
        Controllo e rilascio avvengono sotto lo stesso lock, quindi:
        - idempotente: un secondo evento trova ``current_event_key`` vuoto => no-op;
        - sicuro in concorrenza: un tavolo gia' riassegnato a un altro evento non
          viene mai liberato da un evento terminale vecchio.
        Restituisce True solo se ha rilasciato.
        """
        key = str(event_key or "")
        if not key:
            return False
        with self._lock:
            table = self._get(int(table_id))
            if not table or table.current_event_key != key:
                return False
            self.force_unlock(int(table_id))
            return True

    # =========================================================
    # LOOKUPS
    # =========================================================
    def find_by_event_key(self, event_key: str) -> Optional[TableState]:
        with self._lock:
            event_key = str(event_key or "")
            for table in self._tables.values():
                if table.current_event_key == event_key:
                    return TableState(**table.to_dict())
            return None

    def get_table(self, table_id: int) -> Optional[TableState]:
        with self._lock:
            table = self._get(table_id)
            if not table:
                return None
            return TableState(**table.to_dict())

    def active_tables(self) -> List[TableState]:
        with self._lock:
            return [
                TableState(**t.to_dict())
                for t in self._tables.values()
                if t.status in {"ACTIVE", "RECOVERY", "LOCKED"} and t.current_event_key
            ]

    def recovery_tables(self) -> List[TableState]:
        with self._lock:
            return [
                TableState(**t.to_dict())
                for t in self._tables.values()
                if t.in_recovery or t.loss_amount > 0.0 or t.status == "RECOVERY"
            ]

    def total_exposure(self) -> float:
        with self._lock:
            # Questa somma alimenta il cap assoluto in euro `max_open_exposure`
            # (runtime_controller, Enforcement A2 #320): non deve MAI sotto-stimare
            # l'esposizione aperta, altrimenti un ordine passerebbe oltre il cap.
            total = 0.0
            for t in self._tables.values():
                exp = t.current_exposure
                # Fail-closed su valori NON FINITI (NaN/inf) = stato corrotto: NON
                # trattarli come 0 — `max(0.0, NaN)` darebbe 0.0 e li nasconderebbe,
                # sotto-stimando il totale e facendo passare il cap. Restituisci inf
                # cosi' il cap A2 blocca ogni nuovo ordine finche' lo stato e' corrotto.
                if not math.isfinite(exp):
                    return float("inf")
                # Clamp per-tavolo a >= 0 (M-06): un `current_exposure` negativo non
                # deve DEFLAZIONARE il totale (sovra-stima al piu', mai sotto-stima).
                total += max(0.0, exp)
            return float(total)

    def mark_placed(self, table_id: int, event_key: str, *, at: float, customer_ref: str = "") -> bool:
        """#461 PR28-e: l'ordine della riserva ``event_key`` sul tavolo e' confermato a mercato (primo esito,
        istante monotonic ``at``). La marca vive in ``meta``, che ogni rilascio/riattivazione azzera: non
        sopravvive alla riserva. Vale il primo esito. Se la riserva porta il ``customer_ref`` del suo ordine,
        l'esito deve portare lo stesso ref (un esito tardivo di un ordine precedente sullo stesso tavolo non
        marca la nuova riserva). True se marcata."""
        with self._lock:
            table = self._get(int(table_id))
            if not table or not event_key or table.current_event_key != str(event_key):
                return False
            reserved_ref = str((table.meta or {}).get("customer_ref") or "")
            if reserved_ref and reserved_ref != str(customer_ref or ""):
                return False
            table.meta.setdefault("placed_at", float(at))
            return True

    def mark_placed_by_ref(self, customer_ref: str, *, at: float) -> bool:
        """#461 PR28-e: come `mark_placed` per un esito senza ``table_id`` (CMD_QUICK_BET dell'auto-next): si marca
        il solo tavolo la cui riserva porta questo ``customer_ref``. Nessuna corrispondenza (riserva gia'
        rilasciata, ordine di un altro tavolo) o piu' di una => nessuna marca (restano addebitate, prudente)."""
        with self._lock:
            ref = str(customer_ref or "")
            matches = [t for t in self._tables.values()
                       if ref and t.current_event_key and str((t.meta or {}).get("customer_ref") or "") == ref]
            if len(matches) != 1:
                return False
            matches[0].meta.setdefault("placed_at", float(at))
            return True

    def pending_exposure(self, *, since: Optional[float]) -> float:
        """#461 PR28-e: esposizione delle riserve il cui ordine NON risulta a mercato prima dell'istante
        monotonic ``since`` (inizio della lettura del conto): sono quelle che il disponibile letto non
        riflette ancora. ``since`` None => tutte pendenti. Come `total_exposure`: NaN/inf => inf,
        negativi a 0 (mai sotto-stima)."""
        with self._lock:
            total = 0.0
            for t in self._tables.values():
                exp = t.current_exposure
                if not math.isfinite(exp):
                    return float("inf")
                placed_at = (t.meta or {}).get("placed_at")
                if since is not None and placed_at is not None and float(placed_at) <= float(since):
                    continue
                total += max(0.0, exp)
            return float(total)

    # =========================================================
    # RESET / SNAPSHOT
    # =========================================================
    def reset_all(self, keep_table_ids=None) -> None:
        """Azzera i tavoli. ``keep_table_ids``: tavoli che portano una riserva
        con esito incerto (AMBIGUOUS, PF-ALL-F024) e NON vanno liberati."""
        keep = {int(t) for t in (keep_table_ids or ())}
        with self._lock:
            for table in self._tables.values():
                if table.table_id in keep:
                    continue
                table.status = "FREE"
                table.current_event_key = ""
                table.current_exposure = 0.0
                table.loss_amount = 0.0
                table.in_recovery = False
                table.market_id = ""
                table.selection_id = ""
                table.meta = {}
                self._touch(table)

    def recovery_state(self) -> Dict[int, float]:
        """RSR-12: tavoli con recovery attiva (perdita da recuperare), anche senza ordini aperti."""
        with self._lock:
            return {t.table_id: float(t.loss_amount) for t in self._tables.values()
                    if t.loss_amount > 0.0 or t.in_recovery}

    def restore_recovery_state(self, state: Dict[int, float]) -> None:
        """RSR-12: ripristina al riavvio la perdita da recuperare dei tavoli. Non tocca posizione ed
        esposizione di un tavolo gia' occupato (ricostruito da F018): ne ripristina solo la perdita."""
        with self._lock:
            for table_id, loss in (state or {}).items():
                table = self._get(int(table_id))
                loss = float(loss)
                if not math.isfinite(loss):
                    # JSON NaN/Infinity (rilievo Codex #512): debito ignoto => stato illeggibile,
                    # il runtime blocca la capacita' di recovery (fail-closed)
                    raise ValueError(f"perdita di recovery non finita per il tavolo {table_id}: {loss!r}")
                if loss < 0.0:
                    # Debito negativo (rilievo Codex r4312360615 su #512): non e' "nessun debito", e' stato
                    # incoerente. Saltarlo cancellava la perdita persistita: illeggibile => fail-closed.
                    raise ValueError(f"perdita di recovery negativa per il tavolo {table_id}: {loss!r}")
                if loss == 0.0:
                    continue
                if table is None:
                    # Tavolo oltre table_count (config ridotta a bot acceso, `resize` lo
                    # conservava con la sua perdita): al riavvio va ricreato, altrimenti
                    # il debito sparisce e la capacita' di recovery e' sottostimata
                    # (rilievo Codex #512).
                    table = TableState(table_id=int(table_id))
                    self._tables[int(table_id)] = table
                table.loss_amount = loss
                table.in_recovery = True
                table.status = "RECOVERY"
                self._touch(table)

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [t.to_dict() for t in self._tables.values()]