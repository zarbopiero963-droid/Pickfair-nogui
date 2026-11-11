import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

# Esiti di run_sync (campo "status" del risultato).
COMPLETED = "COMPLETED"
SKIPPED_FRESH = "SKIPPED_FRESH"
SKIPPED_BUSY = "SKIPPED_BUSY"
INCOMPLETE = "INCOMPLETE"
FAILED = "FAILED"

SOCCER_EVENT_TYPE_ID = "1"
MARKET_TYPES = ['MATCH_ODDS', 'OVER_UNDER_05', 'OVER_UNDER_15', 'OVER_UNDER_25', 'CORRECT_SCORE']
CHUNK_SIZE = 20
MAX_RESULTS = 200


def _utc_now() -> datetime:
    """UTC senza fuso: stesso formato dei ``last_sync_at`` gia' salvati."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class CatalogSyncService:
    """
    Sincronizza il catalogo Betfair (Eventi, Mercati, Runner) nel database locale.
    Implementa la Fase A1 (Catalogo Cache).
    """
    # Un solo sync alla volta per processo: l'avvio e il comando manuale creano
    # istanze diverse sullo stesso DB, e due sync intrecciati si cancellerebbero
    # a vicenda le righe (la pulizia tiene solo il proprio sync_id).
    _sync_lock = threading.Lock()

    def __init__(self, db, betfair_client):
        self.db = db
        self.client = betfair_client
        self.is_syncing = False

    @staticmethod
    def _result(status: str, reason: str = "", events: int = 0, markets: int = 0, runners: int = 0) -> Dict[str, Any]:
        return {"status": status, "reason": reason, "events": events, "markets": markets, "runners": runners}

    def _recently_synced(self) -> bool:
        """True solo con metadati leggibili e recenti; ogni dubbio = sync necessario."""
        try:
            meta = self.db.get_sync_meta()
            if not meta:
                return False
            last_sync = datetime.fromisoformat(str(meta['last_sync_at']))
            if last_sync.tzinfo is not None:
                last_sync = last_sync.astimezone(timezone.utc).replace(tzinfo=None)
            age = _utc_now() - last_sync
        except Exception as exc:
            logger.warning("[CatalogSync] Metadati di sync illeggibili (%s): sync eseguito.", exc)
            return False
        return timedelta(0) <= age < timedelta(hours=12)

    def _fetch_markets(self, event_ids: List[str]) -> Tuple[list, bool]:
        """Mercati degli eventi dati e flag di troncamento.

        Una risposta al limite ``MAX_RESULTS`` puo' essere troncata: il gruppo viene
        diviso a meta' e richiesto di nuovo, finche' ogni risposta sta sotto il
        limite. Un singolo evento che da solo raggiunge il limite resta troncato.
        """
        markets = self.client.list_market_catalogue(
            [SOCCER_EVENT_TYPE_ID],
            event_ids=event_ids,
            market_type_codes=MARKET_TYPES,
            max_results=MAX_RESULTS,
        )
        if len(markets) < MAX_RESULTS:
            return list(markets), False
        if len(event_ids) <= 1:
            logger.warning(
                "[CatalogSync] Evento %s al limite maxResults=%d: possibile troncamento.",
                event_ids, MAX_RESULTS,
            )
            return list(markets), True
        half = len(event_ids) // 2
        left, left_truncated = self._fetch_markets(event_ids[:half])
        right, right_truncated = self._fetch_markets(event_ids[half:])
        return left + right, left_truncated or right_truncated

    def run_sync(self, force: bool = False) -> Dict[str, Any]:
        """Sincronizza il catalogo e restituisce l'esito (``status``, ``reason``, conteggi).

        Solo ``COMPLETED`` significa catalogo aggiornato e ripulito; ogni altro
        esito lascia il catalogo esistente e i metadati di sync com'erano.
        """
        if not CatalogSyncService._sync_lock.acquire(blocking=False):
            logger.info("[CatalogSync] Sync gia' in corso: richiesta ignorata.")
            return self._result(SKIPPED_BUSY, "sync_in_corso")
        self.is_syncing = True
        try:
            if not force and self._recently_synced():
                logger.info("[CatalogSync] Sync non necessario: catalogo recente.")
                return self._result(SKIPPED_FRESH, "catalogo_recente")
            return self._sync()
        except Exception as e:
            # Qualsiasi errore (incl. un chunk market-catalogue malformato che
            # solleva) interrompe il sync PRIMA del cleanup: il catalogo esistente
            # resta intatto (fail-safe money-path, nessuna cancellazione).
            logger.error(
                "[CatalogSync] Errore durante la sincronizzazione: %s "
                "(cleanup non eseguito, catalogo esistente preservato).", e,
            )
            return self._result(FAILED, type(e).__name__)
        finally:
            self.is_syncing = False
            CatalogSyncService._sync_lock.release()

    def _sync(self) -> Dict[str, Any]:
        # Microsecondi sempre presenti: due sync nello stesso secondo non condividono l'ID.
        sync_id = _utc_now().isoformat(timespec="microseconds")
        logger.info("[CatalogSync] Avvio sincronizzazione catalogo (ID: %s)...", sync_id)

        # 1. Recupero Eventi Soccer (Betfair listEvents; formato nativo:
        #    ogni elemento e' {"event": {...}, "marketCount": N}).
        events = self.client.list_events([SOCCER_EVENT_TYPE_ID], in_play_only=False)

        # Fail-safe (rilievo GPT/Grok): un sync che non recupera NESSUN evento
        # non deve toccare il catalogo esistente. Succede in SIM (il catalogo
        # arriva dai feed streaming, non via API) o in LIVE su risposta
        # vuota/anomala. Senza questo guard, cleanup_stale_bf_data cancella
        # tutto il catalogo -> trading bloccato. Skip: preserva l'esistente.
        if not events:
            logger.info("[CatalogSync] Nessun evento restituito: catalogo esistente preservato (skip cleanup).")
            return self._result(INCOMPLETE, "nessun_evento")

        # Metadati evento indicizzati per id (name/openDate da listEvents).
        event_meta: Dict[str, Dict[str, Any]] = {}
        for ev in events:
            e = ev.get('event') or {}
            eid = e.get('id')
            if eid:
                event_meta[eid] = e
        event_ids = list(event_meta.keys())

        # 2. Mercati in BATCH per evitare N+1 / rate-limit 429 in LIVE
        #    (rilievo Greptile/Codacy/Grok/Fable): listMarketCatalogue accetta
        #    piu' eventIds e riporta event + competition in ogni market
        #    (marketProjection). Chunk per non superare maxResults.
        #    maxResults=200 (NON 1000): con marketProjection MARKET_DESCRIPTION
        #    Betfair applica un limite di peso dati (maxResults x weight) e 1000
        #    genera APINGException TOO_MUCH_DATA in LIVE (rilievo Fable/Fugu) --
        #    stessa classe d'errore gia' corretta su listEvents. Un chunk da 20
        #    eventi x ~5 tipi mercato ~= 100 mercati << 200: 200 basta e avanza.
        markets_by_event: Dict[str, list] = {}
        truncated = False
        for i in range(0, len(event_ids), CHUNK_SIZE):
            chunk = event_ids[i:i + CHUNK_SIZE]
            # Batch misto (rilievo CodeRabbit Major): se un chunk fallisce a
            # meta' loop, list_market_catalogue SOLLEVA (errore upstream o
            # risposta non-lista) -> l'eccezione risale a run_sync e il
            # cleanup NON viene eseguito, preservando il catalogo. Un chunk
            # che ritorna una lista vuota GENUINA (eventi senza mercati) e'
            # invece lecito e non blocca il cleanup.
            chunk_markets, chunk_truncated = self._fetch_markets(chunk)
            # Completeness guard (rilievo GPT-5.6 Sol): una risposta al limite
            # anche dopo la bisezione puo' essere troncata -> sync INCOMPLETO:
            # niente cleanup/meta, per non cancellare mercati ancora validi.
            truncated = truncated or chunk_truncated
            for mk in chunk_markets:
                mk_event_id = (mk.get('event') or {}).get('id')
                if mk_event_id:
                    markets_by_event.setdefault(mk_event_id, []).append(mk)

        event_count = 0
        market_count = 0
        runner_count = 0

        # 3. Upsert per evento
        for event_id, event in event_meta.items():
            ev_markets = markets_by_event.get(event_id, [])

            # Competition dal primo mercato CHE ne ha una (rilievo Grok):
            # non tutti i tipi mercato la riportano.
            competition: Dict[str, Any] = {}
            for mk in ev_markets:
                comp = mk.get('competition') or {}
                if comp.get('id') or comp.get('name'):
                    competition = comp
                    break

            self.db.upsert_bf_event(
                event_id=event_id,
                name=event.get('name', ''),
                competition_id=str(competition.get('id', '')),
                competition_name=competition.get('name', ''),
                event_type_id=SOCCER_EVENT_TYPE_ID,
                open_date=event.get('openDate', ''),
                sync_id=sync_id
            )
            event_count += 1

            for mk in ev_markets:
                market_id = mk.get('marketId')
                if not market_id:
                    continue
                self.db.upsert_bf_market(
                    market_id=market_id,
                    event_id=event_id,
                    market_name=mk.get('marketName', ''),
                    market_type=(mk.get('description') or {}).get('marketType', ''),
                    total_matched=mk.get('totalMatched', 0.0),
                    open_date=mk.get('marketStartTime', ''),
                    sync_id=sync_id
                )
                market_count += 1

                # 3b. Runner: skip se privo di selectionId (rilievo
                #     Greptile/Fable) per non violare vincoli DB con chiave "".
                for rn in (mk.get('runners') or []):
                    selection_id = str(rn.get('selectionId', '') or '')
                    if not selection_id:
                        continue
                    self.db.upsert_bf_runner(
                        market_id=market_id,
                        selection_id=selection_id,
                        runner_name=rn.get('runnerName', ''),
                        handicap=rn.get('handicap', 0.0),
                        sort_priority=rn.get('sortPriority', 0),
                        sync_id=sync_id
                    )
                    runner_count += 1

        # 4. Pulizia + meta SOLO se il sync e' COMPLETO: eventi popolati,
        #    almeno un mercato, e nessun batch troncato. Fail-safe che
        #    impedisce di cancellare il catalogo su sync a 0 eventi, troncato
        #    o senza mercati (rilievo GPT/Grok/Fugu/Fable/CodeRabbit). Un
        #    cleanup su catalogo parziale rimuoverebbe mercati/runner ancora
        #    validi -> rischio blocco/errore nel trading LIVE.
        if event_count > 0 and market_count == 0:
            # Eventi presenti ma ZERO mercati su tutti: quasi sempre indica un
            # market-fetch fallito/anomalo (una risposta non-lista degradata a
            # [], o un errore upstream), non un catalogo realmente vuoto. Non
            # eseguiamo cleanup: preserviamo i mercati/runner esistenti.
            logger.warning(
                "[CatalogSync] %d eventi ma 0 mercati: market-fetch sospetto incompleto, "
                "catalogo esistente preservato (skip cleanup).", event_count,
            )
            return self._result(INCOMPLETE, "zero_mercati", event_count)
        elif event_count > 0 and not truncated:
            self.db.cleanup_stale_bf_data(sync_id)
            self.db.update_sync_meta(
                last_sync_at=sync_id,
                last_sync_id=sync_id,
                events=event_count,
                markets=market_count,
                runners=runner_count
            )
            logger.info("[CatalogSync] Sync completato: %d eventi, %d mercati, %d runner.", event_count, market_count, runner_count)
            return self._result(COMPLETED, "", event_count, market_count, runner_count)
        elif truncated:
            logger.warning(
                "[CatalogSync] Sync INCOMPLETO (batch troncato): catalogo esistente "
                "preservato, cleanup e meta saltati. Eventi visti: %d, mercati: %d.",
                event_count, market_count,
            )
            return self._result(INCOMPLETE, "batch_troncato", event_count, market_count, runner_count)
        else:
            logger.info("[CatalogSync] 0 eventi validi: catalogo esistente preservato (skip cleanup).")
            return self._result(INCOMPLETE, "nessun_evento_valido")

