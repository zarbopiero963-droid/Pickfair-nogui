import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class CatalogSyncService:
    """
    Sincronizza il catalogo Betfair (Eventi, Mercati, Runner) nel database locale.
    Implementa la Fase A1 (Catalogo Cache).
    """
    def __init__(self, db, betfair_client):
        self.db = db
        self.client = betfair_client
        self.is_syncing = False

    def run_sync(self, force: bool = False):
        if self.is_syncing:
            return
        
        # Controllo se è necessaria la sincronizzazione (es. una volta al giorno)
        meta = self.db.get_sync_meta()
        if not force and meta:
            last_sync = datetime.fromisoformat(meta['last_sync_at'])
            if datetime.utcnow() - last_sync < timedelta(hours=12):
                logger.info("[CatalogSync] Sync non necessario, ultima esecuzione: %s", last_sync)
                return

        self.is_syncing = True
        sync_id = datetime.utcnow().isoformat()
        logger.info("[CatalogSync] Avvio sincronizzazione catalogo (ID: %s)...", sync_id)
        
        # Tipi mercato d'interesse per il soccer.
        SOCCER_EVENT_TYPE_ID = "1"
        market_types = ['MATCH_ODDS', 'OVER_UNDER_05', 'OVER_UNDER_15', 'OVER_UNDER_25', 'CORRECT_SCORE']

        try:
            # 1. Recupero Eventi Soccer (Betfair listEvents; formato nativo:
            #    ogni elemento e' {"event": {...}, "marketCount": N}).
            events = self.client.list_events([SOCCER_EVENT_TYPE_ID], in_play_only=False)

            # Fail-safe (rilievo GPT/GLM): un sync che non recupera NESSUN evento
            # non deve toccare il catalogo esistente. Succede in SIM (il catalogo
            # arriva dai feed streaming, non via API) o in LIVE su risposta
            # vuota/anomala. Senza questo guard, cleanup_stale_bf_data cancella
            # tutto il catalogo -> trading bloccato. Skip: preserva l'esistente.
            if not events:
                logger.info("[CatalogSync] Nessun evento restituito: catalogo esistente preservato (skip cleanup).")
                return

            # Metadati evento indicizzati per id (name/openDate da listEvents).
            event_meta: Dict[str, Dict[str, Any]] = {}
            for ev in events:
                e = ev.get('event') or {}
                eid = e.get('id')
                if eid:
                    event_meta[eid] = e
            event_ids = list(event_meta.keys())

            # 2. Mercati in BATCH per evitare N+1 / rate-limit 429 in LIVE
            #    (rilievo Greptile/Codacy/GLM/Fable): listMarketCatalogue accetta
            #    piu' eventIds e riporta event + competition in ogni market
            #    (marketProjection). Chunk per non superare maxResults.
            markets_by_event: Dict[str, list] = {}
            chunk_size = 20
            for i in range(0, len(event_ids), chunk_size):
                chunk = event_ids[i:i + chunk_size]
                for mk in self.client.list_market_catalogue(
                    [SOCCER_EVENT_TYPE_ID],
                    event_ids=chunk,
                    market_type_codes=market_types,
                    max_results=1000,
                ):
                    mk_event_id = (mk.get('event') or {}).get('id')
                    if mk_event_id:
                        markets_by_event.setdefault(mk_event_id, []).append(mk)

            event_count = 0
            market_count = 0
            runner_count = 0

            # 3. Upsert per evento
            for event_id, event in event_meta.items():
                ev_markets = markets_by_event.get(event_id, [])

                # Competition dal primo mercato CHE ne ha una (rilievo GLM):
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

            # 4. Pulizia + meta SOLO se abbiamo popolato eventi: fail-safe che
            #    impedisce di cancellare il catalogo su un sync a 0 eventi
            #    (rilievo GPT/GLM/Fable).
            if event_count > 0:
                self.db.cleanup_stale_bf_data(sync_id)
                self.db.update_sync_meta(
                    last_sync_at=sync_id,
                    last_sync_id=sync_id,
                    events=event_count,
                    markets=market_count,
                    runners=runner_count
                )
                logger.info("[CatalogSync] Sync completato: %d eventi, %d mercati, %d runner.", event_count, market_count, runner_count)
            else:
                logger.info("[CatalogSync] 0 eventi validi: catalogo esistente preservato (skip cleanup).")

        except Exception as e:
            logger.error("[CatalogSync] Errore durante la sincronizzazione: %s", e)
        finally:
            self.is_syncing = False
