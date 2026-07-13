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
            event_count = 0
            market_count = 0
            runner_count = 0

            for ev in events:
                event = ev.get('event') or {}
                event_id = event.get('id')
                if not event_id:
                    continue

                # 2. Recupero Mercati per l'evento (Match Odds, Over/Under,
                #    Correct Score). Il catalogo porta anche competition e runner.
                markets = self.client.list_market_catalogue(
                    [SOCCER_EVENT_TYPE_ID],
                    event_ids=[event_id],
                    market_type_codes=market_types,
                    max_results=50,
                )

                # La competition non e' in listEvents: la prendo dal catalogo
                # mercati (marketProjection COMPETITION).
                competition = {}
                if markets:
                    competition = markets[0].get('competition') or {}

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

                for mk in markets:
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

                    # 3. Recupero Runner
                    runners = mk.get('runners') or []
                    for rn in runners:
                        self.db.upsert_bf_runner(
                            market_id=market_id,
                            selection_id=str(rn.get('selectionId', '')),
                            runner_name=rn.get('runnerName', ''),
                            handicap=rn.get('handicap', 0.0),
                            sort_priority=rn.get('sortPriority', 0),
                            sync_id=sync_id
                        )
                        runner_count += 1

            # 4. Pulizia dati obsoleti
            self.db.cleanup_stale_bf_data(sync_id)
            
            # 5. Aggiornamento Meta
            self.db.update_sync_meta(
                last_sync_at=sync_id,
                last_sync_id=sync_id,
                events=event_count,
                markets=market_count,
                runners=runner_count
            )
            logger.info("[CatalogSync] Sync completato: %d eventi, %d mercati, %d runner.", event_count, market_count, runner_count)

        except Exception as e:
            logger.error("[CatalogSync] Errore durante la sincronizzazione: %s", e)
        finally:
            self.is_syncing = False
