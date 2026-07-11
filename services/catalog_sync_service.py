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
        
        try:
            # 1. Recupero Eventi Soccer Live/Prossimi
            events = self.client.list_soccer_events(live_only=False)
            event_count = 0
            market_count = 0
            runner_count = 0

            for ev in events:
                event_id = ev['event_id']
                self.db.upsert_bf_event(
                    event_id=event_id,
                    name=ev['name'],
                    competition_id=ev.get('competition_id', ''),
                    competition_name=ev.get('competition_name', ''),
                    event_type_id="1", # Soccer
                    open_date=ev.get('open_date', ''),
                    sync_id=sync_id
                )
                event_count += 1

                # 2. Recupero Mercati per l'evento (Match Odds, Over/Under, Correct Score)
                market_types = ['MATCH_ODDS', 'OVER_UNDER_05', 'OVER_UNDER_15', 'OVER_UNDER_25', 'CORRECT_SCORE']
                markets = self.client.list_market_catalogue(
                    event_ids=[event_id],
                    market_types=market_types,
                    max_results=50
                )

                for mk in markets:
                    market_id = mk['marketId']
                    self.db.upsert_bf_market(
                        market_id=market_id,
                        event_id=event_id,
                        market_name=mk['marketName'],
                        market_type=mk.get('description', {}).get('marketType', ''),
                        total_matched=mk.get('totalMatched', 0.0),
                        open_date=mk.get('marketStartTime', ''),
                        sync_id=sync_id
                    )
                    market_count += 1

                    # 3. Recupero Runner
                    runners = mk.get('runners', [])
                    for rn in runners:
                        self.db.upsert_bf_runner(
                            market_id=market_id,
                            selection_id=str(rn['selectionId']),
                            runner_name=rn['runnerName'],
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
