import logging
import json
from database import Database
from services.deterministic_resolver import DeterministicResolver
from services.delimiter_parser import DelimiterParser
from core.dutching_calculator import DutchingCalculator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HardTestV2")

def run_hard_test():
    db = Database(db_path=":memory:")
    resolver = DeterministicResolver(db)
    parser = DelimiterParser(db)
    
    logger.info("--- Fase 1: Setup Provider e Alias ---")
    pid = db.save_provider("TipsterPro")
    db.save_name_alias(pid, "Manchester City", "man city", "UK")
    db.save_name_alias(pid, "Real Madrid", "real madrid", "ES")
    db.save_market_alias(pid, "risultato esatto", "CORRECT_SCORE", market_name="Correct Score")
    
    logger.info("--- Fase 2: Setup Catalogo Betfair Mock ---")
    sync_id = "test_sync_1"
    db.upsert_bf_event("E1", "Manchester City v Real Madrid", "C1", "Champions League", "1", "2024-07-11T21:00:00", sync_id)
    db.upsert_bf_market("M1", "E1", "Correct Score", "CORRECT_SCORE", 1000.0, "2024-07-11T21:00:00", sync_id)
    db.upsert_bf_runner("M1", "11", "1 - 1", 0, 1, sync_id)
    db.upsert_bf_runner("M1", "21", "2 - 1", 0, 2, sync_id)
    db.upsert_bf_runner("M1", "12", "1 - 2", 0, 3, sync_id)

    logger.info("--- Fase 3: Setup Parser a Delimitatori ---")
    parser_def = {
        "type": "DUTCHING",
        "fields": {
            "event": [{"START_AFTER": "PARTITA:", "END_BEFORE": "\n"}],
            "market": [{"START_AFTER": "MERCATO:", "END_BEFORE": "\n"}]
        },
        "dutching": {
            "fields": [{"START_AFTER": "SCORE:", "END_BEFORE": "✅"}]
        }
    }
    db.save_parser("TestParser", pid, json.dumps(parser_def))
    db.set_parser_for_chat("CHAT123", 1) # Associa TestParser a CHAT123

    logger.info("--- Fase 4: Test Parsing Messaggio ---")
    msg = """
    🔔 NUOVO SEGNALE
    PARTITA: Man City v Real Madrid
    MERCATO: Risultato Esatto
    SCORE: 1-1, 2-1, 1-2 ✅
    """
    parsed = parser.parse_message("CHAT123", msg)
    assert parsed["event"] == "Man City v Real Madrid"
    assert parsed["selections"] == ["1-1", "2-1", "1-2"]
    logger.info("Parsing OK: %s", parsed)

    logger.info("--- Fase 5: Test Risoluzione Deterministica ---")
    event_id = resolver.resolve_event(pid, parsed["event"].split(' v ')[0]) # Man City
    assert event_id == "E1"
    
    market_info = resolver.resolve_market(pid, event_id, parsed["market"])
    assert market_info["market_id"] == "M1"
    
    selection_ids = []
    for sel in parsed["selections"]:
        # Normalizzazione score 1-1 -> 1 - 1
        norm_sel = sel.replace("-", " - ")
        sid = resolver.resolve_runner(market_info["market_id"], norm_sel)
        assert sid is not None
        selection_ids.append(sid)
    
    logger.info("Risoluzione OK: Event=%s, Market=%s, Selections=%s", event_id, market_info["market_id"], selection_ids)

    logger.info("--- Fase 6: Test Dutching Calculator ---")
    prices = {"11": 8.0, "21": 9.5, "12": 12.0}
    stakes = DutchingCalculator.calculate_stakes(0.50, prices)
    total_calc = sum(s["stake"] for s in stakes.values())
    logger.info("Dutching OK: Stakes=%s, Total=%.2f", stakes, total_calc)
    assert 0.49 <= total_calc <= 0.51

    logger.info("✅ TUTTI I TEST HARD SUPERATI!")

if __name__ == "__main__":
    run_hard_test()
