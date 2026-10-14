"""B-RED-12: crash del processo tra l'invio al broker e la persistenza della risposta.

argv: TREE DBPATH MARKER PATH(exec|direct)
Il broker attende 0.5 s (il thread chiamante dell'engine ha il tempo di persistere cio' che persiste),
registra l'invio nel MARKER e termina il processo con os._exit(1): nessun finally, nessun flush applicativo.
"""
import os, sys, tempfile, time
TREE, DBPATH, MARKER, PATH = sys.argv[1:5]
os.chdir(tempfile.mkdtemp(prefix="pfcrash_")); sys.path.insert(0, TREE)
from database import Database
from core.event_bus import EventBus
from executor_manager import ExecutorManager
from services.setting_service import SettingsService
from services.betfair_service import BetfairService
from simulation_broker import SimulationBroker
from core.trading_engine import TradingEngine
from core.risk_gate import RiskGate, RoserpinaRiskLimits
from safe_mode import get_safe_mode_manager


class CrashBroker(SimulationBroker):
    def place_bet(self, **kw):
        time.sleep(0.5)
        with open(MARKER, "w") as fh:
            fh.write("SENT B-CRASH-1\n")
        os._exit(1)


db = Database(db_path=DBPATH); bus = EventBus(); settings = SettingsService(db); bf = BetfairService(settings)
br = CrashBroker(starting_balance=1000.0); bf.simulation_broker = br; bf.set_simulation_mode(True)
br.update_market_book({"marketId": "1.9", "status": "OPEN", "runners": [{"selectionId": 7, "status": "ACTIVE",
    "ex": {"availableToBack": [{"price": 3.0, "size": 500}], "availableToLay": [{"price": 3.1, "size": 500}]}}]})
ex = ExecutorManager(max_workers=2, default_timeout=10) if PATH == "exec" else None
eng = TradingEngine(bus=bus, db=db, client_getter=bf.get_client, executor=ex, safe_mode=get_safe_mode_manager(),
                    risk_middleware=RiskGate(config=RoserpinaRiskLimits(settings)))
eng.submit_quick_bet({"market_id": "1.9", "selection_id": 7, "side": "BACK", "bet_type": "BACK", "stake": 0.5, "price": 3.0,
                      "event_key": "A v B", "event_name": "A v B", "customer_ref": "r-crash-" + PATH, "simulation_mode": True})
time.sleep(5)  # il crash avviene nel thread del broker
os._exit(3)  # non dovrebbe mai arrivare qui
