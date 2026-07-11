
import unittest
import threading
import time
from unittest.mock import MagicMock, patch
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode

class TestZeroTouchSync(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock()
        self.bus = MagicMock()
        self.settings = MagicMock()
        self.betfair = MagicMock()
        self.telegram = MagicMock()
        
        # Mock per evitare crash durante l'init
        config = MagicMock()
        config.table_count = 5
        config.max_daily_loss = 100.0
        config.max_drawdown_hard_stop_pct = 10.0
        config.max_open_exposure = 50.0
        config.bankroll_initial = 1000.0
        self.settings.load_roserpina_config.return_value = config
        self.db.get_sync_meta.return_value = None
        
    @patch('core.runtime_controller.CatalogSyncService')
    @patch('core.runtime_controller.threading.Thread')
    def test_auto_sync_on_start(self, mock_thread, mock_sync_service):
        # Inizializza controller
        controller = RuntimeController(
            bus=self.bus,
            db=self.db,
            settings_service=self.settings,
            betfair_service=self.betfair,
            telegram_service=self.telegram
        )
        
        # Mock dei servizi necessari per start()
        self.betfair.connect.return_value = "session_token"
        self.betfair.get_account_funds.return_value = {"available": 1000.0}
        self.telegram.start.return_value = True
        
        # Esegue start
        controller.start(simulation_mode=True)
        
        # Verifica che il thread di sync sia stato creato
        self.assertTrue(mock_thread.called, "Il thread di sincronizzazione automatica non è stato avviato al boot.")
        
        # Verifica che il servizio di sync sia stato istanziato
        self.assertTrue(mock_sync_service.called, "CatalogSyncService non è stato istanziato nel controller.")

if __name__ == "__main__":
    unittest.main()
