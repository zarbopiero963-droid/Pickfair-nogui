import unittest
import sys
import os
from unittest.mock import MagicMock, patch

# Aggiungi la directory corrente al path
sys.path.append(os.getcwd())

from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode, DeskMode
from trading_config import MIN_STAKE

class TestIssue320Hard(unittest.TestCase):
    def setUp(self):
        self.bus = MagicMock()
        self.db = MagicMock()
        self.settings = MagicMock()
        self.betfair = MagicMock()
        self.telegram = MagicMock()
        
        # Configurazione mock
        self.config = MagicMock()
        self.config.table_count = 5
        self.config.min_stake = 0.10
        self.config.max_daily_loss = 100.0
        self.config.max_drawdown_hard_stop_pct = 10.0
        self.config.max_open_exposure = 50.0
        self.config.max_recovery_tables = 2
        self.config.allow_recovery = True
        self.config.anti_duplication_enabled = False
        
        self.settings.load_roserpina_config.return_value = self.config
        
        # Inizializza i tavoli nel mock settings prima di creare il controller
        self.config.table_count = 5
        
        self.controller = RuntimeController(
            bus=self.bus,
            db=self.db,
            settings_service=self.settings,
            betfair_service=self.betfair,
            telegram_service=self.telegram
        )
        self.controller.mode = RuntimeMode.ACTIVE
        self.controller.execution_mode = "LIVE"

    def test_min_stake_alignment(self):
        """Verifica che il MIN_STAKE globale sia 0.10."""
        self.assertEqual(MIN_STAKE, 0.10)
        print("Test MIN_STAKE Alignment: PASS")

    def test_a1_drawdown_hard_stop(self):
        """Test Enforcement A1: Drawdown Hard Stop."""
        # Forza la configurazione nel controller (mock getattr)
        self.controller.config.max_drawdown_hard_stop_pct = 10.0
        
        # Simula drawdown del 16% (limite è 10%)
        self.controller.risk_desk.equity_peak = 1000.0
        self.controller.risk_desk.bankroll_current = 840.0 # 16% DD
        
        # Mock force_lockdown per mutare lo stato (come farebbe il controller reale)
        def mock_lockdown(reason):
            self.controller.mode = RuntimeMode.LOCKDOWN
        self.controller.force_lockdown = MagicMock(side_effect=mock_lockdown)
        
        signal = {"market_id": "1.123", "selection_id": "456", "price": 2.0}
        self.controller._on_signal_received(signal)
        
        # Deve essere in LOCKDOWN e il segnale rifiutato
        self.assertEqual(self.controller.mode, RuntimeMode.LOCKDOWN)
        print("Test A1 Drawdown Hard Stop: PASS")

    def test_a2_max_open_exposure(self):
        """Test Enforcement A2: Max Open Exposure (Cap Assoluto)."""
        self.controller.config.max_open_exposure = 50.0
        
        # Esposizione attuale 45€, limite 50€, nuova bet 10€ -> deve fallire
        with patch.object(self.controller.table_manager, 'total_exposure', return_value=45.0):
            # Mock mm.calculate per restituire una bet approvata di 10€
            decision = MagicMock()
            decision.approved = True
            decision.recommended_stake = 10.0
            decision.table_id = 1
            self.controller.mm.calculate = MagicMock(return_value=decision)
            
            signal = {"market_id": "1.123", "selection_id": "456", "price": 2.0}
            self.controller._on_signal_received(signal)
            
            # Verifica che decision.approved sia stato flippato a False
            self.assertFalse(decision.approved)
            self.assertIn("max_open_exposure_exceeded", decision.reason)
        print("Test A2 Max Open Exposure: PASS")

    def test_a3_max_recovery_tables(self):
        """Test Enforcement A3: Limite Tavoli Recovery."""
        self.controller.config.max_recovery_tables = 2
        
        # Simula 2 tavoli già in recovery (1-indexed!)
        for i in range(1, 3):
            t = self.controller.table_manager._tables[i]
            t.status = "RECOVERY"
            t.in_recovery = True
            
        # Tenta di allocare un segnale che richiederebbe un terzo tavolo in recovery
        # (simuliamo che tutti gli altri tavoli siano ACTIVE, non FREE)
        for i in range(3, 6):
            self.controller.table_manager._tables[i].status = "ACTIVE"
            
        signal = {"market_id": "1.123", "selection_id": "456", "price": 2.0}
        self.controller._on_signal_received(signal)
        
        rejection_call = [call for call in self.bus.publish.call_args_list if call[0][0] == "SIGNAL_REJECTED"]
        self.assertTrue(any("max_recovery_tables_reached" in str(c) for c in rejection_call))
        print("Test A3 Max Recovery Tables: PASS")

    def test_c1_sl_tp_monitoring(self):
        """Test C1: Monitoraggio SL/TP."""
        # Configura un tavolo attivo con SL (1-indexed!)
        table = self.controller.table_manager._tables[1]
        table.status = "ACTIVE"
        table.market_id = "M1"
        table.selection_id = "S1"
        table.current_exposure = 10.0
        table.meta = {"price": 2.0, "bet_type": "BACK", "stop_loss": 5.0}
        
        # Simula snapshot di mercato con prezzo crollato (PnL negativo)
        market_book = {
            "marketId": "M1",
            "runners": [{"selectionId": "S1", "lastPriceTraded": 0.5}] # PnL = 10 * (0.5-2)/2 = -7.5€ (sfonda SL 5€)
        }
        
        # Mock _cashout_chain_wired per ritornare True
        with patch.object(self.controller, '_cashout_chain_wired', return_value=True):
            self.controller._monitor_sl_tp_trailing(market_book)
        
        # Deve pubblicare REQ_EXECUTE_CASHOUT
        self.bus.publish.assert_any_call("REQ_EXECUTE_CASHOUT", unittest.mock.ANY)
        print("Test C1 SL/TP Monitoring: PASS")

if __name__ == "__main__":
    unittest.main()
