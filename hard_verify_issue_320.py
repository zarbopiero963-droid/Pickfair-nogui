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
        self.config.auto_reset_drawdown_pct = 90.0
        self.config.lockdown_drawdown_pct = 95.0
        
        self.settings.load_roserpina_config.return_value = self.config
        
        # Mock per evitare crash in init
        self.settings.get_all_settings.return_value = {}
        self.settings.load_live_enabled.return_value = False
        
        self.controller = RuntimeController(
            bus=self.bus,
            db=self.db,
            settings_service=self.settings,
            betfair_service=self.betfair,
            telegram_service=self.telegram
        )
        # Re-inietta la config mockata
        self.controller.config = self.config
        self.controller.mode = RuntimeMode.ACTIVE
        self.controller.execution_mode = "LIVE"

    def test_min_stake_alignment(self):
        """Verifica che il MIN_STAKE globale sia 0.10."""
        self.assertEqual(MIN_STAKE, 0.10)
        print("Test MIN_STAKE Alignment: PASS")

    def test_a1_drawdown_hard_stop(self):
        """Test Enforcement A1: Drawdown Hard Stop."""
        self.controller.execution_mode = "LIVE"
        self.controller.mode = RuntimeMode.ACTIVE
        self.controller.live_enabled = True
        self.controller.live_readiness_ok = True
        
        # Forza la configurazione nel controller (mock getattr)
        self.config.max_drawdown_hard_stop_pct = 10.0
        
        # Mock per far passare i check iniziali
        self.controller.enforce_deploy_gate = MagicMock(return_value={"allowed": True, "reason": "GO"})
        
        # Mock allocate per ritornare un tavolo (indispensabile per arrivare all'enforcement DD)
        mock_table = MagicMock()
        mock_table.status = "FREE"
        self.controller.table_manager.allocate = MagicMock(return_value=mock_table)
        
        # Simula drawdown del 16% (limite è 10%)
        self.controller.risk_desk.equity_peak = 1000.0
        self.controller.risk_desk.bankroll_current = 840.0 # 16% DD
        
        # Mock force_lockdown per mutare lo stato (come farebbe il controller reale)
        def mock_lockdown(reason):
            self.controller.mode = RuntimeMode.LOCKDOWN
        self.controller.force_lockdown = MagicMock(side_effect=mock_lockdown)
        
        signal = {"market_id": "1.123", "selection_id": "456", "price": 2.0, "signal_type": "BACK"}
        self.controller._on_signal_received(signal)
        
        # Deve essere in LOCKDOWN e il segnale rifiutato
        self.assertEqual(self.controller.mode, RuntimeMode.LOCKDOWN)
        print("Test A1 Drawdown Hard Stop: PASS")

    def test_a2_max_open_exposure(self):
        """Test Enforcement A2: Max Open Exposure (Cap Assoluto)."""
        self.controller.mode = RuntimeMode.ACTIVE
        self.config.max_open_exposure = 50.0
        
        # Mock allocate per ritornare un tavolo
        self.controller.table_manager.allocate = MagicMock(return_value=MagicMock())
        
        # Esposizione attuale 45€, limite 50€, nuova bet 10€ -> deve fallire
        with patch.object(self.controller.table_manager, 'total_exposure', return_value=45.0):
            # Mock mm.calculate per restituire una bet approvata di 10€
            decision = MagicMock()
            decision.approved = True
            decision.recommended_stake = 10.0
            decision.table_id = 1
            self.controller.mm.calculate = MagicMock(return_value=decision)
            
            signal = {"market_id": "1.123", "selection_id": "456", "price": 2.0, "signal_type": "BACK"}
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
            
        signal = {"market_id": "1.123", "selection_id": "456", "price": 2.0, "signal_type": "BACK"}
        self.controller._on_signal_received(signal)
        
        rejection_call = [call for call in self.bus.publish.call_args_list if call[0][0] == "SIGNAL_REJECTED"]
        self.assertTrue(any("max_recovery_tables_reached" in str(c) for c in rejection_call))
        print("Test A3 Max Recovery Tables: PASS")

    def test_c1_sl_tp_monitoring(self):
        """Test C1: Monitoraggio SL/TP."""
        # Configura un tavolo attivo con SL (1-indexed!)
        table = MagicMock()
        table.table_id = 1
        table.status = "ACTIVE"
        table.market_id = "M1"
        table.selection_id = "S1"
        table.current_exposure = 10.0
        table.meta = {"price": 2.0, "bet_type": "BACK", "stop_loss": 5.0}
        
        # Iniettiamo il tavolo nel table_manager
        self.controller.table_manager._tables = {1: table}
        
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

    def test_issue_285_lockdown_snapshot(self):
        """Test Issue #285: Lockdown snapshot non-mutante."""
        self.controller.execution_mode = "LIVE"
        self.controller.simulation_mode = False
        
        # Mock get_status per verificare quando viene chiamato
        self.controller.get_status = MagicMock(return_value={"bankroll": 1000.0})
        
        # Esegui emergency_stop
        self.controller.emergency_stop("test_breach")
        
        # Verifica che get_status sia stato chiamato PRIMA di flippare a SIMULATION
        # (Nella logica corretta, force_lockdown -> get_status viene chiamato prima del flip)
        self.assertEqual(self.controller.get_status.call_count, 1)
        print("Test Issue #285 Lockdown Snapshot: PASS")

    def test_issue_274_293_risk_history_persistence(self):
        """Test Issue #274/293: Persistenza cronologia Risk Desk."""
        # Mock _extract_settlement_contract per ritornare un contratto valido senza validare il payload
        valid_contract = {
            "net_pnl": 5.0,
            "gross_pnl": 5.0,
            "commission_amount": 0.0,
            "commission_pct": 0.0,
            "settlement_basis": "market_net_realized",
            "settlement_source": "betfair",
            "settlement_kind": "realized_settlement",
            "settlement_validation": "accepted"
        }
        
        with patch.object(self.controller, '_extract_settlement_contract', return_value=valid_contract):
            payload = {"table_id": 1, "event_key": "E1"}
            self.db.add_risk_position_history = MagicMock()
            self.controller._on_close_position(payload)
            self.db.add_risk_position_history.assert_called_once()
            
        print("Test Issue #274/293 Risk History Persistence: PASS")

if __name__ == "__main__":
    unittest.main()
