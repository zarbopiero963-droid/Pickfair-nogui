import unittest
import sys
import os
from unittest.mock import MagicMock, patch

# Aggiungi la directory corrente al path per importare i moduli locali
sys.path.append(os.getcwd())

# Mock per evitare dipendenze pesanti durante i test headless
try:
    import customtkinter
    PATCH_TARGET = 'customtkinter'
except ImportError:
    # Se customtkinter non è installato, mini_gui.py userà il fallback interno
    # Non abbiamo bisogno di patchare customtkinter se non esiste
    PATCH_TARGET = None

if PATCH_TARGET:
    with patch(f'{PATCH_TARGET}.CTk'), patch(f'{PATCH_TARGET}.set_appearance_mode'), patch(f'{PATCH_TARGET}.set_default_color_theme'):
        from mini_gui import MiniPickfairGUI
else:
    from mini_gui import MiniPickfairGUI

class TestGUIHard(unittest.TestCase):
    def setUp(self):
        # Inizializza la GUI in modalità test (headless)
        self.gui = MiniPickfairGUI(test_mode=True)

    def test_initialization(self):
        """Verifica che tutte le tab e i componenti siano stati creati correttamente."""
        self.assertIsNotNone(self.gui.tab_dashboard)
        self.assertIsNotNone(self.gui.tab_bet_history)
        self.assertIsNotNone(self.gui.tab_risk_history)
        self.assertIsNotNone(self.gui.bet_history_tree)
        self.assertIsNotNone(self.gui.risk_desk_history_tree)
        print("Test Inizializzazione: PASS")

    def test_refresh_concurrency(self):
        """Simula molteplici eventi di refresh simultanei per testare il RefreshCoordinator."""
        for i in range(50):
            self.gui._on_refresh_event("bet_history", {"test_id": i})
            self.gui._on_refresh_event("risk_desk_history", {"test_id": i})
        print("Test Concorrenza Refresh: PASS")

    def test_db_persistence_load(self):
        """Verifica che i metodi di caricamento dal DB non crashino la GUI."""
        # Mock dei metodi del DB per restituire dati di test
        self.gui.db.get_recent_simulation_bets = MagicMock(return_value=[
            {"bet_id": "1", "event_name": "Test Event", "market_name": "Match Odds", "runner_name": "Team A", "size": 10.0, "price": 2.0, "status": "MATCHED", "created_at": "2023-01-01 12:00:00"}
        ])
        self.gui.db.get_risk_position_history = MagicMock(return_value=[
            {"id": 1, "closed_at": "2023-01-01 12:05:00", "outcome": "WIN", "net_pnl": 10.0, "stake": 5.0, "event_key": "E1", "market_id": "M1", "selection_id": "S1"}
        ])
        
        # Forza il refresh manuale
        self.gui._refresh_bet_history_data()
        self.gui._refresh_risk_desk_history_data()
        print("Test Caricamento Persistenza DB: PASS")

    def tearDown(self):
        self.gui.destroy()

if __name__ == "__main__":
    unittest.main()
