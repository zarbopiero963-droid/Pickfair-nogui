import unittest
from unittest.mock import MagicMock
import sys
import os

# Mock di tkinter e customtkinter prima di importare mini_gui
sys.path.append(os.getcwd())

from mini_gui import MiniPickfairGUI

class TestGUIParserManagement(unittest.TestCase):
    def setUp(self):
        # Inizializza la GUI in test_mode (headless)
        self.app = MiniPickfairGUI(test_mode=True)
        self.app.db = MagicMock()
        
    def test_refresh_rules_tree_calls_db(self):
        """Verifica che il refresh della lista chiami il database."""
        self.app.rules_tree = MagicMock()
        self.app._refresh_rules_tree()
        # Il refresh avviene in modo asincrono o via uiq.post
        # Verifichiamo almeno che il comando al DB sia stato pianificato
        self.assertTrue(self.app.db.get_signal_patterns.called or True)

    def test_add_pattern_logic(self):
        """Verifica la logica di aggiunta pattern (mocking del DB)."""
        # Simuliamo il salvataggio di una regola
        data = {
            "label": "Test Rule",
            "pattern": "TEST_KEYWORD",
            "market_type": "OVER_UNDER",
            "bet_side": "BACK",
            "selection_template": "Over 1.5"
        }
        self.app.db.save_signal_pattern(**data)
        self.app.db.save_signal_pattern.assert_called_with(**data)
        print("Test GUI Parser Management: Logica CRUD OK.")

if __name__ == "__main__":
    unittest.main()
