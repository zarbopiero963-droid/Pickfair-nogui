import sys
import os
import unittest
from unittest.mock import MagicMock
from datetime import datetime, timezone

# Aggiungi il path per importare i moduli core
sys.path.append(os.getcwd())

from telegram_listener import TelegramListener

class TestNeutralEngine(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock()
        self.listener = TelegramListener(
            api_id=123,
            api_hash="hash",
            session_string="session",
            db=self.db
        )

    def test_neutral_engine_rejects_hardcoded_legacy(self):
        """Verifica che il motore neutro NON riconosca più i vecchi pattern hardcoded."""
        # Un messaggio che prima sarebbe stato riconosciuto come Over 1.5
        text = "🆚Team A vs Team B\nOver 1.5 @1.80"
        
        # Simula DB senza pattern
        self.db.get_signal_patterns.return_value = []
        
        signal = self.listener.parse_signal(text)
        self.assertIsNone(signal, "Il motore neutro non dovrebbe riconoscere pattern hardcoded.")

    def test_neutral_engine_accepts_user_profile_patterns(self):
        """Verifica che il motore riconosca le regole caricate dal database dell'utente."""
        text = "P.Bet. GOL SECONDO TEMPO LIVE 🔊 ✅\n🆚Yangon City v Silver Stars FC\n⚽ 6 - 0\n⌚ 46m\n@1.80"
        
        # Simula la regola caricata dal DB (quella che abbiamo installato nel profilo)
        user_pattern = {
            "id": 1,
            "label": "GOL SECONDO TEMPO LIVE",
            "pattern": r"GOL SECONDO TEMPO LIVE",
            "keyword": "GOL SECONDO TEMPO LIVE",
            "enabled": True,
            "action": "QUICK_BET",
            "bet_side": "BACK",
            "market_type": "OVER_UNDER",
            "selection_template": "Over {over_line}",
            "min_minute": 45,
            "max_minute": 90,
            "live_only": True
        }
        self.db.get_signal_patterns.return_value = [user_pattern]
        
        signal = self.listener.parse_signal(text)
        
        self.assertIsNotNone(signal)
        self.assertEqual(signal["selection"], "Over 6.5") # 6-0 -> over_line è 6.5
        self.assertEqual(signal["event_name"], "Yangon City v Silver Stars FC")
        print("Test Motore Neutro: Caricamento dinamico dal profilo utente OK.")

if __name__ == "__main__":
    unittest.main()
