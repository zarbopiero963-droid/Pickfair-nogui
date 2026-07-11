import re
import json
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

class DelimiterParser:
    """
    Parser avanzato basato su delimitatori (START_AFTER, END_BEFORE).
    Implementa la Fase A2 (Parser Personalizzato).
    """
    def __init__(self, db):
        self.db = db

    def parse_message(self, chat_id: str, text: str) -> Optional[Dict[str, Any]]:
        # 1. Recupera il parser associato alla chat
        parser_cfg = self.db.get_parser_for_chat(chat_id)
        if not parser_cfg:
            return None

        definition = json.loads(parser_cfg['definition'])
        provider_id = parser_cfg['provider_id']
        
        result = {
            "provider_id": provider_id,
            "raw_text": text,
            "type": definition.get("type", "SINGLE") # SINGLE o DUTCHING
        }

        # 2. Estrazione Campi (Event, Market Phrase, Selection, Price)
        fields = definition.get("fields", {})
        for field_name, rules in fields.items():
            val = self._extract_field(text, rules)
            if val:
                result[field_name] = val

        # 3. Gestione Dutching (Correct Score)
        if result["type"] == "DUTCHING":
            dutch_rules = definition.get("dutching", {})
            result["selections"] = self._extract_dutching_selections(text, dutch_rules)

        return result

    def _extract_field(self, text: str, rules: List[Dict[str, str]]) -> Optional[str]:
        for rule in rules:
            start = rule.get("START_AFTER")
            end = rule.get("END_BEFORE")
            
            try:
                if start and end:
                    pattern = re.escape(start) + r"(.*?)" + re.escape(end)
                    match = re.search(pattern, text, re.DOTALL)
                    if match:
                        return match.group(1).strip()
                elif start:
                    idx = text.find(start)
                    if idx != -1:
                        return text[idx + len(start):].strip().split('\n')[0]
            except Exception:
                continue
        return None

    def _extract_dutching_selections(self, text: str, rules: Dict[str, Any]) -> List[str]:
        # Estrae una lista di selezioni (es. "1-1, 2-1, 1-2")
        raw = self._extract_field(text, rules.get("fields", []))
        if not raw:
            return []
        
        # Split per virgola o spazio
        selections = [s.strip() for s in re.split(r'[, ]+', raw) if s.strip()]
        return selections
