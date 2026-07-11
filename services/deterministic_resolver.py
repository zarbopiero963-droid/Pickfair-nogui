import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class DeterministicResolver:
    """
    Risolve Eventi, Mercati e Runner usando il catalogo locale e le mappe alias.
    Implementa la Fase A1 (Resolver Deterministico).
    """
    def __init__(self, db):
        self.db = db

    def resolve_event(self, provider_id: int, event_name: str) -> Optional[str]:
        """
        Cerca l'event_id Betfair partendo dal nome fornito dal provider.
        1. Cerca negli alias diretti (name_aliases)
        2. Cerca nel catalogo locale (bf_events) con normalizzazione
        """
        # Normalizzazione nome provider
        alias_norm = event_name.lower().strip()
        
        # 1. Alias diretto
        rows = self.db._execute(
            "SELECT betfair_name FROM name_aliases WHERE provider_id = ? AND alias_norm = ?",
            (provider_id, alias_norm),
            fetch=True, commit=False
        )
        if rows:
            bf_name = rows[0]['betfair_name']
            # Cerca l'event_id per quel nome esatto o come parte del nome "Home v Away"
            ev = self.db._execute(
                "SELECT event_id FROM bf_events WHERE name = ? OR name LIKE ? OR name LIKE ? LIMIT 1",
                (bf_name, f"{bf_name} v %", f"% v {bf_name}"),
                fetchone=True, commit=False
            )
            if ev:
                return ev['event_id']

        # 2. Ricerca diretta nel catalogo (Match esatto o parte di "Home v Away")
        ev = self.db._execute(
            "SELECT event_id FROM bf_events WHERE LOWER(name) = ? OR LOWER(name) LIKE ? OR LOWER(name) LIKE ? LIMIT 1",
            (alias_norm, f"{alias_norm} v %", f"% v {alias_norm}"),
            fetchone=True, commit=False
        )
        if ev:
            return ev['event_id']
            
        return None

    def resolve_market(self, provider_id: int, event_id: str, market_phrase: str) -> Optional[Dict[str, Any]]:
        """
        Risolve il mercato usando le frasi alias del provider.
        """
        phrase_norm = market_phrase.lower().strip()
        
        # Cerca negli alias dei mercati
        rows = self.db._execute(
            "SELECT market_type, market_name, selection_name FROM market_aliases WHERE provider_id = ? AND phrase_norm = ?",
            (provider_id, phrase_norm),
            fetch=True, commit=False
        )
        if not rows:
            return None
            
        alias = rows[0]
        m_type = alias['market_type']
        
        # Cerca il market_id reale per quell'evento e tipo
        mk = self.db._execute(
            "SELECT market_id, market_name FROM bf_markets WHERE event_id = ? AND market_type = ? LIMIT 1",
            (event_id, m_type),
            fetchone=True, commit=False
        )
        if not mk:
            return None
            
        return {
            "market_id": mk['market_id'],
            "market_type": m_type,
            "market_name": mk['market_name'],
            "selection_name": alias['selection_name']
        }

    def resolve_runner(self, market_id: str, selection_name: str) -> Optional[str]:
        """
        Trova il selection_id Betfair per un dato runner name nel mercato.
        """
        rn = self.db._execute(
            "SELECT selection_id FROM bf_runners WHERE market_id = ? AND LOWER(runner_name) = ? LIMIT 1",
            (market_id, selection_name.lower().strip()),
            fetchone=True, commit=False
        )
        if rn:
            return str(rn['selection_id'])
        return None
