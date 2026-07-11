from typing import List, Dict, Any

class DutchingCalculator:
    """
    Calcola gli stake per una scommessa in Dutching (Correct Score).
    """
    @staticmethod
    def calculate_stakes(total_stake: float, runner_prices: Dict[str, float]) -> Dict[str, Dict[str, float]]:
        """
        total_stake: Stake totale da dividere (es. 0.50€)
        runner_prices: Dict {selection_id: price}
        
        Ritorna: Dict {selection_id: {"stake": float, "price": float}}
        """
        if not runner_prices:
            return {}

        # 1. Calcolo probabilità implicite (1/quota)
        probabilities = {sid: 1.0 / price for sid, price in runner_prices.items()}
        sum_probs = sum(probabilities.values())

        # 2. Distribuzione stake proporzionale alla probabilità
        results = {}
        for sid, prob in probabilities.items():
            # Stake = (Prob / Somma Prob) * Total Stake
            # In alternativa, per profitto uguale: Stake = (1/Price) / Sum(1/Price) * Total Stake
            stake = (prob / sum_probs) * total_stake
            results[sid] = {
                "stake": round(stake, 2),
                "price": runner_prices[sid]
            }

        return results
