"""Stake mode follower — resolver PURO (Fase 2.3 / B7.1).

Funzione PURA che sceglie lo stake del follower in base alla **modalità**, dati i
candidati (master/fixed/MM), **fail-closed** verso il comportamento odierno:

- **MM** (default): usa lo stake calcolato da Roserpina (``mm_stake``) — il
  comportamento attuale.
- **MASTER**: usa lo stake del master (``master_stake``) trasmesso nel broadcast.
- **FIXED**: usa uno stake fisso da config (``fixed_stake``).

**Sicurezza**:
- modalità ignota/mancante, o candidato scelto non valido (non finito o ``<= 0``)
  ⇒ **fallback a MM** (mai un bet inventato); se anche ``mm_stake`` non è valido
  ⇒ ``0.0`` (nessun bet);
- ``cap`` opzionale (es. il max-single di Roserpina): per MASTER/FIXED lo stake
  scelto viene **clampato** al cap, così i limiti di sicurezza del follower non
  vengono mai superati. MM non viene capato (è già entro i limiti).

Zero dipendenze da broker/bus/runtime: solo dati in input → stake scelto. Il
wiring sul percorso copy (broadcast master B7.2, applicazione follower B7.3) è
separato; di default la modalità è ``MM`` ⇒ nessun cambiamento.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional

#: Modalità supportate.
MODE_MM = "MM"
MODE_MASTER = "MASTER"
MODE_FIXED = "FIXED"
_VALID_MODES = (MODE_MM, MODE_MASTER, MODE_FIXED)


def resolve_stake(
    *,
    mode: Any,
    master_stake: Any = None,
    fixed_stake: Any = None,
    mm_stake: Any = None,
    cap: Any = None,
) -> Dict[str, Any]:
    """Risolve lo stake del follower per modalità (fail-closed).

    Ritorna sempre un dict con:
    - ``stake``: lo stake da usare (mai negativo; ``0.0`` = nessun bet);
    - ``mode``: la modalità **effettiva** applicata (``MM`` se è scattato un
      fallback);
    - ``reason``: ``ok`` | ``capped`` | ``invalid_mode`` |
      ``invalid_master_stake`` | ``invalid_fixed_stake`` | ``invalid_mm_stake``;
    - ``requested_mode``: la modalità richiesta in input (normalizzata).
    """
    requested = str(mode or "").strip().upper()
    mm = _positive_float(mm_stake)

    def use_mm(reason: str) -> Dict[str, Any]:
        return {
            "stake": mm if mm is not None else 0.0,
            "mode": MODE_MM,
            "reason": reason if mm is not None else "invalid_mm_stake",
            "requested_mode": requested,
        }

    if requested == MODE_MM:
        return use_mm("ok")
    if requested not in _VALID_MODES:
        return use_mm("invalid_mode")

    # MASTER / FIXED: candidato esplicito, altrimenti fail-closed a MM.
    if requested == MODE_MASTER:
        candidate = _positive_float(master_stake)
        invalid_reason = "invalid_master_stake"
    else:  # FIXED
        candidate = _positive_float(fixed_stake)
        invalid_reason = "invalid_fixed_stake"

    if candidate is None:
        return use_mm(invalid_reason)

    reason = "ok"
    cap_val = _positive_float(cap)
    if cap_val is not None and candidate > cap_val:
        candidate = cap_val   # clamp ai limiti di sicurezza del follower
        reason = "capped"

    return {
        "stake": candidate,
        "mode": requested,
        "reason": reason,
        "requested_mode": requested,
    }


def _positive_float(value: Any) -> Optional[float]:
    """``float`` finito e ``> 0``, altrimenti ``None`` (bool rifiutato)."""
    if isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(out) or out <= 0.0:
        return None
    return out
