import sqlite3
import datetime
import json

def setup_user_profile():
    db_path = '/home/ubuntu/Pickfair-nogui/pickfair.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    # Regola per il messaggio specifico: GOL SECONDO TEMPO LIVE
    # P.Bet. GOL SECONDO TEMPO LIVE  🔊 ✅
    pattern_rule = {
        "label": "GOL SECONDO TEMPO LIVE",
        "pattern": r"GOL SECONDO TEMPO LIVE",
        "enabled": 1,
        "action": "QUICK_BET",
        "bet_side": "BACK",
        "market_type": "OVER_UNDER",
        "selection_template": "Over {over_line}", # Esempio: se 6-0 -> Over 6.5
        "min_minute": 45,
        "max_minute": 90,
        "live_only": 1,
        "priority": 1,
        "extra_json": json.dumps({"keyword": "GOL SECONDO TEMPO LIVE"}),
        "created_at": now,
        "updated_at": now
    }
    
    try:
        cursor.execute("""
            INSERT INTO signal_patterns 
            (label, pattern, enabled, action, bet_side, market_type, selection_template, min_minute, max_minute, live_only, priority, extra_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pattern_rule["label"],
            pattern_rule["pattern"],
            pattern_rule["enabled"],
            pattern_rule["action"],
            pattern_rule["bet_side"],
            pattern_rule["market_type"],
            pattern_rule["selection_template"],
            pattern_rule["min_minute"],
            pattern_rule["max_minute"],
            pattern_rule["live_only"],
            pattern_rule["priority"],
            pattern_rule["extra_json"],
            pattern_rule["created_at"],
            pattern_rule["updated_at"]
        ))
        conn.commit()
        print("Profilo Utente: Regola 'GOL SECONDO TEMPO LIVE' installata con successo.")
    except Exception as e:
        print(f"Errore durante l'installazione della regola: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    setup_user_profile()
