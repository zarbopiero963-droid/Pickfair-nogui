import sqlite3

def check():
    conn = sqlite3.connect('/home/ubuntu/Pickfair-nogui/pickfair.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        rows = cursor.execute("SELECT * FROM signal_patterns").fetchall()
        print(f"Trovati {len(rows)} pattern nel database.")
        for row in rows:
            print("-" * 30)
            print(f"ID: {row['id']} | Label: {row['label']}")
            print(f"Pattern (Regex): {row['pattern']}")
            print(f"Selection Template: {row['selection_template']}")
            print(f"Extra JSON: {row['extra_json']}")
    except Exception as e:
        print(f"Errore: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check()
