REAL_SIGNAL_MESSAGES = [
    {
        "id": "real_001",
        "chat_id": 1001,
        "message_id": 1,
        "text": """🆚Rangers v Motherwell
🏆Scottish Premiership
⌚ time, 24m, 0 - 0

🔥 P.Exc. OVER 1,5 IN LEVA 🔊 ✅

📊88.18%""",
        "expected": {
            "match": "Rangers v Motherwell",
            "competition": "Scottish Premiership",
            "minute": 24,
            "score_home": 0,
            "score_away": 0,
            "total_goals": 0,
            "target_market": "Over 0.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_002",
        "chat_id": 1001,
        "message_id": 2,
        "text": """🆚Porto v AVS Futebol SAD
🏆Portuguese Primeira Liga
⌚ time, 6m, 0 - 0

🔥 P.Exc. NON TERMINA 0-0 🔊 ✅

📊98.37%""",
        "expected": {
            "match": "Porto v AVS Futebol SAD",
            "competition": "Portuguese Primeira Liga",
            "minute": 6,
            "score_home": 0,
            "score_away": 0,
            "total_goals": 0,
            "target_market": "Over 0.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_003",
        "chat_id": 1001,
        "message_id": 3,
        "text": """🆚Huddersfield v Northampton
🏆English Sky Bet League 1
⌚ time, 45m, 0 - 0

🔥 P.Exc. GOL 2 TEMPO 🔊 ✅

📊86.47%""",
        "expected": {
            "match": "Huddersfield v Northampton",
            "competition": "English Sky Bet League 1",
            "minute": 45,
            "score_home": 0,
            "score_away": 0,
            "total_goals": 0,
            "target_market": "Over 0.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_004",
        "chat_id": 1001,
        "message_id": 4,
        "text": """🆚Reading v Burton Albion
🏆English Sky Bet League 1
⌚ time, 11m, 0 - 0

🔥 P.Exc. NEXT GOL 🔊 ✅

📊88.33%""",
        "expected": {
            "match": "Reading v Burton Albion",
            "competition": "English Sky Bet League 1",
            "minute": 11,
            "score_home": 0,
            "score_away": 0,
            "total_goals": 0,
            "target_market": "Over 0.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_005",
        "chat_id": 1001,
        "message_id": 5,
        "text": """🆚Barnet v Crawley Town
🏆English Sky Bet League 2
⌚ time, 60m, 1 - 1

🔥 P.Exc. NEXT GOL 2 TEMPO 🔊 ✅

📊79.10%""",
        "expected": {
            "match": "Barnet v Crawley Town",
            "competition": "English Sky Bet League 2",
            "minute": 60,
            "score_home": 1,
            "score_away": 1,
            "total_goals": 2,
            "target_market": "Over 2.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_006",
        "chat_id": 1001,
        "message_id": 6,
        "text": """P.Bet.70/80 OVER SUCCESSIVO  🔊 ✅

🏆English Premier League
🆚Nottm Forest v Everton
⚽ 0 - 1
⌚ 47m
🥅Tiri in Porta  1-2
🎯Tiri Fuori  7-2
Possesso Palla: 70-30

📈Quota 0,5 HT Prematch:1.42

📊81.19%""",
        "expected": {
            "match": "Nottm Forest v Everton",
            "competition": "English Premier League",
            "minute": 47,
            "score_home": 0,
            "score_away": 1,
            "total_goals": 1,
            "target_market": "Over 1.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_007",
        "chat_id": 1001,
        "message_id": 7,
        "text": """P.Bet. PREMACHT 30/0,5HT/1,5HT/1 ASIATICO 🔊

🏆Myanmar National League
🆚Mahar United FC v Hanthawaddy United FC
⚽ 0 - 0
⌚ 1m
🥅Tiri in Porta  0-0
🎯Tiri Fuori  0-0
Possesso Palla: 46-54

📈Quota 0,5 HT Prematch:0

📊74.33%""",
        "expected": {
            "match": "Mahar United FC v Hanthawaddy United FC",
            "competition": "Myanmar National League",
            "minute": 1,
            "score_home": 0,
            "score_away": 0,
            "total_goals": 0,
            "target_market": "Over 0.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_008",
        "chat_id": 1001,
        "message_id": 8,
        "text": """P.Bet. PREMACHT 0,5HT 🔊 ✅

🏆Saudi Professional League
🆚Al-Kholood Club v Al-Hilal
⚽ 0 - 0
⌚ 1m
🥅Tiri in Porta  0-0
🎯Tiri Fuori  0-0
Possesso Palla: 50-50

📈Quota 0,5 HT Prematch:0

📊74.50%""",
        "expected": {
            "match": "Al-Kholood Club v Al-Hilal",
            "competition": "Saudi Professional League",
            "minute": 1,
            "score_home": 0,
            "score_away": 0,
            "total_goals": 0,
            "target_market": "Over 0.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_009",
        "chat_id": 1001,
        "message_id": 9,
        "text": """P.Bet. 0,5 HT LIVE 🔊 ❌

🏆Welsh Premiership
🆚Colwyn Bay v Flint Town United
⚽ 0 - 0
⌚ 5m
🥅Tiri in Porta  0-1
🎯Tiri Fuori  0-2
Possesso Palla: 48-52

📈Quota 0,5 HT Prematch:1.3

📊70.21%""",
        "expected": {
            "match": "Colwyn Bay v Flint Town United",
            "competition": "Welsh Premiership",
            "minute": 5,
            "score_home": 0,
            "score_away": 0,
            "total_goals": 0,
            "target_market": "Over 0.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_010",
        "chat_id": 1001,
        "message_id": 10,
        "text": """P.Bet. OVER SUCCESSIVO 🔊 ❌

🏆English Premier League
🆚Liverpool v Leeds
⚽ 0 - 0
⌚ 60m
🥅Tiri in Porta  4-2
🎯Tiri Fuori  8-0
Possesso Palla: 66-34

📈Quota 0,5 HT Prematch:1.27

📊76.92%""",
        "expected": {
            "match": "Liverpool v Leeds",
            "competition": "English Premier League",
            "minute": 60,
            "score_home": 0,
            "score_away": 0,
            "total_goals": 0,
            "target_market": "Over 0.5",
            "should_trade": True,
        },
    },
    {
        "id": "real_011",
        "chat_id": 1001,
        "message_id": 11,
        "text": """P.Bet. GOL SECONDO TEMPO LIVE  🔊 ✅

🏆Myanmar National League 2
🆚Yangon City v Silver Stars FC
⚽ 6 - 0
⌚ 46m
🥅Tiri in Porta  15-1
🎯Tiri Fuori  3-0
Possesso Palla: 59-41

📈Quota 0,5 HT Prematch:0

📊81.29%""",
        "expected": {
            "match": "Yangon City v Silver Stars FC",
            "competition": "Myanmar National League 2",
            "minute": 46,
            "score_home": 6,
            "score_away": 0,
            "total_goals": 6,
            "target_market": "Over 6.5",
            "should_trade": True,
        },
    },
]

SYNTHETIC_SIGNAL_MESSAGES = [
    {
        "id": "dup_exact_001",
        "chat_id": 1001,
        "message_id": 101,
        "text": """🆚Reading v Burton Albion
🏆English Sky Bet League 1
⌚ time, 11m, 0 - 0

🔥 P.Exc. NEXT GOL 🔊 ✅

📊88.33%""",
        "expected": {
            "duplicate_group": "reading_11_0_0",
            "should_trade": True,
        },
    },
    {
        "id": "dup_exact_002",
        "chat_id": 1001,
        "message_id": 102,
        "text": """🆚Reading v Burton Albion
🏆English Sky Bet League 1
⌚ time, 11m, 0 - 0

🔥 P.Exc. NEXT GOL 🔊 ✅

📊88.33%""",
        "expected": {
            "duplicate_group": "reading_11_0_0",
            "should_trade": False,
        },
    },
    {
        "id": "dup_cosmetic_001",
        "chat_id": 1001,
        "message_id": 103,
        "text": """🆚Reading v Burton Albion
🏆English Sky Bet League 1
⌚ time, 11m, 0 - 0

🔥 P.Exc. NEXT GOL 🔊 ✅
📊88.33%""",
        "expected": {
            "duplicate_group": "reading_11_0_0_cosmetic",
            "should_trade": True,
        },
    },
    {
        "id": "dup_cosmetic_002",
        "chat_id": 1001,
        "message_id": 104,
        "text": """🆚Reading v Burton Albion
🏆English Sky Bet League 1
⌚ time, 11m, 0 - 0

🔥 P.Exc.   NEXT GOL   🔊 ✅

📊88.33% ✅""",
        "expected": {
            "duplicate_group": "reading_11_0_0_cosmetic",
            "should_trade": False,
        },
    },
    {
        "id": "missing_score_001",
        "chat_id": 1001,
        "message_id": 105,
        "text": """🆚Roma v Milan
🏆Serie A
⌚ time, 70m

🔥 NEXT GOL 🔊 ✅

📊81.00%""",
        "expected": {
            "should_trade": False,
            "reason": "missing_score",
        },
    },
    {
        "id": "malformed_score_001",
        "chat_id": 1001,
        "message_id": 106,
        "text": """🆚Roma v Milan
🏆Serie A
⌚ time, 70m, 0 -- 0

🔥 NEXT GOL 🔊 ✅

📊81.00%""",
        "expected": {
            "should_trade": False,
            "reason": "malformed_score",
        },
    },
    {
        "id": "truncated_001",
        "chat_id": 1001,
        "message_id": 107,
        "text": """🆚Roma v Milan
🏆Serie A
⌚ time, 70m, 1 -""",
        "expected": {
            "should_trade": False,
            "reason": "truncated_message",
        },
    },
    {
        "id": "missing_probability_001",
        "chat_id": 1001,
        "message_id": 108,
        "text": """🆚Roma v Milan
🏆Serie A
⌚ time, 70m, 1 - 1

🔥 NEXT GOL 🔊 ✅""",
        "expected": {
            "match": "Roma v Milan",
            "competition": "Serie A",
            "minute": 70,
            "score_home": 1,
            "score_away": 1,
            "total_goals": 2,
            "target_market": "Over 2.5",
            "should_trade": True,
        },
    },
    {
        "id": "missing_odds_001",
        "chat_id": 1001,
        "message_id": 109,
        "text": """P.Bet. OVER SUCCESSIVO 🔊 ✅

🏆English Premier League
🆚Arsenal v Chelsea
⚽ 2 - 1
⌚ 60m
📊75.12%""",
        "expected": {
            "match": "Arsenal v Chelsea",
            "competition": "English Premier League",
            "minute": 60,
            "score_home": 2,
            "score_away": 1,
            "total_goals": 3,
            "target_market": "Over 3.5",
            "should_trade": True,
        },
    },
    {
        "id": "dash_parentheses_001",
        "chat_id": 1001,
        "message_id": 110,
        "text": """🆚Paris SG - Marseille (Women)
🏆French Division 1
⌚ time, 33m, 0 - 1

🔥 NEXT GOL 🔊 ✅

📊84.10%""",
        "expected": {
            "match": "Paris SG - Marseille (Women)",
            "competition": "French Division 1",
            "minute": 33,
            "score_home": 0,
            "score_away": 1,
            "total_goals": 1,
            "target_market": "Over 1.5",
            "should_trade": True,
        },
    },
    {
        "id": "out_of_order_newer",
        "chat_id": 1001,
        "message_id": 111,
        "text": """🆚Roma v Milan
🏆Serie A
⌚ time, 72m, 1 - 1

🔥 NEXT GOL 🔊 ✅

📊81.00%""",
        "expected": {
            "ordering_group": "roma_milan_ordering",
            "minute": 72,
            "should_trade": True,
        },
    },
    {
        "id": "out_of_order_older",
        "chat_id": 1001,
        "message_id": 112,
        "text": """🆚Roma v Milan
🏆Serie A
⌚ time, 70m, 1 - 1

🔥 NEXT GOL 🔊 ✅

📊81.00%""",
        "expected": {
            "ordering_group": "roma_milan_ordering",
            "minute": 70,
            "should_trade": False,
            "reason": "stale_message",
        },
    },
]

ALL_TELEGRAM_SIGNAL_MESSAGES = REAL_SIGNAL_MESSAGES + SYNTHETIC_SIGNAL_MESSAGES


def expected_over_market_from_score(score_home: int, score_away: int) -> str:
    total_goals = score_home + score_away
    return f"Over {total_goals + 0.5}"


EXPECTED_MARKET_MAPPING = [
    {"score": (0, 0), "target_market": "Over 0.5"},
    {"score": (0, 1), "target_market": "Over 1.5"},
    {"score": (1, 0), "target_market": "Over 1.5"},
    {"score": (1, 1), "target_market": "Over 2.5"},
    {"score": (2, 0), "target_market": "Over 2.5"},
    {"score": (0, 2), "target_market": "Over 2.5"},
    {"score": (2, 1), "target_market": "Over 3.5"},
    {"score": (1, 2), "target_market": "Over 3.5"},
    {"score": (3, 0), "target_market": "Over 3.5"},
    {"score": (6, 0), "target_market": "Over 6.5"},
]