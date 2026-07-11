# Report: Neutralizzazione Motore Parser e Separazione Dati Utente

Come richiesto, ho rimosso ogni logica di parsing specifica (hardcoded) dal codice sorgente di `Pickfair-nogui` per garantire che GitHub contenga solo un motore neutro. Le strategie di scommessa e le regole dei tipster ora vivono esclusivamente nel **Profilo Utente**.

## 🚀 Cambiamenti Apportati

### 1. Motore Neutro (`telegram_listener.py`)
- **Rimozione Legacy**: Eliminati tutti i pattern regex hardcoded (Over/Under, Next Goal, ecc.) che erano cablati nel software.
- **Caricamento Dinamico**: Il bot ora agisce come un contenitore vuoto. All'avvio, interroga il database locale dell'utente (`pickfair.db`) per caricare le regole di parsing.
- **Agnosticismo**: Il codice sorgente non sa più chi sia il tipster o quale sia il formato del messaggio finché l'utente non installa le sue regole.

### 2. Profilo Utente (`pickfair.db`)
- **Persistenza**: Le regole (Keyword, Regex, Template) sono salvate nella tabella `signal_patterns`.
- **Flessibilità**: L'utente può creare, modificare o eliminare le regole tramite la GUI o script di setup, senza mai toccare il codice Python.
- **Sicurezza**: Le strategie private dell'utente non vengono mai pusshate su GitHub poiché il database è locale.

## 🧪 Validazione
Ho eseguito uno script di test (`test_neutral_engine.py`) che ha confermato:
1.  **Rifiuto Hardcoded**: Il bot ignora i messaggi che prima riconosceva "per abitudine" se la regola non è nel DB.
2.  **Successo Dinamico**: Il bot riconosce perfettamente il messaggio di esempio (Myanmar League) solo dopo che la regola è stata inserita nel profilo utente.

## 📦 Deliverables
- `telegram_listener.py`: Versione "pulita" e neutra.
- `user_setup_parser.py`: Script (da usare post-installazione) per caricare le proprie regole nel profilo.
- `check_patterns.py`: Utilità per verificare quali regole sono attive nel proprio profilo.

**Il sistema è ora architetturalmente corretto: Motore su GitHub, Cervello nel Profilo.**
