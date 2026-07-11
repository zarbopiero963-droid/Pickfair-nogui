#!/bin/bash

# Script di installazione automatica per Pickfair-nogui su Linux
# Installa le dipendenze di sistema, crea l'ambiente virtuale e configura il profilo utente.

set -e

echo "--- Pickfair-nogui Linux Installer ---"

# 1. Controllo dipendenze di sistema
echo "[1/4] Controllo dipendenze di sistema (sudo richiesto)..."
if [ -f /etc/debian_version ]; then
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip python3-venv python3-tk sqlite3
elif [ -f /etc/redhat-release ]; then
    sudo yum install -y python3 python3-pip tk sqlite
else
    echo "Distribuzione non supportata automaticamente. Assicurati di avere python3, pip, venv e tkinter installati."
fi

# 2. Creazione Ambiente Virtuale
echo "[2/4] Creazione ambiente virtuale (venv)..."
python3 -m venv venv
source venv/bin/activate

# 3. Installazione Dipendenze Python
echo "[3/4] Installazione dipendenze Python..."
pip install --upgrade pip
pip install -r requirements.txt
pip install customtkinter

# 4. Inizializzazione Database Profilo Utente
echo "[4/4] Inizializzazione database profilo utente..."
python3 -c "from database import Database; db = Database(); db.close_all_connections(); print('Database inizializzato.')"

echo "--------------------------------------"
echo "INSTALLAZIONE COMPLETATA CON SUCCESSO!"
echo "--------------------------------------"
echo "Per avviare il programma:"
echo "  source venv/bin/activate"
echo "  python3 main.py"
echo ""
echo "Per avviare la versione headless (solo Telegram):"
echo "  python3 main.py --headless"
echo "--------------------------------------"
