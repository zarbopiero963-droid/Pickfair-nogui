@echo off
title Pickfair — Installazione dipendenze
color 0B
echo.
echo  Pickfair Market API — Installazione
echo  =====================================
echo.

cd /d "%~dp0"

python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRORE] Python non trovato.
    echo          Scaricalo da: https://www.python.org/downloads/
    echo          Spunta "Add Python to PATH" durante l'installazione!
    pause
    exit /b 1
)

echo [INFO] Python trovato:
python --version
echo.

echo [INFO] Installazione dipendenze in corso...
pip install fastapi uvicorn requests colorlog python-dateutil pytz typing-extensions

echo.
echo [OK] Dipendenze installate.
echo.
echo [PROSSIMO PASSO]
echo   1. Copia .env.example in .env
echo   2. Inserisci le tue credenziali Betfair in .env
echo   3. Copia i tuoi certificati (.pem) nella cartella
echo   4. Doppio click su AVVIA_API.bat
echo.
pause
