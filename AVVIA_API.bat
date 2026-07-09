@echo off
title Pickfair Market API
color 0A

echo.
echo  ██████╗ ██╗ ██████╗██╗  ██╗███████╗ █████╗ ██╗██████╗
echo  ██╔══██╗██║██╔════╝██║ ██╔╝██╔════╝██╔══██╗██║██╔══██╗
echo  ██████╔╝██║██║     █████╔╝ █████╗  ███████║██║██████╔╝
echo  ██╔═══╝ ██║██║     ██╔═██╗ ██╔══╝  ██╔══██║██║██╔══██╗
echo  ██║     ██║╚██████╗██║  ██╗██║     ██║  ██║██║██║  ██║
echo  ╚═╝     ╚═╝ ╚═════╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝╚═╝  ╚═╝
echo.
echo   Market Sync API — Betfair Exchange
echo   =====================================
echo.

:: Vai nella cartella dove si trova questo .bat
cd /d "%~dp0"

:: Controlla se Python e' installato
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERRORE] Python non trovato. Installalo da https://www.python.org/downloads/
    echo          Assicurati di spuntare "Add Python to PATH" durante l'installazione.
    pause
    exit /b 1
)

:: Controlla se esiste il file .env
if not exist ".env" (
    echo [AVVISO] File .env non trovato.
    echo          Copia .env.example in .env e compila le tue credenziali Betfair.
    echo.
    if exist ".env.example" (
        echo   Trovato .env.example — lo copio in .env per te...
        copy ".env.example" ".env"
        echo.
        echo   Apri il file .env e inserisci le tue credenziali, poi riavvia.
    )
    pause
    exit /b 1
)

:: Controlla dipendenze
python -c "import fastapi, uvicorn" >nul 2>&1
if errorlevel 1 (
    echo [INFO] Dipendenze mancanti. Avvio installazione automatica...
    echo.
    pip install fastapi uvicorn requests
    if errorlevel 1 (
        echo [ERRORE] Installazione dipendenze fallita.
        pause
        exit /b 1
    )
)

echo [OK] Avvio Pickfair Market API...
echo      Premi CTRL+C per fermare.
echo.
echo      Docs API: http://localhost:8765/docs
echo      Summary:  http://localhost:8765/summary
echo.

python betfair_market_api.py

echo.
echo [INFO] Server arrestato.
pause
