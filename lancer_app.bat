@echo off
cd /d "%~dp0"
echo ================================================
echo   Suivi Creances Clients - Demarrage
echo ================================================
echo.

REM Installation des dependances si necessaire
python -m pip show streamlit >nul 2>&1
if errorlevel 1 (
    echo Installation des dependances...
    python -m pip install -r requirements.txt
    echo.
)

echo Lancement de l'application...
echo L'application s'ouvrira dans votre navigateur.
echo Pour arreter : fermez cette fenetre.
echo.
python -m streamlit run app.py
pause
