@echo off
REM =======================================================
REM  start_connexa_monitor.bat
REM  Arranca el panel Streamlit de Connexa Monitor
REM =======================================================

REM --- Configuración de rutas ---
set "VENV_PY=E:\ETL\ETL_DIARCO\venv\Scripts\python.exe"
set "APP_DIR=E:\ETL\connexa_monitor"
set "APP_FILE=app.py"
set "LOG_DIR=E:\ETL\logs\connexa_monitor"
set "LOG_FILE=%LOG_DIR%\streamlit_connexa.log"

REM --- Crear carpeta de logs si no existe ---
if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

REM --- Ir al directorio de la app ---
cd /d "%APP_DIR%"

REM --- Escribir una marca de inicio en el log ---
echo =======================================================>> "%LOG_FILE%"
echo Iniciando Connexa Monitor %DATE% %TIME% >> "%LOG_FILE%"
echo =======================================================>> "%LOG_FILE%"

REM --- Lanzar Streamlit (modo headless, puerto 8501, todas las IPs) ---
"%VENV_PY%" -m streamlit run "%APP_FILE%" ^
  --server.address 0.0.0.0 ^
  --server.port 8501 ^
  --server.headless true >> "%LOG_FILE%" 2>&1
