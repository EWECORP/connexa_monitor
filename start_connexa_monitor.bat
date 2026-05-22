@echo off
REM =======================================================
REM  start_connexa_monitor.bat
REM  Arranca el panel Streamlit de Connexa Monitor
REM =======================================================

REM --- Configuración de rutas desde .env ---
set "SCRIPT_DIR=%~dp0"
set "APP_DIR=%SCRIPT_DIR:~0,-1%"
set "ENV_FILE=%APP_DIR%\.env"

for /f "usebackq delims=" %%A in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$envFile='%ENV_FILE%'; $base=$env:BASE_DIR; if (-not $base -and (Test-Path -LiteralPath $envFile)) { $line = Get-Content -LiteralPath $envFile | Where-Object { $_ -match '^\s*BASE_DIR\s*=' } | Select-Object -First 1; if ($line) { $base = ($line -replace '^\s*BASE_DIR\s*=\s*', '') -replace '\s+#.*$', ''; $base = $base.Trim().Trim([char]34).Trim([char]39) } }; if (-not $base) { $base = (Resolve-Path -LiteralPath (Join-Path '%APP_DIR%' '..')).Path }; [Console]::Write($base)"`) do set "BASE_DIR=%%A"

if exist "%BASE_DIR%\connexa_monitor\app.py" set "APP_DIR=%BASE_DIR%\connexa_monitor"
set "ENV_FILE=%APP_DIR%\.env"

set "VENV_PY=%BASE_DIR%\ETL_DIARCO\venv\Scripts\python.exe"
if not exist "%VENV_PY%" set "VENV_PY=%BASE_DIR%\venv\Scripts\python.exe"
if not exist "%VENV_PY%" set "VENV_PY=%APP_DIR%\.venv\Scripts\python.exe"

set "APP_FILE=app.py"
set "LOG_DIR=%BASE_DIR%\logs\connexa_monitor"
set "LOG_FILE=%LOG_DIR%\streamlit_connexa.log"
set "ETL_ENV_PATH=%ENV_FILE%"

REM --- Crear carpeta de logs si no existe ---
if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

REM --- Ir al directorio de la app ---
cd /d "%APP_DIR%"

REM --- Escribir una marca de inicio en el log ---
echo =======================================================>> "%LOG_FILE%"
echo Iniciando Connexa Monitor %DATE% %TIME% >> "%LOG_FILE%"
echo BASE_DIR=%BASE_DIR% >> "%LOG_FILE%"
echo APP_DIR=%APP_DIR% >> "%LOG_FILE%"
echo ETL_ENV_PATH=%ETL_ENV_PATH% >> "%LOG_FILE%"
echo VENV_PY=%VENV_PY% >> "%LOG_FILE%"
echo =======================================================>> "%LOG_FILE%"

REM --- Lanzar Streamlit (modo headless, puerto 8501, todas las IPs) ---
"%VENV_PY%" -m streamlit run "%APP_FILE%" ^
  --server.address 0.0.0.0 ^
  --server.port 8501 ^
  --server.headless true >> "%LOG_FILE%" 2>&1
