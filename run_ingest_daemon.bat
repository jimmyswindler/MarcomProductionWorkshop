@echo off
cd /d "%~dp0"

REM Activate Virtual Environment if it exists
IF EXIST "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
) ELSE (
    echo Warning: venv not found at %cd%\venv
)

REM Configuration for Windows Server
REM Update INPUT_DIR to the actual network share or local path on the new server
SET INPUT_DIR=C:\FTP\MarcomWebOrder_CRpass1705\PULL\
SET INTERVAL_SECONDS=300

echo =================================================
echo  Starting Data Ingest Polling Daemon (Windows)
echo  Monitoring: %INPUT_DIR%
echo  Interval: Every %INTERVAL_SECONDS% seconds.
echo  Press Ctrl+C to stop.
echo =================================================

REM Start the daemon
python ingest_daemon.py --input-dir "%INPUT_DIR%" --interval %INTERVAL_SECONDS%

pause
