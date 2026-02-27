@echo off
setlocal EnableExtensions
title Stock Analysis System Launcher
cls

echo ========================================
echo    Stock Analysis System Launcher
echo ========================================
echo.

:: Check conda environment
echo Checking conda environment...
call conda activate replay
if %errorlevel% neq 0 (
    echo ERROR: Cannot activate replay environment
    echo Please make sure conda is installed and replay environment exists
    pause
    exit /b 1
)

echo Environment activated successfully
echo.

:: Start full system directly
echo Starting full system (Backend + Frontend)...
echo.
goto start_full_system

:start_full_system
echo.
echo ========================================
echo    Starting Full System
echo ========================================
echo.

:: Check if Flask is already running
echo Checking existing processes...
set "FLASK_RUNNING=0"
set "FRONT_RUNNING=0"

for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":5000 .*LISTENING"') do (
    set "FLASK_RUNNING=1"
    goto :check_frontend_port
)

:check_frontend_port
for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":8081 .*LISTENING"') do (
    set "FRONT_RUNNING=1"
    goto :check_ports_done
)

:check_ports_done
if "%FLASK_RUNNING%"=="1" (
    echo WARNING: Port 5000 is already in use. Backend may already be running.
)
if "%FRONT_RUNNING%"=="1" (
    echo WARNING: Port 8081 is already in use. Frontend may already be running.
)
if "%FLASK_RUNNING%"=="1" echo.
if "%FRONT_RUNNING%"=="1" echo.

:: Start Flask backend
echo [1/2] Starting Flask backend (port 5000)...
start "Flask Backend - Stock Analysis System" cmd /k "cd /d ""%~dp0"" && set RUN_MODE=prod && python flask_app.py"

echo Waiting for backend to start...
timeout /t 5 /nobreak >nul

:: Start Vue frontend
echo [2/2] Starting Vue frontend (port 8081)...
start "Vue Frontend - Stock Analysis System" cmd /k "cd /d ""%~dp0frontend"" && npm run serve"

echo.
echo ========================================
echo    System Startup Complete
echo ========================================
echo.
echo Backend Service: http://localhost:5000
echo Frontend Interface: http://localhost:8081
echo.
echo Waiting for services to fully start...
timeout /t 8 /nobreak >nul

echo Opening frontend interface...
start http://localhost:8081

echo.
echo System startup completed!
echo To close the system, please close the corresponding command windows
goto end

:end
echo.
echo Press any key to close launcher...
pause >nul
