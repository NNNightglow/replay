@echo off
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
tasklist /FI "IMAGENAME eq python.exe" | findstr flask_app >nul 2>&1
if %errorlevel% equ 0 (
    echo WARNING: Flask process may already be running
    echo Please close existing processes to avoid port conflicts
    echo.
)

:: Start Flask backend
echo [1/2] Starting Flask backend (port 5000)...
start "Flask Backend - Stock Analysis System" cmd /c "conda activate replay && python flask_app.py"

echo Waiting for backend to start...
timeout /t 5 /nobreak >nul

:: Start Vue frontend
echo [2/2] Starting Vue frontend (port 8081)...
start "Vue Frontend - Stock Analysis System" cmd /c "conda activate replay && cd frontend && npm run serve"

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
