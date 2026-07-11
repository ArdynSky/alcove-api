@echo off
setlocal
cd /d "%~dp0"

set "PYTHON=python"
if exist "venv\Scripts\python.exe" set "PYTHON=venv\Scripts\python.exe"
if exist "Bot-Review\ALCOVE_FOX\venv\Scripts\python.exe" set "PYTHON=Bot-Review\ALCOVE_FOX\venv\Scripts\python.exe"

if not exist "venv\Scripts\python.exe" (
  if not exist "Bot-Review\ALCOVE_FOX\venv\Scripts\python.exe" (
    echo Creating local venv in %CD%\venv ...
    python -m venv venv
    if errorlevel 1 (
      echo Could not create venv. Install Python 3.10+ and try again.
      pause
      exit /b 1
    )
    set "PYTHON=venv\Scripts\python.exe"
  )
)

echo Installing helper dependencies...
"%PYTHON%" -m pip install -q --upgrade pip
"%PYTHON%" -m pip install -q -r local_wheel_host_helper_requirements.txt
if errorlevel 1 (
  echo pip install failed. Try manually: pip install yt-dlp
  pause
  exit /b 1
)

echo.
"%PYTHON%" local_wheel_host_helper.py
pause
endlocal
