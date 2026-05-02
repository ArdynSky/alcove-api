@echo off
setlocal
cd /d "%~dp0"

if not exist "Bot-Review\ALCOVE_FOX\venv\Scripts\python.exe" (
  echo Could not find Python virtual environment at Bot-Review\ALCOVE_FOX\venv
  pause
  exit /b 1
)

"Bot-Review\ALCOVE_FOX\venv\Scripts\python.exe" local_wheel_host_helper.py

endlocal
