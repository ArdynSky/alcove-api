@echo off
setlocal
cd /d "%~dp0"

if not exist "Bot-Review\ALCOVE_FOX\venv\Scripts\python.exe" (
  echo Could not find Python virtual environment at Bot-Review\ALCOVE_FOX\venv
  pause
  exit /b 1
)

set "ALCOVE_FFMPEG_EXE=F:\Downloads - Copy\ffmpeg-8.0.1-full_build (2)\ffmpeg-8.0.1-full_build\bin\ffmpeg.exe"

"Bot-Review\ALCOVE_FOX\venv\Scripts\python.exe" local_wheel_host_helper.py

endlocal
