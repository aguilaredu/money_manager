@echo off
REM Activate the virtual environment
call .venv\Scripts\activate.bat

REM Run your Python script
python -i src\python\main.py

REM Deactivate the virtual environment
deactivate

pause