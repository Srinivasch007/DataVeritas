@echo off
cd /d "%~dp0"
call "%~dp0venv\Scripts\activate.bat"
streamlit run app.py
