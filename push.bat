@echo off
cd /d "C:\Users\Administrator\Documents\Airflow"

echo.
echo === Crypto Pipeline — Push to GitHub ===
echo.

git add -A

set /p MSG="Commit message: "

git commit -m "%MSG%"

git push origin main

echo.
echo Done. Airflow will pick up changes in ~90 seconds.
echo.
pause
