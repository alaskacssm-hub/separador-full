@echo off
setlocal
cd /d "%~dp0"
title FULL Control - Servidor

echo.
echo ==========================================
echo        FULL CONTROL - SERVIDOR
echo ==========================================
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [ERRO] Python nao encontrado.
    echo Instale Python 3.11+ e marque "Add Python to PATH".
    pause
    exit /b 1
)

echo [1/4] Encerrando servidor antigo na porta 8000...
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
    taskkill /PID %%P /F >nul 2>&1
)

echo [2/4] Verificando dependencias...
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo.
    echo [ERRO] Falha ao instalar as dependencias.
    pause
    exit /b 1
)

echo [3/4] Iniciando servidor...
start "FULL Control Server" cmd /k "cd /d ""%~dp0"" && python -m uvicorn main:app --host 0.0.0.0 --port 8000"

echo [4/4] Aguardando servidor...
set READY=0
for /l %%N in (1,1,15) do (
    powershell -NoProfile -Command "try { $r=Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/health -TimeoutSec 1; if($r.StatusCode -eq 200){exit 0}else{exit 1} } catch { exit 1 }" >nul 2>&1
    if not errorlevel 1 (
        set READY=1
        goto :ready
    )
    timeout /t 1 >nul
)

:ready
if "%READY%"=="1" (
    echo.
    echo ==========================================
    echo Servidor iniciado com sucesso!
    echo http://localhost:8000
    echo ==========================================
    start "" "http://localhost:8000/?mode=home"
) else (
    echo.
    echo [ERRO] O servidor nao respondeu na porta 8000.
    echo Verifique a janela "FULL Control Server".
)

echo.
echo Esta janela pode ser fechada.
echo O servidor continua na janela "FULL Control Server".
pause
endlocal
