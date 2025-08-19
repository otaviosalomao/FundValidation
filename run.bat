@echo off
echo ========================================
echo    APLICACAO DE COLETA DE DADOS
echo ========================================
echo.

echo Verificando dependencias...
python -c "import requests, pandas" 2>nul
if errorlevel 1 (
    echo Instalando dependencias...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo Erro ao instalar dependencias!
        pause
        exit /b 1
    )
)

echo.
echo Executando teste de conectividade...
python test_app.py
if errorlevel 1 (
    echo.
    echo Teste falhou! Verifique a conectividade com a API.
    pause
    exit /b 1
)

echo.
echo Executando teste de banco de dados...
python test_database.py
if errorlevel 1 (
    echo.
    echo Teste de banco falhou! Verifique a conexão com o SQL Server.
    pause
    exit /b 1
)

echo.
echo Iniciando coleta de dados...
python main.py

echo.
echo Pressione qualquer tecla para sair...
pause >nul
