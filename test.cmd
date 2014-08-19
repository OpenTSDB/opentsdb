@echo off
set TSDB_HOME=%~dp0
set TSDB_SRC=%TSDB_HOME%\src

echo =
echo ==================================================
echo TSDB HOME: %TSDB_HOME%
echo ==================================================
echo =


set CURRRDIR=%%~dp0
echo "CURRRDIR: %~dp0"

:: for /D %%D in (%TSDB_HOME%\*) do (
:: 	SETLOCAL
:: 	SET FD=%TSDB_HOME%\%%~nxD
:: 		echo  %FD%
:: 	ENDLOCAL
:: )	

