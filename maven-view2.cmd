:: ###########################################
:: ## Script to create a standard maven 
:: ## project image of opentsdb linked back 
:: ## into the original
:: ###########################################
@echo off
GOTO:starthere

:mkdirtree
	IF EXIST %~1 GOTO MKDIRTREEDONE
	echo "Creating DirTree %~1"
	md %~1
:MKDIRTREEDONE	
GOTO:EOF



:linkdir
	IF EXIST %~2 GOTO LINKDIRTREEDONE
	echo "	Linking Dir %~2  --->  %~1"
	mklink /D %~2 %~1	
:LINKDIRTREEDONE
GOTO:EOF

:linkfile
	IF EXIST %~2 GOTO LINKFILEDONE
	echo "	Linking File %~2  --->  %~1"
	mklink %~2 %~1	
:LINKFILEDONE
GOTO:EOF




:starthere
set TSDB_HOME=%~dp0
REM
REM 
echo =============================================================================
echo Adding Mavenized View to OpenTSDB project at %TSDB_HOME%
echo =============================================================================
REM 
REM 

set SRCMAIN=%TSDB_HOME%src-main
CALL :mkdirtree %SRCMAIN%
CALL :mkdirtree %SRCMAIN%\net
CALL :mkdirtree %SRCMAIN%\tsd

pushd .
cd %SRCMAIN%\net
CALL :linkdir %TSDB_HOME%src %SRCMAIN%\net\opentsdb
popd

pushd .
cd %SRCMAIN%\tsd
CALL :linkfile %TSDB_HOME%src\tsd\QueryUi.gwt.xml %SRCMAIN%\tsd\QueryUi.gwt.xml
CALL :linkdir %TSDB_HOME%\src\tsd\client %SRCMAIN%\tsd\client
popd

set SRCTEST=%TSDB_HOME%src-test
CALL :mkdirtree %SRCTEST%\net
pushd .
cd %SRCTEST%\net
 CALL :linkdir %TSDB_HOME%test %SRCTEST%\net\opentsdb
popd

echo =============================================================================
echo DONE
echo =============================================================================



:: domavenview






