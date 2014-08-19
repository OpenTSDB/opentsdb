:: ###########################################
:: ## Script to create a standard maven 
:: ## project image of opentsdb linked back 
:: ## into the original
:: ###########################################
@echo off
GOTO:starthere

:mkdirtree
	IF EXIST %~1 GOTO MKDIRTREEDONE
	echo Creating DirTree %~1
	md %~1
:MKDIRTREEDONE	
GOTO:EOF



:linkdir
	IF EXIST %~2 GOTO LINKDIRTREEDONE
	mklink /D %~2 %~1
:LINKDIRTREEDONE
GOTO:EOF

:linkfile
	IF EXIST %~2 GOTO LINKFILEDONE
	mklink %~2 %~1
:LINKFILEDONE
GOTO:EOF




:starthere
set TSDB_HOME=%~dp0
set TSDB_SRC=%TSDB_HOME%src
set TSDB_TEST=%TSDB_HOME%test
set TSDB_MVN=%TSDB_HOME%mavenview

echo[
echo[
echo =============================================================================
echo Adding Mavenized View to OpenTSDB project at %TSDB_HOME%
echo =============================================================================
echo[
echo[

echo Creating base maven source tree

CALL :mkdirtree %TSDB_MVN%
CALL :mkdirtree %TSDB_MVN%\src\main\java\net\opentsdb\tsd
CALL :mkdirtree %TSDB_MVN%\src\main\java\tsd
CALL :mkdirtree %TSDB_MVN%\src\main\resources\shell
CALL :mkdirtree %TSDB_MVN%\src\test\java\net\opentsdb
CALL :mkdirtree %TSDB_MVN%\src\test\resources

echo[
echo[

echo Linking base maven source tree (except tsd)

for /f "tokens=*" %%G in ('dir /b /a:d "%TSDB_SRC%\*"') do (
	IF /I NOT %%G==tsd (
		echo Linking Directory %TSDB_MVN%\src\main\java\net\opentsdb\%%G ----to---- %TSDB_SRC%\%%G
		CALL :linkdir %TSDB_SRC%\%%G %TSDB_MVN%\src\main\java\net\opentsdb\%%G
	) ELSE (
		echo Skipping %%G
	)
)

echo[
echo[

echo Linking base maven source tree for tsd directories

for /f "tokens=*" %%G in ('dir /b "%TSDB_SRC%\tsd\*"') do (
	IF /I NOT %%G==client (
		IF EXIST %TSDB_SRC%\tsd\%%G\NUL (
			echo Linking Directory %TSDB_MVN%\src\main\java\net\opentsdb\tsd\%%G ----to---- %TSDB_SRC%\tsd\%%G
			CALL :linkdir %TSDB_SRC%\tsd\%%G %TSDB_MVN%\src\main\java\net\opentsdb\tsd\%%G
		) ELSE (
			IF /I NOT %%G==QueryUi.gwt.xml (
::				echo Linking File %TSDB_MVN%\src\main\java\net\opentsdb\tsd\%%G ----to---- %TSDB_SRC%\tsd\%%G
				CALL :linkfile %TSDB_SRC%\tsd\%%G %TSDB_MVN%\src\main\java\net\opentsdb\tsd\%%G
			) else (
				echo Skipping %%G
			)
		)
	) ELSE (
		echo Skipping %%G
	)
)

echo[
echo[

echo Linking Directory %TSDB_MVN%\src\main\java\tsd\client ----to---- %TSDB_SRC%\tsd\client
CALL :linkdir %TSDB_SRC%\tsd\client %TSDB_MVN%\src\main\java\tsd\client
CALL :linkfile %TSDB_SRC%\tsd\QueryUi.gwt.xml %TSDB_MVN%\src\main\java\tsd\QueryUi.gwt.xml

echo[
echo[


echo Linking Test Source Directories from %TSDB_TEST%

for /f "tokens=*" %%G in ('dir /ad /b "%TSDB_TEST%\*"') do (
	CALL :linkdir %TSDB_TEST%\%%G %TSDB_MVN%\src\test\java\net\opentsdb\%%G
)


echo[
echo[

echo Linking Main Resources

CALL :linkfile %TSDB_SRC%\logback.xml %TSDB_MVN%\src\main\resources\logback.xml
CALL :linkfile %TSDB_SRC%\opentsdb.conf %TSDB_MVN%\src\main\resources\opentsdb.conf
CALL :linkfile %TSDB_SRC%\opentsdb.conf.json %TSDB_MVN%\src\main\resources\opentsdb.conf.json


echo[
echo[


echo Linking Shell Resources

CALL :linkfile %TSDB_SRC%\create_table.sh %TSDB_MVN%\src\main\resources\shell\create_table.sh
CALL :linkfile %TSDB_SRC%\mygnuplot.bat %TSDB_MVN%\src\main\resources\shell\mygnuplot.bat
CALL :linkfile %TSDB_SRC%\mygnuplot.sh %TSDB_MVN%\src\main\resources\shell\mygnuplot.sh

echo[
echo[

echo Linking Build Aux

CALL :linkdir %TSDB_HOME%\build-aux %TSDB_MVN%\build-aux
CALL :linkfile %TSDB_HOME%\build-aux\BuildData.java %TSDB_MVN%\src\main\java\net\opentsdb\BuildData.java

echo[
echo[

echo Linking Maven POM

CALL :linkfile %TSDB_HOME%\maven-view.pom.xml %TSDB_MVN%\pom.xml




:: ===============================================================================
:: ===============================================================================


echo[
echo[
echo[
echo =============================================================================
echo DUMP ...
echo =============================================================================
echo[
echo[
echo[

dir %TSDB_MVN% /s /b


echo[
echo[
echo[
echo =============================================================================
echo DONE
echo =============================================================================
echo[
echo[
echo[




:: domavenview






