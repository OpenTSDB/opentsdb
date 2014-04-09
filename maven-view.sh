#!/bin/bash
###########################################
## Script to create a fully standard maven 
## project image of opentsdb linked back 
## into the original
###########################################

if [ "$1" == "" ]; then
	echo "ERROR: directory specificity underflow"
	echo "Please specify the TSDB Project Root"
	echo "e.g. maven-view.sh /home/chappie/projects/opentsdb"
	echo "     or even maven-view.sh ."
	exit 1
fi

export TSDB_HOME=$(readlink -f $1)
export TSDB_SRC=$TSDB_HOME/src
export TSDB_MVN=$TSDB_HOME/mavenview

function mkdirtree() {
	if [ ! -d "$1" ]; then
		echo "Creating DirTree $1"
		mkdir -p $1
	fi
}

function linkdir() {   #  target, link	
	if [ ! -d "$2" ]; then
		echo "	Linking $2  --->  $1"
		ln -s $1 $2
	fi
}


echo ""
echo ""
echo "============================================================================="
read -p "About to mavenize TSDB at project root $TSDB_HOME. Are you sure? " -n 1 -r
echo    # (optional) move to a new line
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
	echo " ... and we're outa here."
    exit 1
fi

echo ""
echo ""
echo "============================================================================="
echo "Adding Mavenized View to OpenTSDB project at $TSDB_MVN"
echo "============================================================================="
echo ""
echo ""

echo "Checking mavenized view root"

mkdirtree "$TSDB_MVN"

echo "Creating base maven source tree"

mkdirtree "$TSDB_MVN/src/main/java/net/opentsdb/tsd"
mkdirtree "$TSDB_MVN/src/main/java/tsd/client"
mkdirtree "$TSDB_MVN/src/main/resources/shell"
mkdirtree "$TSDB_MVN/src/test/java/net/opentsdb"
mkdirtree "$TSDB_MVN/src/test/resources"

echo "Linking core source to directory tree matching package structure"

for dir in $TSDB_SRC/*
do
        dir=`basename $dir`

        if [[ -d "$TSDB_SRC/$dir" && ! -d $TSDB_MVN/src/main/java/net/opentsdb/$dir && $dir != "tsd" ]]; then
                echo " Linking $TSDB_MVN/src/main/java/net/opentsdb/$dir  --->  $TSDB_SRC/$dir"
                ln -s $TSDB_SRC/$dir $TSDB_MVN/src/main/java/net/opentsdb/$dir
        fi
done

echo "Linking tsd exceptions source to directory tree matching package structure"

for dir in $TSDB_SRC/tsd/*
do
        dir=`basename $dir`
        if [[ $dir != "client" ]]; then
        	linkdir "$TSDB_SRC/tsd/$dir" "$TSDB_MVN/src/main/java/net/opentsdb/tsd/$dir"
        fi
done


echo "Linking client core source to directory tree matching package structure"

for dir in $TSDB_SRC/tsd/client/*
do
        dir=`basename $dir`
        linkdir "$TSDB_SRC/tsd/client/$dir" "$TSDB_MVN/src/main/java/tsd/client/$dir"
done

echo "Linking client gwt.xml"
linkdir "$TSDB_SRC/tsd/QueryUi.gwt.xml" "$TSDB_MVN/src/main/java/tsd/QueryUi.gwt.xml"



echo "Linking test source to directory tree matching package structure"

for dir in $TSDB_HOME/test/*
do
        dir=`basename $dir`
        if [[ -d "$TSDB_HOME/test/$dir" &&  $dir != "META-INF" && $dir != "test"  &&  ! -d $TSDB_MVN/src/test/java/net/opentsdb/$dir ]]; then
                echo " Linking $TSDB_MVN/src/test/java/net/opentsdb/$dir  --->  $TSDB_HOME/test/$dir"
                ln -s $TSDB_HOME/test/$dir $TSDB_MVN/src/test/java/net/opentsdb/$dir
        fi
done

echo "Linking main resources"

if [ ! -e "$TSDB_MVN/src/main/resources/logback.xml" ]; then
	echo "	Linking $TSDB_MVN/src/main/resources/logback.xml  --->  $TSDB_SRC/logback.xml"
	ln -s $TSDB_SRC/logback.xml $TSDB_MVN/src/main/resources/logback.xml
fi

if [ ! -e "$TSDB_MVN/src/main/resources/opentsdb.conf" ]; then
	echo "	Linking $TSDB_MVN/src/main/resources/opentsdb.conf  --->  $TSDB_SRC/opentsdb.conf"
	ln -s $TSDB_SRC/opentsdb.conf $TSDB_MVN/src/main/resources/opentsdb.conf
fi


echo "Linking shell scripts main resources"

for shf in create_table.sh mygnuplot.sh mygnuplot.bat
do
	if [ ! -e "$TSDB_MVN/src/main/resources/shell/$shf" ]; then
		echo "	Linking $TSDB_MVN/src/main/resources/shell/$shf --->  $TSDB_SRC/$shf"
		ln -s $TSDB_SRC/$shf $TSDB_MVN/src/main/resources/shell/$shf
	fi
done

echo "Linking test resources"

if [ ! -e "$TSDB_MVN/src/test/resources/META-INF" ]; then
	echo "	Linking $TSDB_MVN/src/test/resources/META-INF  --->  $TSDB_HOME/test/META-INF"
	ln -s $TSDB_HOME/test/META-INF $TSDB_MVN/src/test/resources/META-INF
fi

echo "Linking build-aux"

linkdir "$TSDB_HOME/build-aux" "$TSDB_MVN/build-aux"

echo "Linking maven-view.pom.xml"
linkdir "$TSDB_HOME/maven-view.pom.xml" "$TSDB_MVN/pom.xml"






