#!/bin/bash -x
BUILDROOT=./build;
TOOLS=./tools
DOCKER=$BUILDROOT/docker;
rm -r $DOCKER;
mkdir -p $DOCKER;
SOURCE_PATH=$BUILDROOT;
DEST_PATH=$DOCKER/libs;
mkdir -p $DEST_PATH;
cp ${TOOLS}/docker/Dockerfile ${DOCKER};
cp ${BUILDROOT}/../src/opentsdb.conf ${DOCKER};
cp ${BUILDROOT}/../src/logback.xml ${DOCKER};
#cp ${BUILDROOT}/../src/mygnuplot.sh ${DOCKER};
cp ${SOURCE_PATH}/tsdb-2.3.0-RC1.jar ${DOCKER};
cp ${SOURCE_PATH}/third_party/*/*.jar ${DEST_PATH};
docker build -t opentsdb/opentsdb $DOCKER
