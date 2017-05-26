FROM java:openjdk-8-alpine

MAINTAINER clarsen@yahoo-inc.com

# Important, must be passed in e.g. "$ docker build --build-arg projectVersion=3.0.0-SNAPSHOT"
ARG       projectVersion

ENV       SRCDIR src/resources/docker
ENV       SHAREDIR /usr/share
ENV       WORKDIR /usr/share/opentsdb
ENV       LOGDIR  /var/log/opentsdb

RUN       mkdir $LOGDIR
RUN       apk add --no-cache vim

ADD       opentsdb-$projectVersion.tar.gz $SHAREDIR
ADD       opentsdb.conf $WORKDIR/conf/opentsdb.conf

# It is expected these might need to be passed in with the -e flag
ENV       CONFIG $WORKDIR/conf/opentsdb.conf
ENV       LOGBACK $WORKDIR/conf/logback.xml
ENV       CLASSPATH  $WORKDIR/lib/*:
ENV       JAVA_OPTS="-Xms512m -Xmx2048m -enableassertions -enablesystemassertions"
ENV       TSDB_PORT  4242
ENV       CLASS net.opentsdb.tsd.TSDMain

ENTRYPOINT java ${JAVA_OPTS} -classpath ${CLASSPATH} -Dlogback.configurationFile=$LOGBACK ${CLASS} --config=${CONFIG} 