FROM java:openjdk-8-alpine

MAINTAINER jonathan.creasy@gmail.com

ENV       VERSION 2.3.0-RC1
ENV       WORKDIR /usr/share/opentsdb
ENV       LOGDIR  /var/log/opentsdb
ENV       DATADIR /data/opentsdb
ENV       ETCDIR /etc/opentsdb

RUN       mkdir -p $WORKDIR/static
RUN       mkdir -p $WORKDIR/libs
RUN       mkdir -p $WORKDIR/third_party
RUN       mkdir -p $WORKDIR/resources
RUN       mkdir -p $DATADIR/cache
RUN       mkdir -p $LOGDIR
RUN       mkdir -p $ETCDIR

ENV       CONFIG $ETCDIR/opentsdb.conf
ENV       STATICROOT $WORKDIR/static
ENV       CACHEDIR $DATADIR/cache

ENV       CLASSPATH  $WORKDIR:$WORKDIR/tsdb-$VERSION.jar:$WORKDIR/libs/*:$WORKDIR/logback.xml
ENV       CLASS net.opentsdb.tools.TSDMain

# It is expected these might need to be passed in with the -e flag
ENV       JAVA_OPTS="-Xms512m -Xmx2048m"
ENV       ZKQUORUM zookeeper:2181
ENV       ZKBASEDIR /hbase
ENV       TSDB_OPTS "--read-only --disable-ui"
ENV       TSDB_PORT  4244

WORKDIR   $WORKDIR

ADD       libs $WORKDIR/libs
ADD       logback.xml $WORKDIR
ADD       tsdb-$VERSION.jar $WORKDIR
ADD       opentsdb.conf $ETCDIR/opentsdb.conf

VOLUME    ["/etc/openstsdb"]
VOLUME    ["/data/opentsdb"]

ENTRYPOINT java -enableassertions -enablesystemassertions -classpath ${CLASSPATH} ${CLASS} --config=${CONFIG} --staticroot=${STATICROOT} --cachedir=${CACHEDIR} --port=${TSDB_PORT} --zkquorum=${ZKQUORUM} --zkbasedir=${ZKBASEDIR} ${TSDB_OPTS}
