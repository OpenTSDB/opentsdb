ARG       jdkVersion

FROM openjdk:$jdkVersion-slim

MAINTAINER clarsen@verizonmedia.com

# Important, must be passed in e.g. "$ docker build --build-arg projectVersion=3.0.0-SNAPSHOT"
ARG       projectVersion

ENV       SRCDIR src/resources/docker
ENV       SHAREDIR /usr/share
ENV       WORKDIR /usr/share/opentsdb
ENV       LOGDIR  /var/log/opentsdb

RUN       mkdir $LOGDIR

ADD       opentsdb-$projectVersion.tar.gz $SHAREDIR
RUN       chown -R root $WORKDIR
RUN       chgrp -R root $WORKDIR
ADD       public $WORKDIR/static
ADD       opentsdb.yaml $WORKDIR/conf/opentsdb.yaml
ADD       opentsdb_dev.yaml $WORKDIR/conf/opentsdb_dev.yaml
ADD       tsdb $WORKDIR/bin/tsdb

# It is expected these might need to be passed in with the -e flag
ENV       CONFIG file://$WORKDIR/conf/opentsdb.yaml
ENV       LOGBACK $WORKDIR/conf/logback.xml
ENV       JAVA_OPTS="-Xms512m -Xmx2048m -enableassertions -enablesystemassertions"
ENV       CLI_ARGS=""

ENTRYPOINT /bin/sh $WORKDIR/bin/tsdb tsd --config.providers=${CONFIG} ${CLI_ARGS}
