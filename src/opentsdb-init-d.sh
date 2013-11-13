#!/bin/sh
#
# opentsdb      This shell script takes care of starting and stopping OpenTSDB.
#
# chkconfig: 35 99 01
# description: OpenTSDB is a distributed, scalable Time Series Database (TSDB) \
# written on top of HBase. OpenTSDB was written to address a common need: store, \
# index and serve metrics collected from computer systems (network gear, operating \
# systems, applications) at a large scale, and make this data easily accessible \
# and graphable.
#
# Based in part on a shell script by Jacek Masiulaniec at
# https://github.com/masiulaniec/opentsdb-rhel/blob/master/src/tsdb-server.init.

### BEGIN INIT INFO
# Provides: opentsdb
# Required-Start: $network $local_fs $remote_fs
# Required-Stop: $network $local_fs $remote_fs
# Short-Description: start and stop opentsdb
# Description: OpenTSDB is a distributed, scalable Time Series Database (TSDB)
#              written on top of HBase. OpenTSDB was written to address a
#              common need: store, index and serve metrics collected from
#              computer systems (network gear, operating systems, applications)
#              at a large scale, and make this data easily accessible and
#              graphable.
### END INIT INFO

# Source init functions
. /etc/init.d/functions

# Set this so that you can run as many opentsdb instances you want as long as
# the name of this script is changed (or a symlink is used)
NAME=`basename $0`

# Maximum number of open files
MAX_OPEN_FILES=65535

# Default program options
PROG=/usr/bin/tsdb
HOSTNAME=$(hostname --fqdn)
USER=root
CONFIG=/etc/opentsdb/${NAME}.conf

# Default directories
LOG_DIR=/var/log/opentsdb
LOCK_DIR=/var/lock/subsys
PID_DIR=/var/run/opentsdb

# Global and Local sysconfig files
[ -e /etc/sysconfig/opentsdb ] && . /etc/sysconfig/opentsdb
[ -e /etc/sysconfig/$NAME ] && . /etc/sysconfig/$NAME

# Set file names
LOG_FILE=$LOG_DIR/$NAME-$HOSTNAME-
LOCK_FILE=$LOCK_DIR/$NAME
PID_FILE=$PID_DIR/$NAME.pid

# Create dirs if they don't exist
[ -e $LOG_DIR ] || (mkdir -p $LOG_DIR && chown $USER: $LOG_DIR)
[ -e $PID_DIR ] || mkdir -p $PID_DIR

PROG_OPTS="tsd --config=${CONFIG}"

start() {
  echo -n "Starting ${NAME}: "
  ulimit -n $MAX_OPEN_FILES

  # TODO: Support non-root user and group. Currently running as root
  # is required because /usr/share/opentsdb/opentsdb_restart.py
  # must be called as root.  This could be fixed with a sudo.

  # Set a default value for JVMARGS
  : ${JVMXMX:=-Xmx6000m}
  : ${JVMARGS:=-DLOG_FILE_PREFIX=${LOG_FILE} -enableassertions -enablesystemassertions $JVMXMX -XX:OnOutOfMemoryError=/usr/share/opentsdb/opentsdb_restart.py}
  export JVMARGS
  daemon --user $USER --pidfile $PID_FILE "$PROG $PROG_OPTS 1> ${LOG_FILE}opentsdb.out 2> ${LOG_FILE}opentsdb.err &"
  retval=$?
  sleep 2
  echo
  [ $retval -eq 0 ] && (findproc > $PID_FILE && touch $LOCK_FILE)
  return $retval
}

stop() {
  echo -n "Stopping ${NAME}: "
  killproc -p $PID_FILE $NAME
  retval=$?
  echo
  [ $retval -eq 0 ] && (rm -f $PID_FILE && rm -f $LOCK_FILE)
  return $retval
}

restart() {
    stop
    start
}

reload() {
    restart
}

force_reload() {
    restart
}

rh_status() {
    # run checks to determine if the service is running or use generic status
    status -p $PID_FILE -l $LOCK_FILE $NAME
}

rh_status_q() {
    rh_status >/dev/null 2>&1
}

findproc() {
    pgrep -f "^java .* net.opentsdb.tools.TSDMain .*${NAME}"
}

case "$1" in
    start)
        rh_status_q && exit 0
        $1
        ;;
    stop)
        rh_status_q || exit 0
        $1
        ;;
    restart)
        $1
        ;;
    reload)
        rh_status_q || exit 7
        $1
        ;;
    force-reload)
        force_reload
        ;;
    status)
        rh_status
        ;;
    condrestart|try-restart)
        rh_status_q || exit 0
        restart
        ;;
    *)
        echo $"Usage: $0 {start|stop|status|restart|condrestart|try-restart|reload|force-reload}"
        exit 2
esac
exit $?
