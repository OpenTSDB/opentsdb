#!/bin/sh
# Small script to setup the HBase tables used by OpenTSDB.

test -n "$HBASE_HOME" || {
  echo >&2 'The environment variable HBASE_HOME must be set'
  exit 1
}
test -d "$HBASE_HOME" || {
  echo >&2 "No such directory: HBASE_HOME=$HBASE_HOME"
  exit 1
}

TSDB_TABLE=${TSDB_TABLE-'tsdb'}
UID_TABLE=${UID_TABLE-'tsdb-uid'}
# LZO requires lzo2 64bit to be installed + the hadoop-gpl-compression jar.
COMPRESSION=${COMPRESSION-'LZO'}

# HBase scripts also use a variable named `HBASE_HOME', and having this
# variable in the environment with a value somewhat different from what
# they expect can confuse them in some cases.  So rename the variable.
hbh=$HBASE_HOME
unset HBASE_HOME
exec "$hbh/bin/hbase" shell <<EOF
create '$UID_TABLE',
  {NAME => 'id', COMPRESSION => '$COMPRESSION'},
  {NAME => 'name', COMPRESSION => '$COMPRESSION'}

create '$TSDB_TABLE',
  {NAME => 't', COMPRESSION => '$COMPRESSION'}
EOF
