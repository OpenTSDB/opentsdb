#!/bin/bash
#
# Script which installs HBase, OpenTSDB and TCollector on OSX
#
# This file is part of OpenTSDB.
# Copyright (C) 2010-2012  The OpenTSDB Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 2.1 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
#
#
if [ $# -eq 0 ]
  then
    echo "No arguments supplied, please suggest an installation path, ex. $HOME"
    exit 1;
fi
BASE_DIR=$1
SUBBASE_DIR=opentsdb_stack;
if [ ! -d "${BASE_DIR}" ] ; then
    echo "$BASE_DIR is not a directory";
    exit 1;
fi
export INSTALL_DIR=$BASE_DIR/$SUBBASE_DIR;
/bin/echo "Installing into $INSTALL_DIR";
/bin/mkdir -p $INSTALL_DIR;
cd $INSTALL_DIR;
/usr/bin/curl -q http://mirror.cogentco.com/pub/apache/hbase/1.2.5/hbase-1.2.5-bin.tar.gz -o $INSTALL_DIR/hbase-1.2.5-bin.tar.gz 2>/dev/null;
/usr/bin/tar -xzvf hbase-1.2.5-bin.tar.gz -C $INSTALL_DIR/;
cd $INSTALL_DIR/hbase-1.2.5;
/bin/mkdir -p $INSTALL_DIR/data/hbase;
/bin/mkdir -p $INSTALL_DIR/data/zookeeper;
/bin/cat <<EOF > conf/hbase-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
-->
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>file://$INSTALL_DIR/data/hbase</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>$INSTALL_DIR/data/zookeeper</value>
  </property>
</configuration>
EOF
$INSTALL_DIR/hbase-1.2.5/bin/start-hbase.sh;
cd $INSTALL_DIR
/usr/bin/git clone https://github.com/OpenTSDB/opentsdb.git;
cd opentsdb
$INSTALL_DIR/opentsdb/build.sh clean; $INSTALL_DIR/opentsdb/build.sh;
/bin/mkdir $INSTALL_DIR/opentsdb/build/cache;
export HBASE_HOME=$INSTALL_DIR/hbase-1.2.5;
export COMPRESSION=NONE;
$INSTALL_DIR/opentsdb/src/create_table.sh;
$INSTALL_DIR/opentsdb/build/tsdb tsd --config=$INSTALL_DIR/opentsdb/src/opentsdb.conf --staticroot=$INSTALL_DIR/opentsdb/build/staticroot --cachedir=$INSTALL_DIR/opentsdb/build/cache --port=4242 --zkquorum=localhost:2181 --zkbasedir=/hbase --auto-metric &
cd $INSTALL_DIR
/usr/bin/git clone https://github.com/OpenTSDB/tcollector.git;
cd $INSTALL_DIR/tcollector/tcollector
/bin/rm -rf $INSTALL_DIR/tcollector/collectors/0/*;
/usr/bin/curl -q https://raw.githubusercontent.com/aalpern/tcollector-osx/master/dfstat.py -o $INSTALL_DIR/tcollector/collectors/0/dfstat.py 2>/dev/null;
/usr/bin/curl -q https://raw.githubusercontent.com/aalpern/tcollector-osx/master/iostat.py -o $INSTALL_DIR/tcollector/collectors/0/iostat.py 2>/dev/null;
/usr/bin/curl -q https://raw.githubusercontent.com/aalpern/tcollector-osx/master/vmstat.py -o $INSTALL_DIR/tcollector/collectors/0/vmstat.py 2>/dev/null;
/bin/chmod a+x collectors/0/*.py;
$INSTALL_DIR/tcollector/tcollector.py -L localhost:4242 -t host=`hostname` -t domain=dev -P $INSTALL_DIR/tcollector/tcollector.pid --logfile $INSTALL_DIR/tcollector/tcollector.log &
/bin/sleep 30;
/usr/bin/open http://localhost:4242/#start=10m-ago\&m=sum:df.inodes.free\&autoreload=15;
