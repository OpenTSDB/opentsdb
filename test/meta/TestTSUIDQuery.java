// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static net.opentsdb.uid.UniqueId.UniqueIdType;
import static org.junit.Assert.assertEquals;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({KeyValue.class, Scanner.class})
public final class TestTSUIDQuery {
  private static byte[] NAME_FAMILY = "name".getBytes(Const.CHARSET_ASCII);
  private TSDB tsdb;
  private TSUIDQuery query;
  
  @Before
  public void before() throws Exception {
    Map<String, String> overrides = new HashMap<String, String>();
    overrides.put("tsd.core.meta.enable_tsuid_incrementing", "TRUE");
    overrides.put("tsd.core.meta.enable_realtime_ts", "TRUE");
    Config config = new Config(false, overrides);

    MemoryStore tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);

    tsdb_store.addColumn(new byte[]{0, 0, 1}, NAME_FAMILY,
      "metrics".getBytes(Const.CHARSET_ASCII),
      "sys.cpu.user".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1}, NAME_FAMILY,
      "metric_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.user\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"System CPU\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      "metrics".getBytes(Const.CHARSET_ASCII),
      "sys.cpu.nice".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      "metric_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000002\",\"type\":\"METRIC\",\"name\":\"sys.cpu.nice\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"System CPU\"}")
        .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1}, NAME_FAMILY,
      "tagk".getBytes(Const.CHARSET_ASCII),
      "host".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1}, NAME_FAMILY,
      "tagk_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000001\",\"type\":\"TAGK\",\"name\":\"host\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"Host server name\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      "tagk".getBytes(Const.CHARSET_ASCII),
      "datacenter".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      "tagk_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000002\",\"type\":\"TAGK\",\"name\":\"datacenter\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"Datecenter name\"}")
        .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1}, NAME_FAMILY,
      "tagv".getBytes(Const.CHARSET_ASCII),
      "web01".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1}, NAME_FAMILY,
      "tagv_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000001\",\"type\":\"TAGV\",\"name\":\"web01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"Web server 1\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      "tagv".getBytes(Const.CHARSET_ASCII),
      "web02".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 2}, NAME_FAMILY,
      "tagv_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000002\",\"type\":\"TAGV\",\"name\":\"web02\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"Web server 2\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      "tagv".getBytes(Const.CHARSET_ASCII),
      "dc01".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 3}, NAME_FAMILY,
      "tagv_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"uid\":\"000003\",\"type\":\"TAGV\",\"name\":\"dc01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
        "1328140801,\"displayName\":\"Web server 2\"}")
        .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}, NAME_FAMILY,
      "ts_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"tsuid\":\"000001000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}, NAME_FAMILY,
      "ts_ctr".getBytes(Const.CHARSET_ASCII),
      Bytes.fromLong(1L));
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 2}, NAME_FAMILY,
      "ts_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"tsuid\":\"000001000001000002\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 2}, NAME_FAMILY,
      "ts_ctr".getBytes(Const.CHARSET_ASCII),
      Bytes.fromLong(1L));
    tsdb_store.addColumn(new byte[]{0, 0, 2, 0, 0, 2, 0, 0, 3, 0, 0, 1, 0, 0, 1},
      NAME_FAMILY, "ts_meta".getBytes(Const.CHARSET_ASCII),
      ("{\"tsuid\":\"000002000002000003000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 2, 0, 0, 2, 0, 0, 3, 0, 0, 1, 0, 0, 1},
      NAME_FAMILY, "ts_ctr".getBytes(Const.CHARSET_ASCII),
      Bytes.fromLong(1L));

    tsdb_store.allocateUID("sys.cpu.user", new byte[] {0, 0, 1}, UniqueIdType.METRIC);
    tsdb_store.allocateUID("sys.cpu.nice", new byte[] {0, 0, 2}, UniqueIdType.METRIC);

    tsdb_store.allocateUID("host", new byte[] {0, 0, 1}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("datacenter", new byte[] {0, 0, 2}, UniqueIdType.TAGK);

    tsdb_store.allocateUID("web01", new byte[] {0, 0, 1}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("web02", new byte[] {0, 0, 2}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("dc01", new byte[] {0, 0, 3}, UniqueIdType.TAGV);
  }
  
  @Test
  public void setQuery() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setQuery("sys.cpu.user", tags);
  }
  
  @Test
  public void setQueryEmtpyTags() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery("sys.cpu.user", new HashMap<String, String>(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUMetric() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery("sys.cpu.system", new HashMap<String, String>(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUTagk() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("dc", "web01");
    query.setQuery("sys.cpu.user", tags);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUTagv() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web03");
    query.setQuery("sys.cpu.user", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getLastWriteTimesQueryNotSet() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.getLastWriteTimes().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
  
  @Test
  public void getTSMetasSingle() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web01");
    query.setQuery("sys.cpu.user", tags);
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, tsmetas.size());
  }
  
  @Test
  public void getTSMetasMulti() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setQuery("sys.cpu.user", tags);
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(2, tsmetas.size());
  }
  
  @Test
  public void getTSMetasMultipleTags() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setQuery("sys.cpu.nice", tags);
    tags.put("host", "web01");
    tags.put("datacenter", "dc01");
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1, tsmetas.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTSMetasNullMetric() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.getTSMetas().joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

}
