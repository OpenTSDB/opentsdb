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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;

import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  Scanner.class, TSMeta.class, AtomicIncrementRequest.class})
public final class TestTSUIDQuery extends BaseTsdbTest {
  private static byte[] NAME_FAMILY = "name".getBytes(MockBase.ASCII());
  private TSUIDQuery query;
  
  @Before
  public void beforeLocal() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()),
        METRIC_STRING.getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "metric_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.user\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"System CPU\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.nice".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "metric_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000002\",\"type\":\"METRIC\",\"name\":\"sys.cpu.nice\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"System CPU\"}")
        .getBytes(MockBase.ASCII()));
    
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagk".getBytes(MockBase.ASCII()),
        TAGK_STRING.getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagk_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"TAGK\",\"name\":\"host\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Host server name\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagk".getBytes(MockBase.ASCII()),
        "datacenter".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagk_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000002\",\"type\":\"TAGK\",\"name\":\"datacenter\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Datecenter name\"}")
        .getBytes(MockBase.ASCII()));

    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        TAGV_STRING.getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagv_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"TAGV\",\"name\":\"web01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 1\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        "web02".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagv_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000002\",\"type\":\"TAGV\",\"name\":\"web02\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 2\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 3 }, NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        "dc01".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 3 }, NAME_FAMILY,
        "tagv_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000003\",\"type\":\"TAGV\",\"name\":\"dc01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 2\"}")
        .getBytes(MockBase.ASCII()));

    storage.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, NAME_FAMILY,
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000001000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, NAME_FAMILY,
        "ts_ctr".getBytes(MockBase.ASCII()),
        Bytes.fromLong(1L));
    storage.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 }, NAME_FAMILY,
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000001000001000002\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 }, NAME_FAMILY,
        "ts_ctr".getBytes(MockBase.ASCII()),
        Bytes.fromLong(1L));
    storage.addColumn(new byte[] { 0, 0, 2, 0, 0, 2, 0, 0, 3, 0, 0, 1, 0, 0, 1 },
        NAME_FAMILY, "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000002000002000003000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2, 0, 0, 2, 0, 0, 3, 0, 0, 1, 0, 0, 1 },
        NAME_FAMILY, "ts_ctr".getBytes(MockBase.ASCII()),
        Bytes.fromLong(1L)); 
  }
  
  @Test
  public void setQuery() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(TAGK_STRING, TAGV_STRING);
    query.setQuery(METRIC_STRING, tags);
  }
  
  @Test
  public void setQueryEmtpyTags() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery(METRIC_STRING, new HashMap<String, String>(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUMetric() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery(NSUN_METRIC, new HashMap<String, String>(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUTagk() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(NSUN_TAGK, TAGV_STRING);
    query.setQuery(METRIC_STRING, tags);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUTagv() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put(TAGK_STRING, "web03");
    query.setQuery(METRIC_STRING, tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getLastWriteTimesQueryNotSet() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.getLastWriteTimes().joinUninterruptibly();
  }
  
  @Test
  public void getTSMetasSingle() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    tags.put(TAGK_STRING, TAGV_STRING);
    query.setQuery(METRIC_STRING, tags);
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(1, tsmetas.size());
  }
  
  @Test
  public void getTSMetasMulti() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setQuery(METRIC_STRING, tags);
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(2, tsmetas.size());
  }
  
  @Test
  public void getTSMetasMultipleTags() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setQuery(METRIC_STRING, tags);
    tags.put(TAGK_STRING, TAGV_STRING);
    tags.put(TAGK_B_STRING, TAGV_B_STRING);
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(2, tsmetas.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTSMetasNullMetric() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.getTSMetas().joinUninterruptibly();
  }

}
