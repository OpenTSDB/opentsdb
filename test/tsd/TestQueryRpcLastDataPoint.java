// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TestTSUIDQuery;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, HBaseClient.class, Config.class, HttpQuery.class, 
  Query.class, Deferred.class, UniqueId.class, DateTime.class, KeyValue.class,
  Scanner.class })
public class TestQueryRpcLastDataPoint {
  private Config config;
  private TSDB tsdb;
  private HBaseClient client;
  private QueryRpc rpc;
  private MockBase storage;
  private Map<String, String> tags;
  
  @Before
  public void before() throws Exception {
    tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    rpc = new QueryRpc();
    config = mock(Config.class);
    when(config.getString("tsd.storage.hbase.data_table")).thenReturn("tsdb");
    when(config.getString("tsd.storage.hbase.uid_table")).thenReturn("tsdb-uid");
    when(config.getString("tsd.storage.hbase.meta_table")).thenReturn("tsdb-meta");
    when(config.getString("tsd.storage.hbase.tree_table")).thenReturn("tsdb-tree");
    when(config.getString("tsd.http.show_stack_trace")).thenReturn("true");
    when(config.enable_tsuid_incrementing()).thenReturn(true);
    when(config.enable_realtime_ts()).thenReturn(true);
    client = mock(HBaseClient.class);
    
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    tsdb = new TSDB(config);
    Whitebox.setInternalState(tsdb, "client", client);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    TestTSUIDQuery.setupStorage(tsdb, storage);
  }
  
  @Test
  public void qsMetricMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricMetaScan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricMetaScanResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user&resolve");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void qsMetricMetaScanOneMissing() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertFalse(json.contains("\"value\":\"42\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricMetaScanNoResults() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals("[]", json);
  }
  
  @Test
  public void qsMetricMetaScanBackscanZero() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user&back_scan=0");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricBackscanResolved() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
        + "&back_scan=1&resolve=true");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
  }
  
  @Test
  public void qsMetricBackscanNoResult() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("[]", json);
  }
  
  @Test
  public void qsMetricTwoQueriesBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
            + "&timeseries=sys.cpu.user{host=web02}&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricTwoQueriesBackscanResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
            + "&timeseries=sys.cpu.user{host=web02}&back_scan=1&resolve");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void qsMetricTwoQueriesOneMissingBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
            + "&timeseries=sys.cpu.user{host=web02}&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertFalse(json.contains("\"value\":\"42\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsMetricBackscanMissingTags() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user&back_scan=1");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("Tags"));
    }
  }
  
  @Test
  public void qsMetricNSUNMetric() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.system{host=web01}");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such name"));
      assertTrue(e.getMessage().contains("metric"));
    }
  }
  
  @Test
  public void qsMetricNSUNTagk() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{dc=web01}");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such name"));
      assertTrue(e.getMessage().contains("tagk"));
    }
  }
  
  @Test
  public void qsMetricNSUNTagv() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web03}");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such name"));
      assertTrue(e.getMessage().contains("tagv"));
    }
  }
  
  @Test
  public void qsTSUIDMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDMetaCommaSeparated() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001,000001000001000002");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDMetaTwoQueries() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001&tsuids=000001000001000002");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDMetaNoResults() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals("[]", json);
  }
  
  @Test
  public void qsTSUIDBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDBackscanNoResult() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("[]", json);
  }
  
  @Test
  public void qsTSUIDCommaSeparatedBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001,000001000001000002&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDCommaSeparatedOneMissingBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001,000001000001000002&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDTwoQueriesBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001"
        + "&tsuids=000001000001000002&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDTwoQueriesOneMissingBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000001"
        + "&tsuids=000001000001000002&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsTSUIDNSUIMetric() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    storage.addColumn(MockBase.stringToBytes("00000350E22700000001000001"), 
        new byte[] { 0, 0 }, new byte[] { 0x2A });
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000003000001000001&back_scan=1&resolve");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such unique ID"));
      assertTrue(e.getMessage().contains("metric"));
    }
  }
  
  @Test
  public void qsTSUIDNSUITagk() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    storage.addColumn(MockBase.stringToBytes("00000150E22700000003000001"), 
        new byte[] { 0, 0 }, new byte[] { 0x2A });
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000003000001&back_scan=1&resolve");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such unique ID"));
      assertTrue(e.getMessage().contains("tagk"));
    }
  }
  
  @Test
  public void qsTSUIDNSUITagv() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000004"), 
        new byte[] { 0, 0 }, new byte[] { 0x2A });
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=000001000001000004&back_scan=1&resolve");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) {
      assertTrue(e.getMessage().contains("No such unique ID"));
      assertTrue(e.getMessage().contains("tagv"));
    }
  }
  
  @Test
  public void qsDualMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
        + "&tsuids=000001000001000002");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsDualBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}"
        + "&tsuids=000001000001000002&back_scan=1");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void qsEmpty() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/query/last");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) { }
  }
  
  @Test
  public void postMetricMetaWithTags() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}}]}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postMetricMetaWithoutTags() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\"}]}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postMetricMetaWithoutTagsResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\"}],\"resolveNames\":true}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void postMetricMetaTwoQueries() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}},"
        + "{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web02\"}}]}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postMetricBackscanWithTags() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}}],\"backScan\":1}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postTSUIDMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\"]}]}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postTSUIDMetaList() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\","
        + "\"000001000001000002\"]}]}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postTSUIDMetaResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\","
        + "\"000001000001000002\"]}],\"resolveNames\":true}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void postTSUIDMetaTwoQueries() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\"]},"
        + "{\"tsuids\":[\"000001000001000002\"]}]}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postTSUIDBackscan() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"tsuids\":[\"000001000001000001\"]}],\"backScan\":1}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1356998400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertFalse(json.contains("\"value\":\"24\""));
    assertFalse(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postDualMeta() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}},"
        + "{\"tsuids\":[\"000001000001000002\"]}]}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertFalse(json.contains("\"metric\""));
    assertFalse(json.contains("\"tags\""));
  }
  
  @Test
  public void postDualMetaResolve() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400L, 24, tags);
    
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[{\"metric\":\"sys.cpu.user\",\"tags\":"
        + "{\"host\":\"web01\"}},"
        + "{\"tsuids\":[\"000001000001000002\"]}],"
        + "\"resolveNames\":true}");
    rpc.execute(tsdb, query);
    final String json = getContent(query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(json.contains("\"timestamp\":1388534400000"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000001\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"000001000001000002\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void postEmpty() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{\"queries\":[]}");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) { }
  }
  
  @Test
  public void postEmptyList() throws Exception {
    final HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query/last",
        "{}");
    try {
      rpc.execute(tsdb, query);
      fail("Expected a BadRequestException");
    } catch (BadRequestException e) { }
  }
  
  /**
   * Returns the content of the response buffer
   * @param query The query to parse
   * @return Some string if we were lucky
   */
  private String getContent(final HttpQuery query) {
    return query.response().getContent().toString(Charset.forName("UTF-8"));
  }
}
