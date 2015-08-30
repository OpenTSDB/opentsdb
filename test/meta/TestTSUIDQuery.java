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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.hbase.async.Bytes.ByteMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  Scanner.class, TSMeta.class, AtomicIncrementRequest.class, DateTime.class })
public final class TestTSUIDQuery {
  private static final byte[] NAME_FAMILY = "name".getBytes(MockBase.ASCII());
  private static final byte[] TSUID = new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 };
  private static final byte[] QUAL = new byte[] { 0, 0 };
  private static final byte[] VAL = new byte[] { 0x2A };
  private TSDB tsdb;
  private Config config;
  private HBaseClient client;
  private MockBase storage;
  private TSUIDQuery query;
  private Map<String, String> tags;
  
  @Before
  public void before() throws Exception {
    tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    
    config = mock(Config.class);
    client = mock(HBaseClient.class);
    when(config.getString("tsd.storage.hbase.data_table")).thenReturn("tsdb");
    when(config.getString("tsd.storage.hbase.uid_table")).thenReturn("tsdb-uid");
    when(config.getString("tsd.storage.hbase.meta_table")).thenReturn("tsdb-meta");
    when(config.getString("tsd.storage.hbase.tree_table")).thenReturn("tsdb-tree");
    when(config.enable_tsuid_incrementing()).thenReturn(true);
    when(config.enable_realtime_ts()).thenReturn(true);
    
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    tsdb = new TSDB(config);
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    setupStorage(tsdb, storage);
  }

  @Test
  public void ctorDefault() throws Exception {
    query = new TSUIDQuery(tsdb);
    assertNotNull(query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullTSDB() throws Exception {
    query = new TSUIDQuery(null);
  }
  
  @Test
  public void ctorTSUID() throws Exception {
    query = new TSUIDQuery(tsdb, TSUID);
    assertNotNull(query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullTSUID() throws Exception {
    query = new TSUIDQuery(tsdb, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullTSDBforTSUID() throws Exception {
    query = new TSUIDQuery(null, TSUID);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyTSUID() throws Exception {
    query = new TSUIDQuery(tsdb, new byte[] { });
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorShortTSUID() throws Exception {
    query = new TSUIDQuery(tsdb, new byte[] { 0, 0, 1, 0, 0, 1 });
  }
  
  @Test
  public void ctorMetric() throws Exception {
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    assertNotNull(query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorMetricNullTSDB() throws Exception {
    query = new TSUIDQuery(null, "sys.cpu.user", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorMetricNullMetric() throws Exception {
    query = new TSUIDQuery(tsdb, null, tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorMetricEmptyMetric() throws Exception {
    query = new TSUIDQuery(tsdb, "", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorMetricNullTags() throws Exception {
    query = new TSUIDQuery(tsdb, "sys.cpu.user", null);
  }
  
  @Test
  public void ctorMetricEmptyTags() throws Exception {
    tags.clear();
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    assertNotNull(query);
  }
  
  @Test
  public void getLastWriteTimes() throws Exception {
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final ByteMap<Long> tsuids = query.getLastWriteTimes().joinUninterruptibly();
    assertEquals(1, tsuids.size());
    assertEquals(1388534400015L, (long)tsuids.get(TSUID));
  }
  
  @Test
  public void getLastWriteTimesSetQuery() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery("sys.cpu.user", tags);
    final ByteMap<Long> tsuids = query.getLastWriteTimes().joinUninterruptibly();
    assertEquals(1, tsuids.size());
    assertEquals(1388534400015L, (long)tsuids.get(TSUID));
  }
  
  @Test
  public void getLastWriteTimesEmptyTags() throws Exception {
    tags.clear();
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final ByteMap<Long> tsuids = query.getLastWriteTimes().joinUninterruptibly();
    assertEquals(2, tsuids.size());
    assertEquals(1388534400015L, (long)tsuids.get(TSUID));
    assertEquals(1388534400017L, 
        (long)tsuids.get(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 }));
  }
  
  @Test
  public void getLastWriteTimesEmptyTagsSetQuery() throws Exception {
    tags.clear();
    query = new TSUIDQuery(tsdb);
    query.setQuery("sys.cpu.user", tags);
    final ByteMap<Long> tsuids = query.getLastWriteTimes().joinUninterruptibly();
    assertEquals(2, tsuids.size());
    assertEquals(1388534400015L, (long)tsuids.get(TSUID));
    assertEquals(1388534400017L, 
        (long)tsuids.get(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getLastWriteTimesQueryNotSet() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.getLastWriteTimes().joinUninterruptibly();
  }
  
  @Test
  public void getLastWriteTimesNoMatch() throws Exception {
    storage.flushStorage();
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final ByteMap<Long> tsuids = query.getLastWriteTimes().joinUninterruptibly();
    assertTrue(tsuids.isEmpty());
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getLastWriteTimesNSUNMetric() throws Exception {
    query = new TSUIDQuery(tsdb, "sys.cpu.system", tags);
    query.getLastWriteTimes().joinUninterruptibly();
  }
  
  @Test (expected = DeferredGroupException.class)
  public void getLastWriteTimesNSUNTagk() throws Exception {
    tags.clear();
    tags.put("dc", "web01");
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    query.getLastWriteTimes().joinUninterruptibly();
  }
  
  @Test (expected = DeferredGroupException.class)
  public void getLastWriteTimesNSUNTagv() throws Exception {
    tags.put("host", "web03");
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    query.getLastWriteTimes().joinUninterruptibly();
  }
  
  @Test
  public void getTSMetasSingle() throws Exception {
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(1, tsmetas.size());
    assertEquals("sys.cpu.user", tsmetas.get(0).getMetric().getName());
    assertEquals("host", tsmetas.get(0).getTags().get(0).getName());
    assertEquals("web01", tsmetas.get(0).getTags().get(1).getName());
  }
  
  @Test
  public void getTSMetasSingleSetQuery() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery("sys.cpu.user", tags);
    final List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(1, tsmetas.size());
    assertEquals("sys.cpu.user", tsmetas.get(0).getMetric().getName());
    assertEquals("host", tsmetas.get(0).getTags().get(0).getName());
    assertEquals("web01", tsmetas.get(0).getTags().get(1).getName());
  }
  
  @Test
  public void getTSMetasMultipleResults() throws Exception {
    tags.clear();
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(2, tsmetas.size());
    assertEquals("sys.cpu.user", tsmetas.get(0).getMetric().getName());
    assertEquals("host", tsmetas.get(0).getTags().get(0).getName());
    assertEquals("web01", tsmetas.get(0).getTags().get(1).getName());
    assertEquals("sys.cpu.user", tsmetas.get(1).getMetric().getName());
    assertEquals("host", tsmetas.get(1).getTags().get(0).getName());
    assertEquals("web02", tsmetas.get(1).getTags().get(1).getName());
  }
  
  @Test
  public void getTSMetasMultipleTags() throws Exception {
    tags.put("datacenter", "dc01");
    query = new TSUIDQuery(tsdb, "sys.cpu.nice", tags);
    final List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(1, tsmetas.size());
    assertEquals("sys.cpu.nice", tsmetas.get(0).getMetric().getName());
    assertEquals("host", tsmetas.get(0).getTags().get(0).getName());
    assertEquals("web01", tsmetas.get(0).getTags().get(1).getName());
    assertEquals("datacenter", tsmetas.get(0).getTags().get(2).getName());
    assertEquals("dc01", tsmetas.get(0).getTags().get(3).getName());
  }
  
  @Test (expected = DeferredGroupException.class)
  public void getTSMetasNSUITagk() throws Exception {
    tags.put("datacenter", "web03");
    query = new TSUIDQuery(tsdb, "sys.cpu.nice", tags);
    query.getTSMetas().joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTSMetasNullMetric() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.getTSMetas().joinUninterruptibly();
  }

  @Test
  public void tsuidFromMetric() throws Exception {
    byte[] tsuid = TSUIDQuery.tsuidFromMetric(tsdb, "sys.cpu.user", tags).join();
    assertArrayEquals(TSUID, tsuid);
  }
  
  @Test
  public void tsuidFromMetricTwoTags() throws Exception {
    tags.put("datacenter", "dc01");
    byte[] tsuid = TSUIDQuery.tsuidFromMetric(tsdb, "sys.cpu.user", tags).join();
    assertArrayEquals(
        new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 3 }, tsuid);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void tsuidFromMetricNSUNMetric() throws Exception {
    TSUIDQuery.tsuidFromMetric(tsdb, "sys.cpu.system", tags).join();
  }
  
  @Test (expected = DeferredGroupException.class)
  public void tsuidFromMetricNSUNTagk() throws Exception {
    tags.clear();
    tags.put("dc", "web01");
    TSUIDQuery.tsuidFromMetric(tsdb, "sys.cpu.user", tags).join();
  }
  
  @Test (expected = DeferredGroupException.class)
  public void tsuidFromMetricNSUNTagv() throws Exception {
    tags.put("host", "web03");
    TSUIDQuery.tsuidFromMetric(tsdb, "sys.cpu.user", tags).join();
  }
  
  @Test (expected = NullPointerException.class)
  public void tsuidFromMetricNullTSDB() throws Exception {
    TSUIDQuery.tsuidFromMetric(null, "sys.cpu.user", tags).join();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void tsuidFromMetricNullMetric() throws Exception {
    TSUIDQuery.tsuidFromMetric(tsdb, null, tags).join();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void tsuidFromMetricEmptyMetric() throws Exception {
    TSUIDQuery.tsuidFromMetric(tsdb, "", tags).join();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void tsuidFromMetricNullTags() throws Exception {
    TSUIDQuery.tsuidFromMetric(tsdb, "sys.cpu.user", null).join();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void tsuidFromMetricEmptyTags() throws Exception {
    tags.clear();
    TSUIDQuery.tsuidFromMetric(tsdb, "sys.cpu.user", tags).join();
  }
  
  @Test
  public void getLastPointMetricZeroBackscanOnePoint() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final IncomingDataPoint dp = query.getLastPoint(false, 0).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }
  
  @Test
  public void getLastPointMetricZeroBackscanMostRecent() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    tsdb.addPoint("sys.cpu.user", 1356998401L, 24, tags);
    tsdb.addPoint("sys.cpu.user", 1356998402L, 1, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final IncomingDataPoint dp = query.getLastPoint(false, 0).join();
    assertEquals(1356998402000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("1", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }
  
  @Test
  public void getLastPointMetricZeroBackscanOutOfRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1357002000000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    assertNull(query.getLastPoint(false, 0).join());
  }
  
  @Test
  public void getLastPointMetricOneBackscanInRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1357002000000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final IncomingDataPoint dp = query.getLastPoint(false, 1).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }
  
  @Test
  public void getLastPointMetricOneBackscanOutOfRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1357010600000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    assertNull(query.getLastPoint(false, 1).join());
  }

  @Test
  public void getLastPointMetricManyBackscanInRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1360681200000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final IncomingDataPoint dp = query.getLastPoint(false, 1024).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }
  
  @Test
  public void getLastPointMetricManyBackscanOutOfRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1360681200000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    assertNull(query.getLastPoint(false, 1022).join());
  }

  @Test (expected = IllegalArgumentException.class)
  public void getLastPointMetricNegativeBackscan() throws Exception {
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    query.getLastPoint(false, -1).join();
  }
  
  @Test
  public void getLastPointMetricResolve() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    final IncomingDataPoint dp = query.getLastPoint(true, 0).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertEquals("sys.cpu.user", dp.getMetric());
    assertSame(tags, dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getLastPointMetricNSUNMetric() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.system", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    query.getLastPoint(false, 0).join();
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getLastPointMetricNSUNTagk() throws Exception {
    tags.clear();
    tags.put("dc", "web01");
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    query.getLastPoint(false, 0).join();
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getLastPointMetricNSUNTagv() throws Exception {
    tags.put("host", "web03");
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    query.getLastPoint(false, 0).join();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getLastPointMetricEmptyTags() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tags.clear();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, "sys.cpu.user", tags);
    query.getLastPoint(false, 0).join();
  }
  
  @Test
  public void getLastPointTSUIDZeroBackscanRecent() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, TSUID);
    final IncomingDataPoint dp = query.getLastPoint(false, 0).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }
  
  @Test
  public void getLastPointTSUIDZeroBackscanRecentOutOfRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1357002000000L);
    query = new TSUIDQuery(tsdb, TSUID);
    assertNull(query.getLastPoint(false, 0).join());
  }

  @Test
  public void getLastPointTSUIDOneBackscanInRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1357002000000L);
    query = new TSUIDQuery(tsdb, TSUID);
    final IncomingDataPoint dp = query.getLastPoint(false, 1).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }
  
  @Test
  public void getLastPointTSUIDOneBackscanRecentOutOfRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1357010600000L);
    query = new TSUIDQuery(tsdb, TSUID);
    assertNull(query.getLastPoint(false, 1).join());
  }

  @Test
  public void getLastPointTSUIDManyBackscanInRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1360681200000L);
    query = new TSUIDQuery(tsdb, TSUID);
    final IncomingDataPoint dp = query.getLastPoint(false, 1024).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }

  @Test
  public void getLastPointTSUIDManyBackscanRecentOutOfRange() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags);
    
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1360681200000L);
    query = new TSUIDQuery(tsdb, TSUID);
    assertNull(query.getLastPoint(false, 1022).join());
  }

  // While these NSUI shouldn't happen, it's possible if someone deletes a metric
  // or tag but not the actual data.
  @Test
  public void getLastPointTSUIDMetricNSUINotResolved() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    storage.addColumn(MockBase.stringToBytes("00000350E22700000001000001"), 
        QUAL, VAL);
    final byte[] tsuid = new byte[] { 0, 0, 3, 0, 0, 1, 0, 0, 1 };
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, tsuid);
    final IncomingDataPoint dp = query.getLastPoint(false, 0).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(tsuid), dp.getTSUID());
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getLastPointTSUIDMetricNSUI() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    storage.addColumn(MockBase.stringToBytes("00000350E22700000001000001"), 
        QUAL, VAL);
    final byte[] tsuid = new byte[] { 0, 0, 3, 0, 0, 1, 0, 0, 1 };
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, tsuid);
    query.getLastPoint(true, 0).join();
  }
  
  @Test
  public void getLastPointTSUIDTagkNSUINotResolved() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    storage.addColumn(MockBase.stringToBytes("00000150E22700000003000001"), 
        QUAL, VAL);
    final byte[] tsuid = new byte[] { 0, 0, 1, 0, 0, 3, 0, 0, 1 };
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, tsuid);
    final IncomingDataPoint dp = query.getLastPoint(false, 0).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(tsuid), dp.getTSUID());
  }
  
  @Test (expected = DeferredGroupException.class)
  public void getLastPointTSUIDTagkNSUI() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    storage.addColumn(MockBase.stringToBytes("00000150E22700000003000001"), 
        QUAL, VAL);
    final byte[] tsuid = new byte[] { 0, 0, 1, 0, 0, 3, 0, 0, 1 };
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, tsuid);
    query.getLastPoint(true, 0).join();
  }

  @Test
  public void getLastPointTSUIDTagvNSUINotResolved() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000003"), 
        QUAL, VAL);
    final byte[] tsuid = new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 3 };
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, tsuid);
    final IncomingDataPoint dp = query.getLastPoint(false, 0).join();
    assertEquals(1356998400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(tsuid), dp.getTSUID());
  }
  
  @Test (expected = DeferredGroupException.class)
  public void getLastPoitTSUIDTagvNSUI() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    storage.flushStorage();
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000004"), 
        QUAL, VAL);
    final byte[] tsuid = new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 4 };
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, tsuid);
    query.getLastPoint(true, 0).join();
  }
  
  @Test
  public void getLastPointTSUIDMeta() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(false);
    when(config.enable_realtime_ts()).thenReturn(false);
    tsdb.addPoint("sys.cpu.user", 1388534400L, 42, tags);
    
    when(config.enable_tsuid_incrementing()).thenReturn(true);
    when(config.enable_realtime_ts()).thenReturn(true);

    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, TSUID);
    final IncomingDataPoint dp = query.getLastPoint(false, 0).join();
    assertEquals(1388534400000L, dp.getTimestamp());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
    assertEquals("42", dp.getValue());
    assertEquals(UniqueId.uidToString(TSUID), dp.getTSUID());
  }
  
  @Test
  public void getLastPointTSUIDMetaNoPoint() throws Exception {
    when(config.enable_tsuid_incrementing()).thenReturn(true);
    when(config.enable_realtime_ts()).thenReturn(true);

    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1356998400000L);
    query = new TSUIDQuery(tsdb, TSUID);
    assertNull(query.getLastPoint(false, 0).join());
  }

  /**
   * Public for sharing with other UT classes
   * @param tsdb The mock TSDB client
   * @throws Exception If something went pear shaped
   */
  public static void setupStorage(final TSDB tsdb, final MockBase storage) 
      throws Exception {
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.user".getBytes(MockBase.ASCII()));
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
        "host".getBytes(MockBase.ASCII()));
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
        "web01".getBytes(MockBase.ASCII()));
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

    storage.addColumn(TSUID, NAME_FAMILY,
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000001000001000001\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(TSUID, NAME_FAMILY,
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
    storage.addColumn(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 3 },
        NAME_FAMILY, "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000002000001000001000002000003\",\"" +
        "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
        "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
        "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
        .getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 3 },
        NAME_FAMILY, "ts_ctr".getBytes(MockBase.ASCII()),
        Bytes.fromLong(1L));

    final UniqueId metrics = mock(UniqueId.class);
    final UniqueId tag_names = mock(UniqueId.class);
    final UniqueId tag_values = mock(UniqueId.class);
    
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "tag_names", tag_names);
    Whitebox.setInternalState(tsdb, "tag_values", tag_values);
    
    // mock UniqueId
    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getIdAsync("sys.cpu.user")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }))
        .thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("sys.cpu.user"))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getIdAsync("sys.cpu.system"))
      .thenReturn(Deferred.<byte[]>fromError(
        new NoSuchUniqueName("sys.cpu.system", "metric")));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getIdAsync("sys.cpu.nice"))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(metrics.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("sys.cpu.nice"));
    when(metrics.getNameAsync(new byte[] { 0, 0, 3 }))
      .thenReturn(Deferred.<String>fromError(new NoSuchUniqueId("metrics", 
          new byte[] { 0, 0, 3 })));
    
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }))
        .thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("host"))
      .thenReturn(Deferred.fromResult("host"));
    when(tag_names.getOrCreateIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getId("dc"))
      .thenThrow(new NoSuchUniqueName("dc", "tagk"));
    when(tag_names.getIdAsync("dc"))
      .thenReturn(Deferred.<byte[]>fromError(
          new NoSuchUniqueName("dc", "tagk")));
    when(tag_names.getId("datacenter")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_names.getIdAsync("datacenter"))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("datacenter"));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 3 }))
      .thenReturn(Deferred.<String>fromError(new NoSuchUniqueId("tagk", 
        new byte[] { 0, 0, 3 })));
    
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("web01"));
    when(tag_values.getOrCreateIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("web02"));
    when(tag_values.getOrCreateIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "tagv"));
    when(tag_values.getIdAsync("web03"))
      .thenReturn(Deferred.<byte[]>fromError(
          new NoSuchUniqueName("web03", "tagv")));
    when(tag_values.getId("dc01")).thenReturn(new byte[] { 0, 0, 3 });
    when(tag_values.getIdAsync("dc01"))
      .thenReturn(Deferred.fromResult(new byte[] { 0, 0, 3 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 3 }))
      .thenReturn(Deferred.fromResult("dc01"));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 4 }))
      .thenReturn(Deferred.<String>fromError(new NoSuchUniqueId("tagv", 
        new byte[] { 0, 0, 4 })));
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }
}
