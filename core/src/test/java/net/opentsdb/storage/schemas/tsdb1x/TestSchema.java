// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage.schemas.tsdb1x;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.storage.StorageException;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdFactory;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.UnitTestException;

public class TestSchema extends SchemaBase {
  
  public static final String TESTID = "UT";
  
  @Test
  public void ctorDefault() throws Exception {
    Schema schema = schema();
    assertEquals(3, schema.metricWidth());
    assertEquals(3, schema.tagkWidth());
    assertEquals(3, schema.tagvWidth());
    assertEquals(20, schema.saltBuckets());
    assertEquals(0, schema.saltWidth());
    assertSame(store, schema.dataStore());
    assertSame(uid_store, schema.uidStore());
    assertSame(metrics, schema.metrics());
    assertSame(tag_names, schema.tagNames());
    assertSame(tag_values, schema.tagValues());
  }
  
  @Test
  public void ctorOverrides() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(store_factory);
    when(store_factory.newInstance(any(TSDB.class), anyString(), any(Schema.class)))
      .thenReturn(store);    
    when(tsdb.registry.getSharedObject("default_uidstore"))
      .thenReturn(uid_store);
    when(tsdb.registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uid_factory);
    tsdb.config.register("tsd.storage.uid.width.metric", 4, false, "UT");
    tsdb.config.register("tsd.storage.uid.width.tagk", 5, false, "UT");
    tsdb.config.register("tsd.storage.uid.width.tagv", 6, false, "UT");
    tsdb.config.register("tsd.storage.salt.buckets", 16, false, "UT");
    tsdb.config.register("tsd.storage.salt.width", 1, false, "UT");
    
    Schema schema = new Schema(tsdb, null);
    assertEquals(4, schema.metricWidth());
    assertEquals(5, schema.tagkWidth());
    assertEquals(6, schema.tagvWidth());
    assertEquals(16, schema.saltBuckets());
    assertEquals(1, schema.saltWidth());
    assertSame(store, schema.dataStore());
    assertSame(uid_store, schema.uidStore());
  }
  
  @Test
  public void ctorID() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    UniqueIdStore us = mock(UniqueIdStore.class);
    UniqueIdFactory uf = mock(UniqueIdFactory.class);
    UniqueId uc = mock(UniqueId.class);
    Tsdb1xDataStoreFactory sf = mock(Tsdb1xDataStoreFactory.class);
    Tsdb1xDataStore s = mock(Tsdb1xDataStore.class);
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(sf);
    when(sf.newInstance(eq(tsdb), eq(TESTID), any(Schema.class))).thenReturn(s);
    when(tsdb.registry.getSharedObject(TESTID + "_uidstore"))
      .thenReturn(us);
    when(tsdb.registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uf);
    when(uf.newInstance(eq(tsdb), anyString(), 
        any(UniqueIdType.class), eq(us))).thenReturn(uc);
    
    tsdb.config.register("tsd.storage." + TESTID + ".uid.width.metric", 4, false, "UT");
    tsdb.config.register("tsd.storage." + TESTID + ".uid.width.tagk", 5, false, "UT");
    tsdb.config.register("tsd.storage." + TESTID + ".uid.width.tagv", 6, false, "UT");
    tsdb.config.register("tsd.storage." + TESTID + ".salt.buckets", 16, false, "UT");
    tsdb.config.register("tsd.storage." + TESTID + ".salt.width", 1, false, "UT");
    tsdb.config.register("tsd.storage." + TESTID + ".data.store", TESTID, false, "UT");
    
    Schema schema = new Schema(tsdb, TESTID);
    assertEquals(4, schema.metricWidth());
    assertEquals(5, schema.tagkWidth());
    assertEquals(6, schema.tagvWidth());
    assertEquals(16, schema.saltBuckets());
    assertEquals(1, schema.saltWidth());
    assertSame(s, schema.dataStore());
    assertSame(us, schema.uidStore());
    assertSame(uc, schema.metrics());
    assertSame(uc, schema.tagNames());
    assertSame(uc, schema.tagValues());
  }
  
  @Test
  public void ctorNoStoreFactory() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    tsdb.config.register("tsd.storage.data.store", "NOTTHERE", false, "UT");
    try {
      new Schema(tsdb, null);
      fail("Expected ConfigurationException");
    } catch (ConfigurationException e) { }
  }
  
  @Test
  public void ctorNullStoreFromFactory() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    Tsdb1xDataStoreFactory store_factory = mock(Tsdb1xDataStoreFactory.class);
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(store_factory);
    when(store_factory.newInstance(eq(tsdb), eq(null), any(Schema.class)))
      .thenReturn(null);
    try {
      new Schema(tsdb, null);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void ctorStoreInstantiationFailure() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    Tsdb1xDataStoreFactory store_factory = mock(Tsdb1xDataStoreFactory.class);
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(store_factory);
    when(store_factory.newInstance(eq(tsdb), eq(null), any(Schema.class)))
      .thenThrow(new UnitTestException());
    try {
      new Schema(tsdb, null);
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
  }

  @Test
  public void getTSUID() throws Exception {
    Schema schema = schema();
    try {
      schema.getTSUID(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getTSUID(new byte[0]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getTSUID(new byte[4]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    byte[] row = new byte[] { 0, 0, 1, 1, 2, 3, 4, 0, 0, 1, 0, 0, 1 };
    byte[] expected = new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(expected, schema.getTSUID(row));
    
    schema.salt_width = 1;
    row = new byte[] { 42, 0, 0, 1, 1, 2, 3, 4, 0, 0, 1, 0, 0, 1 };
    assertArrayEquals(expected, schema.getTSUID(row));
    
    schema.tagv_width = 5;
    row = new byte[] { 42, 0, 0, 1, 1, 2, 3, 4, 0, 0, 1, 0, 0, 0, 0, 1 };
    expected = new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1 };
    assertArrayEquals(expected, schema.getTSUID(row));
  }
  
  @Test
  public void baseTimestamp() throws Exception {
    Schema schema = schema();
    TimeStamp ts = new MillisecondTimeStamp(0L);
    try {
      schema.baseTimestamp(null, ts);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.baseTimestamp(new byte[0], ts);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.baseTimestamp(new byte[4], ts);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.baseTimestamp(new byte[16], null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    byte[] row = new byte[] { 0, 0, 1, 1, 2, 3, 4, 0, 0, 1, 0, 0, 1 };
    schema.baseTimestamp(row, ts);
    assertEquals(16909060000L, ts.msEpoch());
    
    row = new byte[] { 0, 0, 1, 0x5A, 0x49, 0x7A, 0, 0, 0, 1, 0, 0, 1 };
    schema.baseTimestamp(row, ts);
    assertEquals(1514764800000L, ts.msEpoch());
  }
  
  @Test
  public void uidWidth() throws Exception {
    Schema schema = schema();
    schema.metric_width = 1;
    schema.tagk_width = 2;
    schema.tagv_width = 5;
    try {
      schema.uidWidth(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertEquals(1, schema.uidWidth(UniqueIdType.METRIC));
    assertEquals(2, schema.uidWidth(UniqueIdType.TAGK));
    assertEquals(5, schema.uidWidth(UniqueIdType.TAGV));
    try {
      schema.uidWidth(UniqueIdType.NAMESPACE);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resolveUids() throws Exception {
    Schema schema = schema();
    
    Filter filter = Filter.newBuilder()
        .addFilter(TagVFilter.newBuilder()
            .setFilter("*")
            .setTagk(TAGK_B_STRING)
            .setType("wildcard"))
        .addFilter(TagVFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .setTagk(TAGK_STRING)
            .setType("literal_or"))
        .build();
    
    List<ResolvedFilter> resolutions = schema.resolveUids(filter, null)
        .join();
    assertEquals(2, resolutions.size());
    assertArrayEquals(TAGK_B_BYTES, resolutions.get(0).getTagKey());
    assertNull(resolutions.get(0).getTagValues());
    assertArrayEquals(TAGK_BYTES, resolutions.get(1).getTagKey());
    assertEquals(2, resolutions.get(1).getTagValues().size());
    assertArrayEquals(TAGV_BYTES, resolutions.get(1).getTagValues().get(0));
    assertArrayEquals(TAGV_B_BYTES, resolutions.get(1).getTagValues().get(1));
    
    // one tagv not found
    filter = Filter.newBuilder()
        .addFilter(TagVFilter.newBuilder()
            .setFilter("*")
            .setTagk(TAGK_B_STRING)
            .setType("wildcard"))
        .addFilter(TagVFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING + "|" + NSUN_TAGV)
            .setTagk(TAGK_STRING)
            .setType("literal_or"))
        .build();
    resolutions = schema.resolveUids(filter, null)
        .join();
    assertEquals(2, resolutions.size());
    assertArrayEquals(TAGK_B_BYTES, resolutions.get(0).getTagKey());
    assertNull(resolutions.get(0).getTagValues());
    assertArrayEquals(TAGK_BYTES, resolutions.get(1).getTagKey());
    assertEquals(3, resolutions.get(1).getTagValues().size());
    assertArrayEquals(TAGV_BYTES, resolutions.get(1).getTagValues().get(0));
    assertArrayEquals(TAGV_B_BYTES, resolutions.get(1).getTagValues().get(1));
    assertNull(resolutions.get(1).getTagValues().get(2));
    
    // tagk not found
    filter = Filter.newBuilder()
        .addFilter(TagVFilter.newBuilder()
            .setFilter("*")
            .setTagk(NSUN_TAGK)
            .setType("wildcard"))
        .addFilter(TagVFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + NSUN_TAGV + "|" + TAGV_B_STRING)
            .setTagk(TAGK_STRING)
            .setType("literal_or"))
        .build();
    resolutions = schema.resolveUids(filter, null)
        .join();
    assertEquals(2, resolutions.size());
    assertNull(resolutions.get(0).getTagKey());
    assertNull(resolutions.get(0).getTagValues());
    assertArrayEquals(TAGK_BYTES, resolutions.get(1).getTagKey());
    assertEquals(3, resolutions.get(1).getTagValues().size());
    assertArrayEquals(TAGV_BYTES, resolutions.get(1).getTagValues().get(0));
    assertNull(resolutions.get(1).getTagValues().get(1));
    assertArrayEquals(TAGV_B_BYTES, resolutions.get(1).getTagValues().get(2));
    
    // nothing found at all.
    filter = Filter.newBuilder()
        .addFilter(TagVFilter.newBuilder()
            .setFilter("*")
            .setTagk(NSUN_TAGK)
            .setType("wildcard"))
        .addFilter(TagVFilter.newBuilder()
            .setFilter(NSUN_TAGV)
            .setTagk("nope")
            .setType("literal_or"))
        .build();
    resolutions = schema.resolveUids(filter, null)
        .join();
    assertEquals(2, resolutions.size());
    assertNull(resolutions.get(0).getTagKey());
    assertNull(resolutions.get(0).getTagValues());
    assertNull(resolutions.get(1).getTagKey());
    assertEquals(1, resolutions.get(1).getTagValues().size());
    assertNull(resolutions.get(1).getTagValues().get(0));
  }
  
  @Test
  public void resolveUidsEmptyListAndTrace() throws Exception {
    Schema schema = schema();
    
    assertTrue(schema.resolveUids(mock(Filter.class), null)
        .join().isEmpty());
    
    trace = new MockTrace();
    assertTrue(schema.resolveUids(mock(Filter.class), 
        trace.newSpan("UT").start())
        .join().isEmpty());
    assertEquals(0, trace.spans.size());
    
    trace = new MockTrace(true);
    assertTrue(schema.resolveUids(mock(Filter.class), 
        trace.newSpan("UT").start())
        .join().isEmpty());
    verifySpan(Schema.class.getName() + ".resolveUids");
  }
  
  @Test
  public void resolveUidsIllegalArguments() throws Exception {
    Schema schema = schema();
    
    try {
      schema.resolveUids(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resolveUidsExceptionFromGet() throws Exception {
    Schema schema = schema();
    
    // in tagk
    Filter filter = Filter.newBuilder()
        .addFilter(TagVFilter.newBuilder()
            .setFilter("*")
            .setTagk(TAGK_STRING_EX)
            .setType("wildcard"))
        .addFilter(TagVFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .setTagk(TAGK_STRING)
            .setType("literal_or"))
        .build();
    
    Deferred<List<ResolvedFilter>> deferred = schema.resolveUids(filter, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    // in tag value
    filter = Filter.newBuilder()
        .addFilter(TagVFilter.newBuilder()
            .setFilter("*")
            .setTagk(TAGK_STRING)
            .setType("wildcard"))
        .addFilter(TagVFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_STRING_EX)
            .setTagk(TAGK_STRING)
            .setType("literal_or"))
        .build();
    
    deferred = schema.resolveUids(filter, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
  }
  
  @Test
  public void resolveUidsTraceException() throws Exception {
    Schema schema = schema();
    trace = new MockTrace(true);
    // in tagk
    Filter filter = Filter.newBuilder()
        .addFilter(TagVFilter.newBuilder()
            .setFilter("*")
            .setTagk(TAGK_STRING_EX)
            .setType("wildcard"))
        .addFilter(TagVFilter.newBuilder()
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .setTagk(TAGK_STRING)
            .setType("literal_or"))
        .build();
    
    Deferred<List<ResolvedFilter>> deferred = schema.resolveUids(filter, 
        trace.newSpan("UT").start());
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { 
      verifySpan(Schema.class.getName() + ".resolveUids", 
          StorageException.class, 4);
    }
  }
  
  @Test
  public void getId() throws Exception {
    Schema schema = schema();
    
    try {
      schema.getId(null, METRIC_STRING, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getId(UniqueIdType.METRIC, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getId(UniqueIdType.METRIC, "", null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertArrayEquals(METRIC_BYTES, schema.getId(
        UniqueIdType.METRIC, METRIC_STRING, null).join());
    assertArrayEquals(TAGK_BYTES, schema.getId(
        UniqueIdType.TAGK, TAGK_STRING, null).join());
    assertArrayEquals(TAGV_BYTES, schema.getId(
        UniqueIdType.TAGV, TAGV_STRING, null).join());
    
    assertNull(schema.getId(UniqueIdType.METRIC, NSUN_METRIC, null).join());
    assertNull(schema.getId(UniqueIdType.TAGK, NSUN_TAGK, null).join());
    assertNull(schema.getId(UniqueIdType.TAGV, NSUN_TAGV, null).join());
    
    Deferred<byte[]> deferred = schema.getId(
        UniqueIdType.METRIC, METRIC_STRING_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    deferred = schema.getId(UniqueIdType.TAGK, TAGK_STRING_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    deferred = schema.getId(UniqueIdType.TAGV, TAGV_STRING_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
  }
  
  @Test
  public void getIds() throws Exception {
    Schema schema = schema();
    
    try {
      schema.getIds(null, Lists.newArrayList(METRIC_STRING), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getIds(UniqueIdType.METRIC, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getIds(UniqueIdType.METRIC, Lists.newArrayList(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertArrayEquals(METRIC_BYTES, schema.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_STRING), null).join().get(0));
    assertArrayEquals(TAGK_BYTES, schema.getIds(UniqueIdType.TAGK, 
        Lists.newArrayList(TAGK_STRING), null).join().get(0));
    assertArrayEquals(TAGV_BYTES, schema.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING), null).join().get(0));
    
    assertNull(schema.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(NSUN_METRIC), null).join().get(0));
    assertNull(schema.getIds(UniqueIdType.TAGK, 
        Lists.newArrayList(NSUN_TAGK), null).join().get(0));
    assertNull(schema.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(NSUN_TAGV), null).join().get(0));
    
    Deferred<List<byte[]>> deferred = schema.getIds(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_STRING, METRIC_STRING_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    deferred = schema.getIds(UniqueIdType.TAGK, 
        Lists.newArrayList(TAGK_STRING, TAGK_STRING_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    deferred = schema.getIds(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_STRING, TAGV_STRING_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
  }
  
  @Test
  public void getName() throws Exception {
    Schema schema = schema();
    
    try {
      schema.getName(null, METRIC_BYTES, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getName(UniqueIdType.METRIC, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getName(UniqueIdType.METRIC, new byte[0], null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertEquals(METRIC_STRING, schema.getName(
        UniqueIdType.METRIC, METRIC_BYTES, null).join());
    assertEquals(TAGK_STRING, schema.getName(
        UniqueIdType.TAGK, TAGK_BYTES, null).join());
    assertEquals(TAGV_STRING, schema.getName(
        UniqueIdType.TAGV, TAGV_BYTES, null).join());
    
    assertNull(schema.getName(UniqueIdType.METRIC, NSUI_METRIC, null).join());
    assertNull(schema.getName(UniqueIdType.TAGK, NSUI_TAGK, null).join());
    assertNull(schema.getName(UniqueIdType.TAGV, NSUI_TAGV, null).join());
    
    Deferred<String> deferred = schema.getName(
        UniqueIdType.METRIC, METRIC_BYTES_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    deferred = schema.getName(UniqueIdType.TAGK, TAGK_BYTES_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    deferred = schema.getName(UniqueIdType.TAGV, TAGV_BYTES_EX, null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
  }
  
  @Test
  public void getNames() throws Exception {
    Schema schema = schema();
    
    try {
      schema.getNames(null, Lists.newArrayList(METRIC_BYTES), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getNames(UniqueIdType.METRIC, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.getNames(UniqueIdType.METRIC, Lists.newArrayList(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertEquals(METRIC_STRING, schema.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_BYTES), null).join().get(0));
    assertEquals(TAGK_STRING, schema.getNames(UniqueIdType.TAGK, 
        Lists.newArrayList(TAGK_BYTES), null).join().get(0));
    assertEquals(TAGV_STRING, schema.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES), null).join().get(0));
    
    assertNull(schema.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(NSUI_METRIC), null).join().get(0));
    assertNull(schema.getNames(UniqueIdType.TAGK, 
        Lists.newArrayList(NSUI_TAGK), null).join().get(0));
    assertNull(schema.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(NSUI_TAGV), null).join().get(0));
    
    Deferred<List<String>> deferred = schema.getNames(UniqueIdType.METRIC, 
        Lists.newArrayList(METRIC_BYTES, METRIC_BYTES_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    deferred = schema.getNames(UniqueIdType.TAGK, 
        Lists.newArrayList(TAGK_BYTES, TAGK_BYTES_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
    
    deferred = schema.getNames(UniqueIdType.TAGV, 
        Lists.newArrayList(TAGV_BYTES, TAGV_BYTES_EX), null);
    try {
      deferred.join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
  }
  
  @Test
  public void setBaseTime() throws Exception {
    // defaults
    Schema schema = schema();
    
    try {
      schema.setBaseTime(null, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.setBaseTime(new byte[0], 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      schema.setBaseTime(new byte[schema.metricWidth()], 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    byte[] row = new byte[schema.metricWidth() + Schema.TIMESTAMP_BYTES];
    schema.setBaseTime(row, 42);
    assertEquals(42, row[row.length - 1]);
    
    schema.setBaseTime(row, 1527811200);
    assertArrayEquals(Bytes.fromInt(1527811200), 
        Arrays.copyOfRange(row, schema.metricWidth(), 
            schema.metricWidth() + Schema.TIMESTAMP_BYTES));
    
    // with tags
    row = new byte[schema.metricWidth() + Schema.TIMESTAMP_BYTES
                   + schema.tagkWidth() + schema.tagvWidth()];
    
    schema.setBaseTime(row, 42);
    assertArrayEquals(Bytes.fromInt(42), 
        Arrays.copyOfRange(row, schema.metricWidth(), 
            schema.metricWidth() + Schema.TIMESTAMP_BYTES));
    
    schema.setBaseTime(row, 1527811200);
    assertArrayEquals(Bytes.fromInt(1527811200), 
        Arrays.copyOfRange(row, schema.metricWidth(), 
            schema.metricWidth() + Schema.TIMESTAMP_BYTES));
    
    // salt and diff metric width
    MockTSDB tsdb = new MockTSDB();
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(store_factory);
    when(store_factory.newInstance(any(TSDB.class), anyString(), any(Schema.class)))
      .thenReturn(store);    
    when(tsdb.registry.getSharedObject("default_uidstore"))
      .thenReturn(uid_store);
    when(tsdb.registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uid_factory);
    tsdb.config.register("tsd.storage.uid.width.metric", 4, false, "UT");
    tsdb.config.register("tsd.storage.salt.buckets", 16, false, "UT");
    tsdb.config.register("tsd.storage.salt.width", 1, false, "UT");
    
    schema = new Schema(tsdb, null);
    
    row = new byte[schema.saltWidth() + schema.metricWidth() 
                   + Schema.TIMESTAMP_BYTES + schema.tagkWidth() 
                   + schema.tagvWidth()];
    
    schema.setBaseTime(row, 42);
    assertArrayEquals(Bytes.fromInt(42), 
        Arrays.copyOfRange(row, schema.saltWidth() + schema.metricWidth(), 
            schema.saltWidth() + schema.metricWidth() + Schema.TIMESTAMP_BYTES));
    
    schema.setBaseTime(row, 1527811200);
    assertArrayEquals(Bytes.fromInt(1527811200), 
        Arrays.copyOfRange(row, schema.saltWidth() + schema.metricWidth(), 
            schema.saltWidth() + schema.metricWidth() + Schema.TIMESTAMP_BYTES));
  }
  
  @Test
  public void prefixKeyWithSalt() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(store_factory);
    when(store_factory.newInstance(any(TSDB.class), anyString(), any(Schema.class)))
      .thenReturn(store);    
    when(tsdb.registry.getSharedObject("default_uidstore"))
      .thenReturn(uid_store);
    when(tsdb.registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uid_factory);
    tsdb.config.register("tsd.storage.uid.width.metric", 4, false, "UT");
    tsdb.config.register("tsd.storage.uid.width.tagk", 4, false, "UT");
    tsdb.config.register("tsd.storage.uid.width.tagv", 4, false, "UT");
    tsdb.config.register("tsd.storage.salt.buckets", 20, false, "UT");
    tsdb.config.register("tsd.storage.salt.width", 1, false, "UT");
    
    Schema schema = new Schema(tsdb, null);
    
    byte[] key = new byte[] { 0, 0, -41, -87, -12, 91, 8, 107, 64, 0, 0, 
        0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 69, 124, 52, -106, 0, 0, 0, 22, 
        34, -87, 25, 92, 0, 0, 0, 80, 6, 67, 94, -84, 0, 0, 0, 89, 0, 
        0, -17, -1, 0, 0, 16, 116, 72, 112, 67, 25 };
    schema.prefixKeyWithSalt(key);
    assertEquals(6, key[0]);
    
    key = new byte[] { 0, -27, 89, 20, -100, 91, 8, 65, 16, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 42, -124, 0, 0, 0, 56, 0, 5, 
        79, -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(7, key[0]);
    
    key = new byte[] { 12, -27, 89, 20, -100, 91, 8, 51, 0, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 44, 81, 0, 0, 0, 56, 0, 5, 79, 
        -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(0, key[0]);
    
    // Switch every hour/interval
    tsdb.config.override(Schema.TIMELESS_SALTING_KEY, false);
    tsdb.config.override("tsd.storage.uid.width.metric", 4);
    tsdb.config.override("tsd.storage.uid.width.tagk", 4);
    tsdb.config.override("tsd.storage.uid.width.tagv", 4);
    tsdb.config.override("tsd.storage.salt.buckets", 20);
    tsdb.config.override("tsd.storage.salt.width", 1);
    
    schema = new Schema(tsdb, null);
    
    key = new byte[] { 0, 0, -41, -87, -12, 91, 8, 107, 64, 0, 0, 
        0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 69, 124, 52, -106, 0, 0, 0, 22, 
        34, -87, 25, 92, 0, 0, 0, 80, 6, 67, 94, -84, 0, 0, 0, 89, 0, 
        0, -17, -1, 0, 0, 16, 116, 72, 112, 67, 25 };
    schema.prefixKeyWithSalt(key);
    assertEquals(4, key[0]);
    
    key = new byte[] { 0, -27, 89, 20, -100, 91, 8, 65, 16, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 42, -124, 0, 0, 0, 56, 0, 5, 
        79, -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(3, key[0]);
    
    key = new byte[] { 12, -27, 89, 20, -100, 91, 8, 51, 0, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 44, 81, 0, 0, 0, 56, 0, 5, 79, 
        -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(6, key[0]);
    
    // OLD school. Don't do it!!
    tsdb.config.override(Schema.OLD_SALTING_KEY, true);
    tsdb.config.override(Schema.TIMELESS_SALTING_KEY, false);
    tsdb.config.override("tsd.storage.uid.width.metric", 4);
    tsdb.config.override("tsd.storage.uid.width.tagk", 4);
    tsdb.config.override("tsd.storage.uid.width.tagv", 4);
    tsdb.config.override("tsd.storage.salt.buckets", 20);
    tsdb.config.override("tsd.storage.salt.width", 1);
    
    schema = new Schema(tsdb, null);
    
    key = new byte[] { 0, 0, -41, -87, -12, 91, 8, 107, 64, 0, 0, 
        0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 69, 124, 52, -106, 0, 0, 0, 22, 
        34, -87, 25, 92, 0, 0, 0, 80, 6, 67, 94, -84, 0, 0, 0, 89, 0, 
        0, -17, -1, 0, 0, 16, 116, 72, 112, 67, 25 };
    schema.prefixKeyWithSalt(key);
    assertEquals(10, key[0]);
    
    key = new byte[] { 0, -27, 89, 20, -100, 91, 8, 65, 16, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 42, -124, 0, 0, 0, 56, 0, 5, 
        79, -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(8, key[0]);
    
    key = new byte[] { 12, -27, 89, 20, -100, 91, 8, 51, 0, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 44, 81, 0, 0, 0, 56, 0, 5, 79, 
        -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(4, key[0]);
  }
  
  @Test
  public void prefixKeyWithSaltMultiByte() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    when(tsdb.registry.getDefaultPlugin(Tsdb1xDataStoreFactory.class))
      .thenReturn(store_factory);
    when(store_factory.newInstance(any(TSDB.class), anyString(), any(Schema.class)))
      .thenReturn(store);    
    when(tsdb.registry.getSharedObject("default_uidstore"))
      .thenReturn(uid_store);
    when(tsdb.registry.getPlugin(UniqueIdFactory.class, "LRU"))
      .thenReturn(uid_factory);
    tsdb.config.register("tsd.storage.uid.width.metric", 4, false, "UT");
    tsdb.config.register("tsd.storage.uid.width.tagk", 4, false, "UT");
    tsdb.config.register("tsd.storage.uid.width.tagv", 4, false, "UT");
    tsdb.config.register("tsd.storage.salt.buckets", 20, false, "UT");
    tsdb.config.register("tsd.storage.salt.width", 3, false, "UT");
    
    Schema schema = new Schema(tsdb, null);
    
    byte[] key = new byte[] { 0, 0, 0, 0, -41, -87, -12, 91, 8, 107, 64, 0, 0, 
        0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 69, 124, 52, -106, 0, 0, 0, 22, 
        34, -87, 25, 92, 0, 0, 0, 80, 6, 67, 94, -84, 0, 0, 0, 89, 0, 
        0, -17, -1, 0, 0, 16, 116, 72, 112, 67, 25 };
    schema.prefixKeyWithSalt(key);
    assertEquals(6, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
    
    key = new byte[] { 0, 0, 0, -27, 89, 20, -100, 91, 8, 65, 16, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 42, -124, 0, 0, 0, 56, 0, 5, 
        79, -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(7, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
    
    key = new byte[] { 0, 0, 12, -27, 89, 20, -100, 91, 8, 51, 0, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 44, 81, 0, 0, 0, 56, 0, 5, 79, 
        -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(0, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
    
    // Switch every hour/interval
    tsdb.config.override(Schema.TIMELESS_SALTING_KEY, false);
    tsdb.config.override("tsd.storage.uid.width.metric", 4);
    tsdb.config.override("tsd.storage.uid.width.tagk", 4);
    tsdb.config.override("tsd.storage.uid.width.tagv", 4);
    tsdb.config.override("tsd.storage.salt.buckets", 20);
    tsdb.config.override("tsd.storage.salt.width", 3);
    
    schema = new Schema(tsdb, null);
    
    key = new byte[] { 0, 0, 0, 0, -41, -87, -12, 91, 8, 107, 64, 0, 0, 
        0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 69, 124, 52, -106, 0, 0, 0, 22, 
        34, -87, 25, 92, 0, 0, 0, 80, 6, 67, 94, -84, 0, 0, 0, 89, 0, 
        0, -17, -1, 0, 0, 16, 116, 72, 112, 67, 25 };
    schema.prefixKeyWithSalt(key);
    assertEquals(4, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
    
    key = new byte[] { 0, 0, 0, -27, 89, 20, -100, 91, 8, 65, 16, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 42, -124, 0, 0, 0, 56, 0, 5, 
        79, -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(3, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
    
    key = new byte[] { 0, 0, 12, -27, 89, 20, -100, 91, 8, 51, 0, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 44, 81, 0, 0, 0, 56, 0, 5, 79, 
        -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(6, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
    
    // OLD school. Don't do it!!
    tsdb.config.override(Schema.OLD_SALTING_KEY, true);
    tsdb.config.override(Schema.TIMELESS_SALTING_KEY, false);
    tsdb.config.override("tsd.storage.uid.width.metric", 4);
    tsdb.config.override("tsd.storage.uid.width.tagk", 4);
    tsdb.config.override("tsd.storage.uid.width.tagv", 4);
    tsdb.config.override("tsd.storage.salt.buckets", 20);
    tsdb.config.override("tsd.storage.salt.width", 3);
    
    schema = new Schema(tsdb, null);
    
    key = new byte[] { 0, 0, 0, 0, -41, -87, -12, 91, 8, 107, 64, 0, 0, 
        0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 69, 124, 52, -106, 0, 0, 0, 22, 
        34, -87, 25, 92, 0, 0, 0, 80, 6, 67, 94, -84, 0, 0, 0, 89, 0, 
        0, -17, -1, 0, 0, 16, 116, 72, 112, 67, 25 };
    schema.prefixKeyWithSalt(key);
    assertEquals(10, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
    
    key = new byte[] { 0, 0, 0, -27, 89, 20, -100, 91, 8, 65, 16, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 42, -124, 0, 0, 0, 56, 0, 5, 
        79, -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(8, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
    
    key = new byte[] { 0, 0, 12, -27, 89, 20, -100, 91, 8, 51, 0, 0, 0, 0, 1, 
        0, 0, 0, 1, 0, 0, 0, 2, 16, -1, 44, 81, 0, 0, 0, 56, 0, 5, 79, 
        -91 };
    schema.prefixKeyWithSalt(key);
    assertEquals(4, key[2]);
    assertEquals(0, key[1]);
    assertEquals(0, key[0]);
  }

  @Test
  public void resolveByteId() throws Exception {
    Schema schema = schema();
    TimeSeriesByteId id = BaseTimeSeriesByteId.newBuilder(schema)
        .setNamespace("Ns".getBytes(Const.UTF8_CHARSET))
        .setMetric(METRIC_BYTES)
        .addTags(TAGK_BYTES, TAGV_BYTES)
        .addAggregatedTag(TAGK_B_BYTES)
        .setAlias("alias".getBytes(Const.UTF8_CHARSET))
        .addDisjointTag(UIDS.get("B"))
        .build();
    
    TimeSeriesStringId newid = schema.resolveByteId(id, null).join();
    assertEquals("alias", newid.alias());
    assertEquals("Ns", newid.namespace());
    assertEquals(METRIC_STRING, newid.metric());
    assertEquals(TAGV_STRING, newid.tags().get(TAGK_STRING));
    assertEquals(TAGK_B_STRING, newid.aggregatedTags().get(0));
    assertEquals("B", newid.disjointTags().get(0));
    
    // skip metric
    id = BaseTimeSeriesByteId.newBuilder(schema)
        .setNamespace("Ns".getBytes(Const.UTF8_CHARSET))
        .setMetric("MyMetric".getBytes(Const.UTF8_CHARSET))
        .addTags(TAGK_BYTES, TAGV_BYTES)
        .addAggregatedTag(TAGK_B_BYTES)
        .setAlias("alias".getBytes(Const.UTF8_CHARSET))
        .addDisjointTag(UIDS.get("B"))
        .setSkipMetric(true)
        .build();
    
    newid = schema.resolveByteId(id, null).join();
    assertEquals("alias", newid.alias());
    assertEquals("Ns", newid.namespace());
    assertEquals("MyMetric", newid.metric());
    assertEquals(TAGV_STRING, newid.tags().get(TAGK_STRING));
    assertEquals(TAGK_B_STRING, newid.aggregatedTags().get(0));
    assertEquals("B", newid.disjointTags().get(0));
    
    // exception
    id = BaseTimeSeriesByteId.newBuilder(schema)
        .setNamespace("Ns".getBytes(Const.UTF8_CHARSET))
        .setMetric(METRIC_BYTES_EX)
        .addTags(TAGK_BYTES, TAGV_BYTES)
        .addAggregatedTag(TAGK_B_BYTES)
        .setAlias("alias".getBytes(Const.UTF8_CHARSET))
        .addDisjointTag(UIDS.get("B"))
        .build();
    
    try {
      schema.resolveByteId(id, null).join();
      fail("Expected StorageException");
    } catch (StorageException e) { }
  }
}
