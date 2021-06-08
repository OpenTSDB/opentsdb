// This file is part of OpenTSDB.
// Copyright (C) 2016-2021  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.rollup.RollupInterval;
import org.hbase.async.BinaryPrefixComparator;
import org.hbase.async.FilterList;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.QualifierFilter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pools.ByteArrayPool;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.DummyObjectPool;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.NoDataPartialTimeSeriesPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.WrappedTimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.storage.HBaseExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.PooledPartialTimeSeriesRunnable;
import net.opentsdb.storage.schemas.tsdb1x.PooledPartialTimeSeriesRunnablePool;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xNumericPartialTimeSeries;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xNumericPartialTimeSeriesPool;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xNumericSummaryPartialTimeSeries;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xNumericSummaryPartialTimeSeriesPool;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeriesSet;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeriesSetPool;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class })
public class TestTsdb1xMultiGetPush extends UTBase {

  // GMT: Sunday, April 1, 2018 12:15:00 AM
  public static final int START_TS = TS_DOUBLE_SERIES + 900;
  
  // GMT: Sunday, april 1, 2018 1:15:00 AM
  public static final int END_TS = TS_DOUBLE_SERIES + 900 + 3600;
  
  public static final TimeStamp BASE_TS = new MillisecondTimeStamp(0L);
  private static ObjectPool RUNNABLE_POOL;
  private static ObjectPool NO_DATA_POOL;
  private static ObjectPool SET_POOL;
  private static ObjectPool LONG_ARRAY_POOL;
  private static ObjectPool NUMERIC_POOL;
  private static ObjectPool BYTE_ARRAY_POOL;
  private static ObjectPool NUMERIC_SUMMARY_POOL;
  private static long HASH_A;
  private static long HASH_B;
  private static long HASH_C;
  private static long HASH_D;
  
  public Tsdb1xHBaseQueryNode node;
  public TimeSeriesDataSourceConfig source_config;
  public DefaultRollupConfig rollup_config;
  public QueryPipelineContext context;
  public List<byte[]> tsuids;
  public SemanticQuery query;
  
  @BeforeClass
  public static void beforeClassLocal() throws Exception {
    RUNNABLE_POOL = new DummyObjectPool(tsdb, 
        DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new PooledPartialTimeSeriesRunnablePool())
        .setId(PooledPartialTimeSeriesRunnablePool.TYPE)
        .build());
    NO_DATA_POOL = new DummyObjectPool(tsdb, 
        DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new NoDataPartialTimeSeriesPool())
        .setId(NoDataPartialTimeSeriesPool.TYPE)
        .build());
    Tsdb1xPartialTimeSeriesSetPool allocator = new Tsdb1xPartialTimeSeriesSetPool();
    allocator.initialize(tsdb, null).join();
    SET_POOL = new DummyObjectPool(tsdb, 
        DefaultObjectPoolConfig.newBuilder()
        .setAllocator(allocator)
        .setId(Tsdb1xPartialTimeSeriesSetPool.TYPE)
        .build());
    
    NUMERIC_POOL = new DummyObjectPool(tsdb, 
        DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new Tsdb1xNumericPartialTimeSeriesPool())
        .setId(Tsdb1xNumericPartialTimeSeriesPool.TYPE)
        .build());
    NUMERIC_SUMMARY_POOL = new DummyObjectPool(tsdb, 
        DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new Tsdb1xNumericSummaryPartialTimeSeriesPool())
        .setId(Tsdb1xNumericSummaryPartialTimeSeriesPool.TYPE)
        .build());
    LONG_ARRAY_POOL = new DummyObjectPool(tsdb, 
        DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new LongArrayPool())
        .setId(LongArrayPool.TYPE)
        .build());
    BYTE_ARRAY_POOL = new DummyObjectPool(tsdb, 
        DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new ByteArrayPool())
        .setId(ByteArrayPool.TYPE)
        .build());
    
    when(tsdb.getRegistry().getObjectPool(PooledPartialTimeSeriesRunnablePool.TYPE))
      .thenReturn(RUNNABLE_POOL);
    when(tsdb.getRegistry().getObjectPool(NoDataPartialTimeSeriesPool.TYPE))
      .thenReturn(NO_DATA_POOL);
    when(tsdb.getRegistry().getObjectPool(Tsdb1xPartialTimeSeriesSetPool.TYPE))
      .thenReturn(SET_POOL);
    
    when(tsdb.getRegistry().getObjectPool(Tsdb1xNumericPartialTimeSeriesPool.TYPE))
      .thenReturn(NUMERIC_POOL);
    when(tsdb.getRegistry().getObjectPool(Tsdb1xNumericSummaryPartialTimeSeriesPool.TYPE))
      .thenReturn(NUMERIC_SUMMARY_POOL);
    when(tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE))
      .thenReturn(LONG_ARRAY_POOL);
    when(tsdb.getRegistry().getObjectPool(ByteArrayPool.TYPE))
      .thenReturn(BYTE_ARRAY_POOL);
    
    final byte[] tsuid_a = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    HASH_A = LongHashFunction.xx().hashBytes(tsuid_a);
    
    final byte[] tsuid_b = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_B_BYTES));
    HASH_B = LongHashFunction.xx().hashBytes(tsuid_b);
    
    final byte[] tsuid_c = schema.getTSUID(makeRowKey(
        METRIC_B_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    HASH_C = LongHashFunction.xx().hashBytes(tsuid_c);
    
    final byte[] tsuid_d = schema.getTSUID(makeRowKey(
        METRIC_B_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_B_BYTES));
    HASH_D = LongHashFunction.xx().hashBytes(tsuid_d);
  }
  
  @Before
  public void before() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    context = mock(QueryPipelineContext.class);
    when(node.schema()).thenReturn(schema);
    when(node.pipelineContext()).thenReturn(context);
    when(node.parent()).thenReturn(data_store);
    when(node.fetchDataType(any(byte.class))).thenReturn(true);
    rollup_config = mock(DefaultRollupConfig.class);
    when(schema.rollupConfig()).thenReturn(rollup_config);
    when(context.upstreamOfType(any(QueryNode.class), any()))
      .thenReturn(Collections.emptyList());
    when(context.queryContext()).thenReturn(mock(QueryContext.class));
    when(context.query()).thenReturn(mock(TimeSeriesQuery.class));
    when(node.push()).thenReturn(true);
    tsdb.runnables.clear();
    
    PowerMockito.whenNew(Tsdb1xScanner.class).withAnyArguments()
      .thenAnswer(new Answer<Tsdb1xScanner>() {
        @Override
        public Tsdb1xScanner answer(InvocationOnMock invocation)
            throws Throwable {
          return mock(Tsdb1xScanner.class);
        }
      });
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(Collections.emptyList())
        .build();
    
    source_config = (TimeSeriesDataSourceConfig) baseConfig()
        .build();
    when(context.query()).thenReturn(query);
    when(data_store.dynamicString(Tsdb1xHBaseDataStore.ROLLUP_USAGE_KEY)).thenReturn("Rollup_Fallback");
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY)).thenReturn(2);
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY)).thenReturn(4);
    
    when(rollup_config.getIdForAggregator("sum")).thenReturn(1);
    when(rollup_config.getIdForAggregator("count")).thenReturn(2);
    
    tsuids = Lists.newArrayList();
    // out of order!
    tsuids.add(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_BYTES));
    tsuids.add(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_B_BYTES));
    tsuids.add(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_BYTES));
    tsuids.add(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_B_BYTES));
    storage.getMultiGets().clear();
  }
  
  @Test
  public void resetDefaults() throws Exception {
    try {
      new Tsdb1xMultiGet().reset(null, source_config, tsuids);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xMultiGet().reset(node, null, tsuids);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xMultiGet().reset(node, source_config, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xMultiGet().reset(node, source_config, Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertSame(node, mget.node);
    assertSame(source_config, mget.source_config);
    assertSame(tsuids, mget.tsuids);
    assertEquals(2, mget.concurrency_multi_get);
    assertFalse(mget.reversed);
    assertEquals(4, mget.batch_size);
    assertNull(mget.filter);
    assertFalse(mget.pre_aggregate);
    assertEquals(0, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertEquals(-1, mget.rollup_index);
    assertEquals(1, mget.tables.size());
    assertArrayEquals(DATA_TABLE, mget.tables.get(0));
    assertEquals(0, mget.outstanding.get());
    assertFalse(mget.has_failed.get());
    assertNull(mget.current_result);
    assertEquals(State.CONTINUE, mget.state());
    
    // assert sorted
    assertArrayEquals(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_BYTES), 
        mget.tsuids.get(0));
    assertArrayEquals(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_B_BYTES), 
        mget.tsuids.get(1));
    assertArrayEquals(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_BYTES), 
        mget.tsuids.get(2));
    assertArrayEquals(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_B_BYTES), 
        mget.tsuids.get(3));
    
    assertEquals(START_TS - 900, mget.start_ts.epoch());
    assertEquals(END_TS - 900, mget.end_timestamp.epoch());
    assertEquals(3600, mget.interval);
    assertEquals(2, mget.sets.length());
    assertEquals(2, mget.batches_per_set.length());
    assertEquals(2, mget.finished_batches_per_set.length());
  }
  
  @Test
  public void resetQueryOverrides() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig()
        .addOverride(Tsdb1xHBaseDataStore.PRE_AGG_KEY, "true")
        .addOverride(Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY, "8")
        .addOverride(Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY, "16")
        .addOverride(Schema.QUERY_REVERSE_KEY, "true")
        .setId("m1")
        .build();
    
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    try {
      mget.reset(node, source_config, tsuids);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
//    assertSame(node, mget.node);
//    assertSame(source_config, mget.source_config);
//    assertSame(tsuids, mget.tsuids);
//    assertEquals(8, mget.concurrency_multi_get);
//    assertTrue(mget.reversed);
//    assertEquals(16, mget.batch_size);
//    assertNull(mget.filter);
//    assertFalse(mget.rollups_enabled);
//    assertTrue(mget.pre_aggregate);
//    assertEquals(-1, mget.tsuid_idx);
//    assertEquals(END_TS - 900, mget.timestamp.epoch());
//    assertEquals(-1, mget.fallback_timestamp.epoch());
//    assertEquals(-1, mget.rollup_index);
//    assertEquals(1, mget.tables.size());
//    assertArrayEquals(DATA_TABLE, mget.tables.get(0));
//    assertEquals(0, mget.outstanding);
//    assertFalse(mget.has_failed);
//    assertNull(mget.current_result);
//    assertEquals(State.CONTINUE, mget.state());
//    
//    assertEquals(END_TS - 900, mget.start_ts.epoch());
//    assertEquals(3600, mget.interval);
    // TODO - fix for reverse
//    assertEquals(1, mget.sets.length());
//    assertEquals(1, mget.batches_per_set.length());
//    assertEquals(1, mget.finished_batches_per_set.length());
  }

  @Test
  public void resetRollups() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig()
        .addSummaryAggregation("sum")
        .addSummaryAggregation("count")
        .addRollupInterval("1h")
        .setId("m1")
        .build();
    when(node.rollupIntervals())
      .thenReturn(Lists.<RollupInterval>newArrayList(DefaultRollupInterval.builder()
          .setInterval("1h")
          .setTable("tsdb-1h")
          .setPreAggregationTable("tsdb-agg-1h")
          .setRowSpan("1d")
          .build(),
        DefaultRollupInterval.builder()
          .setInterval("30m")
          .setTable("tsdb-30m")
          .setPreAggregationTable("tsdb-agg-30m")
          .setRowSpan("1d")
          .build()));

    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertFalse(mget.pre_aggregate);
    assertEquals(3, mget.tables.size());
    assertArrayEquals("tsdb-1h".getBytes(), mget.tables.get(0));
    assertArrayEquals("tsdb-30m".getBytes(), mget.tables.get(1));
    assertArrayEquals(DATA_TABLE, mget.tables.get(2));
    assertEquals(0, mget.rollup_index);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertEquals(State.CONTINUE, mget.state());
    assertTrue(mget.filter instanceof FilterList);
    FilterList filter = (FilterList) mget.filter;
    assertEquals(4, filter.filters().size());
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    assertArrayEquals("count".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(2)).comparator()).value());
    assertArrayEquals(new byte[] { 2 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(3)).comparator()).value());
    assertEquals(START_TS - 900, mget.start_ts.epoch());
    assertEquals(START_TS - 900, mget.end_timestamp.epoch());
    assertEquals(86400, mget.interval);
    assertEquals(1, mget.sets.length());
    assertEquals(1, mget.batches_per_set.length());
    assertEquals(1, mget.finished_batches_per_set.length());
    
    // pre-agg
    source_config = (TimeSeriesDataSourceConfig) baseConfig()
        .addSummaryAggregation("sum")
        .addSummaryAggregation("count")
        .addRollupInterval("1h")
        .addOverride(Tsdb1xHBaseDataStore.PRE_AGG_KEY, "true")
        .setId("m1")
        .build();
    when(context.query()).thenReturn(query);
    
    mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertTrue(mget.pre_aggregate);
    assertEquals(3, mget.tables.size());
    assertArrayEquals("tsdb-agg-1h".getBytes(), mget.tables.get(0));
    assertArrayEquals("tsdb-agg-30m".getBytes(), mget.tables.get(1));
    assertArrayEquals(DATA_TABLE, mget.tables.get(2));
    assertEquals(0, mget.rollup_index);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertEquals(State.CONTINUE, mget.state());
    assertTrue(mget.filter instanceof FilterList);
    filter = (FilterList) mget.filter;
    assertEquals(4, filter.filters().size());
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    assertArrayEquals("count".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(2)).comparator()).value());
    assertArrayEquals(new byte[] { 2 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(3)).comparator()).value());
    assertEquals(START_TS - 900, mget.start_ts.epoch());
    assertEquals(START_TS - 900, mget.end_timestamp.epoch());
    assertEquals(86400, mget.interval);
    assertEquals(1, mget.sets.length());
    assertEquals(1, mget.batches_per_set.length());
    assertEquals(1, mget.finished_batches_per_set.length());
    
    // sum
    source_config = (TimeSeriesDataSourceConfig) baseConfig()
        .addSummaryAggregation("sum")
        .addRollupInterval("1h")
        .setId("m1")
        .build();
    when(context.query()).thenReturn(query);
    
    mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertFalse(mget.pre_aggregate);
    assertEquals(3, mget.tables.size());
    assertArrayEquals("tsdb-1h".getBytes(), mget.tables.get(0));
    assertArrayEquals("tsdb-30m".getBytes(), mget.tables.get(1));
    assertArrayEquals(DATA_TABLE, mget.tables.get(2));
    assertEquals(0, mget.rollup_index);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertEquals(State.CONTINUE, mget.state());
    assertTrue(mget.filter instanceof FilterList);
    filter = (FilterList) mget.filter;
    assertEquals(2, filter.filters().size());
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    assertEquals(START_TS - 900, mget.start_ts.epoch());
    assertEquals(START_TS - 900, mget.end_timestamp.epoch());
    assertEquals(86400, mget.interval);
    assertEquals(1, mget.sets.length());
    assertEquals(1, mget.batches_per_set.length());
    assertEquals(1, mget.finished_batches_per_set.length());
    
    // no fallback (still populates all the tables since it's small)
    when(node.rollupUsage()).thenReturn(RollupUsage.ROLLUP_NOFALLBACK);
    
    mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertFalse(mget.pre_aggregate);
    assertEquals(3, mget.tables.size());
    assertArrayEquals("tsdb-1h".getBytes(), mget.tables.get(0));
    assertArrayEquals("tsdb-30m".getBytes(), mget.tables.get(1));
    assertArrayEquals(DATA_TABLE, mget.tables.get(2));
    assertEquals(0, mget.rollup_index);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertEquals(State.CONTINUE, mget.state());
    assertTrue(mget.filter instanceof FilterList);
    filter = (FilterList) mget.filter;
    assertEquals(2, filter.filters().size());
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    assertEquals(START_TS - 900, mget.start_ts.epoch());
    assertEquals(START_TS - 900, mget.end_timestamp.epoch());
    assertEquals(86400, mget.interval);
    assertEquals(1, mget.sets.length());
    assertEquals(1, mget.batches_per_set.length());
    assertEquals(1, mget.finished_batches_per_set.length());
  }

  @Test
  public void resetTimestamps() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig(END_TS, END_TS + 3600)
            .build();
    
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    assertEquals(END_TS - 900, mget.start_ts.epoch());
    assertEquals((END_TS - 900) + 3600, mget.end_timestamp.epoch());
    assertEquals(3600, mget.interval);
    assertEquals(2, mget.sets.length());
    assertEquals(2, mget.batches_per_set.length());
    assertEquals(2, mget.finished_batches_per_set.length());
  }

  @Test
  public void resetTimedSalt() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    when(node.pipelineContext()).thenReturn(context);
    when(node.push()).thenReturn(true);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(false);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    when(node.schema()).thenReturn(schema);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    
    assertArrayEquals(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_BYTES), 
        mget.tsuids.get(0));
    assertArrayEquals(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_B_BYTES), 
        mget.tsuids.get(1));
    assertArrayEquals(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_BYTES), 
        mget.tsuids.get(2));
    assertArrayEquals(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_B_BYTES), 
        mget.tsuids.get(3));
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertEquals(START_TS - 900, mget.start_ts.epoch());
    assertEquals(END_TS - 900, mget.end_timestamp.epoch());
    assertEquals(3600, mget.interval);
    assertEquals(2, mget.sets.length());
    assertEquals(2, mget.batches_per_set.length());
    assertEquals(2, mget.finished_batches_per_set.length());
  }
  
  @Test
  public void resetTimelessSalt() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    when(node.pipelineContext()).thenReturn(context);
    when(node.push()).thenReturn(true);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(true);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    when(node.schema()).thenReturn(schema);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    
    assertArrayEquals(Bytes.concat(new byte[1], METRIC_BYTES, 
        new byte[4], TAGK_BYTES, TAGV_BYTES), 
        mget.tsuids.get(0));
    assertArrayEquals(Bytes.concat(new byte[1], METRIC_BYTES, 
        new byte[4], TAGK_BYTES, TAGV_B_BYTES), 
        mget.tsuids.get(1));
    assertArrayEquals(Bytes.concat(new byte[1], METRIC_B_BYTES, 
        new byte[4], TAGK_BYTES, TAGV_BYTES), 
        mget.tsuids.get(2));
    assertArrayEquals(Bytes.concat(new byte[1], METRIC_B_BYTES, 
        new byte[4], TAGK_BYTES, TAGV_B_BYTES), 
        mget.tsuids.get(3));
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertEquals(START_TS - 900, mget.start_ts.epoch());
    assertEquals(END_TS - 900, mget.end_timestamp.epoch());
    assertEquals(3600, mget.interval);
    assertEquals(2, mget.sets.length());
    assertEquals(2, mget.batches_per_set.length());
    assertEquals(2, mget.finished_batches_per_set.length());
  }
  
  @Test
  public void fetchNext() throws Exception {
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(2, storage.getMultiGets().size());
    assertEquals(4, storage.getMultiGets().get(0).size());
    
    List<GetRequest> gets = storage.getMultiGets().get(0);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(3).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    
    assertEquals(4, storage.getMultiGets().get(1).size());
    gets = storage.getMultiGets().get(1);
    assertArrayEquals(makeRowKey(METRIC_BYTES, END_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, END_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, END_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, END_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(3).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());

    TimeStamp ts = new SecondTimeStamp(START_TS - 900);
    validateDoubleSeries(HASH_A, 0, ts);
    validateDoubleSeries(HASH_B, 1, ts);
    validateDoubleSeries(HASH_C, 2, ts);
    validateDoubleSeries(HASH_D, 6, ts); // shifted funny due to the push cache
    
    ts.add(Duration.ofSeconds(3600));
    validateDoubleSeries(HASH_A, 3, ts);
    validateDoubleSeries(HASH_B, 4, ts);
    validateDoubleSeries(HASH_C, 5, ts);
    validateDoubleSeries(HASH_D, 7, ts); // shifted funny due to the push cache
  }
  
  @Test
  public void fetchNextSmallEvenBatch() throws Exception {
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 2);
    mget.fetchNext(null, null);
    assertEquals(4, storage.getMultiGets().size());
    assertEquals(2, storage.getMultiGets().get(0).size());
    
    List<GetRequest> gets = storage.getMultiGets().get(0);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    
    assertEquals(2, storage.getMultiGets().get(1).size());
    gets = storage.getMultiGets().get(1);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    
    assertEquals(2, storage.getMultiGets().get(2).size());
    gets = storage.getMultiGets().get(2);
    assertArrayEquals(makeRowKey(METRIC_BYTES, END_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, END_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    
    assertEquals(2, storage.getMultiGets().get(3).size());
    gets = storage.getMultiGets().get(3);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, END_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, END_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp ts = new SecondTimeStamp(START_TS - 900);
    validateDoubleSeries(HASH_A, 0, ts);
    validateDoubleSeries(HASH_B, 1, ts);
    validateDoubleSeries(HASH_C, 2, ts);
    validateDoubleSeries(HASH_D, 6, ts); // shifted funny due to the push cache
    
    ts.add(Duration.ofSeconds(3600));
    validateDoubleSeries(HASH_A, 3, ts);
    validateDoubleSeries(HASH_B, 4, ts);
    validateDoubleSeries(HASH_C, 5, ts);
    validateDoubleSeries(HASH_D, 7, ts); // shifted funny due to the push cache
  }
  
  @Test
  public void fetchNextSmallOddBatch() throws Exception {
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 3);
    mget.fetchNext(null, null);
    assertEquals(4, storage.getMultiGets().size());
    assertEquals(3, storage.getMultiGets().get(0).size());
    
    List<GetRequest> gets = storage.getMultiGets().get(0);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    
    assertEquals(1, storage.getMultiGets().get(1).size());
    gets = storage.getMultiGets().get(1);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(0).key());
    
    assertEquals(3, storage.getMultiGets().get(2).size());
    gets = storage.getMultiGets().get(2);
    assertArrayEquals(makeRowKey(METRIC_BYTES, END_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, END_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, END_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    
    assertEquals(1, storage.getMultiGets().get(3).size());
    gets = storage.getMultiGets().get(3);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, END_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(0).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp ts = new SecondTimeStamp(START_TS - 900);
    validateDoubleSeries(HASH_A, 0, ts);
    validateDoubleSeries(HASH_B, 1, ts);
    validateDoubleSeries(HASH_C, 2, ts);
    validateDoubleSeries(HASH_D, 6, ts); // shifted funny due to the push cache
    
    ts.add(Duration.ofSeconds(3600));
    validateDoubleSeries(HASH_A, 3, ts);
    validateDoubleSeries(HASH_B, 4, ts);
    validateDoubleSeries(HASH_C, 5, ts);
    validateDoubleSeries(HASH_D, 7, ts); // shifted funny due to the push cache
  }
  
  @Test
  public void fetchNextNoData() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig(946684800 + 900,
            946684800 + 900 + (3600 * 2))
            .build();
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(3, storage.getMultiGets().size());
    
    int ts = 946684800;
    for (int i = 0; i < 3; i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp timestamp = new SecondTimeStamp(946684800);
    for (int i = 0; i < tsdb.runnables.size(); i++) {
      validateEmptySeries(i, timestamp, 3, false);
      timestamp.add(Duration.ofSeconds(3600));
    }
  }
  
  @Test
  public void fetchNextMultiColumn() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig(
            TS_MULTI_COLUMN_SERIES + 900,
            TS_MULTI_COLUMN_SERIES + 900 + 3600)
            .build();
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(2, storage.getMultiGets().size());
    
    int ts = TS_MULTI_COLUMN_SERIES;
    for (int i = 0; i < 2; i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
        
    TimeStamp timestamp = new SecondTimeStamp(TS_MULTI_COLUMN_SERIES);
    validateSingleMixedSeries(HASH_A, 0, timestamp, 2, 2);
    validateSingleMixedSeries(HASH_C, 2, timestamp, 2, 2);

    timestamp.add(Duration.ofSeconds(3600));
    validateSingleMixedSeries(HASH_A, 1, timestamp, 2, 2);
    validateSingleMixedSeries(HASH_C, 3, timestamp, 2, 2);
  }
  
  @Test
  public void fetchNextAppendColumn() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig(
            TS_APPEND_SERIES + 900,
            TS_APPEND_SERIES + 900 + 3600)
            .build();
    when(context.query()).thenReturn(query);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(2, storage.getMultiGets().size());
    
    int ts = TS_APPEND_SERIES;
    for (int i = 0; i < 2; i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
        
    TimeStamp timestamp = new SecondTimeStamp(TS_APPEND_SERIES);
    validateSingleMixedSeries(HASH_A, 0, timestamp, 2, 1);

    timestamp.add(Duration.ofSeconds(3600));
    validateSingleMixedSeries(HASH_A, 1, timestamp, 2, 1);
  }
  
  @Test
  public void fetchNextRollup() throws Exception {
    // rollup tables
    setMultiRollupQuery();
    when(node.sentData()).thenReturn(true); // pretend we sent it.
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(3, storage.getMultiGets().size());
    int ts = TS_ROLLUP_SERIES;
    for (int i = 0; i < storage.getMultiGets().size(); i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertSame(mget.filter, gets.get(x).getFilter());
      }
      
      ts += 86400;
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    TimeStamp timestamp = new SecondTimeStamp(TS_ROLLUP_SERIES);
    // note the order is all funky due to the mock being single threaded
    validateDoubleSeriesRollup(HASH_A, 0, timestamp);
    validateDoubleSeriesRollup(HASH_C, 3, timestamp);
    
    timestamp.add(Duration.ofSeconds(86400));
    validateDoubleSeriesRollup(HASH_A, 1, timestamp);
    validateDoubleSeriesRollup(HASH_C, 4, timestamp);
    
    timestamp.add(Duration.ofSeconds(86400));
    validateDoubleSeriesRollup(HASH_A, 2, timestamp);
    validateDoubleSeriesRollup(HASH_C, 5, timestamp);
  }
  
  @Test
  public void fetchNextRollupSmallEvenBatch() throws Exception {
    // rollup tables
    setMultiRollupQuery();
    when(node.sentData()).thenReturn(true); // pretend we sent it.
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 2);
    mget.fetchNext(null, null);
    assertEquals(6, storage.getMultiGets().size());
    assertEquals(2, storage.getMultiGets().get(0).size());
    List<GetRequest> gets = storage.getMultiGets().get(0);
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(2, storage.getMultiGets().get(1).size());
    gets = storage.getMultiGets().get(1);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(2, storage.getMultiGets().get(2).size());
    gets = storage.getMultiGets().get(2);
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES + 86400, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES + 86400, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(2, storage.getMultiGets().get(3).size());
    gets = storage.getMultiGets().get(3);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES + 86400, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES + 86400, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(2, storage.getMultiGets().get(4).size());
    gets = storage.getMultiGets().get(4);
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES + (86400 * 2), TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES + (86400 * 2), TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(2, storage.getMultiGets().get(5).size());
    gets = storage.getMultiGets().get(5);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES + (86400 * 2), TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES + (86400 * 2), TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    TimeStamp ts = new SecondTimeStamp(TS_ROLLUP_SERIES);
    // note the order is all funky due to the mock being single threaded
    validateDoubleSeriesRollup(HASH_A, 0, ts);
    validateDoubleSeriesRollup(HASH_C, 3, ts);
    
    ts.add(Duration.ofSeconds(86400));
    validateDoubleSeriesRollup(HASH_A, 1, ts);
    validateDoubleSeriesRollup(HASH_C, 4, ts);
    
    ts.add(Duration.ofSeconds(86400));
    validateDoubleSeriesRollup(HASH_A, 2, ts);
    validateDoubleSeriesRollup(HASH_C, 5, ts);
  }
  
  @Test
  public void fetchNextRollupSmallOddBatch() throws Exception {
    // rollup tables
    setMultiRollupQuery();
    when(node.sentData()).thenReturn(true); // pretend we sent it.
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 3);
    mget.fetchNext(null, null);
    assertEquals(6, storage.getMultiGets().size());
    assertEquals(3, storage.getMultiGets().get(0).size());
    List<GetRequest> gets = storage.getMultiGets().get(0);
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(1, storage.getMultiGets().get(1).size());
    gets = storage.getMultiGets().get(1);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(0).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(3, storage.getMultiGets().get(2).size());
    gets = storage.getMultiGets().get(2);
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES + 86400, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES + 86400, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES + 86400, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(1, storage.getMultiGets().get(3).size());
    gets = storage.getMultiGets().get(3);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES + 86400, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(0).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(3, storage.getMultiGets().get(4).size());
    gets = storage.getMultiGets().get(4);
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES + (86400 * 2), TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(METRIC_BYTES, TS_ROLLUP_SERIES + (86400 * 2), TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES + (86400 * 2), TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertEquals(1, storage.getMultiGets().get(5).size());
    gets = storage.getMultiGets().get(5);
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, TS_ROLLUP_SERIES + (86400 * 2), TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(0).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertSame(mget.filter, gets.get(i).getFilter());
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    TimeStamp ts = new SecondTimeStamp(TS_ROLLUP_SERIES);
    // note the order is all funky due to the mock being single threaded
    validateDoubleSeriesRollup(HASH_A, 0, ts);
    validateDoubleSeriesRollup(HASH_C, 3, ts);
    
    ts.add(Duration.ofSeconds(86400));
    validateDoubleSeriesRollup(HASH_A, 1, ts);
    validateDoubleSeriesRollup(HASH_C, 4, ts);
    
    ts.add(Duration.ofSeconds(86400));
    validateDoubleSeriesRollup(HASH_A, 2, ts);
    validateDoubleSeriesRollup(HASH_C, 5, ts);
  }
  
  @Test
  public void fetchNextFillEarly() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig(START_TS - (3600 * 2),
            END_TS)
            .build();
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(4, storage.getMultiGets().size());
    assertEquals(4, storage.getMultiGets().get(0).size());
    
    int ts = START_TS - 900 - (3600 * 2);
    for (int i = 0; i < storage.getMultiGets().size(); i++) {
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp timestamp = new SecondTimeStamp(START_TS - 900 - (3600 *  2));
    validateEmptySeries(6, timestamp, 4, false);
    
    timestamp.add(Duration.ofSeconds(3600));
    validateEmptySeries(7, timestamp, 4, false);
    
    timestamp.add(Duration.ofSeconds(3600));
    validateDoubleSeries(HASH_A, 0, timestamp, 4);
    validateDoubleSeries(HASH_B, 1, timestamp, 4);
    validateDoubleSeries(HASH_C, 2, timestamp, 4);
    validateDoubleSeries(HASH_D, 8, timestamp, 4); // shifted funny due to the push cache
    
    timestamp.add(Duration.ofSeconds(3600));
    validateDoubleSeries(HASH_A, 3, timestamp, 4);
    validateDoubleSeries(HASH_B, 4, timestamp, 4);
    validateDoubleSeries(HASH_C, 5, timestamp, 4);
    validateDoubleSeries(HASH_D, 9, timestamp, 4); // shifted funny due to the push cache
  }
  
  @Test
  public void fetchNextFillLate() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig(
            START_TS + (3600 * (TS_DOUBLE_SERIES_COUNT - 1)),
            END_TS + (3600 * (TS_DOUBLE_SERIES_COUNT + 1)))
            .build();
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(4, storage.getMultiGets().size());
    assertEquals(4, storage.getMultiGets().get(0).size());
    
    int ts = START_TS + (3600 * (TS_DOUBLE_SERIES_COUNT - 1)) - 900;
    for (int i = 0; i < storage.getMultiGets().size(); i++) {
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp timestamp = new SecondTimeStamp(
        START_TS + (3600 * (TS_DOUBLE_SERIES_COUNT - 1)) - 900);
    validateDoubleSeries(HASH_A, 0, timestamp, 4);
    validateDoubleSeries(HASH_B, 1, timestamp, 4);
    validateDoubleSeries(HASH_C, 2, timestamp, 4);
    validateDoubleSeries(HASH_D, 3, timestamp, 4);
    
    timestamp.add(Duration.ofSeconds(3600));
    validateEmptySeries(4, timestamp, 4, false);
    
    timestamp.add(Duration.ofSeconds(3600));
    validateEmptySeries(5, timestamp, 4, false);
    
    timestamp.add(Duration.ofSeconds(3600));
    validateEmptySeries(6, timestamp, 4, false);
  }
  
  @Test
  public void fetchNextFillMiddle() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig(
            TS_SINGLE_SERIES_GAP + 900,
            TS_SINGLE_SERIES_GAP + 900 + (3600 * TS_SINGLE_SERIES_GAP_COUNT))
            .build();
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(17, storage.getMultiGets().size());
    
    int ts = TS_SINGLE_SERIES_GAP;
    for (int i = 0; i < storage.getMultiGets().size(); i++) {
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertEquals(4, gets.size());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    TimeStamp timestamp = new SecondTimeStamp(TS_SINGLE_SERIES_GAP);
    // note the order is funky since we're single threaded.
    for (int i = 0; i < 5; i++) {
      validateSingleSeries(HASH_A, i, timestamp, 17, 2);
      validateSingleSeries(HASH_C, i + 10, timestamp, 17, 2);
      
      timestamp.add(Duration.ofSeconds(3600));
    }
    
    // straggler
    validateSingleSeries(HASH_A, 15, timestamp, 17, 1);
    
    timestamp.add(Duration.ofSeconds(3600));
    for (int i = 16; i < 20; i++) {
      validateEmptySeries(i, timestamp, 17, false);
      timestamp.add(Duration.ofSeconds(3600));
    }
    
    // straggler
    validateSingleSeries(HASH_C, 20, timestamp, 17, 1);
    
    timestamp.add(Duration.ofSeconds(3600));
    for (int i = 5; i < 10; i++) {
      validateSingleSeries(HASH_A, i, timestamp, 17, 2);
      validateSingleSeries(HASH_C, i + 16, timestamp, 17, 2);
      
      timestamp.add(Duration.ofSeconds(3600));
    }
    
    validateEmptySeries(26, timestamp, 17, false);
  }
  
  @Test
  public void fetchNextRollupFallbackThenFindsData() throws Exception {
    // rollup tables
    setMultiRollupQuery(false, TS_DOUBLE_SERIES);
    when(node.sentData())
      .thenReturn(false)
      .thenReturn(true); // pretend we sent it.
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(52, storage.getMultiGets().size());
    
    int ts = TS_DOUBLE_SERIES;
    for (int i = 0; i < 3; i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertSame(mget.filter, gets.get(x).getFilter());
      }
      
      ts += 86400;
    }
    
    // fallback
    ts = TS_DOUBLE_SERIES;
    for (int i = 3; i < storage.getMultiGets().size(); i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp timestamp = new SecondTimeStamp(TS_DOUBLE_SERIES);
    // note the order is all funky due to the mock being single threaded
    for (int i = 0; i < 48; i += 3) {
      validateDoubleSeries(HASH_A, i, timestamp, 49);
      validateDoubleSeries(HASH_B, i + 1, timestamp, 49);
      validateDoubleSeries(HASH_C, i + 2, timestamp, 49);
      validateDoubleSeries(HASH_D, (i / 3) + 48, timestamp, 49);
      timestamp.add(Duration.ofSeconds(3600));
    }
    
    // it was a rollup query so we have lots of empty stuff
    for (int i = 64; i < storage.getMultiGets().size(); i++) {
      validateEmptySeries(i, timestamp, 49, false);
      timestamp.add(Duration.ofSeconds(3600));
    }
  }
  
  @Test
  public void fetchNextRollupFallbackThenFindsNoData() throws Exception {
    // rollup tables
    setMultiRollupQuery(false, 946684800);
    when(node.sentData()).thenReturn(false);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(52, storage.getMultiGets().size());
    
    int ts = 946684800;
    for (int i = 0; i < 3; i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertSame(mget.filter, gets.get(x).getFilter());
      }
      
      ts += 86400;
    }
    
    // fallback
    ts = 946684800;
    for (int i = 3; i < storage.getMultiGets().size(); i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp timestamp = new SecondTimeStamp(946684800);
    for (int i = 0; i < tsdb.runnables.size(); i++) {
      validateEmptySeries(i, timestamp, 49, false);
      timestamp.add(Duration.ofSeconds(3600));
    }
  }
  
  @Test
  public void fetchNextRollupNoFallback() throws Exception {
    // rollup tables
    setMultiRollupQuery(false, 946684800);
    when(node.sentData()).thenReturn(false);
    when(node.rollupUsage()).thenReturn(RollupUsage.ROLLUP_NOFALLBACK);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(3, storage.getMultiGets().size());
    
    int ts = 946684800;
    for (int i = 0; i < 3; i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals("tsdb-rollup-1h".getBytes(), gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertSame(mget.filter, gets.get(x).getFilter());
      }
      
      ts += 86400;
    }
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp timestamp = new SecondTimeStamp(946684800);
    for (int i = 0; i < tsdb.runnables.size(); i++) {
      validateEmptySeries(i, timestamp, 3, true);
      timestamp.add(Duration.ofSeconds(86400));
    }
  }
  
  @Test
  public void fetchNextErrorFromStorage() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) baseConfig(
            TS_MULTI_SERIES_EX + 900,
            TS_MULTI_SERIES_EX + 900 + (3600 * TS_MULTI_SERIES_EX_COUNT))
            .build();
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    assertEquals(8, storage.getMultiGets().size());
    
    int ts = TS_MULTI_SERIES_EX;
    for (int i = 0; i < 8; i++) {
      assertEquals(4, storage.getMultiGets().get(i).size());
      List<GetRequest> gets = storage.getMultiGets().get(i);
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(0).key());
      assertArrayEquals(makeRowKey(METRIC_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(1).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_BYTES), 
          gets.get(2).key());
      assertArrayEquals(makeRowKey(METRIC_B_BYTES, ts, TAGK_BYTES, TAGV_B_BYTES), 
          gets.get(3).key());
      for (int x = 0; x < gets.size(); x++) {
        assertArrayEquals(DATA_TABLE, gets.get(x).table());
        assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(x).family());
        assertNull(gets.get(x).getFilter());
      }
      
      ts += 3600;
    }
    assertFalse(mget.all_batches_sent.get());
    assertEquals(State.EXCEPTION, mget.state());
    assertEquals(28, tsdb.runnables.size());
    verify(node, times(1)).onError(any(UnitTestException.class));
  }
  
  @Test
  public void fetchNextTimedSalt() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    when(node.pipelineContext()).thenReturn(context);
    when(node.push()).thenReturn(true);
    when(node.sentData()).thenReturn(true);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(false);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    // NOTE: Since we don't mock out schema.prefixKeyWithSalt() the requested salts are 0.
    when(node.schema()).thenReturn(schema);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);    
    mget.fetchNext(null, null);
    
    assertEquals(4, storage.getLastMultiGets().size());
    List<GetRequest> gets = storage.getMultiGets().get(0);
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), START_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), START_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), START_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), START_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(3).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    
    gets = storage.getMultiGets().get(1);
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), END_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), END_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), END_TS - 900, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), END_TS - 900, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(3).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp ts = new SecondTimeStamp(START_TS - 900);
    // TODO - write to a salted mock table
    validateEmptySeries(0, ts, 2, false);
    
    ts.add(Duration.ofSeconds(3600));
    validateEmptySeries(1, ts, 2, false);
  }
  
  @Test
  public void fetchNextTimelessSalt() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    when(node.pipelineContext()).thenReturn(context);
    when(node.push()).thenReturn(true);
    when(node.sentData()).thenReturn(true);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(true);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    // NOTE: Since we don't mock out schema.setBaseTime() the requested timestamps are 0.
    when(node.schema()).thenReturn(schema);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    
    assertEquals(4, storage.getLastMultiGets().size());
    List<GetRequest> gets = storage.getMultiGets().get(0);
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), 0, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), 0, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), 0, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), 0, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(3).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    
    gets = storage.getMultiGets().get(1);
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), 0, TAGK_BYTES, TAGV_BYTES), 
        gets.get(0).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), 0, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(1).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), 0, TAGK_BYTES, TAGV_BYTES), 
        gets.get(2).key());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), 0, TAGK_BYTES, TAGV_B_BYTES), 
        gets.get(3).key());
    for (int i = 0; i < gets.size(); i++) {
      assertArrayEquals(DATA_TABLE, gets.get(i).table());
      assertArrayEquals(Tsdb1xHBaseDataStore.DATA_FAMILY, gets.get(i).family());
      assertNull(gets.get(i).getFilter());
    }
    
    assertTrue(mget.all_batches_sent.get());
    assertEquals(State.COMPLETE, mget.state());
    
    TimeStamp ts = new SecondTimeStamp(START_TS - 900);
    // TODO - write to a salted mock table
    validateEmptySeries(0, ts, 2, false);
    
    ts.add(Duration.ofSeconds(3600));
    validateEmptySeries(1, ts, 2, false);
  }
  
  @Test
  public void close() throws Exception {
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(null, null);
    
    assertEquals(END_TS - 900 + 3600, mget.timestamp.epoch());
    assertEquals(END_TS - 900, mget.end_timestamp.epoch());
    assertSame(tsuids, mget.tsuids);
    assertSame(source_config, mget.source_config);
    assertSame(node, mget.node);
    assertEquals(1, mget.tables.size());
    assertEquals(0, mget.outstanding.get());
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(-1, mget.rollup_index);
    
    mget.close();
    // TODO - restore
//    assertEquals(-1, mget.timestamp.epoch());
//    assertEquals(-1, mget.end_timestamp.epoch());
//    assertNull(mget.tsuids);
//    assertNull(mget.source_config);
//    assertNull(mget.node);
//    assertEquals(0, mget.tables.size());
//    assertEquals(0, mget.outstanding.get());
//    assertEquals(-1, mget.tsuid_idx);
//    assertEquals(-1, mget.rollup_index);
  }
  
  void setMultiRollupQuery() throws Exception {
    setMultiRollupQuery(false, TS_ROLLUP_SERIES);
  }
  
  void setMultiRollupQuery(final boolean reversed, final int start) throws Exception {
    List<DefaultRollupInterval> intervals = Lists.<DefaultRollupInterval>newArrayList(DefaultRollupInterval.builder()
        .setInterval("1h")
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-rollup-1h")
        .setRowSpan("1d")
        .build());
    List<RollupInterval> plainIntervals = Lists.newArrayList(intervals);
    DefaultRollupConfig config = DefaultRollupConfig.newBuilder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .setIntervals(intervals)
        .build();
    for (final RollupInterval interval : intervals) {
      interval.setRollupConfig(rollup_config);
    }
    when(node.rollupIntervals())
      .thenReturn(plainIntervals);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(start + 900))
        .setEnd(Integer.toString(start + (86400 * 2) + 900))
        .setExecutionGraph(Collections.emptyList())
        .build();
    
    source_config = (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setStartTimeStamp(new SecondTimeStamp(start + 900))
        .setEndTimeStamp(new SecondTimeStamp(start + (86400 * 2) + 900))
        .addSummaryAggregation("sum")
        .addSummaryAggregation("count")
        .addRollupInterval("1h")
        .addOverride(Schema.QUERY_REVERSE_KEY, reversed ? "true" : "false")
        .setId("m1")
        .build();
    when(context.query()).thenReturn(query);
  }

  void verifyFourSeries() {
    assertEquals(8, tsdb.runnables.size());
    
    final byte[] tsuid_a = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash_a = LongHashFunction.xx().hashBytes(tsuid_a);
    //validate(hash_a, 0, 3, hours);
    System.out.println("A: " + hash_a);
    final byte[] tsuid_b = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_B_BYTES));
    final long hash_b = LongHashFunction.xx().hashBytes(tsuid_b);
    System.out.println("B: " + hash_b);
    final byte[] tsuid_c = schema.getTSUID(makeRowKey(
        METRIC_B_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash_c = LongHashFunction.xx().hashBytes(tsuid_c);
    System.out.println("C: " + hash_c);
    final byte[] tsuid_d = schema.getTSUID(makeRowKey(
        METRIC_B_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_B_BYTES));
    final long hash_d = LongHashFunction.xx().hashBytes(tsuid_d);
    
    TimeStamp start = new SecondTimeStamp(TS_DOUBLE_SERIES);
    TimeStamp end = start.getCopy();
    end.add(Duration.ofSeconds(3600));
    int idx = 0;
    for (final Runnable runnable : tsdb.runnables) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
//      assertEquals(start.epoch(), set.start().epoch());
//      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(4, set.timeSeriesCount());
      assertEquals(2, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) runnable).pts().value();
      System.out.println("idx: " + idx + "  HASH: " + value.idHash() + "  " + set.start().epoch());
//      assertEquals(0, value.offset());
//      assertEquals(2, value.end());
//      assertEquals(start.epoch(), value.data()[0]);
//      assertEquals(1, value.data()[1]); // just the single value
//      if (idx == 0 || idx % 2 == 0) {
//        assertEquals(hash_a, value.idHash());
//      } else if (idx % 3 == 0) {
//        
//      } else {
//        assertEquals(hash_b, value.idHash());
//        start.add(Duration.ofSeconds(3600));
//        end.add(Duration.ofSeconds(3600));
//      }
      idx++;
    }
  }
  
  void validateDoubleSeries(long hash, int index, TimeStamp start) {
    validateDoubleSeries(hash, index, start, 2);
  }
  
  void validateDoubleSeries(long hash, int index, TimeStamp start, int sets) {
    TimeStamp end = start.getCopy();
    end.add(Duration.ofSeconds(3600));
    Runnable runnable = tsdb.runnables.get(index);
    Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
        ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
    assertEquals(start.epoch(), set.start().epoch());
    assertEquals(end.epoch(), set.end().epoch());
    assertTrue(set.complete());
    assertEquals(4, set.timeSeriesCount());
    assertEquals(sets, set.totalSets());
    assertSame(node, set.node());
    
    Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
        ((PooledPartialTimeSeriesRunnable) runnable).pts().value();
    assertEquals(0, value.offset());
    assertEquals(2, value.end());
    assertEquals(start.epoch(), value.data()[0]);
    assertEquals(1, value.data()[1]); // just the single value
    assertEquals(hash, value.idHash());
  }
  
  void validateSingleSeries(long hash, int index, TimeStamp start, int sets, int series) {
    TimeStamp end = start.getCopy();
    end.add(Duration.ofSeconds(3600));
    Runnable runnable = tsdb.runnables.get(index);
    Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
        ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
    assertEquals(start.epoch(), set.start().epoch());
    assertEquals(end.epoch(), set.end().epoch());
    assertTrue(set.complete());
    assertEquals(series, set.timeSeriesCount());
    assertEquals(sets, set.totalSets());
    assertSame(node, set.node());
    
    Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
        ((PooledPartialTimeSeriesRunnable) runnable).pts().value();
    assertEquals(0, value.offset());
    assertEquals(2, value.end());
    assertEquals(start.epoch(), value.data()[0]);
    assertEquals(1, value.data()[1]); // just the single value
    assertEquals(hash, value.idHash());
  }
  
  void validateSingleMixedSeries(long hash, int index, TimeStamp start, int sets, int series) {
    TimeStamp end = start.getCopy();
    end.add(Duration.ofSeconds(3600));
    Runnable runnable = tsdb.runnables.get(index);
    Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
        ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
    assertEquals(start.epoch(), set.start().epoch());
    assertEquals(end.epoch(), set.end().epoch());
    assertTrue(set.complete());
    assertEquals(series, set.timeSeriesCount());
    assertEquals(sets, set.totalSets());
    assertSame(node, set.node());
    
    Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
        ((PooledPartialTimeSeriesRunnable) runnable).pts().value();
    assertEquals(0, value.offset());
    // TODO - decode
    assertEquals(hash, value.idHash());
  }
  
  void validateEmptySeries(int index, TimeStamp start, int sets, boolean is_rollup) {
    TimeStamp end = start.getCopy();
    end.add(Duration.ofSeconds(is_rollup ? 86400 : 3600));
    Runnable runnable = tsdb.runnables.get(index);
    Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
        ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
    assertEquals(start.epoch(), set.start().epoch());
    assertEquals(end.epoch(), set.end().epoch());
    assertTrue(set.complete());
    assertEquals(0, set.timeSeriesCount());
    assertEquals(sets, set.totalSets());
    assertSame(node, set.node());
    
    assertTrue(((PooledPartialTimeSeriesRunnable) runnable).pts() 
        instanceof NoDataPartialTimeSeries);
  }
  
  void validateDoubleSeriesRollup(long hash, int index, TimeStamp start) {
    TimeStamp end = start.getCopy();
    end.add(Duration.ofSeconds(86400));
    Runnable runnable = tsdb.runnables.get(index);
    Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
        ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
    assertEquals(start.epoch(), set.start().epoch());
    assertEquals(end.epoch(), set.end().epoch());
    assertTrue(set.complete());
    assertEquals(2, set.timeSeriesCount());
    assertEquals(3, set.totalSets());
    assertSame(node, set.node());
    
    Tsdb1xNumericSummaryPartialTimeSeries value = (Tsdb1xNumericSummaryPartialTimeSeries)
        ((PooledPartialTimeSeriesRunnable) runnable).pts().value();
    assertEquals(0, value.offset());
    assertEquals(96, value.end());
    // TODO validate the values
//      assertEquals(start.epoch(), value.data()[0]);
//      assertEquals(1, value.data()[1]); // just the single value
    assertEquals(hash, value.idHash());
  }
  
  void debug(TimeStamp start, int interval) {
    System.out.println("A: " + HASH_A);
    System.out.println("B: " + HASH_B);
    System.out.println("C: " + HASH_C);
    System.out.println("D: " + HASH_D);
    
    int idx = 0;
    for (Runnable runnable : tsdb.runnables) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
      System.out.println("[" + idx++ + "] " + ((PooledPartialTimeSeriesRunnable) runnable).pts().idHash() + "  " + set.start().epoch() + "  " + 
          ((set.start().epoch() - start.epoch()) / interval));
    }
  }

  TimeSeriesDataSourceConfig.Builder baseConfig() {
    return baseConfig(START_TS, END_TS);
  }

  TimeSeriesDataSourceConfig.Builder baseConfig(int start, int end) {
    return DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                    .setMetric(METRIC_STRING)
                    .build())
            .setStartTimeStamp(new SecondTimeStamp(start))
            .setEndTimeStamp(new SecondTimeStamp(end))
            .setId("m1");
  }

}
