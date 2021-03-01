// This file is part of OpenTSDB.
// Copyright (C) 2016-2020  The OpenTSDB Authors.
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

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import io.netty.util.HashedWheelTimer;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
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
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.storage.HBaseExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.utils.UnitTestException;
import org.hbase.async.BinaryPrefixComparator;
import org.hbase.async.FilterList;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.QualifierFilter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class })
public class TestTsdb1xMultiGet extends UTBase {

  //GMT: Sunday, April 1, 2018 12:15:00 AM
  public static final int START_TS = TS_DOUBLE_SERIES + 900;
 
  // GMT: Sunday, april 1, 2018 1:15:00 AM
  public static final int END_TS = TS_DOUBLE_SERIES + 900 + 3600;
  
  public static final TimeStamp BASE_TS = new MillisecondTimeStamp(0L);
  
  public Tsdb1xHBaseQueryNode node;
  public TimeSeriesDataSourceConfig source_config;
  public DefaultRollupConfig rollup_config;
  public QueryPipelineContext context;
  public List<byte[]> tsuids;
  public SemanticQuery query;
  
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
    
    source_config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setId("m1")
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
    
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
  }
  
  @Test
  public void resetQueryOverrides() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addOverride(Tsdb1xHBaseDataStore.PRE_AGG_KEY, "true")
        .addOverride(Tsdb1xHBaseDataStore.MULTI_GET_CONCURRENT_KEY, "8")
        .addOverride(Tsdb1xHBaseDataStore.MULTI_GET_BATCH_KEY, "16")
        .addOverride(Schema.QUERY_REVERSE_KEY, "true")
        .setId("m1")
        .build();
    
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertSame(node, mget.node);
    assertSame(source_config, mget.source_config);
    assertSame(tsuids, mget.tsuids);
    assertEquals(8, mget.concurrency_multi_get);
    assertTrue(mget.reversed);
    assertEquals(16, mget.batch_size);
    assertNull(mget.filter);
    assertTrue(mget.pre_aggregate);
    assertEquals(0, mget.tsuid_idx);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    assertEquals(-1, mget.rollup_index);
    assertEquals(1, mget.tables.size());
    assertArrayEquals(DATA_TABLE, mget.tables.get(0));
    assertEquals(0, mget.outstanding.get());
    assertNull(mget.current_result);
    assertEquals(State.CONTINUE, mget.state());
  }

  @Test
  public void resetRollups() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addSummaryAggregation("sum")
        .addSummaryAggregation("count")
        .addRollupInterval("1h")
        .setId("m1")
        .build();
    when(node.rollupIntervals())
      .thenReturn(Lists.<RollupInterval>newArrayList(RollupInterval.builder()
          .setInterval("1h")
          .setTable("tsdb-1h")
          .setPreAggregationTable("tsdb-agg-1h")
          .setRowSpan("1d")
          .build(),
        RollupInterval.builder()
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
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
    
    // pre-agg
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(Collections.emptyList())
        .build();
    source_config = (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
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
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
    
    // sum
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(Collections.emptyList())
        .build();
    source_config = (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
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
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
    
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
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
  }

  @Test
  public void resetTimestamps() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(END_TS))
        .setEnd(Integer.toString(END_TS + 3600))
        .setExecutionGraph(Collections.emptyList())
        .build();
    when(context.query()).thenReturn(query);
    
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS + 3600, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
    
    // with padding
    source_config = (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addSummaryAggregation("max")
        .setPrePadding("2h")
        .setPostPadding("2h")
        .setId("m1")
        .build();
    mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertEquals(END_TS - 900 - (3600 * 2), mget.timestamp.epoch());
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS + (3600 * 3), mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
  }
  
  @Test
  public void resetTimestampsOffset() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(END_TS))
        .setEnd(Integer.toString(END_TS + 3600))
        .setExecutionGraph(Collections.emptyList())
        .build();
    
    source_config = new WrappedTimeSeriesDataSourceConfig(
        "m1-previous-P1D",
        (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric(METRIC_STRING)
                .build())
            .addSummaryAggregation("max")
            .setTimeShiftInterval("1d")
            .setId("m1")
            .build(),
        true);
    when(context.query()).thenReturn(query);
    
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    assertEquals(END_TS - 900 - 86400, mget.timestamp.epoch());
    assertEquals(END_TS + + 3600 - 86400, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
  }

  @Test
  public void resetTimedSalt() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    when(node.pipelineContext()).thenReturn(context);
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
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
  }
  
  @Test
  public void resetTimelessSalt() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    when(node.pipelineContext()).thenReturn(context);
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
    assertEquals(-1, mget.start_ts.epoch()); // not used
    assertEquals(END_TS, mget.end_timestamp.epoch()); // not used
    assertEquals(0, mget.interval);
    assertNull(mget.sets);
    assertNull(mget.batches_per_set);
    assertNull(mget.finished_batches_per_set);
  }
  
  @Test
  public void fetchNext() throws Exception {
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
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
    verify(result, times(8)).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextClosed() throws Exception {
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    when(node.pipelineContext().queryContext().isClosed())
      .thenReturn(true);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
    
    assertFalse(mget.all_batches_sent.get());
    assertEquals(State.EXCEPTION, mget.state());
    verify(result, never()).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  
  @Test
  public void fetchNextSmallEvenBatch() throws Exception {
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 2);
    mget.fetchNext(result, null);
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
    verify(result, times(8)).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextSmallOddBatch() throws Exception {
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 3);
    mget.fetchNext(result, null);
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
    verify(result, times(8)).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextNoData() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(946684800 + 900))
        .setEnd(Integer.toString(946684800 + 900 + (3600 * 2)))
        .setExecutionGraph(Collections.emptyList())
        .build();
    when(context.query()).thenReturn(query);
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
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
    verify(result, never()).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextRollup() throws Exception {
    // rollup tables
    setMultiRollupQuery();
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    List<TimeSeries> series = mock(List.class);
    when(series.isEmpty()).thenReturn(false);
    when(result.timeSeries()).thenReturn(series);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
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
    verify(result, times(6)).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextRollupSmallEvenBatch() throws Exception {
    // rollup tables
    setMultiRollupQuery();
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    List<TimeSeries> series = mock(List.class);
    when(series.isEmpty()).thenReturn(false);
    when(result.timeSeries()).thenReturn(series);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 2);
    mget.fetchNext(result, null);
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
    verify(result, times(6)).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextRollupSmallOddBatch() throws Exception {
    // rollup tables
    setMultiRollupQuery();
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    List<TimeSeries> series = mock(List.class);
    when(series.isEmpty()).thenReturn(false);
    when(result.timeSeries()).thenReturn(series);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 3);
    mget.fetchNext(result, null);
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
    verify(result, times(6)).decode(any(ArrayList.class), any(RollupInterval.class));
  }

  @Test
  public void fetchNextRollupFallbackThenFindsData() throws Exception {
    // rollup tables
    setMultiRollupQuery(false, TS_DOUBLE_SERIES);
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    List<TimeSeries> series = mock(List.class);
    when(series.isEmpty())
      .thenReturn(true)
      .thenReturn(false);
    when(result.timeSeries()).thenReturn(series);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
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
    verify(result, times(64)).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextRollupFallbackThenFindsNoData() throws Exception {
    // rollup tables
    setMultiRollupQuery(false, 946684800);
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
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
    verify(result, never()).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextRollupNoFallback() throws Exception {
    // rollup tables
    setMultiRollupQuery(false, 946684800);
    when(node.sentData()).thenReturn(false);
    when(node.rollupUsage()).thenReturn(RollupUsage.ROLLUP_NOFALLBACK);
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
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
    verify(result, never()).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextErrorFromStorage() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(TS_MULTI_SERIES_EX + 900))
        .setEnd(Integer.toString(TS_MULTI_SERIES_EX + 900 + (3600 * TS_MULTI_SERIES_EX_COUNT)))
        .setExecutionGraph(Collections.emptyList())
        .build();
    when(context.query()).thenReturn(query);
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
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
    verify(result, times(28)).decode(any(ArrayList.class), any(RollupInterval.class));
    verify(node, never()).onError(any(UnitTestException.class));
    verify(result, times(1)).setException(any(UnitTestException.class));
  }
  
  @Test
  public void fetchNextTimedSalt() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    when(node.pipelineContext()).thenReturn(context);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(false);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    // NOTE: Since we don't mock out schema.prefixKeyWithSalt() the requested salts are 0.
    when(node.schema()).thenReturn(schema);
    Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
    
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
    verify(result, never()).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void fetchNextTimelessSalt() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    when(node.pipelineContext()).thenReturn(context);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(true);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    // NOTE: Since we don't mock out schema.setBaseTime() the requested timestamps are 0.
    when(node.schema()).thenReturn(schema);
    Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
    
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
    verify(result, never()).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void nextBatchClosed() throws Exception {
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    when(node.pipelineContext().queryContext().isClosed())
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(false)
      .thenReturn(true);
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
    assertEquals(1, storage.getMultiGets().size());
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
    
    assertFalse(mget.all_batches_sent.get());
    assertEquals(State.EXCEPTION, mget.state());
    verify(result, times(4)).decode(any(ArrayList.class), any(RollupInterval.class));
  }
  
  @Test
  public void close() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    when(tsdb.getMaintenanceTimer()).thenReturn(mock(HashedWheelTimer.class));
    when(context.tsdb()).thenReturn(tsdb);
    final Tsdb1xQueryResult result = mock(Tsdb1xQueryResult.class);
    when(result.isFull()).thenReturn(true);
    
    Tsdb1xMultiGet mget = new Tsdb1xMultiGet();
    mget.reset(node, source_config, tsuids);
    mget.fetchNext(result, null);
    
    assertEquals(END_TS - 900 + 3600, mget.timestamp.epoch());
    assertEquals(END_TS, mget.end_timestamp.epoch());
    assertSame(tsuids, mget.tsuids);
    assertSame(source_config, mget.source_config);
    assertSame(node, mget.node);
    assertEquals(1, mget.tables.size());
    assertEquals(1, mget.outstanding.get());
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(-1, mget.rollup_index);
    
    mget.outstanding.set(0); // hack it
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
    List<RollupInterval> intervals = Lists.<RollupInterval>newArrayList(RollupInterval.builder()
        .setInterval("1h")
        .setTable("tsdb-rollup-1h")
        .setPreAggregationTable("tsdb-rollup-1h")
        .setRowSpan("1d")
        .build());
    DefaultRollupConfig config = DefaultRollupConfig.newBuilder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .setIntervals(intervals)
        .build();
    for (final RollupInterval interval : intervals) {
      interval.setConfig(rollup_config);
    }
    when(node.rollupIntervals())
      .thenReturn(intervals);
    
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
        .addSummaryAggregation("sum")
        .addSummaryAggregation("count")
        .addRollupInterval("1h")
        .addOverride(Schema.QUERY_REVERSE_KEY, reversed ? "true" : "false")
        .setId("m1")
        .build();
    when(context.query()).thenReturn(query);
  }
}
