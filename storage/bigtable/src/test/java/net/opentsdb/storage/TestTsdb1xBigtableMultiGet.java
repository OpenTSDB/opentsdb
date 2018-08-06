// This file is part of OpenTSDB.
// Copyright (C) 2016-2018  The OpenTSDB Authors.
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
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter.Chain;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.BigtableExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ExecutorService.class, BigtableSession.class, 
  Tsdb1xBigtableQueryNode.class, CredentialOptions.class,
  Tsdb1xBigtableScanners.class, ResultScanner.class })
public class TestTsdb1xBigtableMultiGet extends UTBase {

  // GMT: Monday, January 1, 2018 12:15:00 AM
  public static final int START_TS = 1514765700;
  
  // GMT: Monday, January 1, 2018 1:15:00 AM
  public static final int END_TS = 1514769300;
  
  public static final TimeStamp BASE_TS = new MillisecondTimeStamp(0L);
  
  public Tsdb1xBigtableQueryNode node;
  public QuerySourceConfig source_config;
  public DefaultRollupConfig rollup_config;
  public QueryPipelineContext context;
  public List<byte[]> tsuids;
  public SemanticQuery query;
  
  @Before
  public void before() throws Exception {
    node = mock(Tsdb1xBigtableQueryNode.class);
    context = mock(QueryPipelineContext.class);
    when(node.schema()).thenReturn(schema);
    when(node.pipelineContext()).thenReturn(context);
    when(node.parent()).thenReturn(data_store);
    when(node.fetchDataType(any(byte.class))).thenReturn(true);
    rollup_config = mock(DefaultRollupConfig.class);
    when(schema.rollupConfig()).thenReturn(rollup_config);
    when(context.upstreamOfType(any(QueryNode.class), any()))
      .thenReturn(Collections.emptyList());
    
    PowerMockito.whenNew(Tsdb1xBigtableScanner.class).withAnyArguments()
      .thenAnswer(new Answer<Tsdb1xBigtableScanner>() {
        @Override
        public Tsdb1xBigtableScanner answer(InvocationOnMock invocation)
            throws Throwable {
          return mock(Tsdb1xBigtableScanner.class);
        }
      });
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setId("m1")
        .build();
    
    when(data_store.dynamicString(Tsdb1xBigtableDataStore.ROLLUP_USAGE_KEY)).thenReturn("Rollup_Fallback");
    when(data_store.dynamicInt(Tsdb1xBigtableDataStore.MULTI_GET_CONCURRENT_KEY)).thenReturn(2);
    when(data_store.dynamicInt(Tsdb1xBigtableDataStore.MULTI_GET_BATCH_KEY)).thenReturn(4);
    
    when(rollup_config.getIdForAggregator("sum")).thenReturn(1);
    when(rollup_config.getIdForAggregator("count")).thenReturn(2);
    
    tsuids = Lists.newArrayList();
    // out of order!
    tsuids.add(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_BYTES));
    tsuids.add(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_B_BYTES));
    tsuids.add(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_BYTES));
    tsuids.add(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_B_BYTES));
  }
  
  @Test
  public void ctorDefaults() throws Exception {
    try {
      new Tsdb1xBigtableMultiGet(null, source_config, tsuids);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xBigtableMultiGet(node, null, tsuids);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xBigtableMultiGet(node, source_config, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xBigtableMultiGet(node, source_config, Lists.newArrayList());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertSame(node, mget.node);
    assertSame(source_config, mget.source_config);
    assertSame(tsuids, mget.tsuids);
    assertEquals(2, mget.concurrency_multi_get);
    assertFalse(mget.reversed);
    assertEquals(4, mget.batch_size);
    assertArrayEquals(Tsdb1xBigtableDataStore.DATA_FAMILY, 
        mget.filter.getFamilyNameRegexFilterBytes().toByteArray());
    assertFalse(mget.rollups_enabled);
    assertFalse(mget.pre_aggregate);
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    assertEquals(-1, mget.rollup_index);
    assertEquals(1, mget.tables.size());
    assertArrayEquals(MockBigtable.DATA_TABLE, mget.tables.get(0));
    assertEquals(0, mget.outstanding);
    assertFalse(mget.has_failed);
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
  }
  
  @Test
  public void ctorQueryOverrides() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addOverride(Tsdb1xBigtableDataStore.PRE_AGG_KEY, "true")
        .addOverride(Tsdb1xBigtableDataStore.MULTI_GET_CONCURRENT_KEY, "8")
        .addOverride(Tsdb1xBigtableDataStore.MULTI_GET_BATCH_KEY, "16")
        .addOverride(Schema.QUERY_REVERSE_KEY, "true")
        .setId("m1")
        .build();
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertSame(node, mget.node);
    assertSame(source_config, mget.source_config);
    assertSame(tsuids, mget.tsuids);
    assertEquals(8, mget.concurrency_multi_get);
    assertTrue(mget.reversed);
    assertEquals(16, mget.batch_size);
    assertArrayEquals(Tsdb1xBigtableDataStore.DATA_FAMILY, 
        mget.filter.getFamilyNameRegexFilterBytes().toByteArray());
    assertFalse(mget.rollups_enabled);
    assertTrue(mget.pre_aggregate);
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    assertEquals(-1, mget.rollup_index);
    assertEquals(1, mget.tables.size());
    assertArrayEquals(MockBigtable.DATA_TABLE, mget.tables.get(0));
    assertEquals(0, mget.outstanding);
    assertFalse(mget.has_failed);
    assertNull(mget.current_result);
    assertEquals(State.CONTINUE, mget.state());
  }

  @Test
  public void ctorRollups() throws Exception {
    when(node.downsampleConfig()).thenReturn(
        (DownsampleConfig) DownsampleConfig.newBuilder()
        .setId("ds")
        .setInterval("1h")
        .setAggregator("avg")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setId("interp")
            .setType(NumericType.TYPE.toString())
            .build())
        .build());
    when(node.rollupAggregation()).thenReturn("avg");
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

    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertTrue(mget.rollups_enabled);
    assertFalse(mget.pre_aggregate);
    assertEquals(3, mget.tables.size());
    assertArrayEquals(table_namer.toTableNameStr("tsdb-1h").getBytes(), 
        mget.tables.get(0));
    assertArrayEquals(table_namer.toTableNameStr("tsdb-30m").getBytes(), 
        mget.tables.get(1));
    assertArrayEquals(MockBigtable.DATA_TABLE, mget.tables.get(2));
    assertEquals(0, mget.rollup_index);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    assertEquals(State.CONTINUE, mget.state());
    Chain chain = mget.filter.getChain();
    assertEquals(2, chain.getFiltersCount());
    assertArrayEquals(Tsdb1xBigtableDataStore.DATA_FAMILY, 
        chain.getFilters(0).getFamilyNameRegexFilterBytes().toByteArray());
    Interleave interleave = chain.getFilters(1).getInterleave(); 
    assertEquals(4, interleave.getFiltersCount());
    assertArrayEquals("sum".getBytes(), 
        interleave.getFilters(0).getColumnQualifierRegexFilter().toByteArray());
    assertArrayEquals("count".getBytes(), 
        interleave.getFilters(1).getColumnQualifierRegexFilter().toByteArray());
    assertArrayEquals(new byte[] { 1 }, 
        interleave.getFilters(2).getColumnQualifierRegexFilter().toByteArray());
    assertArrayEquals(new byte[] { 2 }, 
        interleave.getFilters(3).getColumnQualifierRegexFilter().toByteArray());
    
    // pre-agg
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addOverride(Tsdb1xBigtableDataStore.PRE_AGG_KEY, "true")
        .setId("m1")
        .build();
    
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertTrue(mget.rollups_enabled);
    assertTrue(mget.pre_aggregate);
    assertEquals(3, mget.tables.size());
    assertArrayEquals(table_namer.toTableNameStr("tsdb-agg-1h").getBytes(), 
        mget.tables.get(0));
    assertArrayEquals(table_namer.toTableNameStr("tsdb-agg-30m").getBytes(), 
        mget.tables.get(1));
    assertArrayEquals(MockBigtable.DATA_TABLE, mget.tables.get(2));
    assertEquals(0, mget.rollup_index);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    assertEquals(State.CONTINUE, mget.state());
    chain = mget.filter.getChain();
    assertEquals(2, chain.getFiltersCount());
    assertArrayEquals(Tsdb1xBigtableDataStore.DATA_FAMILY, 
        chain.getFilters(0).getFamilyNameRegexFilterBytes().toByteArray());
    interleave = chain.getFilters(1).getInterleave(); 
    assertEquals(4, interleave.getFiltersCount());
    assertArrayEquals("sum".getBytes(), 
        interleave.getFilters(0).getColumnQualifierRegexFilter().toByteArray());
    assertArrayEquals("count".getBytes(), 
        interleave.getFilters(1).getColumnQualifierRegexFilter().toByteArray());
    assertArrayEquals(new byte[] { 1 }, 
        interleave.getFilters(2).getColumnQualifierRegexFilter().toByteArray());
    assertArrayEquals(new byte[] { 2 }, 
        interleave.getFilters(3).getColumnQualifierRegexFilter().toByteArray());
    
    // sum
    when(node.downsampleConfig()).thenReturn(
        (DownsampleConfig) DownsampleConfig.newBuilder()
        .setId("ds")
        .setInterval("1h")
        .setAggregator("sum")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setId("interp")
            .setType(NumericType.TYPE.toString())
            .build())
        .build());
    when(node.rollupAggregation()).thenReturn("sum");
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setId("m1")
        .build();
    
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertTrue(mget.rollups_enabled);
    assertFalse(mget.pre_aggregate);
    assertEquals(3, mget.tables.size());
    assertArrayEquals(table_namer.toTableNameStr("tsdb-1h").getBytes(), 
        mget.tables.get(0));
    assertArrayEquals(table_namer.toTableNameStr("tsdb-30m").getBytes(), 
        mget.tables.get(1));
    assertArrayEquals(MockBigtable.DATA_TABLE, mget.tables.get(2));
    assertEquals(0, mget.rollup_index);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    assertEquals(State.CONTINUE, mget.state());
    chain = mget.filter.getChain();
    assertEquals(2, chain.getFiltersCount());
    assertArrayEquals(Tsdb1xBigtableDataStore.DATA_FAMILY, 
        chain.getFilters(0).getFamilyNameRegexFilterBytes().toByteArray());
    interleave = chain.getFilters(1).getInterleave(); 
    assertEquals(2, interleave.getFiltersCount());
    assertArrayEquals("sum".getBytes(), 
        interleave.getFilters(0).getColumnQualifierRegexFilter().toByteArray());
    assertArrayEquals(new byte[] { 1 }, 
        interleave.getFilters(1).getColumnQualifierRegexFilter().toByteArray());
    
    // no fallback (still populates all the tables since it's small)
    when(node.rollupUsage()).thenReturn(RollupUsage.ROLLUP_NOFALLBACK);
    
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertTrue(mget.rollups_enabled);
    assertFalse(mget.pre_aggregate);
    assertEquals(3, mget.tables.size());
    assertArrayEquals(table_namer.toTableNameStr("tsdb-1h").getBytes(), 
        mget.tables.get(0));
    assertArrayEquals(table_namer.toTableNameStr("tsdb-30m").getBytes(), 
        mget.tables.get(1));
    assertArrayEquals(MockBigtable.DATA_TABLE, mget.tables.get(2));
    assertEquals(0, mget.rollup_index);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    assertEquals(State.CONTINUE, mget.state());
    chain = mget.filter.getChain();
    assertEquals(2, chain.getFiltersCount());
    assertArrayEquals(Tsdb1xBigtableDataStore.DATA_FAMILY, 
        chain.getFilters(0).getFamilyNameRegexFilterBytes().toByteArray());
    interleave = chain.getFilters(1).getInterleave(); 
    assertEquals(2, interleave.getFiltersCount());
    assertArrayEquals("sum".getBytes(), 
        interleave.getFilters(0).getColumnQualifierRegexFilter().toByteArray());
    assertArrayEquals(new byte[] { 1 }, 
        interleave.getFilters(1).getColumnQualifierRegexFilter().toByteArray());
  }

  @Test
  public void ctoreTimestamps() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(END_TS))
        .setEnd(Integer.toString(END_TS + 3600))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    
    // downsample 2 hours
    when(node.downsampleConfig()).thenReturn(
        (DownsampleConfig) DownsampleConfig.newBuilder()
        .setId("ds")
        .setInterval("2h")
        .setAggregator("max")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setId("interp")
            .setType(NumericType.TYPE.toString())
            .build())
        .build());
    when(node.rollupAggregation()).thenReturn("max");
    
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
  }

  @Test
  public void ctorTimedSalt() throws Exception {
    node = mock(Tsdb1xBigtableQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(false);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    when(node.schema()).thenReturn(schema);
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    
    assertArrayEquals(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_BYTES), 
        mget.tsuids.get(0));
    assertArrayEquals(Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_B_BYTES), 
        mget.tsuids.get(1));
    assertArrayEquals(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_BYTES), 
        mget.tsuids.get(2));
    assertArrayEquals(Bytes.concat(METRIC_B_BYTES, TAGK_BYTES, TAGV_B_BYTES), 
        mget.tsuids.get(3));
  }
  
  @Test
  public void ctorTimelessSalt() throws Exception {
    node = mock(Tsdb1xBigtableQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(true);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    when(node.schema()).thenReturn(schema);
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    
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
  }
  
  @Test
  public void advanceNoRollups() throws Exception {
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    
    // first run
    assertFalse(mget.advance());
    assertEquals(0, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    
    // second run increments timestamp
    assertFalse(mget.advance());
    assertEquals(0, mget.tsuid_idx);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    
    // nothing left
    assertTrue(mget.advance());
    assertEquals(0, mget.tsuid_idx);
    assertEquals(END_TS + 3600 - 900, mget.timestamp.epoch());
    
    // sequence end
    when(node.sequenceEnd()).thenReturn(
        new MillisecondTimeStamp((START_TS - 900) * 1000L));
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    
    assertFalse(mget.advance());
    assertEquals(0, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    
    // end of sequence so it resets the TSUID idx.
    assertTrue(mget.advance());
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    
    // no-op since the sequence hasn't changed.
    assertTrue(mget.advance());
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    
    // resume
    when(node.sequenceEnd()).thenReturn(
        new MillisecondTimeStamp((END_TS - 900) * 1000L));
    assertFalse(mget.advance());
    assertEquals(0, mget.tsuid_idx);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    
    // nothing left
    assertTrue(mget.advance());
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(END_TS + 3600 - 900, mget.timestamp.epoch());
    
    // node should finish here but just in case....
    when(node.sequenceEnd()).thenReturn(
        new MillisecondTimeStamp((END_TS + 3600 - 900) * 1000L));
    assertTrue(mget.advance());
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(END_TS + 3600 - 900, mget.timestamp.epoch());
    
    // previous tests had a batch size matching the tsuids size. Now
    // we verify odd offsets.
    when(node.sequenceEnd()).thenReturn(null);
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 3);
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    
    // first run
    assertFalse(mget.advance());
    assertEquals(0, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    
    // second run hast more TSUIDs
    assertFalse(mget.advance());
    assertEquals(3, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    
    // now we hit the last tsuid, so increment timestamp
    assertFalse(mget.advance());
    assertEquals(0, mget.tsuid_idx);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    
    // next set of tsuids
    assertFalse(mget.advance());
    assertEquals(3, mget.tsuid_idx);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    
    // all done
    assertTrue(mget.advance());
    assertEquals(0, mget.tsuid_idx);
    assertEquals(END_TS + 3600 - 900, mget.timestamp.epoch());
  }

  @Test
  public void advanceRollups() throws Exception {
    setMultiRollupQuery();
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertTrue(mget.rollups_enabled);
    assertEquals(0, mget.rollup_index);
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    assertFalse(mget.advance());
    assertEquals(0, mget.rollup_index);
    assertEquals(0, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    assertTrue(mget.advance());
    assertEquals(0, mget.rollup_index);
    assertEquals(0, mget.tsuid_idx);
    assertEquals(START_TS + 86400 - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    // previous tests had a batch size matching the tsuids size. Now
    // we verify odd offsets.
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 3);
    
    assertTrue(mget.rollups_enabled);
    assertEquals(0, mget.rollup_index);
    assertEquals(-1, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    assertFalse(mget.advance());
    assertEquals(0, mget.rollup_index);
    assertEquals(0, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    assertFalse(mget.advance());
    assertEquals(0, mget.rollup_index);
    assertEquals(3, mget.tsuid_idx);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    assertTrue(mget.advance());
    assertEquals(0, mget.rollup_index);
    assertEquals(0, mget.tsuid_idx);
    assertEquals(START_TS + 86400 - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
  }
  
  @Test
  public void incrementTimeStampRollups() throws Exception {
    setMultiRollupQuery();
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    mget.incrementTimestamp();
    assertEquals(START_TS + (86400) - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    // fallback resets to the original
    mget.rollup_index = 1;
    mget.incrementTimestamp();
    assertEquals(START_TS + (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS - 900, mget.fallback_timestamp.epoch());
    
    // now we increment just the fallback timestamp
    mget.incrementTimestamp();
    assertEquals(START_TS + (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS + (3600 * 6) - 900, mget.fallback_timestamp.epoch());
    
    mget.incrementTimestamp();
    assertEquals(START_TS + (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS + (3600 * 12) - 900, mget.fallback_timestamp.epoch());
    
    // fallback to raw now. The onComplete() method has null the fallback timestamp
    mget.fallback_timestamp = null;
    mget.rollup_index = 2;
    mget.incrementTimestamp();
    assertEquals(START_TS + (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS - 900, mget.fallback_timestamp.epoch());
    
    // increment
    mget.incrementTimestamp();
    assertEquals(START_TS + (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS + 3600 - 900, mget.fallback_timestamp.epoch());
    
    // increment
    mget.incrementTimestamp();
    assertEquals(START_TS + (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS + (3600 * 2) - 900, mget.fallback_timestamp.epoch());
  }
  
  @Test
  public void incrementTimeStampReversed() throws Exception {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addOverride(Schema.QUERY_REVERSE_KEY, "true")
        .setId("m1")
        .build();
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    mget.incrementTimestamp();
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    mget.incrementTimestamp();
    assertEquals(START_TS - (3600) - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
  }
  
  @Test
  public void incrementTimeStampRollupsReversed() throws Exception {
    setMultiRollupQuery(true);
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertEquals(START_TS - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    mget.incrementTimestamp();
    assertEquals(START_TS - (86400) - 900, mget.timestamp.epoch());
    assertNull(mget.fallback_timestamp);
    
    // fallback resets to the original
    mget.rollup_index = 1;
    mget.incrementTimestamp();
    assertEquals(START_TS - (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS - 900, mget.fallback_timestamp.epoch());
    
    // now we increment just the fallback timestamp
    mget.incrementTimestamp();
    assertEquals(START_TS - (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS - (3600 * 6) - 900, mget.fallback_timestamp.epoch());
    
    mget.incrementTimestamp();
    assertEquals(START_TS - (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS - (3600 * 12) - 900, mget.fallback_timestamp.epoch());
    
    // fallback to raw now. The onComplete() method has null the fallback timestamp
    mget.fallback_timestamp = null;
    mget.rollup_index = 2;
    mget.incrementTimestamp();
    assertEquals(START_TS - (86400) - 900, mget.timestamp.epoch());
    assertEquals(END_TS - 900, mget.fallback_timestamp.epoch());
    
    // increment
    mget.incrementTimestamp();
    assertEquals(START_TS - (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS - 900, mget.fallback_timestamp.epoch());
    
    // increment
    mget.incrementTimestamp();
    assertEquals(START_TS - (86400) - 900, mget.timestamp.epoch());
    assertEquals(START_TS - 3600 - 900, mget.fallback_timestamp.epoch());
  }
  
  @Test
  public void nextBatch() throws Exception {
    storage.clearMultiGets();
    final Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    when(result.isFull()).thenReturn(true);
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    mget.nextBatch(0, START_TS, null);
    assertEquals(4, storage.getLastMultiGets().getRows().getRowKeysCount());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(0).toByteArray());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(1).toByteArray());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(2).toByteArray());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(3).toByteArray());
    assertArrayEquals(MockBigtable.DATA_TABLE, 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
    
    // smaller batch size
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    Whitebox.setInternalState(mget, "batch_size", 3);
    mget.nextBatch(0, START_TS, null);
    assertEquals(3, storage.getLastMultiGets().getRows().getRowKeysCount());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(0).toByteArray());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(1).toByteArray());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(2).toByteArray());
    assertArrayEquals(MockBigtable.DATA_TABLE, 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
    
    mget.current_result = result; // suppress exceptions
    mget.nextBatch(3, START_TS, null);
   
    assertEquals(1, storage.getLastMultiGets().getRows().getRowKeysCount());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(0).toByteArray());
    assertArrayEquals(MockBigtable.DATA_TABLE, 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
    
    // rollup tables
    setMultiRollupQuery();
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    mget.current_result = result;
    mget.nextBatch(0, START_TS, null);
    assertEquals(4, storage.getLastMultiGets().getRows().getRowKeysCount());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(0).toByteArray());
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(1).toByteArray());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(2).toByteArray());
    assertArrayEquals(makeRowKey(METRIC_B_BYTES, START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(3).toByteArray());
    assertArrayEquals(mget.tables.get(0), 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
    
    mget.rollup_index = 1;
    mget.nextBatch(0, START_TS, null);
    assertEquals(4, storage.getLastMultiGets().getRows().getRowKeysCount());
    assertArrayEquals(mget.tables.get(1), 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
    
    mget.rollup_index = 2;
    mget.nextBatch(0, START_TS, null);
    assertEquals(4, storage.getLastMultiGets().getRows().getRowKeysCount());
    assertArrayEquals(MockBigtable.DATA_TABLE, 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
    
    // salting
    node = mock(Tsdb1xBigtableQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    Schema schema = mock(Schema.class);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    when(node.schema()).thenReturn(schema);
    mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    mget.current_result = result;
    mget.nextBatch(0, START_TS, null);
    assertEquals(4, storage.getLastMultiGets().getRows().getRowKeysCount());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(0).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(1).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(2).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(3).toByteArray());
    assertArrayEquals(MockBigtable.DATA_TABLE, 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
  }
  
  @Test
  public void nextBatchTimedSalt() throws Exception {
    node = mock(Tsdb1xBigtableQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(false);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    when(node.schema()).thenReturn(schema);
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    
    mget.nextBatch(0, START_TS, null);
    assertEquals(4, storage.getLastMultiGets().getRows().getRowKeysCount());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(0).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(1).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), START_TS, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(2).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), START_TS, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(3).toByteArray());
    assertArrayEquals(MockBigtable.DATA_TABLE, 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
  }
  
  @Test
  public void nextBatchTimelessSalt() throws Exception {
    node = mock(Tsdb1xBigtableQueryNode.class);
    when(node.parent()).thenReturn(data_store);
    Schema schema = mock(Schema.class);
    when(schema.timelessSalting()).thenReturn(true);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    when(node.schema()).thenReturn(schema);
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    
    mget.nextBatch(0, START_TS, null);
    assertEquals(4, storage.getLastMultiGets().getRows().getRowKeysCount());
    // time is 0 since we haven't mocked out the schema.setBaseTime() method
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), 0, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(0).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_BYTES), 0, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(1).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), 0, TAGK_BYTES, TAGV_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(2).toByteArray());
    assertArrayEquals(makeRowKey(Bytes.concat(new byte[1], METRIC_B_BYTES), 0, TAGK_BYTES, TAGV_B_BYTES), 
        storage.getLastMultiGets().getRows().getRowKeys(3).toByteArray());
    assertArrayEquals(MockBigtable.DATA_TABLE, 
        storage.getLastMultiGets().getTableNameBytes().toByteArray());
    assertSame(mget.filter, storage.getLastMultiGets().getFilter());
  }
  
  @Test
  public void onError() throws Exception {
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    assertFalse(mget.has_failed);
    verify(node, never()).onError(any(Throwable.class));
    
    mget.new ResponseCB(null).onFailure(new UnitTestException());
    assertTrue(mget.has_failed);
    verify(node, times(1)).onError(any(Throwable.class));
    
    mget.new ResponseCB(null).onFailure(new UnitTestException());
    assertTrue(mget.has_failed);
    verify(node, times(1)).onError(any(Throwable.class));
  }
  
  @Test
  public void responseCB() throws Exception {
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    when(result.isFull()).thenReturn(true);
    mget.current_result = result;
    mget.outstanding = 1;
    
    List<Row> results = Lists.newArrayList();
    Row row = Row.newBuilder()
        .setKey(ByteStringer.wrap(makeRowKey(METRIC_BYTES, START_TS, TAGK_BYTES, TAGV_BYTES)))
        .addFamilies(Family.newBuilder()
            .setNameBytes(ByteStringer.wrap(Tsdb1xBigtableDataStore.DATA_FAMILY))
            .addColumns(Column.newBuilder()
                .setQualifier(ByteStringer.wrap(new byte[] { 0, 0 }))
                .addCells(Cell.newBuilder()
                    .setValue(ByteStringer.wrap(new byte[] { 1 })))))
        .build();
    results.add(row);
    Row.newBuilder()
        .setKey(ByteStringer.wrap(makeRowKey(METRIC_BYTES, START_TS, TAGK_BYTES, TAGV_B_BYTES)))
        .addFamilies(Family.newBuilder()
            .setNameBytes(ByteStringer.wrap(Tsdb1xBigtableDataStore.DATA_FAMILY))
            .addColumns(Column.newBuilder()
                .setQualifier(ByteStringer.wrap(new byte[] { 0, 0 }))
                .addCells(Cell.newBuilder()
                    .setValue(ByteStringer.wrap(new byte[] { 1 })))))
        .build();
    results.add(row);
    
    mget.new ResponseCB(null).onSuccess(results);
    assertEquals(0, mget.outstanding);
    verify(result, times(1)).isFull();
    verify(node, never()).onError(any(Throwable.class));
    
    // empty results
    mget.current_result = result;
    mget.outstanding = 1;
    results.clear();
    
    mget.new ResponseCB(null).onSuccess(results);
    assertEquals(0, mget.outstanding);
    verify(result, times(2)).isFull();
    verify(node, never()).onError(any(Throwable.class));
  }

  @Test
  public void onCompleteFull() throws Exception {
    int gets = storage.getMultiGets().size();
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    when(result.isFull()).thenReturn(true);
    mget.current_result = result;
    mget.outstanding = 1;
    
    // full some outstanding
    mget.onComplete();
    assertEquals(State.CONTINUE, mget.state());
    assertSame(result, mget.current_result);
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    assertEquals(gets, storage.getMultiGets().size());
    
    // all done
    mget.outstanding = 0;
    mget.onComplete();
    assertEquals(State.COMPLETE, mget.state());
    assertNull(mget.current_result);
    verify(node, times(1)).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    assertEquals(gets, storage.getMultiGets().size());
  }
  
  @Test
  public void onCompleteBusy() throws Exception {
    int gets = storage.getMultiGets().size();
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    mget.current_result = result;
    mget.outstanding = 2;
    
    // full some outstanding
    mget.onComplete();
    assertEquals(State.CONTINUE, mget.state());
    assertSame(result, mget.current_result);
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    assertEquals(gets, storage.getMultiGets().size());
  }
  
  @Test
  public void onCompleteNextBatch() throws Exception {
    Tsdb1xBigtableMultiGet mget = spy(new Tsdb1xBigtableMultiGet(node, source_config, tsuids));
    doNothing().when(mget).nextBatch(anyInt(), anyInt(), any(Span.class));
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    mget.current_result = result;
    mget.outstanding = 0;
    
    // fire away
    mget.onComplete();
    assertEquals(State.CONTINUE, mget.state());
    assertSame(result, mget.current_result);
    assertEquals(1, mget.outstanding);
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(1)).nextBatch(0, START_TS - 900, null);
    
    mget.onComplete();
    assertEquals(State.CONTINUE, mget.state());
    assertSame(result, mget.current_result);
    assertEquals(2, mget.outstanding);
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(1)).nextBatch(0, END_TS - 900, null);
    
    // busy
    mget.onComplete();
    assertEquals(State.CONTINUE, mget.state());
    assertSame(result, mget.current_result);
    assertEquals(2, mget.outstanding);
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(1)).nextBatch(0, END_TS - 900, null);
    
    // nothing left to do
    mget.outstanding = 0;
    mget.onComplete();
    assertEquals(State.COMPLETE, mget.state());
    assertNull(mget.current_result);
    verify(node, times(1)).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(1)).nextBatch(0, END_TS - 900, null);
  }
  
  @Test
  public void onCompleteFallback() throws Exception {
    setMultiRollupQuery();
    
    Tsdb1xBigtableMultiGet mget = spy(new Tsdb1xBigtableMultiGet(node, source_config, tsuids));
    doNothing().when(mget).nextBatch(anyInt(), anyInt(), any(Span.class));
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    mget.current_result = result;
    mget.outstanding = 0;
    mget.timestamp = new MillisecondTimeStamp((END_TS + 3600 - 900) * 1000L);
    assertEquals(0, mget.rollup_index);
    
    // fires off up to concurrency_multi_get gets
    mget.onComplete();
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(1)).nextBatch(0, START_TS - 900, null);
    verify(mget, never()).nextBatch(0, START_TS  + (3600 * 6) - 900, null);
    assertEquals(START_TS  + (3600 * 6) - 900, mget.fallback_timestamp.epoch());
    assertEquals(1, mget.rollup_index);
    
    // should fallback to raw now
    mget.outstanding = 0;
    mget.onComplete();
    assertEquals(State.CONTINUE, mget.state());
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(2)).nextBatch(0, START_TS - 900, null);
    verify(mget, never()).nextBatch(0, START_TS  + (3600 * 6) - 900, null);
    assertEquals(END_TS  - 900, mget.fallback_timestamp.epoch());
    assertEquals(2, mget.rollup_index);
  }
  
  @Test
  public void onCompleteFallbackRaw() throws Exception {
    setMultiRollupQuery();
    when(node.rollupUsage()).thenReturn(RollupUsage.ROLLUP_FALLBACK_RAW);
    
    Tsdb1xBigtableMultiGet mget = spy(new Tsdb1xBigtableMultiGet(node, source_config, tsuids));
    doNothing().when(mget).nextBatch(anyInt(), anyInt(), any(Span.class));
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    mget.current_result = result;
    mget.outstanding = 0;
    mget.timestamp = new MillisecondTimeStamp((END_TS + 3600 - 900) * 1000L);
    assertEquals(0, mget.rollup_index);

    // should fallback to raw now
    mget.outstanding = 0;
    mget.onComplete();
    assertEquals(State.CONTINUE, mget.state());
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(1)).nextBatch(0, START_TS - 900, null);
    verify(mget, never()).nextBatch(0, START_TS  + (3600 * 6) - 900, null);
    assertEquals(END_TS  - 900, mget.fallback_timestamp.epoch());
    assertEquals(2, mget.rollup_index);
  }
  
  @Test
  public void onCompleteNoFallback() throws Exception {
    setMultiRollupQuery();
    when(node.rollupUsage()).thenReturn(RollupUsage.ROLLUP_NOFALLBACK);
    
    Tsdb1xBigtableMultiGet mget = spy(new Tsdb1xBigtableMultiGet(node, source_config, tsuids));
    doNothing().when(mget).nextBatch(anyInt(), anyInt(), any(Span.class));
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    mget.current_result = result;
    mget.outstanding = 0;
    mget.timestamp = new MillisecondTimeStamp((END_TS + 3600 - 900) * 1000L);
    assertEquals(0, mget.rollup_index);

    // should fallback to raw now
    mget.outstanding = 0;
    mget.onComplete();
    assertEquals(State.COMPLETE, mget.state());
    verify(node, times(1)).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, never()).nextBatch(0, START_TS - 900, null);
    verify(mget, never()).nextBatch(0, START_TS  + (3600 * 6) - 900, null);
    assertNull(mget.fallback_timestamp);
    assertEquals(0, mget.rollup_index);
  }
  
  @Test
  public void fetchNext() throws Exception {
    Tsdb1xBigtableMultiGet mget = spy(new Tsdb1xBigtableMultiGet(node, source_config, tsuids));
    doNothing().when(mget).nextBatch(anyInt(), anyInt(), any(Span.class));
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    
    mget.fetchNext(result, null);
    assertEquals(State.CONTINUE, mget.state());
    assertEquals(2, mget.outstanding);
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(1)).nextBatch(0, START_TS - 900, null);
    verify(mget, times(1)).nextBatch(0, END_TS - 900, null);
    assertEquals(END_TS - 900, mget.timestamp.epoch());
    
    try {
      mget.fetchNext(mock(Tsdb1xBigtableQueryResult.class), null);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // all done, nothing left
    mget.current_result = null;
    mget.outstanding = 0;
    mget.fetchNext(result, null);
    assertEquals(State.CONTINUE, mget.state());
    verify(node, times(1)).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(mget, times(1)).nextBatch(0, START_TS - 900, null);
    verify(mget, times(1)).nextBatch(0, END_TS - 900, null);
    verify(mget, never()).nextBatch(0, END_TS + 3600 - 900, null);
    assertEquals(END_TS + 3600 - 900, mget.timestamp.epoch());
  }
  
  @Test
  public void fetchNextRealTraced() throws Exception {
    trace = new MockTrace(true);
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(TS_SINGLE_SERIES))
        .setEnd(Integer.toString(TS_SINGLE_SERIES + 
            (TS_SINGLE_SERIES_COUNT * TS_SINGLE_SERIES_INTERVAL)))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addOverride(Schema.QUERY_REVERSE_KEY, "true")
        .setId("m1")
        .build();
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    
    mget.fetchNext(result, trace.newSpan("UT").start());
    assertEquals(State.COMPLETE, mget.state());
    assertEquals(0, mget.outstanding);
    verify(node, times(1)).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, never()).onError(any(Throwable.class));
    verify(result, times(32)).decode(any(Row.class), 
        any(RollupInterval.class));
    verifySpan(Tsdb1xBigtableMultiGet.class.getName() + ".fetchNext", 18);
  }

  @Test
  public void fetchNextRealException() throws Exception {
    trace = new MockTrace(true);
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(TS_MULTI_SERIES_EX))
        .setEnd(Integer.toString(TS_MULTI_SERIES_EX + 
            (TS_MULTI_SERIES_EX_COUNT * TS_MULTI_SERIES_INTERVAL)))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xBigtableMultiGet mget = new Tsdb1xBigtableMultiGet(node, source_config, tsuids);
    Tsdb1xBigtableQueryResult result = mock(Tsdb1xBigtableQueryResult.class);
    
    mget.fetchNext(result, trace.newSpan("UT").start());
    assertEquals(State.EXCEPTION, mget.state());
    assertEquals(0, mget.outstanding);
    verify(node, never()).onNext(result);
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(node, times(1)).onError(any(Throwable.class));
    verify(result, times(28)).decode(any(Row.class), 
        any(RollupInterval.class));
    verifySpan(Tsdb1xBigtableMultiGet.class.getName() + ".fetchNext", 
        ExecutionException.class, 9);
  }
  
  void setMultiRollupQuery() throws Exception {
    setMultiRollupQuery(false);
  }
  
  void setMultiRollupQuery(final boolean reversed) throws Exception {
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
          .setRowSpan("6h")
          .build()));
    
    when(node.downsampleConfig()).thenReturn(
        (DownsampleConfig) DownsampleConfig.newBuilder()
        .setId("ds")
        .setInterval("1h")
        .setAggregator("avg")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setId("interp")
            .setType(NumericType.TYPE.toString())
            .build())
        .build());
    when(node.rollupAggregation()).thenReturn("avg");
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("graph")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource"))
            .build())
        .build();
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addOverride(Schema.QUERY_REVERSE_KEY, reversed ? "true" : "false")
        .setId("m1")
        .build();
  }
}
