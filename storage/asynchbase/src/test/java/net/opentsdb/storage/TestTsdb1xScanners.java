// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.hbase.async.BinaryPrefixComparator;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.FilterList;
import org.hbase.async.FuzzyRowFilter;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.QualifierFilter;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.junit.Before;
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
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.Registry;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.QuerySourceConfig.Builder;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.DefaultNamedFilter;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueLiteralOrFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.query.filter.TagValueWildcardFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.storage.HBaseExecutor.State;
import net.opentsdb.storage.MockBase.MockScanner;
import net.opentsdb.storage.Tsdb1xScanners.FilterCB;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class, Scanner.class, Tsdb1xScanners.class, 
  Tsdb1xScanner.class })
public class TestTsdb1xScanners extends UTBase {

  private Tsdb1xQueryNode node;
  private QuerySourceConfig source_config;
  private DefaultRollupConfig rollup_config;
  private QueryPipelineContext context;
  private SemanticQuery query;
  
  @Before
  public void before() throws Exception {
    node = mock(Tsdb1xQueryNode.class);
    when(node.schema()).thenReturn(schema);
    when(node.parent()).thenReturn(data_store);
    rollup_config = mock(DefaultRollupConfig.class);
    when(schema.rollupConfig()).thenReturn(rollup_config);
    
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
    
    when(data_store.dynamicString(Tsdb1xHBaseDataStore.ROLLUP_USAGE_KEY)).thenReturn("Rollup_Fallback");
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.EXPANSION_LIMIT_KEY)).thenReturn(4096);
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.ROWS_PER_SCAN_KEY)).thenReturn(1024);
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.MAX_MG_CARDINALITY_KEY)).thenReturn(4096);
    
    when(rollup_config.getIdForAggregator("sum")).thenReturn(1);
    when(rollup_config.getIdForAggregator("count")).thenReturn(2);
    
    context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    when(context.upstreamOfType(any(QueryNode.class), any()))
      .thenReturn(Collections.emptyList());
  }
  
  @Test
  public void ctorDefaults() throws Exception {
    try {
      new Tsdb1xScanners(null, source_config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xScanners(node, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    assertSame(node, scanners.node);
    assertSame(source_config, scanners.source_config);
    assertFalse(scanners.pre_aggregate);
    assertFalse(scanners.skip_nsun_tagks);
    assertFalse(scanners.skip_nsun_tagvs);
    assertEquals(4096, scanners.expansion_limit);
    assertEquals(1024, scanners.rows_per_scan);
    assertEquals(4096, scanners.max_multi_get_cardinality);
    assertFalse(scanners.enable_fuzzy_filter);
    assertFalse(scanners.reverse_scan);
    assertFalse(scanners.initialized);
    assertNull(scanners.scanners);
    assertEquals(0, scanners.scanner_index);
    assertNull(scanners.filter_cb);
    assertEquals(0, scanners.scanners_done);
    assertNull(scanners.current_result);
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.has_failed);
    assertEquals(State.CONTINUE, scanners.state());
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
        .addOverride(Tsdb1xHBaseDataStore.EXPANSION_LIMIT_KEY, "128")
        .addOverride(Tsdb1xHBaseDataStore.ROWS_PER_SCAN_KEY, "64")
        .addOverride(Tsdb1xHBaseDataStore.ROLLUP_USAGE_KEY, "ROLLUP_RAW")
        .addOverride(Tsdb1xHBaseDataStore.PRE_AGG_KEY, "true")
        .addOverride(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGK_KEY, "true")
        .addOverride(Tsdb1xHBaseDataStore.SKIP_NSUN_TAGV_KEY, "true")
        .addOverride(Tsdb1xHBaseDataStore.FUZZY_FILTER_KEY, "true")
        .addOverride(Schema.QUERY_REVERSE_KEY, "true")
        .addOverride(Tsdb1xHBaseDataStore.MAX_MG_CARDINALITY_KEY, "36")
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    assertSame(node, scanners.node);
    assertSame(source_config, scanners.source_config);
    assertTrue(scanners.pre_aggregate);
    assertTrue(scanners.skip_nsun_tagks);
    assertTrue(scanners.skip_nsun_tagvs);
    assertEquals(128, scanners.expansion_limit);
    assertEquals(64, scanners.rows_per_scan);
    assertEquals(36, scanners.max_multi_get_cardinality);
    assertTrue(scanners.enable_fuzzy_filter);
    assertTrue(scanners.reverse_scan);
    assertFalse(scanners.initialized);
    assertNull(scanners.scanners);
    assertEquals(0, scanners.scanner_index);
    assertNull(scanners.filter_cb);
    assertEquals(0, scanners.scanners_done);
    assertNull(scanners.current_result);
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.has_failed);
    assertEquals(State.CONTINUE, scanners.state());
  }
  
  @Test
  public void ctorRollups() throws Exception {
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
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    assertEquals(State.CONTINUE, scanners.state());
  }

  @Test
  public void setStartKey() throws Exception {
    RollupInterval interval = RollupInterval.builder()
        .setInterval("1h")
        .setTable("tsdb-1h")
        .setPreAggregationTable("tsdb-1h")
        .setRowSpan("1d")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    byte[] start = scanners.setStartKey(METRIC_BYTES, null, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, null), start);
    
    // with fuzzy
    byte[] fuzzy = Bytes.concat(new byte[3], new byte[4], TAGK_BYTES, new byte[3]);
    start = scanners.setStartKey(METRIC_BYTES, null, fuzzy);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, new byte[3]), start);
    
    // rollup
    start = scanners.setStartKey(METRIC_BYTES, interval, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, null), start);
    
    // rollup further in
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
    scanners = new Tsdb1xScanners(node, source_config);
    start = scanners.setStartKey(METRIC_BYTES, interval, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, null), start);
    
    // rollup with rate on edge
    when(context.upstreamOfType(any(QueryNode.class), any()))
      .thenReturn(Lists.newArrayList(mock(QueryNode.class)));
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS - 900))
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
    scanners = new Tsdb1xScanners(node, source_config);
    start = scanners.setStartKey(METRIC_BYTES, interval, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900 - 86400, null), start);
    
    // downsample
    when(context.upstreamOfType(any(QueryNode.class), any()))
      .thenReturn(Collections.emptyList());
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
    when(node.downsampleConfig()).thenReturn(
        (DownsampleConfig) DownsampleConfig.newBuilder()
        .setId("ds")
        .setInterval("1h")
        .setAggregator("max")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setId("interp")
            .setType(NumericType.TYPE.toString())
            .build())
        .build());
    when(node.rollupAggregation()).thenReturn("max");
    scanners = new Tsdb1xScanners(node, source_config);
    start = scanners.setStartKey(METRIC_BYTES, null, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, END_TS - 900, null), start);
    
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
    scanners = new Tsdb1xScanners(node, source_config);
    start = scanners.setStartKey(METRIC_BYTES, null, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, START_TS - 900, null), start);
  }
  
  @Test
  public void setStopKey() throws Exception {
    RollupInterval interval = RollupInterval.builder()
        .setInterval("1h")
        .setTable("tsdb-1h")
        .setPreAggregationTable("tsdb-1h")
        .setRowSpan("1d")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    byte[] stop = scanners.setStopKey(METRIC_BYTES, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, END_TS + (3600 - 900), null), stop);
    
    // rollup
    stop = scanners.setStopKey(METRIC_BYTES, interval);
    assertArrayEquals(makeRowKey(METRIC_BYTES, 1514851200, null), stop);
    
    // rollup further in
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
    
    scanners = new Tsdb1xScanners(node, source_config);
    stop = scanners.setStopKey(METRIC_BYTES, interval);
    assertArrayEquals(makeRowKey(METRIC_BYTES, 1514851200, null), stop);
    
    // downsample
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
    scanners = new Tsdb1xScanners(node, source_config);
    stop = scanners.setStopKey(METRIC_BYTES, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, (END_TS - 900 + 7200), null), stop);
    
    // downsample 2 hours
    when(node.downsampleConfig()).thenReturn(
        (DownsampleConfig) DownsampleConfig.newBuilder()
        .setId("ds")
        .setInterval("2h")
        .setAggregator("avg")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setId("interp")
            .setType(NumericType.TYPE.toString())
            .build())
        .build());
    when(node.rollupAggregation()).thenReturn("avg");
    scanners = new Tsdb1xScanners(node, source_config);
    stop = scanners.setStopKey(METRIC_BYTES, null);
    assertArrayEquals(makeRowKey(METRIC_BYTES, (END_TS - 900 + 10800), null), stop);
  }

  @Test
  public void setupScannersNoRollupNoFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(1, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(1, caught.size());
    assertArrayEquals(DATA_TABLE, storage.getLastScanner().table());
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    verify(caught.get(0), never()).setFilter(any(ScanFilter.class));
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    
    trace = new MockTrace(true);
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.setupScanners(METRIC_BYTES, trace.newSpan("UT").start());
    verifySpan(Tsdb1xScanners.class.getName() + ".setupScanners");
  }
  
  @Test
  public void setupScannersNoRollupNoFilterWithSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(saltedNode(), source_config);
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(1, scanners.scanners.size());
    assertEquals(6, scanners.scanners.get(0).length);
    assertEquals(6, caught.size());
    verify(caught.get(0), times(6)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(6)).setMaxNumRows(1024);
    verify(caught.get(0), times(6)).setReversed(false);
    for (int i = 0; i < 6; i++) {
      verify(caught.get(0), times(1)).setStartKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) i }, METRIC_BYTES), START_TS - 900, null));
      verify(caught.get(0), times(1)).setStopKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) i }, METRIC_BYTES), END_TS - 900 + 3600, null));
    }
    verify(caught.get(0), never()).setFilter(any(ScanFilter.class));
    assertTrue(scanners.initialized);
    for (int i = 0; i < 6; i++) {
      verify(scanners.scanners.get(0)[i], times(1))
        .fetchNext(any(Tsdb1xQueryResult.class), any());
    }
  }
  
  @Test
  public void setupScannersNoRollupRegexpFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(true, null, false);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.row_key_literals = new ByteMap<List<byte[]>>();
    scanners.row_key_literals.put(TAGK_BYTES, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES));
    
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(1, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(1, caught.size());
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    verify(caught.get(0), times(1)).setFilter(any(ScanFilter.class));
    assertTrue(caught.get(0).getFilter() instanceof KeyRegexpFilter);
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void setupScannersNoRollupRegexpFilterWithSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(true, null, false);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(saltedNode(), source_config);
    scanners.row_key_literals = new ByteMap<List<byte[]>>();
    scanners.row_key_literals.put(TAGK_BYTES, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES));
    
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(1, scanners.scanners.size());
    assertEquals(6, scanners.scanners.get(0).length);
    assertEquals(6, caught.size());
    verify(caught.get(0), times(6)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(6)).setMaxNumRows(1024);
    verify(caught.get(0), times(6)).setReversed(false);
    for (int i = 0; i < 6; i++) {
      verify(caught.get(0), times(1)).setStartKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) i }, METRIC_BYTES), START_TS - 900, null));
      verify(caught.get(0), times(1)).setStopKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) i }, METRIC_BYTES), END_TS - 900 + 3600, null));
    }
    verify(caught.get(0), times(6)).setFilter(any(KeyRegexpFilter.class));
    assertTrue(scanners.initialized);
    for (int i = 0; i < 6; i++) {
      verify(scanners.scanners.get(0)[i], times(1))
        .fetchNext(any(Tsdb1xQueryResult.class), any());
    }
  }
  
  @Test
  public void setupScannersNoRollupFuzzyEnabledFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(true, null, false);
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(ExplicitTagsFilter.newBuilder()
                    .setFilter(ChainFilter.newBuilder()
                      .addFilter(TagValueLiteralOrFilter.newBuilder()
                        .setTagKey(TAGK_STRING)
                        .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
                        .build())
                      .addFilter(TagValueWildcardFilter.newBuilder()
                          .setTagKey(TAGK_B_STRING)
                          .setFilter("*")
                         .build())
                      .build())
                    .build())
                .build())
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Whitebox.setInternalState(scanners, "enable_fuzzy_filter", true);
    FilterCB filter_cb = mock(FilterCB.class);
    Whitebox.setInternalState(filter_cb, "explicit_tags", true);
    Whitebox.setInternalState(scanners, "filter_cb", filter_cb);
    scanners.row_key_literals = new ByteMap<List<byte[]>>();
    scanners.row_key_literals.put(TAGK_BYTES, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES));
    
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(1, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(1, caught.size());
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, new byte[3]));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    FilterList filter = (FilterList) storage.getLastScanner().getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof FuzzyRowFilter);
    assertTrue(filter.filters().get(1) instanceof KeyRegexpFilter);
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void setupScannersRollupNoFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(false, "sum", false);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(3, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(3, caught.size());
    
    List<MockScanner> scnrs = storage.getScanners();
    
    assertArrayEquals("tsdb-1h".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 3).table());
    assertArrayEquals("tsdb-30m".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 2).table());
    assertArrayEquals(DATA_TABLE, storage.getLastScanner().table());
    
    // 1h
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(0), times(1)).setFilter(any(FilterList.class));
    FilterList filter = (FilterList) scnrs.get(scnrs.size() - 3).getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof QualifierFilter);
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    // 30m
    verify(caught.get(1), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(1), times(1)).setMaxNumRows(1024);
    verify(caught.get(1), times(1)).setReversed(false);
    verify(caught.get(1), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(1), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(1), times(1)).setFilter(any(ScanFilter.class));
    filter = (FilterList) scnrs.get(scnrs.size() - 2).getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof QualifierFilter);
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    // raw
    verify(caught.get(2), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(2), times(1)).setMaxNumRows(1024);
    verify(caught.get(2), times(1)).setReversed(false);
    verify(caught.get(2), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(2), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    verify(caught.get(2), never()).setFilter(any(ScanFilter.class));
    
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(1)[0], never())
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(2)[0], never())
    .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void setupScannersRollupNoFallbackNoFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(false, "sum", false);
    when(node.rollupUsage()).thenReturn(RollupUsage.ROLLUP_NOFALLBACK);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(1, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(1, caught.size());
    
    assertArrayEquals("tsdb-1h".getBytes(Const.ASCII_CHARSET), storage.getLastScanner().table());
    
    // 1h
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(0), times(1)).setFilter(any(FilterList.class));
    FilterList filter = (FilterList) storage.getLastScanner().getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof QualifierFilter);
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void setupScannersRollupPreAggNoFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(false, "sum", true);
    
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
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(3, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(3, caught.size());
    
    List<MockScanner> scnrs = storage.getScanners();
    
    assertArrayEquals("tsdb-agg-1h".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 3).table());
    assertArrayEquals("tsdb-agg-30m".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 2).table());
    assertArrayEquals(DATA_TABLE, storage.getLastScanner().table());
    
    // 1h
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(0), times(1)).setFilter(any(FilterList.class));
    FilterList filter = (FilterList) scnrs.get(scnrs.size() - 3).getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof QualifierFilter);
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    // 30m
    verify(caught.get(1), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(1), times(1)).setMaxNumRows(1024);
    verify(caught.get(1), times(1)).setReversed(false);
    verify(caught.get(1), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(1), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(1), times(1)).setFilter(any(ScanFilter.class));
    filter = (FilterList) scnrs.get(scnrs.size() - 2).getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof QualifierFilter);
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    // raw
    verify(caught.get(2), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(2), times(1)).setMaxNumRows(1024);
    verify(caught.get(2), times(1)).setReversed(false);
    verify(caught.get(2), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(2), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    verify(caught.get(2), never()).setFilter(any(ScanFilter.class));
    
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(1)[0], never())
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(2)[0], never())
    .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void setupScannersRollupAvgNoFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(false, "avg", false);
    
    when(node.rollupIntervals())
      .thenReturn(Lists.<RollupInterval>newArrayList(RollupInterval.builder()
          .setInterval("1h")
          .setTable("tsdb-1h")
          .setPreAggregationTable("tsdb-agg-1h")
          .setRowSpan("1d")
          .build()));
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(2, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(2, caught.size());
    
    List<MockScanner> scnrs = storage.getScanners();
    
    assertArrayEquals("tsdb-1h".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 2).table());
    assertArrayEquals(DATA_TABLE, storage.getLastScanner().table());
    
    // 1h
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(0), times(1)).setFilter(any(FilterList.class));
    FilterList filter = (FilterList) scnrs.get(scnrs.size() - 2).getFilter();
    assertEquals(4, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof QualifierFilter);
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals("count".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(2)).comparator()).value());
    assertArrayEquals(new byte[] { 2 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(3)).comparator()).value());
    
    // raw
    verify(caught.get(1), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(1), times(1)).setMaxNumRows(1024);
    verify(caught.get(1), times(1)).setReversed(false);
    verify(caught.get(1), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(1), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    verify(caught.get(1), never()).setFilter(any(ScanFilter.class));
    
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(1)[0], never())
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void setupScannersRollupNoFilterWithSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    Tsdb1xQueryNode node = saltedNode();
    final List<byte[]> tables = Lists.newArrayList();
    final List<ScanFilter> filters = Lists.newArrayList();
    catchTables(node, tables, filters);
    setConfig(false, "sum", false);
    
    when(node.rollupIntervals())
      .thenReturn(Lists.<RollupInterval>newArrayList(RollupInterval.builder()
          .setInterval("1h")
          .setTable("tsdb-1h")
          .setPreAggregationTable("tsdb-agg-1h")
          .setRowSpan("1d")
          .build()));
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(2, scanners.scanners.size());
    assertEquals(6, scanners.scanners.get(0).length);
    assertEquals(12, caught.size());
    
    assertEquals(12, tables.size());
    for (int i = 0; i < 6; i++) {
      assertArrayEquals("tsdb-1h".getBytes(Const.ASCII_CHARSET), tables.get(i));
    }
    for (int i = 6; i < 12; i++) {
      assertArrayEquals(DATA_TABLE, tables.get(i));
    }
    
    // 1h
    for (int i = 0; i < 6; i++) {
      verify(caught.get(i), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
      verify(caught.get(i), times(1)).setMaxNumRows(1024);
      verify(caught.get(i), times(1)).setReversed(false);
//      verify(caught.get(i), times(1)).setStartKey(
//          makeRowKey(MockBase.concatByteArrays(new byte[] { 0 }, METRIC_BYTES), START_TS - 900, null));
//      verify(caught.get(i), times(1)).setStopKey(
//          makeRowKey(MockBase.concatByteArrays(new byte[] { 0 }, METRIC_BYTES), 1514851200, null));
      verify(caught.get(i), times(1)).setStartKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) i }, METRIC_BYTES), START_TS - 900, null));
      verify(caught.get(i), times(1)).setStopKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) i }, METRIC_BYTES), 1514851200, null));
      verify(caught.get(i), times(1)).setFilter(any(FilterList.class));
      FilterList filter = (FilterList) filters.get(i);
      assertEquals(2, filter.filters().size());
      assertTrue(filter.filters().get(0) instanceof QualifierFilter);
      assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
      assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    }
    
    // raw
    for (int i = 6; i < 12; i++) {
      verify(caught.get(i), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
      verify(caught.get(i), times(1)).setMaxNumRows(1024);
      verify(caught.get(i), times(1)).setReversed(false);
      verify(caught.get(i), times(1)).setStartKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) (i - 6) }, METRIC_BYTES), START_TS - 900, null));
      verify(caught.get(i), times(1)).setStopKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) (i - 6) }, METRIC_BYTES), END_TS - 900 + 3600, null));
      verify(caught.get(i), never()).setFilter(any(ScanFilter.class));
    }
    assertTrue(scanners.initialized);
    for (int i = 0; i < 6; i++) {
      verify(scanners.scanners.get(0)[i], times(1))
        .fetchNext(any(Tsdb1xQueryResult.class), any());
      verify(scanners.scanners.get(1)[i], never())
        .fetchNext(any(Tsdb1xQueryResult.class), any());
    }
  }
  
  @Test
  public void setupScannersRollupAvgNoFilterWithSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    Tsdb1xQueryNode node = saltedNode();
    final List<byte[]> tables = Lists.newArrayList();
    final List<ScanFilter> filters = Lists.newArrayList();
    catchTables(node, tables, filters);
    setConfig(false, "avg", false);
    
    when(node.rollupIntervals())
      .thenReturn(Lists.<RollupInterval>newArrayList(RollupInterval.builder()
          .setInterval("1h")
          .setTable("tsdb-1h")
          .setPreAggregationTable("tsdb-agg-1h")
          .setRowSpan("1d")
          .build()));
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(2, scanners.scanners.size());
    assertEquals(6, scanners.scanners.get(0).length);
    assertEquals(12, caught.size());
    
    assertEquals(12, tables.size());
    for (int i = 0; i < 6; i++) {
      assertArrayEquals("tsdb-1h".getBytes(Const.ASCII_CHARSET), tables.get(i));
    }
    for (int i = 6; i < 12; i++) {
      assertArrayEquals(DATA_TABLE, tables.get(i));
    }
    
    // 1h
    for (int i = 0; i < 6; i++) {
      verify(caught.get(i), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
      verify(caught.get(i), times(1)).setMaxNumRows(1024);
      verify(caught.get(i), times(1)).setReversed(false);
      verify(caught.get(i), times(1)).setStartKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) i }, METRIC_BYTES), START_TS - 900, null));
      verify(caught.get(i), times(1)).setStopKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) i }, METRIC_BYTES), 1514851200, null));
      verify(caught.get(i), times(1)).setFilter(any(FilterList.class));
      FilterList filter = (FilterList) filters.get(i);
      assertEquals(4, filter.filters().size());
      assertTrue(filter.filters().get(0) instanceof QualifierFilter);
      assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
      assertArrayEquals("count".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
      assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(2)).comparator()).value());
      assertArrayEquals(new byte[] { 2 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(3)).comparator()).value());
    }
    
    // raw
    for (int i = 6; i < 12; i++) {
      verify(caught.get(i), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
      verify(caught.get(i), times(1)).setMaxNumRows(1024);
      verify(caught.get(i), times(1)).setReversed(false);
      verify(caught.get(i), times(1)).setStartKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) (i - 6) }, METRIC_BYTES), START_TS - 900, null));
      verify(caught.get(i), times(1)).setStopKey(
          makeRowKey(MockBase.concatByteArrays(new byte[] { (byte) (i - 6) }, METRIC_BYTES), END_TS - 900 + 3600, null));
      verify(caught.get(i), never()).setFilter(any(ScanFilter.class));
    }
    assertTrue(scanners.initialized);
    for (int i = 0; i < 6; i++) {
      verify(scanners.scanners.get(0)[i], times(1))
        .fetchNext(any(Tsdb1xQueryResult.class), any());
      verify(scanners.scanners.get(1)[i], never())
        .fetchNext(any(Tsdb1xQueryResult.class), any());
    }
  }
  
  @Test
  public void setupScannersRollupRegexpFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(true, "sum", false);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.row_key_literals = new ByteMap<List<byte[]>>();
    scanners.row_key_literals.put(TAGK_BYTES, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES));
    
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(3, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(3, caught.size());
    
    List<MockScanner> scnrs = storage.getScanners();
    
    assertArrayEquals("tsdb-1h".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 3).table());
    assertArrayEquals("tsdb-30m".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 2).table());
    assertArrayEquals(DATA_TABLE, storage.getLastScanner().table());
    
    // 1h
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(0), times(1)).setFilter(any(FilterList.class));
    FilterList filter = (FilterList) scnrs.get(scnrs.size() - 3).getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof KeyRegexpFilter);
    assertTrue(filter.filters().get(1) instanceof FilterList);
    filter = (FilterList) ((FilterList) filter.filters().get(1));
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    // 30m
    verify(caught.get(1), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(1), times(1)).setMaxNumRows(1024);
    verify(caught.get(1), times(1)).setReversed(false);
    verify(caught.get(1), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(1), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(1), times(1)).setFilter(any(ScanFilter.class));
    filter = (FilterList) scnrs.get(scnrs.size() - 2).getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof KeyRegexpFilter);
    assertTrue(filter.filters().get(1) instanceof FilterList);
    filter = (FilterList) ((FilterList) filter.filters().get(1));
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    // raw
    verify(caught.get(2), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(2), times(1)).setMaxNumRows(1024);
    verify(caught.get(2), times(1)).setReversed(false);
    verify(caught.get(2), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(2), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    verify(caught.get(2), times(1)).setFilter(any(KeyRegexpFilter.class));
    
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(1)[0], never())
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void setupScannersRollupFuzzyDisabledFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(true, "sum", false);
    
    when(node.rollupIntervals())
      .thenReturn(Lists.<RollupInterval>newArrayList(RollupInterval.builder()
          .setInterval("1h")
          .setTable("tsdb-1h")
          .setPreAggregationTable("tsdb-agg-1h")
          .setRowSpan("1d")
          .build()));
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.row_key_literals = new ByteMap<List<byte[]>>();
    scanners.row_key_literals.put(TAGK_BYTES, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES));
    
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(2, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(2, caught.size());
    
    List<MockScanner> scnrs = storage.getScanners();
    
    assertArrayEquals("tsdb-1h".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 2).table());
    assertArrayEquals(DATA_TABLE, storage.getLastScanner().table());
    
    // 1h
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(0), times(1)).setFilter(any(FilterList.class));
    FilterList filter = (FilterList) scnrs.get(scnrs.size() - 2).getFilter();
    assertEquals(2, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof KeyRegexpFilter);
    assertTrue(filter.filters().get(1) instanceof FilterList);
    filter = (FilterList) ((FilterList) filter.filters().get(1));
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    // raw
    verify(caught.get(1), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(1), times(1)).setMaxNumRows(1024);
    verify(caught.get(1), times(1)).setReversed(false);
    verify(caught.get(1), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, null));
    verify(caught.get(1), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    verify(caught.get(1), times(1)).setFilter(any(KeyRegexpFilter.class));
    
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(1)[0], never())
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void setupScannersRollupFuzzyEnabledFilterNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(true, "sum", false);
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(ExplicitTagsFilter.newBuilder()
                    .setFilter(ChainFilter.newBuilder()
                      .addFilter(TagValueLiteralOrFilter.newBuilder()
                        .setTagKey(TAGK_STRING)
                        .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
                        .build())
                      .addFilter(TagValueWildcardFilter.newBuilder()
                          .setTagKey(TAGK_B_STRING)
                          .setFilter("*")
                         .build())
                      .build())
                    .build())
                .build())
            .build())
        .setId("m1")
        .build();
    
    when(node.rollupIntervals())
      .thenReturn(Lists.<RollupInterval>newArrayList(RollupInterval.builder()
          .setInterval("1h")
          .setTable("tsdb-1h")
          .setPreAggregationTable("tsdb-agg-1h")
          .setRowSpan("1d")
          .build()));
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Whitebox.setInternalState(scanners, "enable_fuzzy_filter", true);
    FilterCB filter_cb = mock(FilterCB.class);
    Whitebox.setInternalState(filter_cb, "explicit_tags", true);
    Whitebox.setInternalState(scanners, "filter_cb", filter_cb);
    scanners.row_key_literals = new ByteMap<List<byte[]>>();
    scanners.row_key_literals.put(TAGK_BYTES, 
        Lists.newArrayList(TAGV_BYTES, TAGV_B_BYTES));
    
    scanners.setupScanners(METRIC_BYTES, null);
    assertEquals(2, scanners.scanners.size());
    assertEquals(1, scanners.scanners.get(0).length);
    assertEquals(2, caught.size());
    
    List<MockScanner> scnrs = storage.getScanners();
    
    assertArrayEquals("tsdb-1h".getBytes(Const.ASCII_CHARSET), scnrs.get(scnrs.size() - 2).table());
    assertArrayEquals(DATA_TABLE, storage.getLastScanner().table());
    
    // 1h
    verify(caught.get(0), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(0), times(1)).setMaxNumRows(1024);
    verify(caught.get(0), times(1)).setReversed(false);
    verify(caught.get(0), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, new byte[3]));
    verify(caught.get(0), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, 1514851200, null));
    verify(caught.get(0), times(1)).setFilter(any(FilterList.class));
    FilterList filter = (FilterList) scnrs.get(scnrs.size() - 2).getFilter();
    assertEquals(3, filter.filters().size());
    assertTrue(filter.filters().get(0) instanceof FuzzyRowFilter);
    assertTrue(filter.filters().get(1) instanceof KeyRegexpFilter);
    assertTrue(filter.filters().get(2) instanceof FilterList);
    filter = (FilterList) ((FilterList) filter.filters().get(2));
    assertArrayEquals("sum".getBytes(), ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(0)).comparator()).value());
    assertArrayEquals(new byte[] { 1 }, ((BinaryPrefixComparator) ((QualifierFilter) filter.filters().get(1)).comparator()).value());
    
    // raw
    verify(caught.get(1), times(1)).setFamily(Tsdb1xHBaseDataStore.DATA_FAMILY);
    verify(caught.get(1), times(1)).setMaxNumRows(1024);
    verify(caught.get(1), times(1)).setReversed(false);
    verify(caught.get(1), times(1)).setStartKey(
        makeRowKey(METRIC_BYTES, START_TS - 900, TAGK_BYTES, new byte[3]));
    verify(caught.get(1), times(1)).setStopKey(
        makeRowKey(METRIC_BYTES, END_TS - 900 + 3600, null));
    verify(caught.get(1), times(1)).setFilter(any(FuzzyRowFilter.class));
    verify(caught.get(1), times(1)).setFilter(any(KeyRegexpFilter.class));
    
    assertTrue(scanners.initialized);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(1)[0], never())
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void filterCBNoKeepers() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .build())
          .addFilter(TagValueWildcardFilter.newBuilder()
              .setTagKey(TAGK_B_STRING)
              .setFilter("*")
             .build())
          .build();
        
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(filter)
                .build())
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(2, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
    
    // regex tags now
    filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .build())
          .addFilter(TagValueRegexFilter.newBuilder()
              .setTagKey(TAGK_B_STRING)
              .setFilter("^.*$")
             .build())
          .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(filter)
                .build())
            .build())
        .setId("m1")
        .build();
    
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.current_result = results;
    cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(2, scanners.row_key_literals.size());
    uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }
  
  @Test
  public void filterCBKeepers() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .build())
          .addFilter(TagValueWildcardFilter.newBuilder()
              .setTagKey(TAGK_B_STRING)
              .setFilter("*yahoo.com")
             .build())
          .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(filter)
                .build())
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(2, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertTrue(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
    
    // regexp
    filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .build())
          .addFilter(TagValueRegexFilter.newBuilder()
              .setTagKey(TAGK_B_STRING)
              .setFilter("pre.*fix")
             .build())
          .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(filter)
                .build())
            .build())
        .setId("m1")
        .build();
    
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.current_result = results;
    cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(2, scanners.row_key_literals.size());
    uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertTrue(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }

  @Test
  public void filterCBMultiGetable() throws Exception {
    QueryFilter filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey(TAGK_STRING)
        .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
        .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(filter)
                .build())
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(1, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertFalse(scanners.filterDuringScan());
    assertTrue(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
    
    // under the cardinality threshold.
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.current_result = results;
    Whitebox.setInternalState(scanners, "max_multi_get_cardinality", 1);
    cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(1, scanners.row_key_literals.size());
    uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }
  
  @Test
  public void filterCBDupeTagKeys() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING)
            .build())
          .addFilter(TagValueLiteralOrFilter.newBuilder()
              .setTagKey(TAGK_STRING)
              .setFilter(TAGV_B_STRING)
             .build())
          .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(filter)
                .build())
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(1, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertFalse(scanners.filterDuringScan());
    assertTrue(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }
  
  @Test
  public void filterCBAllNullLiteralOrValues() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
      .addFilter(TagValueLiteralOrFilter.newBuilder()
        .setTagKey(NSUN_TAGK)
        .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
        .build())
      .addFilter(TagValueWildcardFilter.newBuilder()
          .setTagKey(TAGK_B_STRING)
          .setFilter("*")
         .build())
      .build();
    setConfig(filter, null, false);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    try {
      cb.call(schema.resolveUids(filter, null).join());
      fail("Expected NoSuchUniqueName");
    } catch (NoSuchUniqueName e) { }
    
    // skipping won't solve this
    Whitebox.setInternalState(scanners, "skip_nsun_tagvs", true);
    cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    try {
      cb.call(schema.resolveUids(filter, null).join());
      fail("Expected NoSuchUniqueName");
    } catch (NoSuchUniqueName e) { }
    
    // and ditto if all uids were null.
    filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
          .setTagKey(TAGK_STRING)
          .setFilter(NSUN_TAGV + "|" + "none")
          .build())
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setTagKey(TAGK_B_STRING)
            .setFilter("*")
           .build())
        .build();
    setConfig(filter, null, false);
      
    cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    try {
      cb.call(schema.resolveUids(filter, null).join());
      fail("Expected NoSuchUniqueName");
    } catch (NoSuchUniqueName e) { }
  }
  
  @Test
  public void filterCBNullTagV() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
          .setTagKey(TAGK_STRING)
          .setFilter(NSUN_TAGV + "|" + TAGV_B_STRING)
          .build())
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setTagKey(TAGK_B_STRING)
            .setFilter("*")
           .build())
        .build();
    setConfig(filter, null, false);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    try {
      cb.call(schema.resolveUids(filter, null).join());
      fail("Expected NoSuchUniqueName");
    } catch (NoSuchUniqueName e) { }
    
    // skipping works
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.current_result = results;
    Whitebox.setInternalState(scanners, "skip_nsun_tagvs", true);
    cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(2, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(1, uids.size());
    assertArrayEquals(TAGV_B_BYTES, uids.get(0));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }
  
  @Test
  public void filterCBExpansionLimit() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
          .setTagKey(TAGK_STRING)
          .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
          .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_B_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
           .build())
        .build();
    setConfig(filter, null, false);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    Whitebox.setInternalState(scanners, "expansion_limit", 3);
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(2, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertTrue(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }
  
  @Test
  public void filterCBCurrentResultsNull() throws Exception {
    QueryFilter filter = setConfig(true, null, false);
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    try {
      cb.call(schema.resolveUids(filter, null).join());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    assertEquals(2, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }
  
  @Test
  public void filterNotNoTags() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .build())
          .addFilter(NotFilter.newBuilder()
              .setFilter(MetricLiteralFilter.newBuilder()
                .setMetric("sys.cpu.user")
                .build())
             .build())
          .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(filter)
                .build())
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(1, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertFalse(scanners.filterDuringScan());
    assertTrue(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }
  
  @Test
  public void filterNotWithTags() throws Exception {
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .build())
          .addFilter(NotFilter.newBuilder()
              .setFilter(TagValueLiteralOrFilter.newBuilder()
                  .setTagKey(TAGK_B_STRING)
                  .setFilter(TAGV_STRING)
                  .build())
             .build())
          .build();
    
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId("f1")
        .setQuery(SemanticQuery.newBuilder()
            .setMode(QueryMode.SINGLE)
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setExecutionGraph(ExecutionGraph.newBuilder()
                .addNode(ExecutionGraphNode.newBuilder()
                    .setId("datasource")
                    .build())
                .build())
            .addFilter(DefaultNamedFilter.newBuilder()
                .setId("f1")
                .setFilter(filter)
                .build())
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    FilterCB cb = scanners.new FilterCB(METRIC_BYTES, null);
    Whitebox.setInternalState(scanners, "filter_cb", cb);
    cb.call(schema.resolveUids(filter, null).join());
    
    assertEquals(1, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertTrue(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
  }
  
  @Test
  public void initializeResolveMetricOnly() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    
    assertNull(scanners.row_key_literals);
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
    assertTrue(scanners.initialized);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    
    trace = new MockTrace(true);
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(trace.newSpan("UT").start());
    verifySpan(Tsdb1xScanners.class.getName() + ".initialize", 3);
  }
  
  @Test
  public void initializeResolveTags() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    
    QueryFilter filter = TagValueLiteralOrFilter.newBuilder()
          .setTagKey(TAGK_STRING)
          .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
          .build();
    setConfig(filter, null, false);
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    
    assertEquals(1, scanners.row_key_literals.size());
    List<byte[]> uids = scanners.row_key_literals.get(TAGK_BYTES);
    assertEquals(2, uids.size());
    assertArrayEquals(TAGV_BYTES, uids.get(0));
    assertArrayEquals(TAGV_B_BYTES, uids.get(1));
    assertNull(scanners.row_key_literals.get(TAGK_B_BYTES));
    assertFalse(scanners.filterDuringScan());
    assertTrue(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
    assertTrue(scanners.initialized);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void initializeNSUNMetric() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setQuery(query)
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(NSUN_METRIC)
            .build())
        .setId("m1")
        .build();
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    
    assertNull(scanners.row_key_literals);
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertNull(scanners.scanners);
    assertFalse(scanners.initialized);
    verify(node, times(1)).onError(any(NoSuchUniqueName.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    trace = new MockTrace(true);
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(trace.newSpan("UT").start());
    verifySpan(Tsdb1xScanners.class.getName() + ".initialize", 
        NoSuchUniqueName.class, 3);
  }
  
  @Test
  public void initializeNSUNTagk() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    QueryFilter filter = ExplicitTagsFilter.newBuilder()
        .setFilter(ChainFilter.newBuilder()
          .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .build())
          .addFilter(TagValueWildcardFilter.newBuilder()
              .setTagKey(NSUN_TAGK)
              .setFilter("*")
             .build())
          .build())
        .build();
    setConfig(filter, null, false);
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    
    assertEquals(1, scanners.row_key_literals.size());
    assertFalse(scanners.filterDuringScan());
    assertTrue(scanners.couldMultiGet());
    assertNull(scanners.scanners);
    assertFalse(scanners.initialized);
    verify(node, times(1)).onError(any(NoSuchUniqueName.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    // can't ignore with explicit tags
    scanners = new Tsdb1xScanners(node, source_config);
    Whitebox.setInternalState(scanners, "skip_nsun_tagks", true);
    scanners.initialize(null);
    
    assertEquals(1, scanners.row_key_literals.size());
    assertFalse(scanners.filterDuringScan());
    assertTrue(scanners.couldMultiGet());
    assertNull(scanners.scanners);
    assertFalse(scanners.initialized);
    verify(node, times(2)).onError(any(NoSuchUniqueName.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    // tracing
    trace = new MockTrace(true);
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(trace.newSpan("UT").start());
    verifySpan(Tsdb1xScanners.class.getName() + ".initialize", 
        NoSuchUniqueName.class, 10);
    
    // now we can ignore it
    filter = ChainFilter.newBuilder()
          .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey(TAGK_STRING)
            .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
            .build())
          .addFilter(TagValueWildcardFilter.newBuilder()
              .setTagKey(NSUN_TAGK)
              .setFilter("*")
             .build())
          .build();
    setConfig(filter, null, false);
    scanners = new Tsdb1xScanners(node, source_config);
    Whitebox.setInternalState(scanners, "skip_nsun_tagks", true);
    scanners.initialize(null);
    
    assertEquals(1, scanners.row_key_literals.size());
    assertTrue(scanners.couldMultiGet());
    assertTrue(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
    assertTrue(scanners.initialized);
    verify(node, times(3)).onError(any(NoSuchUniqueName.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void initializeNSUNTagv() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    QueryFilter filter = ChainFilter.newBuilder()
        .addFilter(TagValueLiteralOrFilter.newBuilder()
          .setTagKey(TAGK_STRING)
          .setFilter(TAGV_STRING + "|" + NSUN_TAGV)
          .build())
        .addFilter(TagValueWildcardFilter.newBuilder()
            .setTagKey(TAGK_B_STRING)
            .setFilter("*")
           .build())
        .build();
    setConfig(filter, null, false);
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    
    assertEquals(0, scanners.row_key_literals.size());
    assertFalse(scanners.filterDuringScan());
    assertTrue(scanners.couldMultiGet());
    assertNull(scanners.scanners);
    assertFalse(scanners.initialized);
    verify(node, times(1)).onError(any(NoSuchUniqueName.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    // tracing
    trace = new MockTrace(true);
    scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(trace.newSpan("UT").start());
    verifySpan(Tsdb1xScanners.class.getName() + ".initialize", 
        NoSuchUniqueName.class, 9);
  }
  
  @Test
  public void fetchNext() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    
    Tsdb1xQueryResult results = null;
    try {
      scanners.fetchNext(results, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    results = mock(Tsdb1xQueryResult.class);
    scanners.fetchNext(results, null);
    
    assertSame(results, scanners.current_result);
    assertNull(scanners.row_key_literals);
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
    assertTrue(scanners.initialized);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(scanners.scanners.get(0)[0], times(1))
       .fetchNext(any(Tsdb1xQueryResult.class), any());
    assertEquals(0, scanners.scanner_index);
    
    try {
      scanners.fetchNext(results, null);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    // assume the last run finished.
    scanners.scanner_index = 42;
    results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = null;
    scanners.fetchNext(results, null);
     
    assertSame(results, scanners.current_result);
    assertNull(scanners.row_key_literals);
    assertFalse(scanners.filterDuringScan());
    assertFalse(scanners.couldMultiGet());
    assertEquals(1, scanners.scanners.size());
    assertTrue(scanners.initialized);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    verify(scanners.scanners.get(0)[0], times(2))
    .fetchNext(any(Tsdb1xQueryResult.class), any());
    assertEquals(0, scanners.scanner_index);
  }
  
  @Test
  public void scannerDoneNoSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    scanners.current_result = mock(Tsdb1xQueryResult.class);
    
    assertEquals(0, scanners.scanners_done);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    scanners.scannerDone();
    assertEquals(0, scanners.scanners_done);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, times(1)).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    assertNull(scanners.current_result);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void scannerDoneWithSalt() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    
    Tsdb1xQueryNode node = saltedNode();
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    scanners.current_result = mock(Tsdb1xQueryResult.class);
    
    assertEquals(0, scanners.scanners_done);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    scanners.scannerDone();
    assertEquals(1, scanners.scanners_done);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    assertNotNull(scanners.current_result);
    for (int i = 0; i < 6; i++) {
      verify(scanners.scanners.get(0)[i], times(1))
        .fetchNext(any(Tsdb1xQueryResult.class), any());
    }
    
    // the rest
    for (int i = 0; i < 5; i++) {
      scanners.scannerDone();
    }
    assertEquals(0, scanners.scanners_done);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, times(1)).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    assertNull(scanners.current_result);
    for (int i = 0; i < 6; i++) {
      verify(scanners.scanners.get(0)[i], times(1))
        .fetchNext(any(Tsdb1xQueryResult.class), any());
    }
  }
  
  @Test
  public void scannerDoneFallback() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    setConfig(false, "sum", false);
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    scanners.current_result = mock(Tsdb1xQueryResult.class);
    
    assertEquals(0, scanners.scanners_done);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    scanners.scannerDone();
    assertEquals(0, scanners.scanners_done);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    assertNotNull(scanners.current_result);
    assertEquals(1, scanners.scanner_index);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(1)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
    verify(scanners.scanners.get(2)[0], never())
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void scannerDoneException() throws Exception {
    final List<Scanner> caught = Lists.newArrayList();
    catchTsdb1xScanners(caught);
    
    doThrow(new UnitTestException()).when(node).onNext(any(QueryResult.class));
    
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    scanners.initialize(null);
    scanners.current_result = mock(Tsdb1xQueryResult.class);
    
    assertEquals(0, scanners.scanners_done);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    scanners.scannerDone();
    assertEquals(0, scanners.scanners_done);
    verify(node, times(1)).onError(any(UnitTestException.class));
    verify(node, times(1)).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    assertNull(scanners.current_result);
    verify(scanners.scanners.get(0)[0], times(1))
      .fetchNext(any(Tsdb1xQueryResult.class), any());
  }
  
  @Test
  public void scanNext() throws Exception {
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xScanner scanner = mock(Tsdb1xScanner.class);
    when(scanner.state()).thenReturn(State.CONTINUE);
    scanners.scanners = Lists.<Tsdb1xScanner[]>newArrayList(
        new Tsdb1xScanner[] { scanner }
        );
    
    scanners.scanNext(null);
    verify(scanner, times(1)).fetchNext(null, null);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    doThrow(new UnitTestException()).when(scanner).fetchNext(null, null);
    try {
      scanners.scanNext(null);
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
    verify(node, times(1)).onError(any(UnitTestException.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void scanNextSalted() throws Exception {
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xScanner scanner1 = mock(Tsdb1xScanner.class);
    when(scanner1.state()).thenReturn(State.CONTINUE);
    Tsdb1xScanner scanner2 = mock(Tsdb1xScanner.class);
    when(scanner2.state()).thenReturn(State.CONTINUE);
    scanners.scanners = Lists.<Tsdb1xScanner[]>newArrayList(
        new Tsdb1xScanner[] { scanner1, scanner2 }
        );
    
    scanners.scanNext(null);
    verify(scanner1, times(1)).fetchNext(null, null);
    verify(scanner2, times(1)).fetchNext(null, null);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    doThrow(new UnitTestException()).when(scanner2).fetchNext(null, null);
    try {
      scanners.scanNext(null);
      fail("Expected UnitTestException");
    } catch (UnitTestException e) { }
    verify(node, times(1)).onError(any(UnitTestException.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void scanNextSaltedPartial() throws Exception {
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xQueryResult results = mock(Tsdb1xQueryResult.class);
    scanners.current_result = results;
    Tsdb1xScanner scanner1 = mock(Tsdb1xScanner.class);
    when(scanner1.state()).thenReturn(State.COMPLETE);
    Tsdb1xScanner scanner2 = mock(Tsdb1xScanner.class);
    when(scanner2.state()).thenReturn(State.CONTINUE);
    scanners.scanners = Lists.<Tsdb1xScanner[]>newArrayList(
        new Tsdb1xScanner[] { scanner1, scanner2 }
        );
    
    scanners.scanNext(null);
    verify(scanner1, never()).fetchNext(results, null);
    verify(scanner2, times(1)).fetchNext(results, null);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, never()).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
    
    // all done unexpected
    when(scanner2.state()).thenReturn(State.COMPLETE);
    scanners.scanNext(null);
    verify(scanner1, never()).fetchNext(results, null);
    verify(scanner2, times(1)).fetchNext(results, null);
    verify(node, never()).onError(any(Throwable.class));
    verify(node, times(1)).onNext(any(QueryResult.class));
    verify(node, never()).onComplete(any(QueryNode.class), anyLong(), anyLong());
  }
  
  @Test
  public void exception() throws Exception {
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    assertFalse(scanners.hasException());
    
    scanners.exception(new UnitTestException());
    assertTrue(scanners.hasException());
    verify(node, times(1)).onError(any(UnitTestException.class));
    
    // nother scanner threw a failure
    scanners.exception(new UnitTestException());
    assertTrue(scanners.hasException());
    verify(node, times(1)).onError(any(UnitTestException.class));
  }
  
  @Test
  public void close() throws Exception {
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xScanner scanner = mock(Tsdb1xScanner.class);
    Tsdb1xScanner scanner2 = mock(Tsdb1xScanner.class);
    Tsdb1xScanner scanner3 = mock(Tsdb1xScanner.class);
    scanners.scanners = Lists.<Tsdb1xScanner[]>newArrayList(
        new Tsdb1xScanner[] { scanner, scanner2 },
        new Tsdb1xScanner[] { scanner3 }
        );
    
    scanners.close();
    verify(scanner, times(1)).close();
    verify(scanner2, times(1)).close();
    verify(scanner3, times(1)).close();
    
    doThrow(new UnitTestException()).when(scanner2).close();
    scanners.close();
    verify(scanner, times(2)).close();
    verify(scanner2, times(2)).close();
    verify(scanner3, times(2)).close();
  }

  @Test
  public void state() throws Exception {
    Tsdb1xScanners scanners = new Tsdb1xScanners(node, source_config);
    Tsdb1xScanner[] array = new Tsdb1xScanner[] {
        mock(Tsdb1xScanner.class),
        mock(Tsdb1xScanner.class)
    };
    scanners.scanners = Lists.<Tsdb1xScanner[]>newArrayList(array);
    
    when(array[0].state()).thenReturn(State.COMPLETE);
    when(array[1].state()).thenReturn(State.COMPLETE);
    assertEquals(State.COMPLETE, scanners.state());
    
    when(array[0].state()).thenReturn(State.COMPLETE);
    when(array[1].state()).thenReturn(State.CONTINUE);
    assertEquals(State.CONTINUE, scanners.state());
    
    when(array[0].state()).thenReturn(State.COMPLETE);
    when(array[1].state()).thenReturn(State.EXCEPTION);
    assertEquals(State.EXCEPTION, scanners.state());
  }
  
  Tsdb1xQueryNode saltedNode() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);
    HBaseClient client = mock(HBaseClient.class);
    Configuration config = UnitTestConfiguration.getConfiguration();
    Tsdb1xHBaseDataStore data_store = mock(Tsdb1xHBaseDataStore.class);
    
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    when(data_store.tsdb()).thenReturn(tsdb);
    when(data_store.dataTable()).thenReturn("tsdb".getBytes(Const.ASCII_CHARSET));
    when(data_store.uidTable()).thenReturn(UID_TABLE);
    when(data_store.client()).thenReturn(client);
    
    Schema schema = mock(Schema.class);
    when(schema.saltBuckets()).thenReturn(6);
    when(schema.saltWidth()).thenReturn(1);
    when(schema.metricWidth()).thenReturn(3);
    when(schema.getId(UniqueIdType.METRIC, METRIC_STRING, null))
      .thenReturn(Deferred.fromResult(METRIC_BYTES));
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        int bucket = (int) invocation.getArguments()[1];
        ((byte[]) invocation.getArguments()[0])[0] = (byte) bucket;
        return null;
      }
    }).when(schema).prefixKeyWithSalt(any(byte[].class), anyInt());
    
    when(data_store.dynamicString(Tsdb1xHBaseDataStore.ROLLUP_USAGE_KEY)).thenReturn("Rollup_Fallback");
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.EXPANSION_LIMIT_KEY)).thenReturn(4096);
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.ROWS_PER_SCAN_KEY)).thenReturn(1024);
    
    node = mock(Tsdb1xQueryNode.class);
    when(node.schema()).thenReturn(schema);
    when(node.parent()).thenReturn(data_store);
    when(schema.rollupConfig()).thenReturn(rollup_config);
    
    when(client.newScanner(any(byte[].class))).thenReturn(mock(Scanner.class));
    context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    when(context.upstreamOfType(any(QueryNode.class), any()))
      .thenReturn(Collections.emptyList());
    return node;
  }
  
  void catchTables(final Tsdb1xQueryNode node, final List<byte[]> tables, final List<ScanFilter> filters) {
    when(((Tsdb1xHBaseDataStore) node.parent()).client()
        .newScanner(any(byte[].class))).thenAnswer(new Answer<Scanner>() {
          @Override
          public Scanner answer(InvocationOnMock invocation) throws Throwable {
            tables.add((byte[]) invocation.getArguments()[0]);
            final Scanner scanner = mock(Scanner.class);
            doAnswer(new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                filters.add((ScanFilter) invocation.getArguments()[0]);
                return null;
              }
            }).when(scanner).setFilter(any(ScanFilter.class));
            return scanner;
          }
    });
  }
  
  void catchTsdb1xScanners(final List<Scanner> scanners) throws Exception {
    PowerMockito.whenNew(Tsdb1xScanner.class).withAnyArguments()
      .thenAnswer(new Answer<Tsdb1xScanner>() {
        @Override
        public Tsdb1xScanner answer(InvocationOnMock invocation)
            throws Throwable {
          scanners.add((Scanner) invocation.getArguments()[1]);
          Tsdb1xScanner mock_scanner = mock(Tsdb1xScanner.class);
          when(mock_scanner.state()).thenReturn(State.CONTINUE);
          return mock_scanner;
        }
    });
  }

  private QueryFilter setConfig(final boolean with_filter, final String ds, final boolean pre_agg) {
    QueryFilter filter = ChainFilter.newBuilder()
      .addFilter(TagValueLiteralOrFilter.newBuilder()
        .setTagKey(TAGK_STRING)
        .setFilter(TAGV_STRING + "|" + TAGV_B_STRING)
        .build())
      .addFilter(TagValueWildcardFilter.newBuilder()
          .setTagKey(TAGK_B_STRING)
          .setFilter("*")
         .build())
      .build();
    return setConfig(with_filter ? filter : null, ds, pre_agg);
  }
  
  private QueryFilter setConfig(final QueryFilter filter, final String ds, final boolean pre_agg) {
    SemanticQuery.Builder query_builder = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("datasource")
                .build())
            .build());
    if (filter != null) {
      query_builder.addFilter(DefaultNamedFilter.newBuilder()
          .setId("f1")
          .setFilter(filter)
          .build());
    }
    
    QuerySourceConfig.Builder builder = (Builder) QuerySourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setFilterId(filter != null ? "f1" : null)
        .setQuery(query_builder.build())
        .setId("m1");
    if (pre_agg) {
      builder.addOverride(Tsdb1xHBaseDataStore.PRE_AGG_KEY, "true");
    }
    
    source_config = builder.build();
    
    if (ds != null) {
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
      
      when(node.downsampleConfig()).thenReturn(
          (DownsampleConfig) DownsampleConfig.newBuilder()
          .setId("ds")
          .setInterval("1h")
          .setAggregator(ds)
          .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
              .setFillPolicy(FillPolicy.NONE)
              .setRealFillPolicy(FillWithRealPolicy.NONE)
              .setId("interp")
              .setType(NumericType.TYPE.toString())
              .build())
          .build());
      when(node.rollupAggregation()).thenReturn(ds);
    }
    return filter;
  }
}
