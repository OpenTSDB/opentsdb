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
package net.opentsdb.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.HBaseExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class, Scanner.class })
public class TestTsdb1xScanner extends UTBase {
  private Tsdb1xScanners owner;
  private Tsdb1xQueryNode node;
  private Tsdb1xQueryResult results;
  private Schema schema; 
  private QueryContext context;
  private QuerySourceConfig config;
  
  @Before
  public void before() throws Exception {
    results = mock(Tsdb1xQueryResult.class);
    node = mock(Tsdb1xQueryNode.class);
    owner = mock(Tsdb1xScanners.class);
    schema = spy(new Schema(tsdb, null));
    config = mock(QuerySourceConfig.class);
    when(owner.node()).thenReturn(node);
    when(node.config()).thenReturn(config);
    when(node.fetchDataType(any(byte.class))).thenReturn(true);
    when(node.schema()).thenReturn(schema);
    
    context = mock(QueryContext.class);
    when(context.mode()).thenReturn(QueryMode.SINGLE);
    QueryPipelineContext qpc = mock(QueryPipelineContext.class);
    when(qpc.queryContext()).thenReturn(context);
    when(node.pipelineContext()).thenReturn(qpc);

    when(results.resultIsFullErrorMessage()).thenReturn("Boo!");
  }
  
  @Test
  public void ctorIllegalArguments() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    try {
      new Tsdb1xScanner(null, hbase_scanner, 0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xScanner(owner, null, 0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void scanFilters() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(results, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_DOUBLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(eq(UniqueIdType.METRIC), 
        eq(METRIC_BYTES), any(Span.class));
    
    assertEquals(17, trace.spans.size());
    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
        trace.spans.get(16).id);
    assertEquals("OK", trace.spans.get(16).tags.get("status"));
  }
  
  @Test
  public void scanFiltersReverse() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setReversed(true);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(results, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_DOUBLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(eq(UniqueIdType.METRIC), 
        eq(METRIC_BYTES), any(Span.class));
    
    assertEquals(17, trace.spans.size());
    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
        trace.spans.get(16).id);
    assertEquals("OK", trace.spans.get(16).tags.get("status"));
  }
  
  @Test
  public void scanFiltersNSUI() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter("web.*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.NSUI_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_NSUI_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersNSUISkip() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter("web.*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.NSUI_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.skipNSUI()).thenReturn(true);
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_SINGLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersStorageException() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter("web.*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.MULTI_SERIES_EX, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, never()).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, never()).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersMultiScans() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(17)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_DOUBLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersThrownException() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 2) {
          throw new UnitTestException();
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(4)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(4)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersFullNotSingleMode() throws Exception {
    when(context.mode()).thenReturn(QueryMode.BOUNDED_CLIENT_STREAM);
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
        int count = 0;
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          if (count++ > 2) {
            when(results.isFull()).thenReturn(true);
          }
          invocation.callRealMethod();
          return null;
        }
      }).when(schema).baseTimestamp(any(byte[].class), any(TimeStamp.class));
      
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(1)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(1, scanner.keepers.size());
    assertEquals(1, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + 3600), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersFullSingleMode() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
        int count = 0;
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          if (count++ > 2) {
            when(results.isFull()).thenReturn(true);
          }
          invocation.callRealMethod();
          return null;
        }
      }).when(schema).baseTimestamp(any(byte[].class), any(TimeStamp.class));
      
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(1)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersFullRowBoundaryNotSingleMode() throws Exception {
    when(context.mode()).thenReturn(QueryMode.BOUNDED_CLIENT_STREAM);
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        when(results.isFull()).thenReturn(true);
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
      
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(1)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(1, scanner.keepers.size());
    assertEquals(1, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.CONTINUE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersFullRowBoundarySingleMode() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        when(results.isFull()).thenReturn(true);
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(1)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersOwnerException() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 2) {
          when(owner.hasException()).thenReturn(true);
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(4)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(4)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersSequenceEnd() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    when(node.sequenceEnd()).thenReturn(
        new MillisecondTimeStamp(((long) TS_DOUBLE_SERIES + (long) TS_DOUBLE_SERIES_INTERVAL) * 1000));
      
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(2)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(1, scanner.keepers.size());
    assertEquals(1, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + 7200), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + 7200), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersSequenceEndReverse() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setReversed(true);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    when(node.sequenceEnd()).thenReturn(
        new MillisecondTimeStamp(((long) TS_DOUBLE_SERIES + 
            ((long) TS_DOUBLE_SERIES_INTERVAL * 14)) * 1000));
      
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(2)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(1, scanner.keepers.size());
    assertEquals(1, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + 46800), TAGK_BYTES, TAGV_B_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + 46800), TAGK_BYTES, TAGV_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersSequenceEndMidRow() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
          .setFilter(TAGV_B_STRING + ".*")
          .setTagKey(TAGK_STRING)
          .build();
  when(owner.filterDuringScan()).thenReturn(true);
  when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(4);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    when(node.sequenceEnd()).thenReturn(
        new MillisecondTimeStamp(((long) TS_DOUBLE_SERIES + ((long) TS_DOUBLE_SERIES_INTERVAL * 2)) * 1000));
      
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(1, scanner.keepers.size());
    assertEquals(1, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 3)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 3)), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanFiltersSequenceEndMidRowReverse() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setReversed(true);
    hbase_scanner.setMaxNumRows(4);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    when(node.sequenceEnd()).thenReturn(
        new MillisecondTimeStamp(((long) TS_DOUBLE_SERIES + 
            ((long) TS_DOUBLE_SERIES_INTERVAL * 13)) * 1000));
      
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(1, scanner.keepers.size());
    assertEquals(1, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 12)), TAGK_BYTES, TAGV_B_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 12)), TAGK_BYTES, TAGV_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void scanNoFilters() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(results, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_SINGLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertEquals(2, trace.spans.size());
    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
        trace.spans.get(1).id);
    assertEquals("OK", trace.spans.get(1).tags.get("status"));
  }
  
  @Test
  public void scanNoFiltersMultiScans() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(9)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_SINGLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void scanNoFiltersThrownException() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 2) {
          throw new UnitTestException();
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(4)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void scanNoFiltersFullNotSingle() throws Exception {
    when(context.mode()).thenReturn(QueryMode.BOUNDED_CLIENT_STREAM);
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 3) {
          when(results.isFull()).thenReturn(true);
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(5)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 18000), TAGK_BYTES, TAGV_BYTES));
  }
  
  @Test
  public void scanNoFiltersFullSingle() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 3) {
          when(results.isFull()).thenReturn(true);
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(5)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void scanNoFiltersFullOnRowBoundaryNotSingle() throws Exception {
    when(context.mode()).thenReturn(QueryMode.BOUNDED_CLIENT_STREAM);
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 2) {
          when(results.isFull()).thenReturn(true);
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(4)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void scanNoFiltersFullOnRowBoundarySingle() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 2) {
          when(results.isFull()).thenReturn(true);
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(4)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void scanNoFiltersOwnerException() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 2) {
          when(owner.hasException()).thenReturn(true);
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(4)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void scanNoFiltersSequenceEnd() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd()).thenReturn(
        new MillisecondTimeStamp((TS_SINGLE_SERIES + 3600L) * 1000));
        
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(2)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 7200), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
  }
  
  @Test
  public void scanNoFiltersSequenceEndMidRow() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd()).thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 7200L) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
  }
  
  @Test
  public void fetchNextOwnerException() throws Exception {
    when(owner.hasException()).thenReturn(true);
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, never()).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, never()).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void fetchNextOwnerFullNotSingle() throws Exception {
    when(context.mode()).thenReturn(QueryMode.BOUNDED_CLIENT_STREAM);
    when(results.isFull()).thenReturn(true);
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, never()).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, never()).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void fetchNextOwnerFullSingle() throws Exception {
    when(results.isFull()).thenReturn(true);
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, never()).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, never()).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void fetchNextFiltersBuffer() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp(((long) TS_DOUBLE_SERIES + 
          (long) TS_DOUBLE_SERIES_INTERVAL) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(2)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((long) (TS_DOUBLE_SERIES + 
          TS_DOUBLE_SERIES_INTERVAL * 4) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(6)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(5)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 5)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 5)), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(17)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_DOUBLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void fetchNextFiltersBufferSequenceEndInBuffer() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(6);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp(((long) TS_DOUBLE_SERIES) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(1)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(4, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 1)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 1)), TAGK_BYTES, TAGV_B_BYTES));
    assertArrayEquals(scanner.buffer().get(2).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(3).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((long) (TS_DOUBLE_SERIES + 
          TS_DOUBLE_SERIES_INTERVAL) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(2)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(7)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_DOUBLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void fetchNextFiltersBufferNSUISkip() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter("web.*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.NSUI_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(6);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.skipNSUI()).thenReturn(true);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp(((long) TS_NSUI_SERIES) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(1)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(5, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 1)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 1)), TAGK_BYTES, NSUI_TAGV));
    assertArrayEquals(scanner.buffer().get(2).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(3).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 2)), TAGK_BYTES, NSUI_TAGV));
    verify(schema, times(1)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((long) (TS_NSUI_SERIES + 
          (TS_NSUI_SERIES_INTERVAL * 2)) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 3)), TAGK_BYTES, TAGV_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(7)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_NSUI_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void fetchNextFiltersBufferNSUI() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter("web.*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.NSUI_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(6);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp(((long) TS_NSUI_SERIES) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(1)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(5, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 1)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 1)), TAGK_BYTES, NSUI_TAGV));
    assertArrayEquals(scanner.buffer().get(2).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(3).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 2)), TAGK_BYTES, NSUI_TAGV));
    verify(schema, times(1)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((long) (TS_NSUI_SERIES + 
          (TS_NSUI_SERIES_INTERVAL * 2)) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(State.EXCEPTION, scanner.state());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
  }
  
  @Test
  public void fetchNextNoFiltersBuffer() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 7200L) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 18000L) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(4)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(6)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 21600), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 25200), TAGK_BYTES, TAGV_BYTES));
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(9)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_SINGLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void fetchNextNoFiltersBufferSequenceEndInBuffer() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 3600L) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(2)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 7200), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 7200L) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(9)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_SINGLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void fetchNextNoFiltersBufferFullInBuffer() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 3600L) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(2)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 7200), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    
    // next fetch
    when(node.sequenceEnd()).thenReturn(null);
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (count++ == 0) {
          when(results.isFull()).thenReturn(true);
        }
        return null;
      }
    }).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    when(results.isFull()).thenReturn(false);
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(9)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(TS_SINGLE_SERIES_COUNT)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
  }
  
  @Test
  public void fetchNextNoFiltersBufferException() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 3600L) * 1000));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(results, times(2)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 7200), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    
    // next fetch
    when(node.sequenceEnd()).thenReturn(null);
    doThrow(new UnitTestException()).when(results).decode(
        any(ArrayList.class), any(RollupInterval.class));
    
    scanner.fetchNext(results, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(results, times(3)).decode(
        any(ArrayList.class), any(RollupInterval.class));
    verify(owner, times(1)).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
  }
  
  Scanner metricStartStopScanner(final Series series, final byte[] metric) {
    final Scanner scanner = client.newScanner(DATA_TABLE);
    switch (series) {
    case SINGLE_SERIES:
      scanner.setStartKey(makeRowKey(
          metric, 
          TS_SINGLE_SERIES, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          TS_SINGLE_SERIES + (TS_SINGLE_SERIES_COUNT * TS_SINGLE_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    case DOUBLE_SERIES:
      scanner.setStartKey(makeRowKey(
          metric, 
          TS_DOUBLE_SERIES, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_COUNT * TS_DOUBLE_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    case MULTI_SERIES_EX:
      scanner.setStartKey(makeRowKey(
          metric, 
          TS_MULTI_SERIES_EX, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          TS_MULTI_SERIES_EX + (TS_MULTI_SERIES_EX_COUNT * TS_MULTI_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    case NSUI_SERIES:
      scanner.setStartKey(makeRowKey(
          metric, 
          TS_NSUI_SERIES, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          TS_NSUI_SERIES + (TS_NSUI_SERIES_COUNT * TS_NSUI_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    default:
      throw new RuntimeException("YO! Implement me: " + series);
    }
    return scanner;
  }
}
