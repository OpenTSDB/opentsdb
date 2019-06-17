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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
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
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagValueRegexFilter;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.Span;
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
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Pair;
import net.opentsdb.utils.UnitTestException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class, Scanner.class })
public class TestTsdb1xScannerPush extends UTBase {
  private static ObjectPool RUNNABLE_POOL;
  private static ObjectPool NO_DATA_POOL;
  private static ObjectPool SET_POOL;
  private static ObjectPool LONG_ARRAY_POOL;
  private static ObjectPool NUMERIC_POOL;
  private static ObjectPool BYTE_ARRAY_POOL;
  private static ObjectPool NUMERIC_SUMMARY_POOL;
  
  private Tsdb1xScanners owner;
  private Tsdb1xHBaseQueryNode node;
  private Schema schema; 
  private QueryContext context;
  private TimeSeriesDataSourceConfig config;
  private TLongObjectMap<Tsdb1xPartialTimeSeriesSet> sets;
  private Duration duration;
  
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
  }
  
  @Before
  public void before() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.push()).thenReturn(true);
    owner = mock(Tsdb1xScanners.class);
    schema = spy(new Schema(schema_factory, tsdb, null));
    config = mock(TimeSeriesDataSourceConfig.class);
    when(owner.node()).thenReturn(node);
    when(node.config()).thenReturn(config);
    when(node.fetchDataType(any(byte.class))).thenReturn(true);
    when(node.schema()).thenReturn(schema);
    
    context = mock(QueryContext.class);
    when(context.mode()).thenReturn(QueryMode.SINGLE);
    QueryPipelineContext qpc = mock(QueryPipelineContext.class);
    when(qpc.queryContext()).thenReturn(context);
    when(qpc.tsdb()).thenReturn(tsdb);
    when(node.pipelineContext()).thenReturn(qpc);
    tsdb.runnables.clear();
  }
  
  @After
  public void after() throws Exception {
    for (final Runnable runnable : tsdb.runnables) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
      set.close();
    }
  }
  
  @Test
  public void ctorIllegalArguments() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    try {
      new Tsdb1xScanner().reset(null, hbase_scanner, 0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xScanner().reset(owner, null, 0, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void scanFiltersNoData() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_STRING + "NoSuchData.*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    int series_start = 1483228800;
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, 
        METRIC_B_BYTES, series_start);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, never()).getName(eq(UniqueIdType.METRIC), 
        eq(METRIC_BYTES), any(Span.class));
    assertTrue(scanner.last_pts.isEmpty());
    assertEquals(16, tsdb.runnables.size());
    
    for (int i = 0; i < 16; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      // No ordering in the way we call it so we can't check timestamps.
      //assertEquals(start.epoch(), set.start().epoch());
      //assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(0, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      assertTrue(((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts() 
          instanceof NoDataPartialTimeSeries);
    }
  }
  
  @Test
  public void scanFiltersSingle() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(1)).getName(eq(UniqueIdType.METRIC), 
        eq(METRIC_BYTES), any(Span.class));
    assertTrue(scanner.last_pts.isEmpty());
    verifySingleSeries(16);
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    assertEquals(0, tsdb.runnables.size());
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.skipNSUI()).thenReturn(true);
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyNSUISeries(16);
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, never()).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    assertEquals(0, tsdb.runnables.size());
  }
  
  @Test
  public void scanFiltersOneOfDoubleMultiScans() throws Exception {
    final QueryFilter filter = TagValueRegexFilter.newBuilder()
            .setFilter(TAGV_B_STRING + ".*")
            .setTagKey(TAGK_STRING)
            .build();
    when(owner.filterDuringScan()).thenReturn(true);
    when(config.getFilter()).thenReturn(filter);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(17)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyOneOfDoubleSeries(16);
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    TLongHashSet keepers = new TLongHashSet();
    scanner.keepers = spy(keepers);
    doAnswer(new Answer<Void>() {
      int count = 0;
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        throw new UnitTestException();
      }
    }).when(scanner.keepers).add(anyLong());
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Duration>() {
      @Override
      public Duration answer(InvocationOnMock invocation) throws Throwable {
        if (tsdb.runnables.size() > 2) {
          when(owner.hasException()).thenReturn(true);
        }
        return duration;
      }
    }).when(owner).currentDuration();
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(5)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(0, scanner.keepers.size());
    assertEquals(0, scanner.skips.size());
    assertTrue(scanner.keys_to_ids.isEmpty());
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyOneOfDoubleSeries(4);
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    when(node.sequenceEnd()).thenReturn(
        new SecondTimeStamp(TS_DOUBLE_SERIES + TS_DOUBLE_SERIES_INTERVAL));
      
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, never()).close();
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
    verifyOneOfDoubleSeries(1);
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    when(node.sequenceEnd()).thenReturn(
        new SecondTimeStamp(TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)));
      
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
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
    verifyOneOfDoubleSeries(2);
  }
  
  @Test
  public void scanNoFiltersSingleSeries() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertEquals(2, trace.spans.size());
    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
        trace.spans.get(1).id);
    assertEquals("OK", trace.spans.get(1).tags.get("status"));
    assertTrue(scanner.last_pts.isEmpty());
    verifySingleSeries(16);
  }
  
  @Test
  public void scanNoFiltersSingleSeriesMultiScans() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(9)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertTrue(scanner.last_pts.isEmpty());
    verifySingleSeries(16);
  }
  
  @Test
  public void scanNoFiltersDoubleSeries() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertEquals(2, trace.spans.size());
    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
        trace.spans.get(1).id);
    assertEquals("OK", trace.spans.get(1).tags.get("status"));
    assertTrue(scanner.last_pts.isEmpty());
    verifyDoubleSeries(32);
  }
  
  @Test
  public void scanNoFiltersThrownException() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Duration>() {
      int count = 0;
      @Override
      public Duration answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 2) {
          throw new UnitTestException();
        }
        return duration;
      }
    }).when(owner).currentDuration();
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, never()).scannerDone();
    verify(owner, times(1)).exception(any(UnitTestException.class));
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    
    // only 2 make it through.
    verifySingleSeries(2);
  }
  
  @Test
  public void scanNoFiltersOwnerException() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    doAnswer(new Answer<Duration>() {
      int count = 0;
      @Override
      public Duration answer(InvocationOnMock invocation) throws Throwable {
        if (count++ > 2) {
          when(owner.hasException()).thenReturn(true);
        }
        return duration;
      }
    }).when(owner).currentDuration();
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    
    // only 3 make it through.
    verifySingleSeries(3);
  }
  
  @Test
  public void scanNoFiltersSequenceEnd() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd()).thenReturn(
        new SecondTimeStamp(TS_SINGLE_SERIES + TS_SINGLE_SERIES_INTERVAL));
        
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 7200), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    
    // only one makes it through
    verifySingleSeries(1);
  }
  
  @Test
  public void scanNoFiltersSequenceEndMidRow() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd()).thenReturn(
        new SecondTimeStamp(TS_SINGLE_SERIES + (TS_SINGLE_SERIES_INTERVAL * 2)));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    
    // only 2 make it through
    verifySingleSeries(2);
  }
  
  @Test
  public void scanNoFiltersFillEarlier() throws Exception {
    int series_start = TS_SINGLE_SERIES - (TS_SINGLE_SERIES_INTERVAL * 2);
    Scanner hbase_scanner = metricStartStopScanner(
        Series.SINGLE_SERIES, 
        METRIC_BYTES, 
        series_start);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertEquals(2, trace.spans.size());
    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
        trace.spans.get(1).id);
    assertEquals("OK", trace.spans.get(1).tags.get("status"));
    assertTrue(scanner.last_pts.isEmpty());
    assertEquals(16, tsdb.runnables.size());
    
    final byte[] tsuid = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    
    TimeStamp start = new SecondTimeStamp(series_start);
    TimeStamp end = start.getCopy();
    end.add(duration);
    for (int i = 0; i < 2; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(0, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      assertTrue(((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts() 
          instanceof NoDataPartialTimeSeries);
      
      start.add(duration);
      end.add(duration);
    }
    
    for (int i = 2; i < 16; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().value();
      assertEquals(0, value.offset());
      assertEquals(2, value.end());
      assertEquals(start.epoch(), value.data()[0]);
      assertEquals(1, value.data()[1]); // just the single value
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
  }
  
  @Test
  public void scanNoFiltersFillLater() throws Exception {
    int series_start = TS_SINGLE_SERIES + (TS_SINGLE_SERIES_INTERVAL * 2);
    Scanner hbase_scanner = metricStartStopScanner(
        Series.SINGLE_SERIES, 
        METRIC_BYTES, 
        series_start);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertEquals(2, trace.spans.size());
    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
        trace.spans.get(1).id);
    assertEquals("OK", trace.spans.get(1).tags.get("status"));
    assertTrue(scanner.last_pts.isEmpty());
    assertEquals(16, tsdb.runnables.size());
    
    final byte[] tsuid = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    
    TimeStamp start = new SecondTimeStamp(series_start);
    TimeStamp end = start.getCopy();
    end.add(duration);
    
    for (int i = 0; i < 14; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().value();
      assertEquals(0, value.offset());
      assertEquals(2, value.end());
      assertEquals(start.epoch(), value.data()[0]);
      assertEquals(1, value.data()[1]); // just the single value
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
    
    for (int i = 14; i < 16; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(0, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      assertTrue(((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts() 
          instanceof NoDataPartialTimeSeries);
      
      start.add(duration);
      end.add(duration);
    }
  }
  
  @Test
  public void scanNoFiltersFillMiddle() throws Exception {
    int series_start = TS_SINGLE_SERIES_GAP;
    Scanner hbase_scanner = metricStartStopScanner(
        Series.SINGLE_SERIES_GAP, 
        METRIC_BYTES, 
        series_start);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertEquals(2, trace.spans.size());
    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
        trace.spans.get(1).id);
    assertEquals("OK", trace.spans.get(1).tags.get("status"));
    assertTrue(scanner.last_pts.isEmpty());
    assertEquals(16, tsdb.runnables.size());
    
    final byte[] tsuid = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES_GAP, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    
    TimeStamp start = new SecondTimeStamp(series_start);
    TimeStamp end = start.getCopy();
    end.add(duration);
    
    for (int i = 0; i < 6; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().value();
      assertEquals(0, value.offset());
      assertEquals(2, value.end());
      assertEquals(start.epoch(), value.data()[0]);
      assertEquals(1, value.data()[1]); // just the single value
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
    
    for (int i = 6; i < 11; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(0, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      assertTrue(((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts() 
          instanceof NoDataPartialTimeSeries);
      
      start.add(duration);
      end.add(duration);
    }
    
    for (int i = 11; i < 16; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().value();
      assertEquals(0, value.offset());
      assertEquals(2, value.end());
      assertEquals(start.epoch(), value.data()[0]);
      assertEquals(1, value.data()[1]); // just the single value
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
  }
  
  @Test
  public void scanNoFiltersMultiColumn() throws Exception {
    int series_start = TS_MULTI_COLUMN_SERIES;
    Scanner hbase_scanner = metricStartStopScanner(
        Series.MULTI_COLUMN_SERIES, 
        METRIC_BYTES, 
        series_start);
    hbase_scanner.setMaxNumKeyValues(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(41)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertTrue(scanner.last_pts.isEmpty());
    assertEquals(16, tsdb.runnables.size());
    
    final byte[] tsuid = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_MULTI_COLUMN_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    
    TimeStamp start = new SecondTimeStamp(series_start);
    TimeStamp end = start.getCopy();
    end.add(duration);
    
    for (int i = 0; i < 16; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().value();
      assertEquals(0, value.offset());
      assertEquals(10, value.end());
      int idx = 0;
      for (int x = 0; x < 5; x++) {
        assertEquals(start.epoch() + (x * 60), value.data()[idx++]);
        assertEquals(x, value.data()[idx++]);
      }
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
  }
  
  @Test
  public void scanNoFiltersAppendColumn() throws Exception {
    int series_start = TS_APPEND_SERIES;
    Scanner hbase_scanner = metricStartStopScanner(
        Series.APPEND_SERIES, 
        METRIC_BYTES, 
        series_start);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertTrue(scanner.last_pts.isEmpty());
    assertEquals(16, tsdb.runnables.size());
    
    final byte[] tsuid = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_MULTI_COLUMN_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    
    TimeStamp start = new SecondTimeStamp(series_start);
    TimeStamp end = start.getCopy();
    end.add(duration);
    
    for (int i = 0; i < 16; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().value();
      assertEquals(0, value.offset());
      assertEquals(2, value.end());
      assertEquals(start.epoch() + 60, value.data()[0]);
      assertEquals(42, value.data()[1]);
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
  }
  
  @Test
  public void scanRollups() throws Exception {
    int series_start = TS_ROLLUP_SERIES;
    Scanner hbase_scanner = metricStartStopScanner(
        Series.ROLLUP_SERIES, 
        METRIC_BYTES, 
        series_start,
        true);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, HOURLY_INTERVAL);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertTrue(scanner.last_pts.isEmpty());
    assertEquals(4, tsdb.runnables.size());
    
    final byte[] tsuid = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_MULTI_COLUMN_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    
    TimeStamp start = new SecondTimeStamp(series_start);
    TimeStamp end = start.getCopy();
    end.add(duration);
    
    for (int i = 0; i < 4; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(4, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericSummaryPartialTimeSeries value = (Tsdb1xNumericSummaryPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().value();
      assertEquals(0, value.offset());
      assertEquals(96, value.end());
      int idx = value.offset();
      // TODO - parse out the values when we have better helper methods.
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
  }
  
  @Test
  public void scanRollupAppends() throws Exception {
    int series_start = TS_ROLLUP_APPEND_SERIES;
    Scanner hbase_scanner = metricStartStopScanner(
        Series.ROLLUP_APPEND_SERIES, 
        METRIC_BYTES, 
        series_start,
        true);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, HOURLY_INTERVAL);
    trace = new MockTrace(true);
    
    scanner.fetchNext(null, trace.newSpan("UT").start());
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertTrue(scanner.last_pts.isEmpty());
    assertEquals(4, tsdb.runnables.size());
    
    final byte[] tsuid = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_MULTI_COLUMN_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    
    TimeStamp start = new SecondTimeStamp(series_start);
    TimeStamp end = start.getCopy();
    end.add(duration);
    
    for (int i = 0; i < 4; i++) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(4, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericSummaryPartialTimeSeries value = (Tsdb1xNumericSummaryPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) tsdb.runnables.get(i)).pts().value();
      assertEquals(0, value.offset());
      assertEquals(96, value.end());
      int idx = value.offset();
      // TODO - parse out the values when we have better helper methods.
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
  }
  
  @Test
  public void fetchNextOwnerException() throws Exception {
    when(owner.hasException()).thenReturn(true);
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, never()).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    assertEquals(0, tsdb.runnables.size());
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp(((long) TS_DOUBLE_SERIES + 
          (long) TS_DOUBLE_SERIES_INTERVAL) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(3)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyOneOfDoubleSeries(1);
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((long) (TS_DOUBLE_SERIES + 
          TS_DOUBLE_SERIES_INTERVAL * 4) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(6)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 5)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 5)), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyOneOfDoubleSeries(4);
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(17)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyOneOfDoubleSeries(16);
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp(((long) TS_DOUBLE_SERIES) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
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
    assertEquals(0, tsdb.runnables.size());
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((long) (TS_DOUBLE_SERIES + 
          TS_DOUBLE_SERIES_INTERVAL) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_INTERVAL * 2)), TAGK_BYTES, TAGV_B_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyOneOfDoubleSeries(1);
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(7)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyOneOfDoubleSeries(16);
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.skipNSUI()).thenReturn(true);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp(((long) TS_NSUI_SERIES) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
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
    assertEquals(0, tsdb.runnables.size());
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((long) (TS_NSUI_SERIES + 
          (TS_NSUI_SERIES_INTERVAL * 2)) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(),
        makeRowKey(METRIC_BYTES, (TS_NSUI_SERIES + (TS_NSUI_SERIES_INTERVAL * 3)), TAGK_BYTES, TAGV_BYTES));
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyNSUISeries(2);
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(7)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    verifyNSUISeries(16);
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
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp(((long) TS_NSUI_SERIES) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, never()).close();
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
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(1)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(State.EXCEPTION, scanner.state());
    verify(schema, times(2)).getName(UniqueIdType.METRIC, METRIC_BYTES, null);
    assertEquals(0, tsdb.runnables.size());
  }
  
  @Test
  public void fetchNextNoFiltersBuffer() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 7200L) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    verifySingleSeries(2);
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 18000L) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(4)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 21600), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 25200), TAGK_BYTES, TAGV_BYTES));
    verifySingleSeries(5);
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(9)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verifySingleSeries(16);
  }
  
  @Test
  public void fetchNextNoFiltersBufferSequenceEndInBuffer() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 3600L) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 7200), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    verifySingleSeries(1);
    
    // next fetch
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 7200L) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(2)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(1, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    verifySingleSeries(2);
    
    // final fetch
    when(node.sequenceEnd()).thenReturn(null);
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(9)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(3)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.COMPLETE, scanner.state());
    assertNull(scanner.buffer());
    verifySingleSeries(16);
  }
  
  @Test
  public void fetchNextNoFiltersBufferException() throws Exception {
    Scanner hbase_scanner = metricStartStopScanner(Series.SINGLE_SERIES, METRIC_BYTES);
    hbase_scanner.setMaxNumRows(2);
    Tsdb1xScanner scanner = new Tsdb1xScanner();
    scanner.reset(owner, hbase_scanner, 0, null);
    when(node.sequenceEnd())
      .thenReturn(new MillisecondTimeStamp((TS_SINGLE_SERIES + 3600L) * 1000));
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, never()).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, never()).exception(any(Throwable.class));
    assertEquals(State.CONTINUE, scanner.state());
    assertEquals(2, scanner.buffer().size());
    assertArrayEquals(scanner.buffer().get(0).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 7200), TAGK_BYTES, TAGV_BYTES));
    assertArrayEquals(scanner.buffer().get(1).get(0).key(), 
        makeRowKey(METRIC_BYTES, (TS_SINGLE_SERIES + 10800), TAGK_BYTES, TAGV_BYTES));
    verifySingleSeries(1);
    
    // next fetch
    when(node.sequenceEnd()).thenReturn(null);
    doThrow(new UnitTestException()).when(owner).currentDuration();
    
    scanner.fetchNext(null, null);
    
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
    verify(owner, times(1)).scannerDone();
    verify(owner, times(1)).exception(any(Throwable.class));
    assertEquals(State.EXCEPTION, scanner.state());
    assertNull(scanner.buffer());
    verifySingleSeries(1);
  }
  
  Scanner metricStartStopScanner(final Series series, 
                                 final byte[] metric) throws Exception {
    return metricStartStopScanner(series, metric, 0);
  }
  
  Scanner metricStartStopScanner(final Series series, 
                                 final byte[] metric, 
                                 final int start) throws Exception {
    return metricStartStopScanner(series, metric, start, false);
  }
  
  Scanner metricStartStopScanner(final Series series, 
                                 final byte[] metric, 
                                 final int start,
                                 final boolean is_rollup) throws Exception {
    final Scanner scanner = client.newScanner(is_rollup ? ROLLUP_TABLE : DATA_TABLE);
    switch (series) {
    case SINGLE_SERIES:
      int st = start > 0 ? start : TS_SINGLE_SERIES;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_SINGLE_SERIES_COUNT * TS_SINGLE_SERIES_INTERVAL), 
          (byte[][]) null));
      when(owner.currentTimestamps()).thenReturn(new Pair<TimeStamp, TimeStamp>(
          new SecondTimeStamp(st), 
          new SecondTimeStamp(st + (TS_SINGLE_SERIES_COUNT * TS_SINGLE_SERIES_INTERVAL))));
      duration = Duration.ofSeconds(TS_SINGLE_SERIES_INTERVAL);
      when(owner.currentDuration()).thenReturn(duration);
      setupSets(st, st + (TS_SINGLE_SERIES_COUNT * TS_SINGLE_SERIES_INTERVAL), 1);
      break;
    case SINGLE_SERIES_GAP:
      st = start > 0 ? start : TS_SINGLE_SERIES_GAP;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_SINGLE_SERIES_GAP_COUNT * TS_SINGLE_SERIES_GAP_INTERVAL), 
          (byte[][]) null));
      when(owner.currentTimestamps()).thenReturn(new Pair<TimeStamp, TimeStamp>(
          new SecondTimeStamp(st), 
          new SecondTimeStamp(st + (TS_SINGLE_SERIES_GAP_COUNT * TS_SINGLE_SERIES_GAP_INTERVAL))));
      duration = Duration.ofSeconds(TS_SINGLE_SERIES_GAP_INTERVAL);
      when(owner.currentDuration()).thenReturn(duration);
      setupSets(st, st + (TS_SINGLE_SERIES_GAP_COUNT * TS_SINGLE_SERIES_GAP_INTERVAL), 1);
      break;
    case DOUBLE_SERIES:
      st = start > 0 ? start : TS_DOUBLE_SERIES;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_DOUBLE_SERIES_COUNT * TS_DOUBLE_SERIES_INTERVAL), 
          (byte[][]) null));
      when(owner.currentTimestamps()).thenReturn(new Pair<TimeStamp, TimeStamp>(
          new SecondTimeStamp(st), 
          new SecondTimeStamp(st + (TS_DOUBLE_SERIES_COUNT * TS_DOUBLE_SERIES_INTERVAL))));
      duration = Duration.ofSeconds(TS_DOUBLE_SERIES_INTERVAL);
      when(owner.currentDuration()).thenReturn(duration);
      setupSets(st, st + (TS_DOUBLE_SERIES_COUNT * TS_DOUBLE_SERIES_INTERVAL), 1);
      break;
    case MULTI_SERIES_EX:
      st = start > 0 ? start : TS_MULTI_SERIES_EX;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_MULTI_SERIES_EX_COUNT * TS_MULTI_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    case NSUI_SERIES:
      st = start > 0 ? start : TS_NSUI_SERIES;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_NSUI_SERIES_COUNT * TS_NSUI_SERIES_INTERVAL), 
          (byte[][]) null));
      when(owner.currentTimestamps()).thenReturn(new Pair<TimeStamp, TimeStamp>(
          new SecondTimeStamp(st), 
          new SecondTimeStamp(st + (TS_NSUI_SERIES_COUNT * TS_NSUI_SERIES_INTERVAL))));
      duration = Duration.ofSeconds(TS_NSUI_SERIES_INTERVAL);
      when(owner.currentDuration()).thenReturn(duration);
      setupSets(st, st + (TS_NSUI_SERIES_COUNT * TS_NSUI_SERIES_INTERVAL), 1);
      break;
    case MULTI_COLUMN_SERIES:
      st = start > 0 ? start : TS_MULTI_COLUMN_SERIES;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_MULTI_COLUMN_SERIES_COUNT * TS_MULTI_COLUMN_SERIES_INTERVAL), 
          (byte[][]) null));
      when(owner.currentTimestamps()).thenReturn(new Pair<TimeStamp, TimeStamp>(
          new SecondTimeStamp(st), 
          new SecondTimeStamp(st + (TS_MULTI_COLUMN_SERIES_COUNT * TS_MULTI_COLUMN_SERIES_INTERVAL))));
      duration = Duration.ofSeconds(TS_MULTI_COLUMN_SERIES_INTERVAL);
      when(owner.currentDuration()).thenReturn(duration);
      setupSets(st, st + (TS_MULTI_COLUMN_SERIES_COUNT * TS_MULTI_COLUMN_SERIES_INTERVAL), 1);
      break;
    case APPEND_SERIES:
      st = start > 0 ? start : TS_APPEND_SERIES;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_APPEND_SERIES_COUNT * TS_APPEND_SERIES_INTERVAL), 
          (byte[][]) null));
      when(owner.currentTimestamps()).thenReturn(new Pair<TimeStamp, TimeStamp>(
          new SecondTimeStamp(st), 
          new SecondTimeStamp(st + (TS_APPEND_SERIES_COUNT * TS_APPEND_SERIES_INTERVAL))));
      duration = Duration.ofSeconds(TS_APPEND_SERIES_INTERVAL);
      when(owner.currentDuration()).thenReturn(duration);
      setupSets(st, st + (TS_APPEND_SERIES_COUNT * TS_APPEND_SERIES_INTERVAL), 1);
      break;
    case ROLLUP_SERIES:
      st = start > 0 ? start : TS_ROLLUP_SERIES;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_ROLLUP_SERIES_COUNT * TS_ROLLUP_SERIES_INTERVAL), 
          (byte[][]) null));
      when(owner.currentTimestamps()).thenReturn(new Pair<TimeStamp, TimeStamp>(
          new SecondTimeStamp(st), 
          new SecondTimeStamp(st + (TS_ROLLUP_SERIES_COUNT * TS_ROLLUP_SERIES_INTERVAL))));
      duration = Duration.ofSeconds(TS_ROLLUP_SERIES_INTERVAL);
      when(owner.currentDuration()).thenReturn(duration);
      setupSets(st, st + (TS_ROLLUP_SERIES_COUNT * TS_ROLLUP_SERIES_INTERVAL), 1);
      break;
    case ROLLUP_APPEND_SERIES:
      st = start > 0 ? start : TS_ROLLUP_APPEND_SERIES;
      scanner.setStartKey(makeRowKey(
          metric, 
          st, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          st + (TS_ROLLUP_APPEND_SERIES_COUNT * TS_ROLLUP_APPEND_SERIES_INTERVAL), 
          (byte[][]) null));
      when(owner.currentTimestamps()).thenReturn(new Pair<TimeStamp, TimeStamp>(
          new SecondTimeStamp(st), 
          new SecondTimeStamp(st + (TS_ROLLUP_APPEND_SERIES_COUNT * TS_ROLLUP_APPEND_SERIES_INTERVAL))));
      duration = Duration.ofSeconds(TS_ROLLUP_APPEND_SERIES_INTERVAL);
      when(owner.currentDuration()).thenReturn(duration);
      setupSets(st, st + (TS_ROLLUP_APPEND_SERIES_COUNT * TS_ROLLUP_APPEND_SERIES_INTERVAL), 1);
      break;
    default:
      throw new RuntimeException("YO! Implement me: " + series);
    }
    return scanner;
  }
  
  void setupSets(final long start, final long end, final int num_scanners) {
    TimeStamp start_ts = new SecondTimeStamp(start);
    TimeStamp end_ts = start_ts.getCopy();
    end_ts.add(duration);
    sets = new TLongObjectHashMap<Tsdb1xPartialTimeSeriesSet>();
    // grr, have to compute the total sets
    int total_sets = 0;
    TimeStamp clone = start_ts.getCopy();
    while (clone.epoch() < end) {
      total_sets++;
      clone.add(duration);
    }
    
    while (start_ts.epoch() < end) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) SET_POOL.claim().object();
      set.reset(node, 
          start_ts, 
          end_ts, 
          node.rollupUsage(),
          num_scanners, 
          total_sets);
      sets.put(start_ts.epoch(), set);
      start_ts.add(duration);
      end_ts.add(duration);
    }
    
    when(owner.currentSets()).thenReturn(sets);
    when(owner.scannersSize()).thenReturn(1);
    when(owner.getSet(any(TimeStamp.class))).thenAnswer(
        new Answer<Tsdb1xPartialTimeSeriesSet>() {
          @Override
          public Tsdb1xPartialTimeSeriesSet answer(InvocationOnMock invocation)
              throws Throwable {
            return sets.get(((TimeStamp) invocation.getArguments()[0]).epoch());
          }
    });
  }
  
  void verifySingleSeries(final int size) {
    verifyOneSeries(size, METRIC_BYTES, TAGV_BYTES, TS_SINGLE_SERIES);
  }
  
  void verifyNSUISeries(final int size) {
    verifyOneSeries(size, METRIC_BYTES, TAGV_BYTES, TS_NSUI_SERIES);
  }
  
  void verifyOneOfDoubleSeries(final int size) {
    verifyOneSeries(size, METRIC_BYTES, TAGV_B_BYTES, TS_DOUBLE_SERIES);
  }
  
  void verifyDoubleSeries(final int size) {
    assertEquals(size, tsdb.runnables.size());
    
    final byte[] tsuid_a = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_BYTES));
    final long hash_a = LongHashFunction.xx_r39().hashBytes(tsuid_a);
    final byte[] tsuid_b = schema.getTSUID(makeRowKey(
        METRIC_BYTES, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        TAGV_B_BYTES));
    final long hash_b = LongHashFunction.xx_r39().hashBytes(tsuid_b);
    TimeStamp start = new SecondTimeStamp(TS_DOUBLE_SERIES);
    TimeStamp end = start.getCopy();
    end.add(duration);
    int idx = 0;
    for (final Runnable runnable : tsdb.runnables) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(2, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) runnable).pts().value();
      assertEquals(0, value.offset());
      assertEquals(2, value.end());
      assertEquals(start.epoch(), value.data()[0]);
      assertEquals(1, value.data()[1]); // just the single value
      if (idx % 2 == 0) {
        assertEquals(hash_a, value.idHash());
      } else {
        assertEquals(hash_b, value.idHash());
        start.add(duration);
        end.add(duration);
      }
      idx++;
    }
  }
  
  void verifyOneSeries(final int size, 
                       final byte[] metric, 
                       final byte[] tagv, 
                       final long series_start) {
    assertEquals(size, tsdb.runnables.size());
    
    final byte[] tsuid = schema.getTSUID(makeRowKey(
        metric, 
        TS_SINGLE_SERIES, 
        TAGK_BYTES,
        tagv));
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    
    TimeStamp start = new SecondTimeStamp(series_start);
    TimeStamp end = start.getCopy();
    end.add(duration);
    for (final Runnable runnable : tsdb.runnables) {
      Tsdb1xPartialTimeSeriesSet set = (Tsdb1xPartialTimeSeriesSet) 
          ((PooledPartialTimeSeriesRunnable) runnable).pts().set();
      assertEquals(start.epoch(), set.start().epoch());
      assertEquals(end.epoch(), set.end().epoch());
      assertTrue(set.complete());
      assertEquals(1, set.timeSeriesCount());
      assertEquals(16, set.totalSets());
      assertSame(node, set.node());
      
      Tsdb1xNumericPartialTimeSeries value = (Tsdb1xNumericPartialTimeSeries)
          ((PooledPartialTimeSeriesRunnable) runnable).pts().value();
      assertEquals(0, value.offset());
      assertEquals(2, value.end());
      assertEquals(start.epoch(), value.data()[0]);
      assertEquals(1, value.data()[1]); // just the single value
      assertEquals(hash, value.idHash());
      
      start.add(duration);
      end.add(duration);
    }
  }
}
