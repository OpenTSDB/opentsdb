// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.pools.NoDataPartialTimeSeriesPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.rollup.RollupUtils.RollupUsage;

public class TestTsdb1xPartialTimeSeriesSet {
  private static MockTSDB TSDB;
  private static ObjectPool RUNNABLE_POOL;
  private static ObjectPool NO_DATA_POOL;
  
  private Tsdb1xQueryNode node;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    RUNNABLE_POOL = mock(ObjectPool.class);
    NO_DATA_POOL = mock(ObjectPool.class);
    
    when(RUNNABLE_POOL.claim()).thenAnswer(new Answer<PooledObject>() {
      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        return new PooledPartialTimeSeriesRunnable();
      }
    });
    when(TSDB.registry.getObjectPool(PooledPartialTimeSeriesRunnablePool.TYPE))
      .thenReturn(RUNNABLE_POOL);
    when(NO_DATA_POOL.claim()).thenAnswer(new Answer<PooledObject>() {
      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        final NoDataPartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
        when(pts.object()).thenReturn(pts);
        return pts;
      }
    });
    when(TSDB.registry.getObjectPool(NoDataPartialTimeSeriesPool.TYPE))
      .thenReturn(NO_DATA_POOL);
  }
  
  @Before
  public void before() throws Exception {
    node = mock(Tsdb1xQueryNode.class);
    QueryNodeConfig config = mock(QueryNodeConfig.class);
    when(config.getId()).thenReturn("Mock");
    when(node.config()).thenReturn(config);
    TSDB.runnables.clear();
  }
  
  @Test
  public void ctor() throws Exception {
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    assertSame(RUNNABLE_POOL, set.runnable_pool);
    assertSame(NO_DATA_POOL, set.no_data_pool);
    assertNull(set.node());
    assertEquals(0, set.start().epoch());
    assertEquals(0, set.end().epoch());
    assertEquals(0, set.totalSets());
    assertFalse(set.complete());
    assertEquals(0, set.timeSeriesCount());
    assertNull(set.timeSpecification());
    assertNull(set.pts);
  }
  
  @Test
  public void resetAndClose() throws Exception {
    PooledObject pooled_object = mock(PooledObject.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.setPooledObject(pooled_object);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    assertSame(node, set.node());
    assertEquals(1546300800, set.start().epoch());
    assertEquals(1546304400, set.end().epoch());
    assertEquals(1, set.totalSets());
    assertFalse(set.complete());
    assertEquals(0, set.timeSeriesCount());
    assertNull(set.timeSpecification());
    assertNull(set.pts);
    assertEquals("Mock", set.dataSource());
    
    set.close();
    assertNull(set.node);
    verify(pooled_object, times(1)).release();
  }
  
  @Test
  public void incrementOne() throws Exception {
    Tsdb1xPartialTimeSeries pts = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.increment(pts, false);
    
    verify(node, never()).onNext(pts);
    verify(node, times(1)).setSentData();
    assertEquals(1, set.latch);
    assertEquals(1, set.timeSeriesCount());
    assertFalse(set.complete());
    assertTrue(TSDB.runnables.isEmpty());
    assertSame(pts, set.pts);
  }
  
  @Test
  public void incrementOneComplete() throws Exception {
    Tsdb1xPartialTimeSeries pts = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.increment(pts, true);
    
    verify(node, never()).onNext(pts);
    verify(node, times(1)).setSentData();
    assertEquals(0, set.latch);
    assertEquals(1, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    assertNull(set.pts);
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts);
  }
  
  @Test
  public void incrementTwo() throws Exception {
    Tsdb1xPartialTimeSeries pts1 = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeries pts2 = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.increment(pts1, false);
    
    verify(node, never()).onNext(pts1);
    verify(node, never()).onNext(pts2);
    verify(node, times(1)).setSentData();
    assertEquals(1, set.latch);
    assertEquals(1, set.timeSeriesCount());
    assertFalse(set.complete());
    assertTrue(TSDB.runnables.isEmpty());
    assertSame(pts1, set.pts);
    
    set.increment(pts2, false);
    
    verify(node, never()).onNext(pts1);
    verify(node, never()).onNext(pts2);
    verify(node, times(2)).setSentData();
    assertEquals(1, set.latch);
    assertEquals(2, set.timeSeriesCount());
    assertFalse(set.complete());
    assertEquals(1, TSDB.runnables.size());
    assertSame(pts2, set.pts);
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts1, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts1);
    verify(pts2, never()).dedupe(false, false);
    verify(node, never()).onNext(pts2);
  }
  
  @Test
  public void incrementTwoComplete() throws Exception {
    Tsdb1xPartialTimeSeries pts1 = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeries pts2 = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.increment(pts1, false);
    
    verify(node, never()).onNext(pts1);
    verify(node, never()).onNext(pts2);
    verify(node, times(1)).setSentData();
    assertEquals(1, set.latch);
    assertEquals(1, set.timeSeriesCount());
    assertFalse(set.complete());
    assertTrue(TSDB.runnables.isEmpty());
    assertSame(pts1, set.pts);
    
    set.increment(pts2, true);
    
    verify(node, never()).onNext(pts1);
    verify(node, never()).onNext(pts2);
    verify(node, times(2)).setSentData();
    assertEquals(0, set.latch);
    assertEquals(2, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(2, TSDB.runnables.size());
    assertNull(set.pts);
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts1, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts1);
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(1)).run();
    verify(pts2, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts2);
  }

  @Test
  public void incrementTwoCompleteTwoSalts() throws Exception {
    Tsdb1xPartialTimeSeries pts1 = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeries pts2 = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        2, 1);
    set.increment(pts1, false);
    
    verify(node, never()).onNext(pts1);
    verify(node, never()).onNext(pts2);
    verify(node, times(1)).setSentData();
    assertEquals(2, set.latch);
    assertEquals(1, set.timeSeriesCount());
    assertFalse(set.complete());
    assertTrue(TSDB.runnables.isEmpty());
    assertSame(pts1, set.pts);
    
    set.increment(pts2, true);
    
    verify(node, never()).onNext(pts1);
    verify(node, never()).onNext(pts2);
    verify(node, times(2)).setSentData();
    assertEquals(1, set.latch);
    assertEquals(2, set.timeSeriesCount());
    assertFalse(set.complete());
    assertEquals(1, TSDB.runnables.size());
    assertSame(pts2, set.pts);
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts1, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts1);
  }
  
  @Test
  public void incrementBadPTS() throws Exception {
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    try {
      set.increment(null, false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void setCompleteAndEmptyNoQueueNotFinal() throws Exception {
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.setCompleteAndEmpty(false);
    verify(node, never()).setSentData();
    assertEquals(0, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertTrue(set.complete());
    assertTrue(TSDB.runnables.isEmpty());
  }
  
  @Test
  public void setCompleteAndEmptyNoQueueFinal() throws Exception {
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.setCompleteAndEmpty(true);
    verify(node, never()).setSentData();
    assertEquals(0, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(node, times(1)).onNext(any(NoDataPartialTimeSeries.class));
  }
  
  @Test
  public void setCompleteAndEmptyNoQueueNoFallback() throws Exception {
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_NOFALLBACK, 
        1, 1);
    set.setCompleteAndEmpty(false);
    verify(node, never()).setSentData();
    assertEquals(0, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(node, times(1)).onNext(any(NoDataPartialTimeSeries.class));
  }
  
  @Test
  public void setCompleteAndEmptyWithQueueNotFinal() throws Exception {
    Tsdb1xPartialTimeSeries pts = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.pts = pts;
    set.setCompleteAndEmpty(false);
    verify(node, never()).setSentData();
    assertEquals(0, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts);
  }
  
  @Test
  public void setCompleteAndEmptyWithQueueFinal() throws Exception {
    Tsdb1xPartialTimeSeries pts = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.pts = pts;
    set.setCompleteAndEmpty(true);
    verify(node, never()).setSentData();
    assertEquals(0, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts);
  }
  
  @Test
  public void setCompleteAndEmptyWithQueueNoFallback() throws Exception {
    Tsdb1xPartialTimeSeries pts = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_NOFALLBACK, 
        1, 1);
    set.pts = pts;
    set.setCompleteAndEmpty(false);
    verify(node, never()).setSentData();
    assertEquals(0, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts);
  }

  @Test
  public void setCompleteAndEmptyNoQueueNotFinalTwoSalts() throws Exception {
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        2, 1);
    set.setCompleteAndEmpty(false);
    verify(node, never()).setSentData();
    assertEquals(1, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertFalse(set.complete());
    assertTrue(TSDB.runnables.isEmpty());
  }

  @Test
  public void incrementCompleteThenCompleteEmptySalted() throws Exception {
    Tsdb1xPartialTimeSeries pts = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        2, 1);
    set.increment(pts, true);
    
    verify(node, times(1)).setSentData();
    verify(node, never()).onNext(pts);
    assertEquals(1, set.latch);
    assertEquals(1, set.timeSeriesCount());
    assertFalse(set.complete());
    assertTrue(TSDB.runnables.isEmpty());
    assertSame(pts, set.pts);
    
    set.setCompleteAndEmpty(false);
    verify(node, times(1)).setSentData();
    assertEquals(0, set.latch);
    assertEquals(1, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    assertNull(set.pts);
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts);
  }
  
  @Test
  public void setCompleteEmptyThenIncrementCompleteSalted() throws Exception {
    Tsdb1xPartialTimeSeries pts = mock(Tsdb1xPartialTimeSeries.class);
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        2, 1);
    set.setCompleteAndEmpty(false);
    
    verify(node, never()).setSentData();
    verify(node, never()).onNext(pts);
    assertEquals(1, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertFalse(set.complete());
    assertTrue(TSDB.runnables.isEmpty());
    assertNull(set.pts);
    
    set.increment(pts, true);
    
    verify(node, times(1)).setSentData();
    assertEquals(0, set.latch);
    assertEquals(1, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    assertNull(set.pts);
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(pts, times(1)).dedupe(false, false);
    verify(node, times(1)).onNext(pts);
  }

  @Test
  public void sendEmpty() throws Exception {
    Tsdb1xPartialTimeSeriesSet set = new Tsdb1xPartialTimeSeriesSet(TSDB);
    set.reset(node, 
        new SecondTimeStamp(1546300800), 
        new SecondTimeStamp(1546304400), 
        RollupUsage.ROLLUP_FALLBACK, 
        1, 1);
    set.sendEmpty();
    
    verify(node, never()).setSentData();
    assertEquals(0, set.latch);
    assertEquals(0, set.timeSeriesCount());
    assertTrue(set.complete());
    assertEquals(1, TSDB.runnables.size());
    
    // run it
    ((PooledPartialTimeSeriesRunnable) TSDB.runnables.get(0)).run();
    verify(node, times(1)).onNext(any(NoDataPartialTimeSeries.class));
  }

}
