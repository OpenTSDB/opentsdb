// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.DefaultQueryContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.processor.DefaultTimeSeriesProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessor;

public class TestNumericMillisecondShard {

  private TimeSeriesId id;
  private TimeStamp start;
  private TimeStamp end;
  
  @Before
  public void before() throws Exception {
    id = BaseTimeSeriesId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    start = new MillisecondTimeStamp(0L);
    end = new MillisecondTimeStamp(3600000);
  }
  
  @Test
  public void ctor() throws Exception {
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    assertEquals(4, shard.encodeOn());
    assertEquals(4, shard.offsets().length);
    assertEquals(4, shard.values().length);
    assertEquals(0L, shard.startTime().msEpoch());
    assertEquals(3600000, shard.endTime().msEpoch());
    assertEquals(-1, shard.order());
    assertSame(id, shard.id());
    try {
      shard.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    shard = new NumericMillisecondShard(id, start, end, 42);
    assertEquals(4, shard.encodeOn());
    assertEquals(4, shard.offsets().length);
    assertEquals(4, shard.values().length);
    assertEquals(0L, shard.startTime().msEpoch());
    assertEquals(3600000, shard.endTime().msEpoch());
    assertEquals(42, shard.order());
    assertSame(id, shard.id());
    try {
      shard.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    shard = new NumericMillisecondShard(id, start, end, 42, 100);
    assertEquals(4, shard.encodeOn());
    assertEquals(400, shard.offsets().length);
    assertEquals(400, shard.values().length);
    assertEquals(0L, shard.startTime().msEpoch());
    assertEquals(3600000, shard.endTime().msEpoch());
    assertEquals(42, shard.order());
    assertSame(id, shard.id());
    try {
      shard.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    end = new MillisecondTimeStamp(1L);
    shard = new NumericMillisecondShard(id, start, end, 42, 0);
    end = new MillisecondTimeStamp(86400000L);
    assertEquals(1, shard.encodeOn());
    assertEquals(0, shard.offsets().length);
    assertEquals(0, shard.values().length);
    assertEquals(0L, shard.startTime().msEpoch());
    assertEquals(1L, shard.endTime().msEpoch());
    assertEquals(42, shard.order());
    assertSame(id, shard.id());
    try {
      shard.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    try {
      shard = new NumericMillisecondShard(null, start, end, 42, 100);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      shard = new NumericMillisecondShard(id, end, start, 42, 100);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      shard = new NumericMillisecondShard(id, null, end, 42, 100);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      shard = new NumericMillisecondShard(id, start, null, 42, 100);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      shard = new NumericMillisecondShard(id, start, end, 42, -1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void peek() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045900000L);
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    
    assertNull(shard.peek());

    shard.add(1486045801000L, 42);
    
    TimeSeriesValue<NumericType> v = shard.peek();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(42, v.value().longValue());
  }
  
  @Test
  public void add() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045900000L);
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    assertEquals(3, shard.encodeOn());
    assertEquals(3, shard.offsets().length);
    assertEquals(4, shard.values().length);
    assertEquals(1486045800000L, shard.startTime().msEpoch());
    assertEquals(1486045900000L, shard.endTime().msEpoch());
    
    assertArrayEquals(new byte[] { 0, 0, 0 }, shard.offsets());
    assertArrayEquals(new byte[] { 0, 0, 0, 0 }, shard.values());
    
    shard.add(1486045801000L, 42);
    assertEquals(6, shard.offsets().length); // expanded
    assertEquals(4, shard.values().length);
    assertArrayEquals(new byte[] { 1, -12, 0, 0, 0, 0 }, shard.offsets());
    assertArrayEquals(new byte[] { 42, 0, 0, 0 }, shard.values());
    
    shard.add(1486045871000L, 9866.854);
    assertEquals(12, shard.offsets().length); // expanded
    assertEquals(16, shard.values().length);
    assertArrayEquals(new byte[] { 1, -12, 0, -118, -84, 15, 0, 0, 0, 0, 0, 0 }, 
        shard.offsets());
    assertArrayEquals(new byte[] { 42, 64, -61, 69, 109, 79, -33, 59, 
        100, 0, 0, 0, 0, 0, 0, 0 }, shard.values());
    
    // less than not allowed
    try {
      shard.add(1486045800000L, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    // same not allowed
    try {
      shard.add(1486045871000L, 9866.854);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // too late
    try {
      shard.add(1486045900001L, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // too early
    shard = new NumericMillisecondShard(id, start, end);
    try {
      shard.add(1486045799999L, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // Ugly to test, so use the next() tests for more checks.
  }
  
  @Test
  public void next() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486046000000L);
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    
    TimeSeriesValue<NumericType> v = shard.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = shard.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.00001);
    
    v = shard.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    // add after is ok.
    shard.add(1486045891000L, Long.MAX_VALUE);
    v = shard.next();
    assertEquals(1486045891000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(Long.MAX_VALUE, v.value().longValue());
    
    shard.add(1486045891050L, Long.MIN_VALUE);
    v = shard.next();
    assertEquals(1486045891050L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(Long.MIN_VALUE, v.value().longValue());
    
    shard.add(1486045901571L, Double.MAX_VALUE);
    v = shard.next();
    assertEquals(1486045901571L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(Double.MAX_VALUE, v.value().doubleValue(), 0.00001);
    
    shard.add(1486045901572L, Double.MIN_VALUE);
    v = shard.next();
    assertEquals(1486045901572L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(Double.MIN_NORMAL, v.value().doubleValue(), 0.00001);
    
    shard.add(1486045902000L, 0);
    v = shard.next();
    assertEquals(1486045902000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(0, v.value().longValue());
    
    shard.add(1486045903000L, 0f);
    v = shard.next();
    assertEquals(1486045903000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(0, v.value().doubleValue(), 0.0001);
    
    shard.add(1486045904000L, Double.POSITIVE_INFINITY);
    v = shard.next();
    assertEquals(1486045904000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isInfinite(v.value().doubleValue()));
    
    shard.add(1486045905000L, Double.NaN);
    v = shard.next();
    assertEquals(1486045905000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    try {
      v = shard.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void withContext() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045890000L);
    final TSDB tsdb = mock(TSDB.class);
    final QueryContext context = new DefaultQueryContext(tsdb, 
        mock(ExecutionGraph.class));
    final TimeSeriesProcessor group = new DefaultTimeSeriesProcessor(context);
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    group.addSeries(new SimpleStringGroupId("a"), shard);
    
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(1486045801000L, context.nextTimestamp().msEpoch());
    
    // fill as if another series had an earlier dp.
    context.updateContext(IteratorStatus.HAS_DATA, 
        new MillisecondTimeStamp(1486045800000L));
    assertEquals(IteratorStatus.END_OF_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(Long.MAX_VALUE, context.syncTimestamp().msEpoch());
    assertEquals(1486045800000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = shard.next();
    assertEquals(1486045800000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045800000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045801000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = shard.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    // fill in middle
    context.updateContext(IteratorStatus.HAS_DATA, 
        new MillisecondTimeStamp(1486045851000L));
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045801000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045851000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = shard.next();
    assertEquals(1486045851000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045851000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045871000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = shard.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.00001);
    
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.HAS_DATA, context.nextStatus());
    assertEquals(1486045871000L, context.syncTimestamp().msEpoch());
    assertEquals(1486045881000L, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = shard.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(1486045881000L, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    // post data fill
    context.updateContext(IteratorStatus.HAS_DATA, 
        new MillisecondTimeStamp(1486045891000L));
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = shard.next();
    assertEquals(1486045891000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.currentStatus());
    assertEquals(IteratorStatus.END_OF_DATA, context.nextStatus());
    assertEquals(1486045891000L, context.syncTimestamp().msEpoch());
    assertEquals(Long.MAX_VALUE, context.nextTimestamp().msEpoch());
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void getCopy() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045890000L);
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    
    TimeSeriesValue<NumericType> v = shard.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    NumericMillisecondShard clone = (NumericMillisecondShard) shard.getShallowCopy(null);
    v = clone.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    assertNotSame(shard, clone);
    assertSame(shard.offsets(), clone.offsets());
    assertSame(shard.values(), clone.values());
    
    try {
      shard.add(1486045891000L, 24);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      clone.add(1486045891000L, 24);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void getCopyTimeRange() throws Exception {
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045890000L);
    NumericMillisecondShard shard = new NumericMillisecondShard(id, start, end);
    shard.add(1486045801000L, 42);
    shard.add(1486045871000L, 9866.854);
    shard.add(1486045881000L, -128);
    
    TimeSeriesValue<NumericType> v = shard.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    TimeStamp start = new MillisecondTimeStamp(1486045800000L);
    TimeStamp end = new MillisecondTimeStamp(1486045800900L);
    
    // range before the start of shard
    NumericMillisecondShard clone = (NumericMillisecondShard) shard.getDeepCopy(null,
        start, end);
    assertEquals(IteratorStatus.END_OF_DATA, clone.status());
    // no movement in the original
    assertEquals(IteratorStatus.HAS_DATA, shard.status());
    assertEquals(1486045871000L, shard.peek().timestamp().msEpoch());
    
    // range includes some data
    end = new MillisecondTimeStamp(1486045871000L);
    clone = (NumericMillisecondShard) shard.getDeepCopy(null, start, end);
    assertEquals(IteratorStatus.HAS_DATA, clone.status());
    v = clone.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertEquals(42, v.value().longValue());
    
    assertEquals(IteratorStatus.HAS_DATA, clone.status());
    v = clone.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    assertEquals(IteratorStatus.END_OF_DATA, clone.status());
    // no movement in the original
    assertEquals(IteratorStatus.HAS_DATA, shard.status());
    assertEquals(1486045871000L, shard.peek().timestamp().msEpoch());
    
    // empty middle
    start = new MillisecondTimeStamp(1486045801100L);
    end = new MillisecondTimeStamp(1486045870900L);
    clone = (NumericMillisecondShard) shard.getDeepCopy(null, start, end);
    assertEquals(IteratorStatus.END_OF_DATA, clone.status());
    // no movement in the original
    assertEquals(IteratorStatus.HAS_DATA, shard.status());
    assertEquals(1486045871000L, shard.peek().timestamp().msEpoch());
    
    // range overlaps end
    start = new MillisecondTimeStamp(1486045871000L);
    end = new MillisecondTimeStamp(1486045882000L);
    
    clone = (NumericMillisecondShard) shard.getDeepCopy(null, start, end);
    assertEquals(IteratorStatus.HAS_DATA, clone.status());
    v = clone.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    assertEquals(IteratorStatus.HAS_DATA, clone.status());
    v = clone.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertEquals(-128, v.value().longValue());
    
    assertEquals(IteratorStatus.END_OF_DATA, clone.status());
    // no movement in the original
    assertEquals(IteratorStatus.HAS_DATA, shard.status());
    assertEquals(1486045871000L, shard.peek().timestamp().msEpoch());
    
    // past the end
    start = new MillisecondTimeStamp(1486045882000L);
    end = new MillisecondTimeStamp(1486045884000L);
    clone = (NumericMillisecondShard) shard.getDeepCopy(null, start, end);
    assertEquals(IteratorStatus.END_OF_DATA, clone.status());
    // no movement in the original
    assertEquals(IteratorStatus.HAS_DATA, shard.status());
    assertEquals(1486045871000L, shard.peek().timestamp().msEpoch());
    
    // empty source
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045890000L);
    shard = new NumericMillisecondShard(id, start, end);
    clone = (NumericMillisecondShard) shard.getDeepCopy(null, start, end);
    assertEquals(IteratorStatus.END_OF_DATA, clone.status());
    assertEquals(IteratorStatus.END_OF_DATA, shard.status());
    
    try {
      clone = (NumericMillisecondShard) shard.getDeepCopy(null, null, end);
      fail("Expected IllegalArgumentException");
    } catch(IllegalArgumentException e) { }
    
    try {
      clone = (NumericMillisecondShard) shard.getDeepCopy(null, start, null);
      fail("Expected IllegalArgumentException");
    } catch(IllegalArgumentException e) { }
    
    try {
      clone = (NumericMillisecondShard) shard.getDeepCopy(null, end, start);
      fail("Expected IllegalArgumentException");
    } catch(IllegalArgumentException e) { }
  }
}
