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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.opentracing.Span;
import net.opentsdb.data.DataShard;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.query.context.QueryContext;

public class TestNumericMergeLargest {

  private QueryContext context;
  private TimeSeriesId id;
  private TimeStamp start;
  private TimeStamp end;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
    id = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("a")
        .addMetric("sys.cpu.user")
        .build();
    start = new MillisecondTimeStamp(1486045800000L);
    end = new MillisecondTimeStamp(1486045900000L);
  }
  
  @Test
  public void ctor() throws Exception {
    final NumericMergeLargest merger = new NumericMergeLargest();
    assertEquals(NumericType.TYPE, merger.type());
  }
  
  @Test
  public void mergeUniform() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 1);
    shard_b.add(1486045801000L, 42, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    shard_a.add(1486045871000L, 9866.854, 2);
    shard_b.add(1486045871000L, 9866.854, 2);
    shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -128, 2);
    shard_b.add(1486045881000L, -128, 2);
    shard_c.add(1486045881000L, -128, 2);
    
    final DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeSomeMissing() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);
    
    //shard_a.add(1486045801000L, 42, 1);
    //shard_b.add(1486045801000L, 42, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    shard_a.add(1486045871000L, 9866.854, 2);
    shard_b.add(1486045871000L, 9866.854, 2);
    //shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -128, 2);
    //shard_b.add(1486045881000L, -128, 2);
    //shard_c.add(1486045881000L, -128, 2);
    
    DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    // Total unalignment, just want to make sure it still works.
    shard_a = new NumericMillisecondShard(id, start, end);
    shard_b = new NumericMillisecondShard(id, start, end);
    shard_c = new NumericMillisecondShard(id, start, end);
    
    //shard_a.add(1486045801000L, 42, 1);
    //shard_b.add(1486045801000L, 42, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    //shard_a.add(1486045871000L, 9866.854, 2);
    shard_b.add(1486045871000L, 9866.854, 2);
    //shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -128, 2);
    //shard_b.add(1486045881000L, -128, 2);
    //shard_c.add(1486045881000L, -128, 2);
    
    merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    iterator = merged.iterator();
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    // One shard completely empty.
    shard_a = new NumericMillisecondShard(id, start, end);
    shard_b = new NumericMillisecondShard(id, start, end);
    shard_c = new NumericMillisecondShard(id, start, end);
    
    //shard_a.add(1486045801000L, 42, 1);
    //shard_b.add(1486045801000L, 42, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    //shard_a.add(1486045871000L, 9866.854, 2);
    shard_b.add(1486045871000L, 9866.854, 2);
    shard_c.add(1486045871000L, 9866.854, 2);
    
    //shard_a.add(1486045881000L, -128, 2);
    //shard_b.add(1486045881000L, -128, 2);
    shard_c.add(1486045881000L, -128, 2);
    
    merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    iterator = merged.iterator();
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void mergeDoubleVLong() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 1);
    shard_b.add(1486045801000L, 42.0, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    shard_a.add(1486045871000L, 9866.854, 2);
    shard_b.add(1486045871000L, 9866.854, 2);
    shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -128, 2);
    shard_b.add(1486045881000L, -128.0, 2);
    shard_c.add(1486045881000L, -128, 2);
    
    final DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void mergeDiffSameTypes() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 1);
    shard_b.add(1486045801000L, 84, 1);
    shard_c.add(1486045801000L, 41, 1);
    
    shard_a.add(1486045871000L, 866.854, 2);
    shard_b.add(1486045871000L, 11024.342, 2);
    shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -127, 2);
    shard_b.add(1486045881000L, -132, 2);
    shard_c.add(1486045881000L, -128, 2);
    
    final DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(84, v.value().longValue());
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(11024.342, v.value().doubleValue(), 0.001);
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-127, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeDiffDiffTypes() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 1);
    shard_b.add(1486045801000L, 84.56, 1);
    shard_c.add(1486045801000L, 41.2, 1);
    
    shard_a.add(1486045871000L, 866.854, 2);
    shard_b.add(1486045871000L, 11024, 2);
    shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -127.957, 2);
    shard_b.add(1486045881000L, -132.85, 2);
    shard_c.add(1486045881000L, -128, 2);
    
    final DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(84.56, v.value().doubleValue(), 0.0001);
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(11024, v.value().longValue());
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(-127.957, v.value().doubleValue(), 0.0001);
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeDiffCounts() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 2);
    shard_b.add(1486045801000L, 84.56, 1);
    shard_c.add(1486045801000L, 41.2, 1);
    
    shard_a.add(1486045871000L, 866.854, 2);
    shard_b.add(1486045871000L, 11024, 2);
    shard_c.add(1486045871000L, 9866.854, 3);
    
    shard_a.add(1486045881000L, -127.957, 2);
    shard_b.add(1486045881000L, -132.85, 8);
    shard_c.add(1486045881000L, -128, 2);
    
    final DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(84.56, v.value().doubleValue(), 0.0001);
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(11024, v.value().longValue());
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(-127.957, v.value().doubleValue(), 0.0001);
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeNaNsAndInfinites() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 2);
    shard_b.add(1486045801000L, Double.NaN, 1);
    shard_c.add(1486045801000L, 41.2, 1);
    
    shard_a.add(1486045871000L, 866.854, 2);
    shard_b.add(1486045871000L, Double.POSITIVE_INFINITY, 2);
    shard_c.add(1486045871000L, 9866.854, 3);
    
    shard_a.add(1486045881000L, Double.NEGATIVE_INFINITY, 2);
    shard_b.add(1486045881000L, -132.85, 8);
    shard_c.add(1486045881000L, -128, 2);
    
    DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.0001);
    assertEquals(3, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    // one shard TS nan'd
    shard_a = new NumericMillisecondShard(id, start, end);
    shard_b = new NumericMillisecondShard(id, start, end);
    shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 2);
    shard_b.add(1486045801000L, Double.NaN, 1);
    shard_c.add(1486045801000L, 41.2, 1);
    
    shard_a.add(1486045871000L, Double.NaN, 0);
    shard_b.add(1486045871000L, Double.POSITIVE_INFINITY, 2);
    shard_c.add(1486045871000L, Double.NEGATIVE_INFINITY, 0);
    
    shard_a.add(1486045881000L, Double.NEGATIVE_INFINITY, 2);
    shard_b.add(1486045881000L, -132.85, 8);
    shard_c.add(1486045881000L, -128, 2);
    
    merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    iterator = merged.iterator();
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertEquals(0, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    shard_a = new NumericMillisecondShard(id, start, end);
    shard_b = new NumericMillisecondShard(id, start, end);
    shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 2);
    shard_b.add(1486045801000L, Double.NaN, 1);
    shard_c.add(1486045801000L, 41.2, 1);
    
    shard_a.add(1486045871000L, Double.POSITIVE_INFINITY, 0);
    shard_b.add(1486045871000L, Double.NaN, 2);
    shard_c.add(1486045871000L, Double.NEGATIVE_INFINITY, 0);
    
    shard_a.add(1486045881000L, Double.NEGATIVE_INFINITY, 2);
    shard_b.add(1486045881000L, -132.85, 8);
    shard_c.add(1486045881000L, -128, 2);
    
    merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    iterator = merged.iterator();
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    assertEquals(0, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void mergeAllEmpty() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);

    final DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged.iterator();
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeExceptions() throws Exception {
    NumericMillisecondShard shard_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard shard_c = new NumericMillisecondShard(id, start, end);
    
    shard_a.add(1486045801000L, 42, 1);
    shard_b.add(1486045801000L, 42, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    try {
      new NumericMergeLargest().merge(null, 
          Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
          context, null);
      fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) { }
    
    try {
     new NumericMergeLargest().merge(id, null, context, null);
     fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new NumericMergeLargest().merge(id, 
          Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
          null, null);
      fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) { }
    
    try {
      new NumericMergeLargest().merge(id, 
          Lists.<DataShard<?>>newArrayList(shard_a, null, shard_c), 
          context, null);
      fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) { }
    
    @SuppressWarnings("unchecked")
    DataShard<AnnotationType> mock = mock(DataShard.class);
    when(mock.type()).thenReturn(AnnotationType.TYPE);
    try {
      new NumericMergeLargest().merge(id, 
          Lists.<DataShard<?>>newArrayList(shard_a, mock, shard_c), context, null);
      fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) { }
  }

  @Test
  public void mergeOrder() throws Exception {
    NumericMillisecondShard shard_a = 
        new NumericMillisecondShard(id, start, end, 42);
    // no check on order. First one in wins.
    NumericMillisecondShard shard_b = 
        new NumericMillisecondShard(id, start, end, 24);
    NumericMillisecondShard shard_c = 
        new NumericMillisecondShard(id, start, end, 42);
    
    shard_a.add(1486045801000L, 42, 1);
    shard_b.add(1486045801000L, 42, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    shard_a.add(1486045871000L, 9866.854, 2);
    shard_b.add(1486045871000L, 9866.854, 2);
    shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -128, 2);
    shard_b.add(1486045881000L, -128, 2);
    shard_c.add(1486045881000L, -128, 2);
    
    final DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, null);
    assertSame(id, merged.id());
    assertEquals(42, merged.order());
    
    final TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeWithSpan() throws Exception {
    final Span tracer = mock(Span.class);
    NumericMillisecondShard shard_a = 
        new NumericMillisecondShard(id, start, end, 42);
    // no check on order. First one in wins.
    NumericMillisecondShard shard_b = 
        new NumericMillisecondShard(id, start, end, 24);
    NumericMillisecondShard shard_c = 
        new NumericMillisecondShard(id, start, end, 42);
    
    shard_a.add(1486045801000L, 42, 1);
    shard_b.add(1486045801000L, 42, 1);
    shard_c.add(1486045801000L, 42, 1);
    
    shard_a.add(1486045871000L, 9866.854, 2);
    shard_b.add(1486045871000L, 9866.854, 2);
    shard_c.add(1486045871000L, 9866.854, 2);
    
    shard_a.add(1486045881000L, -128, 2);
    shard_b.add(1486045881000L, -128, 2);
    shard_c.add(1486045881000L, -128, 2);
    
    final DataShard<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<DataShard<?>>newArrayList(shard_a, shard_b, shard_c), 
        context, tracer);
    assertSame(id, merged.id());
    assertEquals(42, merged.order());
    
    final TimeSeriesIterator<NumericType> iterator = merged.iterator();
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    assertEquals(1, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    assertEquals(2, v.realCount());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    assertEquals(2, v.realCount());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    verify(tracer, times(3)).setTag(anyString(), anyInt());
  }
}
