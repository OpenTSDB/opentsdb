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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import io.opentracing.Span;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.BaseTimeSeriesId;
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
    id = BaseTimeSeriesId.newBuilder()
        .setAlias("a")
        .setMetric("sys.cpu.user")
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
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 42);
    it_c.add(1486045801000L, 42);
    
    it_a.add(1486045871000L, 9866.854);
    it_b.add(1486045871000L, 9866.854);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -128);
    it_b.add(1486045881000L, -128);
    it_c.add(1486045881000L, -128);
    
    final TimeSeriesIterator<NumericType> merged = new NumericMergeLargest()
        .merge(id, Lists.<TimeSeriesIterator<?>>newArrayList(
            it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeSomeMissing() throws Exception {
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);
    
    //it_a.add(1486045801000L, 42);
    //it_b.add(1486045801000L, 42);
    it_c.add(1486045801000L, 42);
    
    it_a.add(1486045871000L, 9866.854);
    it_b.add(1486045871000L, 9866.854);
    //it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -128);
    //it_b.add(1486045881000L, -128);
    //it_c.add(1486045881000L, -128);
    
    TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    // Total unalignment, just want to make sure it still works.
    it_a = new NumericMillisecondShard(id, start, end);
    it_b = new NumericMillisecondShard(id, start, end);
    it_c = new NumericMillisecondShard(id, start, end);
    
    //it_a.add(1486045801000L, 42);
    //it_b.add(1486045801000L, 42);
    it_c.add(1486045801000L, 42);
    
    //it_a.add(1486045871000L, 9866.854);
    it_b.add(1486045871000L, 9866.854);
    //it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -128);
    //it_b.add(1486045881000L, -128);
    //it_c.add(1486045881000L, -128);
    
    merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    iterator = merged;
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    // One shard completely empty.
    it_a = new NumericMillisecondShard(id, start, end);
    it_b = new NumericMillisecondShard(id, start, end);
    it_c = new NumericMillisecondShard(id, start, end);
    
    //it_a.add(1486045801000L, 42);
    //it_b.add(1486045801000L, 42);
    it_c.add(1486045801000L, 42);
    
    //it_a.add(1486045871000L, 9866.854);
    it_b.add(1486045871000L, 9866.854);
    it_c.add(1486045871000L, 9866.854);
    
    //it_a.add(1486045881000L, -128);
    //it_b.add(1486045881000L, -128);
    it_c.add(1486045881000L, -128);
    
    merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    iterator = merged;
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void mergeDoubleVLong() throws Exception {
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 42.0);
    it_c.add(1486045801000L, 42);
    
    it_a.add(1486045871000L, 9866.854);
    it_b.add(1486045871000L, 9866.854);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -128);
    it_b.add(1486045881000L, -128.0);
    it_c.add(1486045881000L, -128);
    
    final TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void mergeDiffSameTypes() throws Exception {
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 84);
    it_c.add(1486045801000L, 41);
    
    it_a.add(1486045871000L, 866.854);
    it_b.add(1486045871000L, 11024.342);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -127);
    it_b.add(1486045881000L, -132);
    it_c.add(1486045881000L, -128);
    
    final TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(84, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(11024.342, v.value().doubleValue(), 0.001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-127, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeDiffDiffTypes() throws Exception {
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 84.56);
    it_c.add(1486045801000L, 41.2);
    
    it_a.add(1486045871000L, 866.854);
    it_b.add(1486045871000L, 11024);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -127.957);
    it_b.add(1486045881000L, -132.85);
    it_c.add(1486045881000L, -128);
    
    final TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(84.56, v.value().doubleValue(), 0.0001);
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(11024, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(-127.957, v.value().doubleValue(), 0.0001);
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeDiffCounts() throws Exception {
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 84.56);
    it_c.add(1486045801000L, 41.2);
    
    it_a.add(1486045871000L, 866.854);
    it_b.add(1486045871000L, 11024);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -127.957);
    it_b.add(1486045881000L, -132.85);
    it_c.add(1486045881000L, -128);
    
    final TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(84.56, v.value().doubleValue(), 0.0001);
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(11024, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(-127.957, v.value().doubleValue(), 0.0001);
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeNaNsAndInfinites() throws Exception {
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, Double.NaN);
    it_c.add(1486045801000L, 41.2);
    
    it_a.add(1486045871000L, 866.854);
    it_b.add(1486045871000L, Double.POSITIVE_INFINITY);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, Double.NEGATIVE_INFINITY);
    it_b.add(1486045881000L, -132.85);
    it_c.add(1486045881000L, -128);
    
    TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.0001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    // one shard TS nan'd
    it_a = new NumericMillisecondShard(id, start, end);
    it_b = new NumericMillisecondShard(id, start, end);
    it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, Double.NaN);
    it_c.add(1486045801000L, 41.2);
    
    it_a.add(1486045871000L, Double.NaN);
    it_b.add(1486045871000L, Double.POSITIVE_INFINITY);
    it_c.add(1486045871000L, Double.NEGATIVE_INFINITY);
    
    it_a.add(1486045881000L, Double.NEGATIVE_INFINITY);
    it_b.add(1486045881000L, -132.85);
    it_c.add(1486045881000L, -128);
    
    merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    iterator = merged;
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    it_a = new NumericMillisecondShard(id, start, end);
    it_b = new NumericMillisecondShard(id, start, end);
    it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, Double.NaN);
    it_c.add(1486045801000L, 41.2);
    
    it_a.add(1486045871000L, Double.POSITIVE_INFINITY);
    it_b.add(1486045871000L, Double.NaN);
    it_c.add(1486045871000L, Double.NEGATIVE_INFINITY);
    
    it_a.add(1486045881000L, Double.NEGATIVE_INFINITY);
    it_b.add(1486045881000L, -132.85);
    it_c.add(1486045881000L, -128);
    
    merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    iterator = merged;
    v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }

  @Test
  public void mergeAllEmpty() throws Exception {
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);

    final TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    
    final TimeSeriesIterator<NumericType> iterator = merged;
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeExceptions() throws Exception {
    NumericMillisecondShard it_a = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_b = new NumericMillisecondShard(id, start, end);
    NumericMillisecondShard it_c = new NumericMillisecondShard(id, start, end);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 42);
    it_c.add(1486045801000L, 42);
    
    try {
      new NumericMergeLargest().merge(null, 
          Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
          context, null);
      fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) { }
    
    try {
     new NumericMergeLargest().merge(id, null, context, null);
     fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new NumericMergeLargest().merge(id, 
          Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
          null, null);
      fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) { }
    
    try {
      new NumericMergeLargest().merge(id, 
          Lists.<TimeSeriesIterator<?>>newArrayList(it_a, null, it_c), 
          context, null);
      fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) { }
    
    @SuppressWarnings("unchecked")
    TimeSeriesIterator<AnnotationType> mock = mock(TimeSeriesIterator.class);
    when(mock.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return AnnotationType.TYPE;
      }
    });
    try {
      new NumericMergeLargest().merge(id, 
          Lists.<TimeSeriesIterator<?>>newArrayList(it_a, mock, it_c), context, null);
      fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) { }
  }

  @Test
  public void mergeOrder() throws Exception {
    NumericMillisecondShard it_a = 
        new NumericMillisecondShard(id, start, end, 42);
    // no check on order. First one in wins.
    NumericMillisecondShard it_b = 
        new NumericMillisecondShard(id, start, end, 24);
    NumericMillisecondShard it_c = 
        new NumericMillisecondShard(id, start, end, 42);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 42);
    it_c.add(1486045801000L, 42);
    
    it_a.add(1486045871000L, 9866.854);
    it_b.add(1486045871000L, 9866.854);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -128);
    it_b.add(1486045881000L, -128);
    it_c.add(1486045881000L, -128);
    
    final TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, null);
    assertSame(id, merged.id());
    assertEquals(42, merged.order());
    
    final TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void mergeWithSpan() throws Exception {
    final Span tracer = mock(Span.class);
    NumericMillisecondShard it_a = 
        new NumericMillisecondShard(id, start, end, 42);
    // no check on order. First one in wins.
    NumericMillisecondShard it_b = 
        new NumericMillisecondShard(id, start, end, 24);
    NumericMillisecondShard it_c = 
        new NumericMillisecondShard(id, start, end, 42);
    
    it_a.add(1486045801000L, 42);
    it_b.add(1486045801000L, 42);
    it_c.add(1486045801000L, 42);
    
    it_a.add(1486045871000L, 9866.854);
    it_b.add(1486045871000L, 9866.854);
    it_c.add(1486045871000L, 9866.854);
    
    it_a.add(1486045881000L, -128);
    it_b.add(1486045881000L, -128);
    it_c.add(1486045881000L, -128);
    
    final TimeSeriesIterator<NumericType> merged = new NumericMergeLargest().merge(id, 
        Lists.<TimeSeriesIterator<?>>newArrayList(it_a, it_b, it_c), 
        context, tracer);
    assertSame(id, merged.id());
    assertEquals(42, merged.order());
    
    final TimeSeriesIterator<NumericType> iterator = merged;
    TimeSeriesValue<NumericType> v = iterator.next();
    assertEquals(1486045801000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(42, v.value().longValue());
    
    v = iterator.next();
    assertEquals(1486045871000L, v.timestamp().msEpoch());
    assertFalse(v.value().isInteger());
    assertEquals(9866.854, v.value().doubleValue(), 0.001);
    
    v = iterator.next();
    assertEquals(1486045881000L, v.timestamp().msEpoch());
    assertTrue(v.value().isInteger());
    assertEquals(-128, v.value().longValue());
    
    try {
      iterator.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    verify(tracer, times(3)).setTag(anyString(), anyInt());
  }
}
