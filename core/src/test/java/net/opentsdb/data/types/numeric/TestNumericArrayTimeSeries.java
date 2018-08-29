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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedIterator;

public class TestNumericArrayTimeSeries {

  private static TimeSeriesId ID;
  private static TimeStamp TIMESTAMP;
  
  @BeforeClass
  public static void beforeClass() {
    ID = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    TIMESTAMP = new SecondTimeStamp(1);
  }
  
  @Test
  public void ctor() throws Exception {
    NumericArrayTimeSeries series = new NumericArrayTimeSeries(ID, TIMESTAMP);
    assertSame(ID, series.id());
    assertTrue(series.types().contains(NumericArrayType.TYPE));
    assertFalse(series.iterator(NumericType.TYPE).isPresent());
  }
  
  @Test
  public void addAndIterateLongs() throws Exception {
    NumericArrayTimeSeries series = new NumericArrayTimeSeries(ID, TIMESTAMP);
    
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        series.iterator(NumericArrayType.TYPE).get();
    assertFalse(iterator.hasNext());
    
    series.add(42);
    series.add(-24);
    series.add(1);
    iterator = series.iterator(NumericArrayType.TYPE).get();
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertTrue(value.value().isInteger());
    assertSame(TIMESTAMP, value.timestamp());
    assertArrayEquals(new long[] { 42, -24, 1, 0, 0, 0, 0, 0 }, 
        value.value().longArray());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void addAndIterateDoubles() throws Exception {
    NumericArrayTimeSeries series = new NumericArrayTimeSeries(ID, TIMESTAMP);
    
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        series.iterator(NumericArrayType.TYPE).get();
    assertFalse(iterator.hasNext());
    
    series.add(42.5);
    series.add(-24.3);
    series.add(1.1);
    iterator = series.iterator(NumericArrayType.TYPE).get();
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(value.value().isInteger());
    assertSame(TIMESTAMP, value.timestamp());
    assertArrayEquals(new double[] { 42.5, -24.3, 1.1, 0, 0, 0, 0, 0 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void addAndIterateMixed() throws Exception {
    NumericArrayTimeSeries series = new NumericArrayTimeSeries(ID, TIMESTAMP);
    
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        series.iterator(NumericArrayType.TYPE).get();
    assertFalse(iterator.hasNext());
    
    series.add(42);
    series.add(-24.3);
    series.add(1);
    iterator = series.iterator(NumericArrayType.TYPE).get();
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertFalse(value.value().isInteger());
    assertSame(TIMESTAMP, value.timestamp());
    assertArrayEquals(new double[] { 42, -24.3, 1, 0, 0, 0, 0, 0 }, 
        value.value().doubleArray(), 0.001);
    
    assertFalse(iterator.hasNext());
  }

  @Test
  public void iterators() throws Exception {
    NumericArrayTimeSeries series = new NumericArrayTimeSeries(ID, TIMESTAMP);
    series.add(42);
    series.add(-24);
    series.add(1);
    
    Collection<TypedIterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators
      = series.iterators();
    assertEquals(1, iterators.size());
    TypedIterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator =
        iterators.iterator().next();
    
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(0, value.value().offset());
    assertEquals(3, value.value().end());
    assertTrue(value.value().isInteger());
    assertSame(TIMESTAMP, value.timestamp());
    assertArrayEquals(new long[] { 42, -24, 1, 0, 0, 0, 0, 0 }, 
        value.value().longArray());
    
    assertFalse(iterator.hasNext());
  }
}
