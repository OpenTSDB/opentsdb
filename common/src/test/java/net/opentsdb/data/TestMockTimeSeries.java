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
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeStamp.RelationalOperator;

public class TestMockTimeSeries {

  @Test
  public void ctor() throws Exception {
    MockTimeSeries series = new MockTimeSeries(mock(TimeSeriesId.class));
    assertNotNull(series.id());
    
    try {
      new MockTimeSeries(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void add() throws Exception {
    MockTimeSeries series = new MockTimeSeries(mock(TimeSeriesId.class));
    
    TypeToken<?> type1 = mock(TypeToken.class);
    TypeToken<?> type2 = mock(TypeToken.class);
    
    TimeSeriesValue<?> v1 = mock(TimeSeriesValue.class);
    TimeSeriesValue<?> v2 = mock(TimeSeriesValue.class);
    TimeSeriesValue<?> v3 = mock(TimeSeriesValue.class);
    
    when(v1.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type1;
      }
    });
    when(v2.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type1;
      }
    });
    when(v3.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type2;
      }
    });
    
    assertEquals(0, series.data.size());
    series.addValue(v1);
    assertEquals(1, series.data.size());
    assertEquals(1, series.data.get(type1).size());
    
    series.addValue(v2);
    assertEquals(1, series.data.size());
    assertEquals(2, series.data.get(type1).size());
    
    series.addValue(v3);
    assertEquals(2, series.data.size());
    assertEquals(2, series.data.get(type1).size());
    assertEquals(1, series.data.get(type2).size());
    
    try {
      series.addValue(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void iterators() throws Exception {
    MockTimeSeries series = new MockTimeSeries(mock(TimeSeriesId.class));
    
    TypeToken<?> type1 = mock(TypeToken.class);
    TypeToken<?> type2 = mock(TypeToken.class);
    TypeToken<?> type3 = mock(TypeToken.class);
    
    TimeStamp ts1 = new MillisecondTimeStamp(1000L);
    TimeStamp ts2 = new MillisecondTimeStamp(2000L);
    
    TimeSeriesValue<?> v1 = mock(TimeSeriesValue.class);
    TimeSeriesValue<?> v2 = mock(TimeSeriesValue.class);
    TimeSeriesValue<?> v3 = mock(TimeSeriesValue.class);
    
    when(v1.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type1;
      }
    });
    when(v2.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type1;
      }
    });
    when(v3.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type2;
      }
    });
    
    when(v1.timestamp()).thenReturn(ts1);
    when(v2.timestamp()).thenReturn(ts2);
    when(v3.timestamp()).thenReturn(ts1);
    
    series.addValue(v1);
    series.addValue(v2);
    series.addValue(v3);
    
    assertEquals(2, series.types().size());
    assertTrue(series.types().contains(type1));
    assertTrue(series.types().contains(type2));
    assertFalse(series.types().contains(type3));
    
    assertFalse(series.iterator(type3).isPresent());
    
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        series.iterator(type1).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<? extends TimeSeriesDataType> v = iterator.next();
    assertTrue(v.timestamp().compare(RelationalOperator.EQ, ts1));
    
    assertTrue(iterator.hasNext());
    v = iterator.next();
    assertTrue(v.timestamp().compare(RelationalOperator.EQ, ts2));
    
    assertFalse(iterator.hasNext());
    
    iterator = series.iterator(type2).get();
    assertTrue(iterator.hasNext());
    v = iterator.next();
    assertTrue(v.timestamp().compare(RelationalOperator.EQ, ts1));
    
    assertFalse(iterator.hasNext());
    
    Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators 
      = series.iterators();
    assertEquals(2, iterators.size());
  }
}
