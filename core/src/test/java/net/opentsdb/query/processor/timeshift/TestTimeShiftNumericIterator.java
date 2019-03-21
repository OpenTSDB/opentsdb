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
package net.opentsdb.query.processor.timeshift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.utils.DateTime;

public class TestTimeShiftNumericIterator {

  private static TimeSeries SERIES;
  private TimeShiftResult result;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    SERIES = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    ((MockTimeSeries) SERIES).addValue(
        new MutableNumericValue(new SecondTimeStamp(1546300800L), 42));
    ((MockTimeSeries) SERIES).addValue(
        new MutableNumericValue(new SecondTimeStamp(1546300860L), 24));
    ((MockTimeSeries) SERIES).addValue(
        new MutableNumericValue(new SecondTimeStamp(1546300920L), -8));
    ((MockTimeSeries) SERIES).addValue(
        new MutableNumericValue(new SecondTimeStamp(1546300980L), 1));
  }
  
  @Before
  public void before() throws Exception {
    result = mock(TimeShiftResult.class);
    when(result.isPrevious()).thenReturn(true);
    when(result.amount()).thenReturn(DateTime.parseDuration2("1h"));
  }
  
  @Test
  public void ctor() throws Exception {
    TimeShiftNumericIterator iterator = 
        new TimeShiftNumericIterator(result, SERIES);
    assertTrue(iterator.hasNext());
    
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.iterator(any(TypeToken.class))).thenReturn(Optional.empty());
    iterator = new TimeShiftNumericIterator(result, ts);
    assertFalse(iterator.hasNext());
    
    assertEquals(NumericType.TYPE, iterator.getType());
  }
  
  @Test
  public void previous() throws Exception {
    TimeShiftNumericIterator iterator = 
        new TimeShiftNumericIterator(result, SERIES);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1546304400, value.timestamp().epoch());
    assertEquals(42, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1546304460, value.timestamp().epoch());
    assertEquals(24, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1546304520, value.timestamp().epoch());
    assertEquals(-8, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1546304580, value.timestamp().epoch());
    assertEquals(1, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void next() throws Exception {
    when(result.isPrevious()).thenReturn(false);
    
    TimeShiftNumericIterator iterator = 
        new TimeShiftNumericIterator(result, SERIES);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericType> value = 
        (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1546297200, value.timestamp().epoch());
    assertEquals(42, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1546297260, value.timestamp().epoch());
    assertEquals(24, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1546297320, value.timestamp().epoch());
    assertEquals(-8, value.value().longValue());
    
    value = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1546297380, value.timestamp().epoch());
    assertEquals(1, value.value().longValue());
    
    assertFalse(iterator.hasNext());
  }
}
