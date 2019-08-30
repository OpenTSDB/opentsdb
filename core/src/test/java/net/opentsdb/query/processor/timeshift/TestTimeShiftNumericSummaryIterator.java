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
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.utils.DateTime;

public class TestTimeShiftNumericSummaryIterator {
  private static final long BASE_TIME = 1356998400000L;
  private static TimeSeries SERIES;
  private TimeShiftResult result;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    SERIES = new MockTimeSeries(new BaseTimeSeriesStringId.Builder()
        .setMetric("foo")
        .build());
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    ((MockTimeSeries) SERIES).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME  + 3600000));
    v.resetValue(0, 24);
    ((MockTimeSeries) SERIES).addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME  + (3600000 * 2)));
    v.resetValue(0, 1);
    ((MockTimeSeries) SERIES).addValue(v);
  }
  
  @Before
  public void before() throws Exception {
    result = mock(TimeShiftResult.class);
    when(result.isPrevious()).thenReturn(true);
    when(result.amount()).thenReturn(DateTime.parseDuration2("1h"));
  }
  
  @Test
  public void ctor() throws Exception {
    TimeShiftNumericSummaryIterator iterator = 
        new TimeShiftNumericSummaryIterator(result, SERIES);
    assertTrue(iterator.hasNext());
    
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.iterator(any(TypeToken.class))).thenReturn(Optional.empty());
    iterator = new TimeShiftNumericSummaryIterator(result, ts);
    assertFalse(iterator.hasNext());
    
    assertEquals(NumericSummaryType.TYPE, iterator.getType());
  }
  
  @Test
  public void previous() throws Exception {
    TimeShiftNumericSummaryIterator iterator = 
        new TimeShiftNumericSummaryIterator(result, SERIES);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + 3600000, value.timestamp().msEpoch());
    assertEquals(42, value.value().value(0).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600000 * 2), value.timestamp().msEpoch());
    assertEquals(24, value.value().value(0).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + (3600000 * 3), value.timestamp().msEpoch());
    assertEquals(1, value.value().value(0).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void next() throws Exception {
    when(result.isPrevious()).thenReturn(false);
    
    TimeShiftNumericSummaryIterator iterator = 
        new TimeShiftNumericSummaryIterator(result, SERIES);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME - 3600000, value.timestamp().msEpoch());
    assertEquals(42, value.value().value(0).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME, value.timestamp().msEpoch());
    assertEquals(24, value.value().value(0).longValue());
    
    value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(BASE_TIME + 3600000, value.timestamp().msEpoch());
    assertEquals(1, value.value().value(0).longValue());
    
    assertFalse(iterator.hasNext());
  }

}
