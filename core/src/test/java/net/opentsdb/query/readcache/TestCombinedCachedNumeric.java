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
package net.opentsdb.query.readcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.Test;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;

public class TestCombinedCachedNumeric {
  private static final int BASE_TIME = 1546300800;

  @Test
  public void noGapsLongs() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateLongSeries(3, BASE_TIME, false);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      assertEquals(want++, v.value().longValue());
      ts += 60;
    }
    assertEquals(BASE_TIME + (3600 * 3), ts);
  }
  
  @Test
  public void noGapsDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      if (want % 3 == 0) {
        assertTrue(Double.isNaN(v.value().doubleValue()));
      } else {
        assertEquals(want, v.value().doubleValue(), 0.001);
      }
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 3), ts);
  }

  @Test
  public void gapAtStartLongs() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateLongSeries(3, BASE_TIME + 3600, false);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME + 3600;
    int want = 60;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      assertEquals(want, v.value().longValue());
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 3), ts);
  }
  
  @Test
  public void gapAtStartDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME + 3600, false);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME + 3600;
    int want = 60;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      if (want % 3 == 0) {
        assertTrue(Double.isNaN(v.value().doubleValue()));
      } else {
        assertEquals(want, v.value().doubleValue(), 0.001);
      }
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 3), ts);
  }
  
  @Test
  public void gapAtEndLongs() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateLongSeries(2, BASE_TIME, false);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      assertEquals(want, v.value().longValue());
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 2), ts);
  }
  
  @Test
  public void gapAtEndDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateDoubleSeries(2, BASE_TIME, false);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      if (want % 3 == 0) {
        assertTrue(Double.isNaN(v.value().doubleValue()));
      } else {
        assertEquals(want, v.value().doubleValue(), 0.001);
      }
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 2), ts);
  }
  
  @Test
  public void gapInMiddleLongs() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateLongSeries(4, BASE_TIME, true);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      assertEquals(want, v.value().longValue());
      ts += 60;
      if (ts == BASE_TIME + (3600 * 2)) {
        ts = BASE_TIME + (3600 * 3);
        want += 60;
      }
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 4), ts);
  }
  
  @Test
  public void gapInMiddleDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateDoubleSeries(4, BASE_TIME, true);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      if (want % 3 == 0 || (want >= 120 && want < 180)) {
        assertTrue(Double.isNaN(v.value().doubleValue()));
      } else {
        assertEquals(want, v.value().doubleValue(), 0.001);
      }
      ts += 60;
      if (ts == BASE_TIME + (3600 * 2)) {
        ts = BASE_TIME + (3600 * 3);
        want += 60;
      }
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 4), ts);
  }
  
  @Test
  public void longThenDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false);
    rs[0] = generateLongSeries(0, BASE_TIME);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      if (want < 60) {
        assertEquals(want, v.value().longValue());
      } else if (want % 3 == 0 && want >= 60) {
        assertTrue(Double.isNaN(v.value().doubleValue()));
      } else {
        assertEquals(want, v.value().doubleValue(), 0.001);
      }
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 3), ts);
  }
  
  @Test
  public void doubleThenLongThenDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false);
    rs[1] = generateLongSeries(60, BASE_TIME + 3600);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      if (want >= 60 && want < 120) {
        assertEquals(want, v.value().longValue());
      } else if (want % 3 == 0) {
        assertTrue(Double.isNaN(v.value().doubleValue()));
      } else {
        assertEquals(want, v.value().doubleValue(), 0.001);
      }
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 3), ts);
  }
  
  @Test
  public void doubleThenLong() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false);
    rs[2] = generateLongSeries(120, BASE_TIME + (3600 * 2));
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      if (want >= 120) {
        assertEquals(want, v.value().longValue());
      } else if (want % 3 == 0) {
        assertTrue(Double.isNaN(v.value().doubleValue()));
      } else {
        assertEquals(want, v.value().doubleValue(), 0.001);
      }
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 3), ts);
  }
  
  @Test
  public void twoMissingAtStart() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME + (3600 * 2), false);
    CombinedCachedNumeric iterator = new CombinedCachedNumeric(result, rs);
    
    int ts = BASE_TIME + (3600 * 2);
    int want = 120;
    while(iterator.hasNext()) {
      final TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      if (want % 3 == 0) {
        assertTrue(Double.isNaN(v.value().doubleValue()));
      } else {
        assertEquals(want, v.value().doubleValue(), 0.001);
      }
      ts += 60;
      want++;
    }
    assertEquals(BASE_TIME + (3600 * 3), ts);
  }
  
  TimeSeries generateLongSeries(int first_val, 
                                  int timestamp) {
    QueryResult result = mock(QueryResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(timestamp));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(timestamp + 3600));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(time_spec.stringInterval()).thenReturn("1m");
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    for (int i = 0; i < 60; i++) {
      ts.addValue(new MutableNumericValue(new SecondTimeStamp(timestamp), first_val++));
      timestamp += 60;
    }
    return ts;
  }
  
  TimeSeries[] generateDoubleSeries(final int num_results, 
                                    final int start_timestamp,
                                    final boolean gaps) {
    TimeSeries[] results = new TimeSeries[num_results];
    int timestamp = BASE_TIME;
    int value = 0;
    for (int i = 0; i < num_results; i++) {
      if (start_timestamp > timestamp || 
          (gaps && i > 0 && i % 2 == 0)) {
        value += 60;
        timestamp += 3600;
        continue;
      }
      
      QueryResult result = mock(QueryResult.class);
      TimeSpecification time_spec = mock(TimeSpecification.class);
      when(result.timeSpecification()).thenReturn(time_spec);
      when(time_spec.start()).thenReturn(new SecondTimeStamp(timestamp));
      when(time_spec.end()).thenReturn(new SecondTimeStamp(timestamp + 3600));
      when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
      when(time_spec.stringInterval()).thenReturn("1m");
      
      MockTimeSeries ts = new MockTimeSeries(
          BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build());
      long tstamp = timestamp;
      for (int x = 0; x < 60; x++) {
        if (x % 3 == 0) {
          ts.addValue(new MutableNumericValue(new SecondTimeStamp(tstamp), Double.NaN));
          value++;
        } else {
          ts.addValue(new MutableNumericValue(new SecondTimeStamp(tstamp), (double) value++));
        }
        tstamp += 60;
      }
      results[i] = ts;
      
      timestamp += 3600;
    }
    
    return results;
  }
  
  TimeSeries[] generateLongSeries(final int num_results, 
                                  final int start_timestamp,
                                  final boolean gaps) {
    TimeSeries[] results = new TimeSeries[num_results];
    int timestamp = BASE_TIME;
    int value = 0;
    for (int i = 0; i < num_results; i++) {
      if (start_timestamp > timestamp || 
          (gaps && i > 0 && i % 2 == 0)) {
        value += 60;
        timestamp += 3600;
        continue;
      }
      
      QueryResult result = mock(QueryResult.class);
      MockTimeSeries ts = new MockTimeSeries(
          BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build());
      long tstamp = timestamp;
      for (int x = 0; x < 60; x++) {
        ts.addValue(new MutableNumericValue(new SecondTimeStamp(tstamp), value++));
        tstamp += 60;
      }
      results[i] = ts;
      
      timestamp += 3600;
    }
    
    return results;
  }
}
