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
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import org.junit.Test;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.MockNumericTimeSeries;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.query.QueryResult;

public class TestCombinedCachedNumericArray {
private static final int BASE_TIME = 1546300800;
  
  // TODO - add the NumericType to Array to NumericType and vice-versa tests.

  @Test
  public void noGapsLongs() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateLongSeries(3, BASE_TIME, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.longArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      assertEquals(want, iterator.longArray()[i]);
      want++;
    }
  }
  
  @Test
  public void noGapsDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i % 3 == 0) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void gapAtStartLongs() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateLongSeries(3, BASE_TIME + 3600, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i < 60) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void gapAtStartDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME + 3600, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i % 3 == 0 || i < 60) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void gapAtEndLongs() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateLongSeries(2, BASE_TIME, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i >= 120) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void gapAtEndDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(2, BASE_TIME, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i % 3 == 0 || i >= 120) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void gapInMiddleLongs() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 4)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateLongSeries(4, BASE_TIME, true, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(240, iterator.end());
    assertEquals(240, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i >= 120 && i < 180) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void gapInMiddleDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 4)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(4, BASE_TIME, true, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(240, iterator.end());
    assertEquals(240, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i % 3 == 0 || (i >= 120 && i < 180)) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void longThenDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    rs[0] = generateLongSeries(0, BASE_TIME);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i % 3 == 0 && i >= 60) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void doubleThenLongThenDoubles() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    rs[1] = generateLongSeries(60, BASE_TIME + 3600);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i % 3 == 0 && (i < 60 || i >= 120)) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void doubleThenLong() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    rs[2] = generateLongSeries(120, BASE_TIME + (3600 * 2));
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(180, iterator.end());
    assertEquals(180, iterator.doubleArray().length);
    int want = 0;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (i % 3 == 0 && i < 120) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeFull() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeFullPaddingAtEnd() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    
    QueryResult r = mock(QueryResult.class);
    TimeSpecification ts = mock(TimeSpecification.class);
    when(r.timeSpecification()).thenReturn(ts);
    when(ts.start()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 2) - 360));
    when(ts.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(ts.interval()).thenReturn(Duration.ofSeconds(60));
    when(ts.stringInterval()).thenReturn("1m");
    result.results()[2] = r;
    rs[2] = generateDoubleSeries(114, 65, BASE_TIME + (3600 * 2) - 360);
    
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeFullNumAtEnd() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    rs[2] = generateDoubleNumericSeries(120, 60, BASE_TIME + (3600 * 2));
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeFullNumAtEndWithPadding() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    
    QueryResult r = mock(QueryResult.class);
    TimeSpecification ts = mock(TimeSpecification.class);
    when(r.timeSpecification()).thenReturn(ts);
    when(ts.start()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 2) - 360));
    when(ts.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(ts.interval()).thenReturn(Duration.ofSeconds(60));
    when(ts.stringInterval()).thenReturn("1m");
    result.results()[2] = result;
    rs[2] = generateDoubleNumericSeries(114, 65, BASE_TIME + (3600 * 2) - 360);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    int want = 5;
    int offset = 1;
    
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeFullTwoNumsAtEnd() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    rs[1] = generateDoubleNumericSeries(60, 60, BASE_TIME + 3600);
    rs[2] = generateDoubleNumericSeries(120, 60, BASE_TIME + (3600 * 2));
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeFullNumInMiddle() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME, false, result);
    rs[1] = generateDoubleNumericSeries(60, 60, BASE_TIME + 3600);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeGapAtStart() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME + 3600, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    
    int want = 5;
    int offset = 1;
    
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0 || i < 56) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeGapAtEnd() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(2, BASE_TIME, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0 || i > 115) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void filterQueryTimeGapInMiddle() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 4) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(4, BASE_TIME, true, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(230, iterator.end());
    assertEquals(230, iterator.doubleArray().length);
    
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0 || (i >= 115 && i < 175)) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }

  @Test
  public void filterQueryOneResult() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + 3600 - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(1, BASE_TIME, false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(50, iterator.end());
    assertEquals(50, iterator.doubleArray().length);
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  @Test
  public void twoMissingAtStart() throws Exception {
    CombinedCachedResult result = mock(CombinedCachedResult.class);
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(time_spec);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3) - 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.resultInterval()).thenReturn(1);
    when(result.resultUnits()).thenReturn(ChronoUnit.HOURS);
    
    TimeSeries[] rs = generateDoubleSeries(3, BASE_TIME + (3600 * 2), false, result);
    CombinedCachedNumericArray iterator = new CombinedCachedNumericArray(result, rs);
    
    assertEquals(BASE_TIME + 300, iterator.timestamp().epoch());
    assertEquals(0, iterator.offset());
    assertEquals(170, iterator.end());
    assertEquals(170, iterator.doubleArray().length);
    int want = 5;
    int offset = 1;
    for (int i = iterator.offset(); i < iterator.end(); i++) {
      if (++offset % 3 == 0 || i < 121 - 5) {
        assertTrue(Double.isNaN(iterator.doubleArray()[i]));
      } else {
        assertEquals(want, iterator.doubleArray()[i], 0.001);
      }
      want++;
    }
  }
  
  TimeSeries generateLongSeries(int first_val, final int timestamp) {
    TimeSeries ts = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(timestamp * 1000));
    for (int i = 0; i < 60; i++) {
      ((NumericArrayTimeSeries) ts).add(first_val++);
    }
    return ts;
  }
  
  TimeSeries generateDoubleSeries(int first_val, int count, int timestamp) {
    TimeSeries ts = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build(), new MillisecondTimeStamp(timestamp * 1000));
    for (int i = 0; i < count; i++) {
      if (i % 3 == 0) {
        ((NumericArrayTimeSeries) ts).add(Double.NaN);
        first_val++;
      } else {
        ((NumericArrayTimeSeries) ts).add(first_val++);
      }
    }
    return ts;
  }
  
  TimeSeries generateDoubleNumericSeries(int first_val, int count, int timestamp) {
    TimeSeries ts = new MockNumericTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    for (int i = 0; i < count; i++) {
      if (i % 3 == 0) {
        ((MockNumericTimeSeries) ts).add(new MutableNumericValue(
            new SecondTimeStamp(timestamp), Double.NaN));
        first_val++;
      } else {        
        ((MockNumericTimeSeries) ts).add(new MutableNumericValue(
            new SecondTimeStamp(timestamp), (double) first_val++));
      }
      timestamp += 60;
    }
    return ts;
  }
  
  TimeSeries[] generateDoubleSeries(final int num_results, 
                                    final int start_timestamp,
                                    final boolean gaps,
                                    final CombinedCachedResult combined) {
    TimeSeries[] series = new TimeSeries[num_results];
    QueryResult[] results = new QueryResult[num_results];
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
      results[i] = result;
      
      TimeSeries ts = new NumericArrayTimeSeries(
          BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), new MillisecondTimeStamp(timestamp * 1000));
      for (int x = 0; x < 60; x++) {
        if (x % 3 == 0) {
          ((NumericArrayTimeSeries) ts).add(Double.NaN);
          value++;
        } else {
          ((NumericArrayTimeSeries) ts).add(value++);
        }
      }
      series[i] = ts;
      
      timestamp += 3600;
    }
    
    when(combined.results()).thenReturn(results);
    return series;
  }
  
  TimeSeries[] generateLongSeries(final int num_results, 
                                  final int start_timestamp,
                                  final boolean gaps,
                                  final CombinedCachedResult combined) {
    TimeSeries[] series = new TimeSeries[num_results];
    QueryResult[] results = new QueryResult[num_results];
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
      results[i] = result;
      
      TimeSeries ts = new NumericArrayTimeSeries(
          BaseTimeSeriesStringId.newBuilder()
          .setMetric("a")
          .build(), new MillisecondTimeStamp(timestamp * 1000));
      for (int x = 0; x < 60; x++) {
        ((NumericArrayTimeSeries) ts).add(value++);
      }
      series[i] = ts;
      
      timestamp += 3600;
    }
    
    when(combined.results()).thenReturn(results);
    return series;
  }
}
