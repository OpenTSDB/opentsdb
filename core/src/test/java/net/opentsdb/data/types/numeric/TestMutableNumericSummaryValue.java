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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.time.ZoneId;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.ZonedNanoTimeStamp;

public class TestMutableNumericSummaryValue {

  private MillisecondTimeStamp ts;
  
  @Before
  public void before() {
    ts = new MillisecondTimeStamp(1);
  }
  
  @Test
  public void ctors() throws Exception {
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    assertEquals(0, dp.timestamp().msEpoch());
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value());
    
    // put something in to clone
    dp.resetTimestamp(ts);
    dp.resetValue(0, 42);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(1, dp.value().summariesAvailable().size());
    assertEquals(0, (int) dp.value().summariesAvailable().iterator().next());
    assertEquals(42, dp.value().value(0).longValue());
    
    MutableNumericSummaryValue clone = new MutableNumericSummaryValue(dp);
    assertEquals(1, clone.timestamp().msEpoch());
    assertEquals(1, clone.value().summariesAvailable().size());
    assertEquals(0, (int) clone.value().summariesAvailable().iterator().next());
    assertEquals(42, clone.value().value(0).longValue());
    
    dp.clear();
    clone = new MutableNumericSummaryValue(dp);
    assertEquals(1, clone.timestamp().msEpoch());
    assertTrue(clone.summariesAvailable().isEmpty());
    assertNull(clone.value());
    
    try {
      new MutableNumericSummaryValue(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericSummaryValue(mock(MutableNumericSummaryValue.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resetFullValue() throws Exception {
    MutableNumericSummaryValue source = new MutableNumericSummaryValue();
    source.resetTimestamp(ts);
    source.resetValue(0, 42.5);
    source.resetValue(2, 2);
    
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.reset(source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(42.5, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    // nulled
    source.clear();
    dp.reset(source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value());
    
    try {
      dp.reset(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resetTimestampAndValue() throws Exception {
    MutableNumericSummaryType source = new MutableNumericSummaryType();
    source.set(0, 42.5);
    source.set(2, 2);
    
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.reset(ts, source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(42.5, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    // different
    source.clear();
    source.set(3, 24);
    source.set(256, 0);
    dp.reset(ts, source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(3));
    assertTrue(dp.value().summariesAvailable().contains(256));
    assertEquals(24, dp.value().value(3).longValue());
    assertEquals(0, dp.value().value(256).longValue());
    
    // nulled
    dp.reset(ts, null);
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value());
   
    try {
      dp.reset(null, source);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resetNull() throws Exception {
    MutableNumericSummaryType source = new MutableNumericSummaryType();
    source.set(0, 42.5);
    source.set(2, 2);
    
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.reset(ts, source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(42.5, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    dp.resetNull(ts);
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value());
    
    try {
      dp.resetNull(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void resetValuePrimitive() throws Exception {
    MutableNumericSummaryType source = new MutableNumericSummaryType();
    source.set(0, 42.5);
    source.set(2, 2);
    
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.reset(ts, source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(42.5, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    dp.resetValue(0, 24.75);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    dp.resetValue(2, 4);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value().value(2).longValue());
    
    dp.resetValue(3, 0);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(3, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertTrue(dp.value().summariesAvailable().contains(3));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value().value(2).longValue());
    assertEquals(0, dp.value().value(3).longValue());
  }
  
  @Test
  public void resetValueType() throws Exception {
    MutableNumericSummaryType source = new MutableNumericSummaryType();
    source.set(0, 42.5);
    source.set(2, 2);
    
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.reset(ts, source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(42.5, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    MutableNumericType v = new MutableNumericType(24.75);
    dp.resetValue(0, v);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    v = new MutableNumericType(4);
    dp.resetValue(2, v);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value().value(2).longValue());
    
    v = new MutableNumericType(0);
    dp.resetValue(3, v);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(3, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertTrue(dp.value().summariesAvailable().contains(3));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value().value(2).longValue());
    assertEquals(0, dp.value().value(3).longValue());
    
    // null them all!
    dp.resetValue(3, (NumericType) null);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value().value(2).longValue());
    assertNull(dp.value().value(3));
    
    dp.resetValue(2, (NumericType) null);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(1, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertNull(dp.value().value(2));
    assertNull(dp.value().value(3));
    
    dp.resetValue(0, (NumericType) null);
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value());
  }
  
  @Test
  public void resetValueValue() throws Exception {
    MutableNumericSummaryType source = new MutableNumericSummaryType();
    source.set(0, 42.5);
    source.set(2, 2);
    MillisecondTimeStamp ignored = new MillisecondTimeStamp(2);
    
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.reset(ts, source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(42.5, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    MutableNumericValue v = new MutableNumericValue(ignored, 24.75);
    dp.resetValue(0, (TimeSeriesValue<NumericType>) v);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    v = new MutableNumericValue(ignored, 4);
    dp.resetValue(2, (TimeSeriesValue<NumericType>) v);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value().value(2).longValue());
    
    v = new MutableNumericValue(ignored, 0);
    dp.resetValue(3, (TimeSeriesValue<NumericType>) v);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(3, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertTrue(dp.value().summariesAvailable().contains(3));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value().value(2).longValue());
    assertEquals(0, dp.value().value(3).longValue());
    
    // null them all!
    dp.resetValue(3, (TimeSeriesValue<NumericType>) null);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value().value(2).longValue());
    assertNull(dp.value().value(3));
    
    dp.resetValue(2, (TimeSeriesValue<NumericType>) null);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(1, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertEquals(24.75, dp.value().value(0).doubleValue(), 0.001);
    assertNull(dp.value().value(2));
    assertNull(dp.value().value(3));
    
    dp.resetValue(0, (TimeSeriesValue<NumericType>) null);
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value());
  }

  @Test
  public void nullSummary() throws Exception {
    MutableNumericSummaryType source = new MutableNumericSummaryType();
    source.set(0, 42.5);
    source.set(2, 2);
    
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.reset(ts, source);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(2, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertTrue(dp.value().summariesAvailable().contains(2));
    assertEquals(42.5, dp.value().value(0).doubleValue(), 0.001);
    assertEquals(2, dp.value().value(2).longValue());
    
    dp.nullSummary(2);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(1, dp.value().summariesAvailable().size());
    assertTrue(dp.value().summariesAvailable().contains(0));
    assertEquals(42.5, dp.value().value(0).doubleValue(), 0.001);
    assertNull(dp.value().value(2));
    
    dp.nullSummary(0);
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value());
  }
  
  @Test
  public void resetTimestamp() throws Exception {
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    assertEquals(0, dp.timestamp().msEpoch());
    
    dp.resetTimestamp(ts);
    assertEquals(1, dp.timestamp().msEpoch());
    
    try {
      dp.resetTimestamp(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void resetWithHigherResolutionTimeStamp() throws Exception {
    MutableNumericSummaryType source = new MutableNumericSummaryType();
    source.set(0, 42.5);
    source.set(2, 2);
    
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.reset(ts, source);
    assertEquals(Const.UTC, dp.timestamp().timezone());
    
    final ZoneId denver = ZoneId.of("America/Denver");
    
    final TimeStamp zoned = new ZonedNanoTimeStamp(1000, 500, denver);
    dp.reset(zoned, source);
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
    
    dp = new MutableNumericSummaryValue();
    dp.reset(zoned, source);
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
    
    dp = new MutableNumericSummaryValue();
    dp.resetNull(zoned);
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
    
    dp = new MutableNumericSummaryValue();
    dp.reset(zoned, source);
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
  }
}
