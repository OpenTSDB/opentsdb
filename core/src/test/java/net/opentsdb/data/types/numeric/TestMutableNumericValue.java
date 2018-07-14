// This file is part of OpenTSDB.
// Copyright (C) 2014-2017  The OpenTSDB Authors.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.ZoneId;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.common.Const;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.ZonedNanoTimeStamp;

/**
 * Tests {@link MutableNumericValue}.
 */
public class TestMutableNumericValue {
  private MillisecondTimeStamp ts;
  
  @Before
  public void before() {
    ts = new MillisecondTimeStamp(1);
  }
  
  @Test
  public void ctors() throws Exception {
    MutableNumericValue dp = new MutableNumericValue(ts, 42);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    
    dp = new MutableNumericValue(ts, 42.5);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    
    dp = new MutableNumericValue(dp);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    
    class IntDP implements NumericType {
      @Override
      public boolean isInteger() { return true; }

      @Override
      public long longValue() { return 42; }

      @Override
      public double doubleValue() { throw new ClassCastException(); }

      @Override
      public double toDouble() { return 42D; }
    }
    
    dp = new MutableNumericValue(ts, new IntDP());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    
    class FloatDP implements NumericType {
      @Override
      public boolean isInteger() { return false; }

      @Override
      public long longValue() { throw new ClassCastException(); }

      @Override
      public double doubleValue() { return 42.5D; }

      @Override
      public double toDouble() { return 42.5D; }
    }
    
    dp = new MutableNumericValue(ts, new FloatDP());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    
    TimeSeriesValue<NumericType> mock = mock(TimeSeriesValue.class);
    when(mock.timestamp()).thenReturn(ts);
    dp = new MutableNumericValue(mock);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertNull(dp.value());
    
    dp = new MutableNumericValue(ts, (NumericType) null);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertNull(dp.value());
    
    try {
      new MutableNumericValue(null, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericValue(null, 42.5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericValue((TimeSeriesValue<NumericType>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericValue(null, new IntDP());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void reset() throws Exception {
    final MutableNumericValue dp = new MutableNumericValue(ts, 42.5);
    dp.resetNull(ts);
    assertEquals(1, dp.timestamp().msEpoch());
    assertNull(dp.value());
    
    TimeStamp ts2 = new MillisecondTimeStamp(2);
    dp.reset(ts2, 42);
    assertNotSame(ts, dp.timestamp());
    assertEquals(2, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    
    dp.resetNull(ts2);
    assertEquals(2, dp.timestamp().msEpoch());
    assertNull(dp.value());
    
    ts2 = new MillisecondTimeStamp(3);
    dp.reset(ts2, 24.5);
    assertNotSame(ts, dp.timestamp());
    assertEquals(3, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(24.5, dp.doubleValue(), 0.001);
        
    final MutableNumericValue dupe = new MutableNumericValue();
    assertNotSame(ts, dupe.timestamp());
    assertEquals(0, dupe.timestamp().msEpoch());
    assertTrue(dupe.isInteger());
    assertEquals(0, dupe.longValue());
    
    dupe.reset(dp);
    assertNotSame(ts, dupe.timestamp());
    assertEquals(3, dupe.timestamp().msEpoch());
    assertFalse(dupe.isInteger());
    assertEquals(24.5, dupe.doubleValue(), 0.001);
    
    class IntDP implements NumericType {
      @Override
      public boolean isInteger() { return true; }

      @Override
      public long longValue() { return 42; }

      @Override
      public double doubleValue() { throw new ClassCastException(); }

      @Override
      public double toDouble() { return 42D; }
    }
    
    dp.reset(ts, new IntDP());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    
    dp.resetNull(ts);
    assertEquals(1, dp.timestamp().msEpoch());
    assertNull(dp.value());
    
    class FloatDP implements NumericType {
      @Override
      public boolean isInteger() { return false; }

      @Override
      public long longValue() { throw new ClassCastException(); }

      @Override
      public double doubleValue() { return 42.5D; }

      @Override
      public double toDouble() { return 42.5D; }
    }
    
    dp.reset(ts, new FloatDP());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    
    TimeSeriesValue<NumericType> mock = mock(TimeSeriesValue.class);
    when(mock.timestamp()).thenReturn(ts);
    dp.reset(mock);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertNull(dp.value());
    
    dp.reset(ts, 0);
    dp.resetValue(42);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(42, dp.value().longValue());
    
    dp.resetValue(42.5);
    assertEquals(1, dp.timestamp().msEpoch());
    assertEquals(42.5, dp.value().doubleValue(), 0.001);
    
    dp.resetTimestamp(new MillisecondTimeStamp(2));
    assertEquals(2, dp.timestamp().msEpoch());
    assertEquals(42.5, dp.value().doubleValue(), 0.001);
    
    try {
      dp.reset(null, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      dp.reset(null, 42.5D);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      dp.reset((TimeSeriesValue<NumericType>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      dp.reset(null, new IntDP());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      dp.reset(ts, (NumericType) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      dp.resetNull(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void resetWithHigherResolutionTimeStamp() throws Exception {
    MutableNumericValue dp = new MutableNumericValue(ts, 42.5);
    assertEquals(Const.UTC, dp.timestamp().timezone());
    
    final ZoneId denver = ZoneId.of("America/Denver");
    
    final TimeStamp zoned = new ZonedNanoTimeStamp(1000, 500, denver);
    dp.reset(zoned, 42);
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
    
    dp = new MutableNumericValue(ts, 42.5);
    dp.reset(zoned, 44.1);
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
    
    dp = new MutableNumericValue(ts, 42.5);
    dp.resetNull(zoned);
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
    
    class IntDP implements NumericType {
      @Override
      public boolean isInteger() { return true; }

      @Override
      public long longValue() { return 42; }

      @Override
      public double doubleValue() { throw new ClassCastException(); }

      @Override
      public double toDouble() { return 42D; }
    }
    dp = new MutableNumericValue(ts, 42.5);
    dp.reset(zoned, new IntDP());
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
    
    MutableNumericValue dupe = new MutableNumericValue(dp);
    assertEquals(denver, dupe.timestamp().timezone());
    assertEquals(1000, dupe.timestamp().epoch());
    assertEquals(500, dupe.timestamp().nanos());
    
    dp = new MutableNumericValue(ts, 42.5);
    dp.reset(dupe);
    assertEquals(denver, dp.timestamp().timezone());
    assertEquals(1000, dp.timestamp().epoch());
    assertEquals(500, dp.timestamp().nanos());
  }
}
