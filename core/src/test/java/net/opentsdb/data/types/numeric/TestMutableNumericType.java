// This file is part of OpenTSDB.
// Copyright (C) 2014-2017  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

/**
 * Tests {@link MutableNumericType}.
 */
public class TestMutableNumericType {
  private MillisecondTimeStamp ts;
  
  @Before
  public void before() {
    ts = new MillisecondTimeStamp(1);
  }
  
  @Test
  public void ctors() throws Exception {
    MutableNumericType dp = new MutableNumericType(ts, 42);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    
    dp = new MutableNumericType(ts, 42.5);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    
    dp = new MutableNumericType(dp);
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
    
    dp = new MutableNumericType(ts, new IntDP());
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
    
    dp = new MutableNumericType(ts, new FloatDP());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    
    TimeSeriesValue<NumericType> mock = mock(TimeSeriesValue.class);
    when(mock.timestamp()).thenReturn(ts);
    dp = new MutableNumericType(mock);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertNull(dp.value());
    
    try {
      new MutableNumericType(null, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType(null, 42.5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType((TimeSeriesValue<NumericType>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType(null, new IntDP());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType(ts, (NumericType) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void reset() throws Exception {
    final MutableNumericType dp = new MutableNumericType(ts, 42.5);
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
        
    final MutableNumericType dupe = new MutableNumericType();
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

}
