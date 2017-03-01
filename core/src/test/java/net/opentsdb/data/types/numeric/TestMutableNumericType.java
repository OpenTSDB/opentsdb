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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

/**
 * Tests {@link MutableNumericType}.
 */
public class TestMutableNumericType {
  private SimpleStringTimeSeriesId id;
  private MillisecondTimeStamp ts;
  
  @Before
  public void before() {
    id = SimpleStringTimeSeriesId.newBuilder().setAlias("mydp").build();
    ts = new MillisecondTimeStamp(1);
  }
  
  @Test
  public void ctors() throws Exception {
    MutableNumericType dp = new MutableNumericType(id);
    assertSame(id, dp.id());
    assertEquals(0, dp.timestamp().epoch());
    assertTrue(dp.isInteger());
    assertEquals(0, dp.longValue());
    assertEquals(0, dp.realCount());
    
    dp = new MutableNumericType(id, ts, 42);
    assertSame(id, dp.id());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    assertEquals(0, dp.realCount());
    
    dp = new MutableNumericType(id, ts, 42.5);
    assertSame(id, dp.id());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    assertEquals(0, dp.realCount());
    
    dp = new MutableNumericType(id, ts, 42, 1);
    assertSame(id, dp.id());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    assertEquals(1, dp.realCount());
    
    dp = new MutableNumericType(id, ts, 42.5, 1);
    assertSame(id, dp.id());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    assertEquals(1, dp.realCount());
    
    dp = new MutableNumericType(dp);
    assertSame(id, dp.id());
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    assertEquals(1, dp.realCount());
    
    assertEquals(NumericType.TYPE, dp.type());
    
    try {
      new MutableNumericType((TimeSeriesId) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType((TimeSeriesId) null, ts, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType((TimeSeriesId) null, ts, 42.5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType((TimeSeriesId) null, ts, 42, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType((TimeSeriesId) null, ts, 42.5, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType(id, null, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType(id, null, 42.5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType(id, null, 42, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType(id, null, 42.5, 1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType((TimeSeriesValue<NumericType>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void getCopy() throws Exception {
    final MutableNumericType dp = new MutableNumericType(id, ts, 42.5, 1);
    final TimeSeriesValue<NumericType> copy = dp.getCopy();
    assertSame(id, copy.id());
    assertNotSame(ts, copy.timestamp());
    assertEquals(1, copy.timestamp().msEpoch());
    assertFalse(copy.value().isInteger());
    assertEquals(42.5, copy.value().doubleValue(), 0.001);
    assertEquals(1, copy.realCount());
  }
  
  @Test
  public void reset() throws Exception {
    final MutableNumericType dp = new MutableNumericType(id, ts, 42.5, 1);
    
    TimeStamp ts2 = new MillisecondTimeStamp(2);
    dp.reset(ts2, 42, 0);
    assertSame(id, dp.id());
    assertNotSame(ts, dp.timestamp());
    assertEquals(2, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    assertEquals(0, dp.realCount());
    
    ts2 = new MillisecondTimeStamp(3);
    dp.reset(ts2, 24.5, 3);
    assertSame(id, dp.id());
    assertNotSame(ts, dp.timestamp());
    assertEquals(3, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(24.5, dp.doubleValue(), 0.001);
    assertEquals(3, dp.realCount());
    
    final MutableNumericType dupe = new MutableNumericType(id);
    assertSame(id, dupe.id());
    assertNotSame(ts, dupe.timestamp());
    assertEquals(0, dupe.timestamp().msEpoch());
    assertTrue(dupe.isInteger());
    assertEquals(0, dupe.longValue());
    assertEquals(0, dupe.realCount());
    
    dupe.reset(dp);
    assertSame(id, dupe.id());
    assertNotSame(ts, dupe.timestamp());
    assertEquals(3, dupe.timestamp().msEpoch());
    assertFalse(dupe.isInteger());
    assertEquals(24.5, dupe.doubleValue(), 0.001);
    assertEquals(3, dupe.realCount());
  }

}
