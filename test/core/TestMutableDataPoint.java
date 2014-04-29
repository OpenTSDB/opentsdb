// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


/**
 * Tests {@link MutableDataPoint}.
 */
public class TestMutableDataPoint {

  @Test
  public void testMutableDataPoint() {
    MutableDataPoint dp = new MutableDataPoint();
    assertTrue(dp.isInteger());
    assertEquals(0, dp.longValue());
    assertEquals(Long.MAX_VALUE, dp.timestamp());
    assertEquals(0, dp.toDouble(), 0);
  }

  @Test
  public void testResetWithDoubleValue() {
    MutableDataPoint dp = new MutableDataPoint();
    dp.resetWithDoubleValue(17L, 0.1717);
    assertFalse(dp.isInteger());
    assertEquals(0.1717, dp.doubleValue(), 0);
    assertEquals(17L, dp.timestamp());
    assertEquals(0.1717, dp.toDouble(), 0);
  }

  @Test
  public void testResetWithLongValue() {
    MutableDataPoint dp = new MutableDataPoint();
    dp.resetWithLongValue(19L, 1717171L);
    assertTrue(dp.isInteger());
    assertEquals(1717171L, dp.longValue());
    assertEquals(19L, dp.timestamp());
    assertEquals(1717171L, dp.toDouble(), 0);
  }

  @Test
  public void testReset() {
    MutableDataPoint dp_foo = new MutableDataPoint();
    dp_foo.resetWithLongValue(19L, 1717171L);
    MutableDataPoint dp = new MutableDataPoint();
    dp.reset(dp_foo);
    assertTrue(dp.isInteger());
    assertEquals(1717171L, dp.longValue());
    assertEquals(19L, dp.timestamp());
    assertEquals(1717171L, dp.toDouble(), 0);
    dp_foo.resetWithDoubleValue(17L, 0.1717);
    dp.reset(dp_foo);
    assertFalse(dp.isInteger());
    assertEquals(0.1717, dp.doubleValue(), 0);
    assertEquals(17L, dp.timestamp());
    assertEquals(0.1717, dp.toDouble(), 0);
  }

  @Test
  public void testOfDoubleValue() {
    MutableDataPoint dp = MutableDataPoint.ofDoubleValue(17L, 0.1717);
    assertFalse(dp.isInteger());
    assertEquals(0.1717, dp.doubleValue(), 0);
    assertEquals(17L, dp.timestamp());
    assertEquals(0.1717, dp.toDouble(), 0);
  }

  @Test
  public void testOfLongValue() {
    MutableDataPoint dp = MutableDataPoint.ofLongValue(19L, 1717171L);
    assertTrue(dp.isInteger());
    assertEquals(1717171L, dp.longValue());
    assertEquals(19L, dp.timestamp());
    assertEquals(1717171L, dp.toDouble(), 0);
  }

  @Test(expected = ClassCastException.class)
  public void testDoubleValue_castException() {
    MutableDataPoint dp = new MutableDataPoint();
    dp.resetWithLongValue(19L, 1717171L);
    dp.doubleValue();
  }

  @Test(expected = ClassCastException.class)
  public void testLongValue_castException() {
    MutableDataPoint dp = new MutableDataPoint();
    dp.resetWithDoubleValue(17L, 0.1717);
    dp.longValue();
  }
}
