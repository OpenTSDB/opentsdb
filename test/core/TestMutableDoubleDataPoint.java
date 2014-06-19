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

import org.junit.Test;


/**
 * Tests {@link MutableDoubleDataPoint}.
 */
public class TestMutableDoubleDataPoint {

  @Test
  public void testMutableDoubleDataPoint() {
    MutableDoubleDataPoint dp = new MutableDoubleDataPoint();
    assertFalse(dp.isInteger());
    assertEquals(0, dp.doubleValue(), 0);
    assertEquals(Long.MAX_VALUE, dp.timestamp());
    assertEquals(0, dp.toDouble(), 0);
  }

  @Test
  public void testResetWithDoubleValue() {
    MutableDoubleDataPoint dp = new MutableDoubleDataPoint();
    dp.reset(17L, 0.1717);
    assertFalse(dp.isInteger());
    assertEquals(0.1717, dp.doubleValue(), 0);
    assertEquals(17L, dp.timestamp());
    assertEquals(0.1717, dp.toDouble(), 0);
  }

  @Test
  public void testReset() {
    MutableDoubleDataPoint dp_foo = new MutableDoubleDataPoint();
    dp_foo.reset(17L, 0.1717);
    MutableDoubleDataPoint dp = new MutableDoubleDataPoint();
    dp.reset(dp_foo);
    assertFalse(dp.isInteger());
    assertEquals(0.1717, dp.doubleValue(), 0);
    assertEquals(17L, dp.timestamp());
    assertEquals(0.1717, dp.toDouble(), 0);
  }

  @Test(expected = ClassCastException.class)
  public void testLongValue_castException() {
    MutableDoubleDataPoint dp = new MutableDoubleDataPoint();
    dp.longValue();
  }
}
