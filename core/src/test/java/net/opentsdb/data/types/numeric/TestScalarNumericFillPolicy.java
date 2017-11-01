// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestScalarNumericFillPolicy {

  @Test
  public void ctor() throws Exception {
    ScalarNumericFillPolicy policy = new ScalarNumericFillPolicy(
        (ScalarNumericInterpolatorConfig) ScalarNumericInterpolatorConfig.newBuilder()
          .setValue(42)
          .setFillPolicy(FillPolicy.NOT_A_NUMBER)
          .setRealFillPolicy(FillWithRealPolicy.NONE)
          .build());
    assertTrue(policy.fill().isInteger());
    assertEquals(42, policy.fill().longValue());
    try {
      policy.fill().doubleValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    assertEquals(42, policy.fill().toDouble(), 0.001);
    
    policy = new ScalarNumericFillPolicy(
        (ScalarNumericInterpolatorConfig) ScalarNumericInterpolatorConfig.newBuilder()
          .setValue(42.5D)
          .setFillPolicy(FillPolicy.NOT_A_NUMBER)
          .setRealFillPolicy(FillWithRealPolicy.NONE)
          .build());
    assertFalse(policy.fill().isInteger());
    try {
      policy.fill().longValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    assertEquals(42.5D, policy.fill().doubleValue(), 0.001);
    assertEquals(42.5D, policy.fill().toDouble(), 0.001);
  }
}
