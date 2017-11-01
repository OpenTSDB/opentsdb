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
package net.opentsdb.query.interpolation.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestScalarNumericInterpolatorConfig {

  @Test
  public void build() throws Exception {
    NumericInterpolatorConfig config = ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    assertTrue(((ScalarNumericInterpolatorConfig) config).isInteger());
    assertEquals(42, ((ScalarNumericInterpolatorConfig) config).longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.realFillPolicy());
    
    config = ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42.5D)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    assertFalse(((ScalarNumericInterpolatorConfig) config).isInteger());
    assertEquals(42.5, ((ScalarNumericInterpolatorConfig) config).doubleValue(), 0.01);
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.realFillPolicy());
    
    config = ScalarNumericInterpolatorConfig.newBuilder()
        //.setValue(42) <== defaults to 0
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    assertTrue(((ScalarNumericInterpolatorConfig) config).isInteger());
    assertEquals(0, ((ScalarNumericInterpolatorConfig) config).longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.realFillPolicy());
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(null)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        //.setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        .setFillPolicy(null)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ScalarNumericInterpolatorConfig.newBuilder()
        .setValue(42)
        //.setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
