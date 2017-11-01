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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestNumericInterpolatorConfig {

  @Test
  public void build() throws Exception {
    NumericInterpolatorConfig config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    assertEquals(FillPolicy.NOT_A_NUMBER, config.fillPolicy());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, config.realFillPolicy());
    
    try {
      NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(null)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        //.setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(null)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericInterpolatorConfig.newBuilder()
        //.setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void queryFill() throws Exception {
    NumericInterpolatorConfig config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    QueryFillPolicy<NumericType> fill = config.queryFill();
    assertTrue(Double.isNaN(fill.fill().doubleValue()));
    
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    fill = config.queryFill();
    assertNull(fill.fill());
    
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NULL)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    fill = config.queryFill();
    assertNull(fill.fill());
    
    config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.ZERO)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    fill = config.queryFill();
    assertEquals(0, fill.fill().longValue());
  }
}
