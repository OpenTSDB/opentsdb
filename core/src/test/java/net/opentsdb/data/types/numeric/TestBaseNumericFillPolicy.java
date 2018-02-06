// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestBaseNumericFillPolicy {

  @Test
  public void ctor() throws Exception {
    final BaseNumericFillPolicy fill = 
        new BaseNumericFillPolicy(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NOT_A_NUMBER)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .build());
    assertTrue(Double.isNaN(fill.fill().doubleValue()));
    
    try {
      new BaseNumericFillPolicy(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void fill() throws Exception {
    BaseNumericFillPolicy fill = new BaseNumericFillPolicy(
        NumericInterpolatorConfig.newBuilder()
          .setFillPolicy(FillPolicy.NOT_A_NUMBER)
          .setRealFillPolicy(FillWithRealPolicy.NONE)
          .build());
    assertFalse(fill.isInteger());
    assertTrue(Double.isNaN(fill.fill().doubleValue()));
    assertTrue(Double.isNaN(fill.fill().toDouble()));
    assertEquals(0, fill.fill().longValue());
    
    fill = new BaseNumericFillPolicy(NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.ZERO)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build());
    assertTrue(fill.isInteger());
    assertTrue(Double.isNaN(fill.fill().doubleValue()));
    assertEquals(0, fill.fill().toDouble(), 0.001);
    assertEquals(0, fill.fill().longValue());
    
    fill = new BaseNumericFillPolicy(NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NONE)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build());
    try {
      fill.isInteger();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    assertNull(fill.fill());
    
    fill = new BaseNumericFillPolicy(NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NULL)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build());
    try {
      fill.isInteger();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    assertNull(fill.fill());
    
    fill = new BaseNumericFillPolicy(NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.SCALAR)
        .setRealFillPolicy(FillWithRealPolicy.NONE)
        .build());
    try {
      fill.isInteger();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    try {
      fill.fill();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
}
