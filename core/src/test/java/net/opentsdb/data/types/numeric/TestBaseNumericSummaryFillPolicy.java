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

import org.junit.Test;

import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestBaseNumericSummaryFillPolicy {
  
  @Test
  public void ctor() throws Exception {
    final BaseNumericSummaryFillPolicy fill = 
        new BaseNumericSummaryFillPolicy(
            NumericSummaryInterpolatorConfig.newBuilder()
            .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
            .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
            .addExpectedSummary(0)
            .addExpectedSummary(2)
            .setType(NumericSummaryType.TYPE.toString())
            .build());
    final NumericSummaryType value = fill.fill();
    assertEquals(2, value.summariesAvailable().size());
    assertTrue(value.summariesAvailable().contains(0));
    assertTrue(value.summariesAvailable().contains(2));
    assertTrue(Double.isNaN(value.value(0).doubleValue()));
    assertTrue(Double.isNaN(value.value(2).doubleValue()));
    
    try {
      new BaseNumericSummaryFillPolicy(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    assertTrue(fill.config() instanceof NumericSummaryInterpolatorConfig);
  }
  
  @Test
  public void fill() throws Exception {
    BaseNumericSummaryFillPolicy fill = 
        new BaseNumericSummaryFillPolicy(NumericSummaryInterpolatorConfig.newBuilder()
            .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
            .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
            .addExpectedSummary(0)
            .addExpectedSummary(2)
            .setType(NumericSummaryType.TYPE.toString())
            .build());
    NumericSummaryType value = fill.fill();
    assertEquals(2, value.summariesAvailable().size());
    assertTrue(value.summariesAvailable().contains(0));
    assertTrue(value.summariesAvailable().contains(2));
    assertTrue(Double.isNaN(value.value(0).doubleValue()));
    assertTrue(Double.isNaN(value.value(2).doubleValue()));
    
    fill = new BaseNumericSummaryFillPolicy(NumericSummaryInterpolatorConfig.newBuilder()
            .setDefaultFillPolicy(FillPolicy.ZERO)
            .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
            .addExpectedSummary(0)
            .addExpectedSummary(2)
            .setType(NumericSummaryType.TYPE.toString())
            .build());
    value = fill.fill();
    assertEquals(2, value.summariesAvailable().size());
    assertTrue(value.summariesAvailable().contains(0));
    assertTrue(value.summariesAvailable().contains(2));
    assertEquals(0, value.value(0).longValue());
    assertEquals(0, value.value(2).longValue());
    
    fill = new BaseNumericSummaryFillPolicy(NumericSummaryInterpolatorConfig.newBuilder()
            .setDefaultFillPolicy(FillPolicy.NONE)
            .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
            .addExpectedSummary(0)
            .addExpectedSummary(2)
            .setType(NumericSummaryType.TYPE.toString())
            .build());
    assertNull(fill.fill());
    
    fill = new BaseNumericSummaryFillPolicy(NumericSummaryInterpolatorConfig.newBuilder()
            .setDefaultFillPolicy(FillPolicy.NULL)
            .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
            .addExpectedSummary(0)
            .addExpectedSummary(2)
            .setType(NumericSummaryType.TYPE.toString())
            .build());
    assertNull(fill.fill());
    
    fill = new BaseNumericSummaryFillPolicy(NumericSummaryInterpolatorConfig.newBuilder()
            .setDefaultFillPolicy(FillPolicy.SCALAR)
            .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
            .addExpectedSummary(0)
            .addExpectedSummary(2)
            .setType(NumericSummaryType.TYPE.toString())
            .build());
    try {
      fill.fill();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    
    fill = new BaseNumericSummaryFillPolicy(NumericSummaryInterpolatorConfig.newBuilder()
            .setDefaultFillPolicy(FillPolicy.MAX)
            .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
            .addExpectedSummary(0)
            .addExpectedSummary(2)
            .setType(NumericSummaryType.TYPE.toString())
            .build());
    value = fill.fill();
    assertEquals(2, value.summariesAvailable().size());
    assertTrue(value.summariesAvailable().contains(0));
    assertTrue(value.summariesAvailable().contains(2));
    assertEquals(Double.MAX_VALUE, value.value(0).doubleValue(), 0.001);
    assertEquals(Double.MAX_VALUE, value.value(2).doubleValue(), 0.001);
    
    fill = new BaseNumericSummaryFillPolicy(NumericSummaryInterpolatorConfig.newBuilder()
            .setDefaultFillPolicy(FillPolicy.MIN)
            .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
            .addExpectedSummary(0)
            .addExpectedSummary(2)
            .setType(NumericSummaryType.TYPE.toString())
            .build());
    value = fill.fill();
    assertEquals(2, value.summariesAvailable().size());
    assertTrue(value.summariesAvailable().contains(0));
    assertTrue(value.summariesAvailable().contains(2));
    assertEquals(Double.MIN_VALUE, value.value(0).doubleValue(), 0.001);
    assertEquals(Double.MIN_VALUE, value.value(2).doubleValue(), 0.001);
  }

}
