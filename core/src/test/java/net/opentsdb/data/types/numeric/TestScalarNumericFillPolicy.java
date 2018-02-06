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
