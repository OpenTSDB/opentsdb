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

public class TestMutableNumericSummaryType {

  @Test
  public void ctors() throws Exception {
    MutableNumericSummaryType dp = new MutableNumericSummaryType();
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value(0));
    
    dp = new MutableNumericSummaryType(0, 42);
    assertEquals(1, dp.summariesAvailable().size());
    assertEquals(0, (int) dp.summariesAvailable().iterator().next());
    assertEquals(42, dp.value(0).longValue());
    
    dp = new MutableNumericSummaryType(0, 42.5);
    assertEquals(1, dp.summariesAvailable().size());
    assertEquals(0, (int) dp.summariesAvailable().iterator().next());
    assertEquals(42.5, dp.value(0).doubleValue(), 0.001);
    
    MutableNumericSummaryType clone = new MutableNumericSummaryType(dp);
    assertEquals(1, clone.summariesAvailable().size());
    assertEquals(0, (int) clone.summariesAvailable().iterator().next());
    assertEquals(42.5, clone.value(0).doubleValue(), 0.001);
    
    try {
      new MutableNumericSummaryType(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void clear() throws Exception {
    MutableNumericSummaryType dp = new MutableNumericSummaryType(0, 42);
    assertEquals(1, dp.summariesAvailable().size());
    assertEquals(0, (int) dp.summariesAvailable().iterator().next());
    assertEquals(42, dp.value(0).longValue());
    
    dp.clear();
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value(0));
    
    // no effect
    dp.clear();
    assertTrue(dp.summariesAvailable().isEmpty());
    assertNull(dp.value(0));
  }
  
  @Test
  public void set() throws Exception {
    MutableNumericSummaryType dp = new MutableNumericSummaryType();
    dp.set(0, 42.5);
    assertEquals(1, dp.summariesAvailable().size());
    assertEquals(0, (int) dp.summariesAvailable().iterator().next());
    assertEquals(42.5, dp.value(0).doubleValue(), 0.001);
    
    dp.set(2, 4);
    assertEquals(2, dp.summariesAvailable().size());
    assertTrue(dp.summariesAvailable().contains(0));
    assertTrue(dp.summariesAvailable().contains(2));
    assertEquals(42.5, dp.value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value(2).longValue());
    
    // replace
    dp.set(0, 1.75);
    assertEquals(2, dp.summariesAvailable().size());
    assertTrue(dp.summariesAvailable().contains(0));
    assertTrue(dp.summariesAvailable().contains(2));
    assertEquals(1.75, dp.value(0).doubleValue(), 0.001);
    assertEquals(4, dp.value(2).longValue());
    
    dp.set(2, 1);
    assertEquals(2, dp.summariesAvailable().size());
    assertTrue(dp.summariesAvailable().contains(0));
    assertTrue(dp.summariesAvailable().contains(2));
    assertEquals(1.75, dp.value(0).doubleValue(), 0.001);
    assertEquals(1, dp.value(2).longValue());
    
    MutableNumericSummaryType other = new MutableNumericSummaryType();
    other.set(3, 42);
    other.set(256, 0.01);
    
    dp.set(other);
    assertEquals(2, dp.summariesAvailable().size());
    assertTrue(dp.summariesAvailable().contains(3));
    assertTrue(dp.summariesAvailable().contains(256));
    assertEquals(42, dp.value(3).longValue());
    assertEquals(0.01, dp.value(256).doubleValue(), 0.001);
    
    // changing other doesn't affect the new one
    other.set(3, -1);
    other.set(256, -1);
    assertEquals(2, dp.summariesAvailable().size());
    assertTrue(dp.summariesAvailable().contains(3));
    assertTrue(dp.summariesAvailable().contains(256));
    assertEquals(42, dp.value(3).longValue());
    assertEquals(0.01, dp.value(256).doubleValue(), 0.001);
    
    try {
      dp.set(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
