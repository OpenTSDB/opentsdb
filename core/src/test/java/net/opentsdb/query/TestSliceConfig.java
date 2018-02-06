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
package net.opentsdb.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.SliceConfig.SliceType;

public class TestSliceConfig {

  @Test (expected = IllegalArgumentException.class)
  public void ctorNull() {
    new SliceConfig(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyString() {
    new SliceConfig("");
  }
  
  @Test
  public void ctorPercent() {
    SliceConfig config = new SliceConfig("50%");
    assertEquals(50, config.getQuantity());
    assertEquals("50%", config.getStringConfig());
    assertEquals(SliceType.PERCENT, config.getSliceType());
    assertNull(config.getUnits());
    
    config = new SliceConfig("1%");
    assertEquals(1, config.getQuantity());
    assertEquals("1%", config.getStringConfig());
    assertEquals(SliceType.PERCENT, config.getSliceType());
    assertNull(config.getUnits());
    
    config = new SliceConfig("100%");
    assertEquals(100, config.getQuantity());
    assertEquals("100%", config.getStringConfig());
    assertEquals(SliceType.PERCENT, config.getSliceType());
    assertNull(config.getUnits());
    
    try {
      new SliceConfig("0%");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new SliceConfig("-10%");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new SliceConfig("50.5%");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new SliceConfig("200%");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new SliceConfig("nan%");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void ctorDuration() {
    SliceConfig config = new SliceConfig("1h");
    assertEquals(1, config.getQuantity());
    assertEquals("1h", config.getStringConfig());
    assertEquals(SliceType.DURATION, config.getSliceType());
    assertEquals("h", config.getUnits());
    
    config = new SliceConfig("500ms");
    assertEquals(500, config.getQuantity());
    assertEquals("500ms", config.getStringConfig());
    assertEquals(SliceType.DURATION, config.getSliceType());
    assertEquals("ms", config.getUnits());
    
    config = new SliceConfig("30m");
    assertEquals(30, config.getQuantity());
    assertEquals("30m", config.getStringConfig());
    assertEquals(SliceType.DURATION, config.getSliceType());
    assertEquals("m", config.getUnits());
    
    try {
      new SliceConfig("-30m");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new SliceConfig("30t");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new SliceConfig("nanm");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new SliceConfig("100");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
