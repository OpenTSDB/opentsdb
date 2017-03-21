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
