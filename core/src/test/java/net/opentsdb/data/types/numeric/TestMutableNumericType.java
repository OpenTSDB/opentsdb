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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestMutableNumericType {

  @Test
  public void ctors() throws Exception {
    MutableNumericType dp = new MutableNumericType();
    assertFalse(dp.isInteger());
    assertTrue(Double.isNaN(dp.doubleValue()));
    assertTrue(Double.isNaN(dp.toDouble()));
    try {
      dp.longValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    
    dp = new MutableNumericType(42);
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    assertEquals(42, dp.toDouble(), 0.001);
    try {
      dp.doubleValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    
    dp = new MutableNumericType(42.5);
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    assertEquals(42.5, dp.toDouble(), 0.001);
    try {
      dp.longValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    assertEquals("42.5", dp.toString());
    
    MutableNumericType copy = new MutableNumericType(dp);
    assertFalse(copy.isInteger());
    assertEquals(42.5, copy.doubleValue(), 0.001);
    assertEquals(42.5, copy.toDouble(), 0.001);
    try {
      copy.longValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    assertEquals("42.5", copy.toString());
    
    try {
      new MutableNumericType(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void set() throws Exception {
    MutableNumericType dp = new MutableNumericType();
    assertFalse(dp.isInteger());
    assertTrue(Double.isNaN(dp.doubleValue()));
    assertTrue(Double.isNaN(dp.toDouble()));
    try {
      dp.longValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    
    dp.set(0);
    assertTrue(dp.isInteger());
    assertEquals(0, dp.longValue());
    assertEquals(0, dp.toDouble(), 0.001);
    try {
      dp.doubleValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    assertEquals("0", dp.toString());
    
    dp.set(42.5);
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    assertEquals(42.5, dp.toDouble(), 0.001);
    try {
      dp.longValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    assertEquals("42.5", dp.toString());
    
    dp.set(Long.MAX_VALUE);
    assertTrue(dp.isInteger());
    assertEquals(Long.MAX_VALUE, dp.longValue());
    assertEquals(Long.MAX_VALUE, dp.toDouble(), 0.001);
    try {
      dp.doubleValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    
    dp.set(Double.MIN_VALUE);
    assertFalse(dp.isInteger());
    assertEquals(Double.MIN_VALUE, dp.doubleValue(), 0.001);
    assertEquals(Double.MIN_VALUE, dp.toDouble(), 0.001);
    try {
      dp.longValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    
    MutableNumericType other = new MutableNumericType();
 
    dp.set(other);
    assertFalse(dp.isInteger());
    assertTrue(Double.isNaN(dp.doubleValue()));
    assertTrue(Double.isNaN(dp.toDouble()));
    try {
      dp.longValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    
    other.set(42);
    dp.set(other);
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    assertEquals(42, dp.toDouble(), 0.001);
    try {
      dp.doubleValue();
      fail("Expected ClassCastException");
    } catch (ClassCastException e) { }
    
    try {
      dp.set(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
}
