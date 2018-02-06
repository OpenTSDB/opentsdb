// This file is part of OpenTSDB.
// Copyright (C) 2014-2017  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestPair {

  @Test
  public void defaultCtor() {
    final Pair<String, String> pair = new Pair<String, String>();
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertNull(pair.getValue());
  }
  
  @Test
  public void ctorWithArgs() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    assertNotNull(pair);
    assertEquals("host", pair.getKey());
    assertEquals("web01", pair.getValue());
  }
  
  @Test
  public void ctorWithNullKey() {
    final Pair<String, String> pair = new Pair<String, String>(null, "web01");
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertEquals("web01", pair.getValue());
  }
  
  @Test
  public void ctorWithNullValue() {
    final Pair<String, String> pair = new Pair<String, String>("host", null);
    assertNotNull(pair);
    assertEquals("host", pair.getKey());
    assertNull(pair.getValue());
  }
  
  @Test
  public void ctorWithNulls() {
    final Pair<String, String> pair = new Pair<String, String>(null, null);
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertNull(pair.getValue());
  }
  
  @Test
  public void hashcodeTest() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    assertEquals(109885949, pair.hashCode());
  }
  
  @Test
  public void hashcodeTestNullKey() {
    final Pair<String, String> pair = new Pair<String, String>(null, "web01");
    assertEquals(113003605, pair.hashCode());
  }
  
  @Test
  public void hashcodeTestNullValue() {
    final Pair<String, String> pair = new Pair<String, String>("host", null);
    assertEquals(3208616, pair.hashCode());
  }
  
  @Test
  public void hashcodeTestNulls() {
    final Pair<String, String> pair = new Pair<String, String>();
    assertEquals(0, pair.hashCode());
  }
  
  @Test
  public void equalsTest() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    final Pair<String, String> pair2 = new Pair<String, String>("host", "web01");
    assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestSameReference() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    final Pair<String, String> pair2 = pair;
    assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffKey() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    final Pair<String, String> pair2 = new Pair<String, String>("diff", "web01");
    assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffVal() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    final Pair<String, String> pair2 = new Pair<String, String>("host", "diff");
    assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffNullKey() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    final Pair<String, String> pair2 = new Pair<String, String>(null, "web01");
    assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffNullVal() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    final Pair<String, String> pair2 = new Pair<String, String>("host", null);
    assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestNullKeys() {
    final Pair<String, String> pair = new Pair<String, String>(null, "web01");
    final Pair<String, String> pair2 = new Pair<String, String>(null, "web01");
    assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestNullValues() {
    final Pair<String, String> pair = new Pair<String, String>("host", null);
    final Pair<String, String> pair2 = new Pair<String, String>("host", null);
    assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestNulls() {
    final Pair<String, String> pair = new Pair<String, String>();
    final Pair<String, String> pair2 = new Pair<String, String>();
    assertTrue(pair.equals(pair2));
  }
  
  @Test
  public void equalsTestDiffTypes() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    final Pair<Integer, Integer> pair2 = new Pair<Integer, Integer>(1, 42);
    assertFalse(pair.equals(pair2));
  }
  
  @Test
  public void toStringTest() {
    final Pair<String, String> pair = new Pair<String, String>("host", "web01");
    assertEquals("key=host, value=web01", pair.toString());
  }
  
  @Test
  public void toStringTestNulls() {
    final Pair<String, String> pair = new Pair<String, String>();
    assertEquals("key=null, value=null", pair.toString());
  }
  
  @Test
  public void toStringTestNumbers() {
    final Pair<Integer, Long> pair = new Pair<Integer, Long>(1, 42L);
    assertEquals("key=1, value=42", pair.toString());
  }

  @Test
  public void rawObjects() {
    final Pair<Object, Object> pair = new Pair<Object, Object>("host", "web01");
    assertNotNull(pair);
    assertEquals("host", pair.getKey());
    assertEquals("web01", pair.getValue());
  }
}
