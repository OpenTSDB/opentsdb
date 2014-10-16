// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;

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
  public void serdes() {
    final Pair<String, String> ser = new Pair<String, String>("host", "web01");
    final String json = JSON.serializeToString(ser);
    assertEquals("{\"key\":\"host\",\"value\":\"web01\"}", json);
    
    @SuppressWarnings("unchecked")
    final Pair<String, String> des = JSON.parseToObject(json, Pair.class);
    assertEquals("host", des.getKey());
    assertEquals("web01", des.getValue());
  }
  
  @Test
  public void serdesNulls() {
    final Pair<String, String> ser = new Pair<String, String>();
    final String json = JSON.serializeToString(ser);
    assertEquals("{\"key\":null,\"value\":null}", json);
    
    @SuppressWarnings("unchecked")
    final Pair<String, String> des = JSON.parseToObject(json, Pair.class);
    assertNull(des.getKey());
    assertNull(des.getValue());
  }
  
  @Test
  public void serdesList() {
    final List<Pair<String, String>> ser = 
        new ArrayList<Pair<String, String>>(2);
    ser.add(new Pair<String, String>("host", "web01"));
    ser.add(new Pair<String, String>(null, "keyisnull"));
    final String json = JSON.serializeToString(ser);
    assertEquals("[{\"key\":\"host\",\"value\":\"web01\"}," + 
        "{\"key\":null,\"value\":\"keyisnull\"}]", json);

    final TypeReference<List<Pair<String, String>>> TR = 
        new TypeReference<List<Pair<String, String>>>() {};
        
    final List<Pair<String, String>> des = JSON.parseToObject(json, TR);
    assertEquals(2, des.size());
    assertEquals("host", des.get(0).getKey());
    assertEquals("web01", des.get(0).getValue());
    assertNull(des.get(1).getKey());
    assertEquals("keyisnull", des.get(1).getValue());
  }
  
  @Test
  public void rawObjects() {
    final Pair<Object, Object> pair = new Pair<Object, Object>("host", "web01");
    assertNotNull(pair);
    assertEquals("host", pair.getKey());
    assertEquals("web01", pair.getValue());
  }
}
