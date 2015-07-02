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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PairTest {

  @Test
  public void defaultCtor() {
    final Pair<String, String> pair = Pair.create(null, null);
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertNull(pair.getValue());
  }

  @Test
  public void ctorWithArgs() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    assertNotNull(pair);
    assertEquals("host", pair.getKey());
    assertEquals("web01", pair.getValue());
  }

  @Test
  public void ctorWithNullKey() {
    final Pair<String, String> pair = Pair.create(null, "web01");
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertEquals("web01", pair.getValue());
  }

  @Test
  public void ctorWithNullValue() {
    final Pair<String, String> pair = Pair.create("host", null);
    assertNotNull(pair);
    assertEquals("host", pair.getKey());
    assertNull(pair.getValue());
  }

  @Test
  public void ctorWithNulls() {
    final Pair<String, String> pair = Pair.create(null, null);
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertNull(pair.getValue());
  }

  @Test
  public void hashcodeTest() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    assertEquals(109885949, pair.hashCode());
  }

  @Test
  public void hashcodeTestNullKey() {
    final Pair<String, String> pair = Pair.create(null, "web01");
    assertEquals(113003605, pair.hashCode());
  }

  @Test
  public void hashcodeTestNullValue() {
    final Pair<String, String> pair = Pair.create("host", null);
    assertEquals(3208616, pair.hashCode());
  }

  @Test
  public void hashcodeTestNulls() {
    final Pair<String, String> pair = Pair.create(null, null);
    assertEquals(0, pair.hashCode());
  }

  @Test
  public void equalsTest() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    final Pair<String, String> pair2 = Pair.create("host", "web01");
    assertTrue(pair.equals(pair2));
  }

  @Test
  public void equalsTestSameReference() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    final Pair<String, String> pair2 = pair;
    assertTrue(pair.equals(pair2));
  }

  @Test
  public void equalsTestDiffKey() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    final Pair<String, String> pair2 = Pair.create("diff", "web01");
    assertFalse(pair.equals(pair2));
  }

  @Test
  public void equalsTestDiffVal() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    final Pair<String, String> pair2 = Pair.create("host", "diff");
    assertFalse(pair.equals(pair2));
  }

  @Test
  public void equalsTestDiffNullKey() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    final Pair<String, String> pair2 = Pair.create(null, "web01");
    assertFalse(pair.equals(pair2));
  }

  @Test
  public void equalsTestDiffNullVal() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    final Pair<String, String> pair2 = Pair.create("host", null);
    assertFalse(pair.equals(pair2));
  }

  @Test
  public void equalsTestNullKeys() {
    final Pair<String, String> pair = Pair.create(null, "web01");
    final Pair<String, String> pair2 = Pair.create(null, "web01");
    assertTrue(pair.equals(pair2));
  }

  @Test
  public void equalsTestNullValues() {
    final Pair<String, String> pair = Pair.create("host", null);
    final Pair<String, String> pair2 = Pair.create("host", null);
    assertTrue(pair.equals(pair2));
  }

  @Test
  public void equalsTestNulls() {
    final Pair<String, String> pair = Pair.create(null, null);
    final Pair<String, String> pair2 = Pair.create(null, null);
    assertTrue(pair.equals(pair2));
  }

  @Test
  public void equalsTestDiffTypes() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    final Pair<Integer, Integer> pair2 = Pair.create(1, 42);
    assertFalse(pair.equals(pair2));
  }

  @Test
  public void toStringTest() {
    final Pair<String, String> pair = Pair.create("host", "web01");
    assertEquals("key=host, value=web01", pair.toString());
  }

  @Test
  public void toStringTestNulls() {
    final Pair<String, String> pair = Pair.create(null, null);
    assertEquals("key=null, value=null", pair.toString());
  }

  @Test
  public void toStringTestNumbers() {
    final Pair<Integer, Long> pair = Pair.create(1, 42L);
    assertEquals("key=1, value=42", pair.toString());
  }

  @Test
  public void serdes() throws IOException {
    final Pair<String, String> ser = Pair.create("host", "web01");
    ObjectMapper jsonMapper = new ObjectMapper();

    final String json = jsonMapper.writeValueAsString(ser);

    assertEquals("{\"key\":\"host\",\"value\":\"web01\"}", json);

    final Pair<String, String> des = jsonMapper.reader(Pair.class).readValue(json);

    assertEquals("host", des.getKey());
    assertEquals("web01", des.getValue());
  }

  @Test
  public void serdesNulls() throws IOException {
    final Pair<String, String> ser = Pair.create(null, null);
    ObjectMapper jsonMapper = new ObjectMapper();
    final String json = jsonMapper.writeValueAsString(ser);
    assertEquals("{\"key\":null,\"value\":null}", json);

    final Pair<String, String> des = jsonMapper.reader(Pair.class).readValue(json);
    assertNull(des.getKey());
    assertNull(des.getValue());
  }

  @Test
  public void serdesList() throws IOException {
    final List<Pair<String, String>> ser = new ArrayList<>(2);
    ser.add(Pair.create("host", "web01"));
    ser.add(Pair.<String, String>create(null, "keyisnull"));

    ObjectMapper jsonMapper = new ObjectMapper();

    final String json = jsonMapper.writeValueAsString(ser);
    assertEquals("[{\"key\":\"host\",\"value\":\"web01\"},"
                 + "{\"key\":null,\"value\":\"keyisnull\"}]", json);

    final List<Pair<String, String>> des = jsonMapper.readValue(json, new
        TypeReference<List<Pair<String, String>>>() {
        });

    assertEquals(2, des.size());
    assertEquals("host", des.get(0).getKey());
    assertEquals("web01", des.get(0).getValue());
    assertNull(des.get(1).getKey());
    assertEquals("keyisnull", des.get(1).getValue());
  }

  @Test
  public void rawObjects() {
    final Pair<?, ?> pair = Pair.create("host", "web01");
    assertNotNull(pair);
    assertEquals("host", pair.getKey());
    assertEquals("web01", pair.getValue());
  }
}
