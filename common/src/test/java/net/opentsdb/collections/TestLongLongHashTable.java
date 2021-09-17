// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static net.opentsdb.collections.LongLongHashTable.NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestLongLongHashTable {
  private Random random = new Random();

  @Test
  void testSize() {
    LongLongHashTable table = new LongLongHashTable(5, "Test-Table");
    assertEquals(0, table.size());

    table.put(1, 1);
    assertEquals(1, table.size());

    table.put(2, 2);
    assertEquals(2, table.size());

    table.put(3, 3);
    assertEquals(3, table.size());

    table.put(3, 4);
    assertEquals(3, table.size());

    table.put(5, 5);
    assertEquals(4, table.size());
  }

  @Test
  void testIllegalInitialCapacity() {
    IllegalArgumentException e =
            assertThrows(IllegalArgumentException.class, () -> new LongLongHashTable(-1, "Test-Table"));
    assertEquals("Illegal initial capacity: -1", e.getMessage());
  }

  @Test
  public void testCollisions() {
    LongLongHashTable table = new LongLongHashTable(5, "Test-Table");
    table.put(1, 1);
    table.put(2, 2);
    table.put(3, 3);
    table.put(4, 4);
    table.put(5, 5);
    assertEquals(1, table.get(1));
    assertEquals(2, table.get(2));
    assertEquals(3, table.get(3));
    assertEquals(4, table.get(4));
    assertEquals(5, table.get(5));
    table.put(1, 11);
    table.put(2, 12);
    table.put(3, 13);
    table.put(4, 14);
    table.put(5, 15);
    assertEquals(11, table.get(1));
    assertEquals(12, table.get(2));
    assertEquals(13, table.get(3));
    assertEquals(14, table.get(4));
    assertEquals(15, table.get(5));
  }

  @Test
  public void testGrowableHashTable() {

    LongLongHashTable table = new LongLongHashTable(2, "Test-Table");
    table.put(1, 1);
    table.put(2, 2);
    table.put(3, 3);
    table.put(3, 4);
    table.put(5, 5);

    assertEquals(1, table.get(1));
    assertEquals(2, table.get(2));
    assertEquals(4, table.get(3));
    assertEquals(NOT_FOUND, table.get(4));
    assertEquals(5, table.get(5));
  }

  @Test
  public void testSlotZero() {
    LongLongHashTable table = new LongLongHashTable(2, "Test-Table");
    table.put(4603343307331636453L, -5194788100146168532L);
    table.put(-1228717170234252043L, 5930517268447365248L);
    assertEquals(2, table.size());
  }

  @Test
  void testRemove() {

    long k1 = -8588842706157224800l;
    long k2 = 8262112918518821554l;
    long k3 = -3872077167354140016l;
    long k4 = -2911160090977624713l;
    long k5 = 556178903343935719l;

    long v1 = random.nextLong();
    long v2 = random.nextLong();
    long v3 = random.nextLong();
    long v4 = random.nextLong();
    long v5 = random.nextLong();

    LongLongHashTable table = new LongLongHashTable(2, "Test-Table");
    table.put(k1, v1);
    table.put(k2, v2);
    table.put(k3, v3);
    table.put(k4, v4);
    table.put(k5, v5);

    assertEquals(5, table.size());

    assertEquals(v1, table.remove(k1));
    assertEquals(NOT_FOUND, table.remove(k1));

    assertEquals(4, table.size());

    assertEquals(v2, table.remove(k2));
    assertEquals(v3, table.remove(k3));
    assertEquals(v4, table.remove(k4));

    assertEquals(NOT_FOUND, table.remove(k2));
    assertEquals(NOT_FOUND, table.remove(k3));
    assertEquals(NOT_FOUND, table.remove(k4));

    assertEquals(v5, table.get(k5));
    assertEquals(1, table.size());

    long k6 = random.nextLong();
    long v6 = random.nextLong();

    table.put(k6, v6);
    assertEquals(2, table.size());
  }

  private static Stream<Arguments> buildEntries() {
    Map<Long, Long> entries1 =
            new HashMap() {
              {
                put(1l, 1l);
                put(2l, 2l);
                put(3l, 3l);
                put(4l, 4l);
                put(5l, 5l);
              }
            };

    Map<Long, Long> entries2 =
            new HashMap() {
              {
                put(1l, 1l);
                put(2l, 2l);
                put(3l, 3l);
                put(4l, 4l);
                put(5l, 5l);
                put(6l, 6l);
                put(7l, 7l);
                put(8l, 8l);
                put(9l, 9l);
                put(10l, 10l);
              }
            };

    return Stream.of(
            arguments(new LongLongHashTable(10, "Test-Table"), entries1, "partially filled"),
            arguments(new LongLongHashTable(10, "Test-Table"), entries2, "full"),
            arguments(new LongLongHashTable(10, "Test-Table"), new HashMap(), "empty"));
  }

  @ParameterizedTest(name = "[{index}] {2}")
  @MethodSource("buildEntries")
  void testIteration(LongLongHashTable table, Map<Long, Long> entries, String displayName) {

    for (Map.Entry<Long, Long> entry : entries.entrySet()) {
      table.put(entry.getKey(), entry.getValue());
    }

    Set<Long> actualKeys = new HashSet<>();
    Set<Long> actualValues = new HashSet<>();

    LongLongIterator itr = table.iterator();
    while (itr.hasNext()) {
      itr.next();

      actualKeys.add(itr.key());
      actualValues.add(itr.value());
    }

    assertIterableEquals(entries.keySet(), actualKeys);
    assertIterableEquals(entries.values(), actualValues);
  }

  @Test
  void testIteratorRemove() {
    LongLongHashTable table = new LongLongHashTable(10, "Test-Table");
    table.put(1, 1);
    table.put(2, 2);
    table.put(3, 3);
    table.put(4, 4);
    table.put(5, 5);

    final LongLongIterator itr1 = table.iterator();
    while (itr1.hasNext()) {
      itr1.next();

      if (itr1.key() == 3) {
        itr1.remove();
        IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> itr1.remove());
        assertEquals("Entry removed already", exception.getMessage());
      }
    }

    assertEquals(4, table.size());
    assertEquals(NOT_FOUND, table.get(3));

    Set<Long> keySet = new HashSet<>();
    Set<Long> valueSet = new HashSet<>();
    LongLongIterator itr2 = table.iterator();
    while (itr2.hasNext()) {
      itr2.next();

      keySet.add(itr2.key());
      valueSet.add(itr2.value());
    }

    Set<Long> expected =
            new HashSet() {
              {
                add(1l);
                add(2l);
                add(4l);
                add(5l);
              }
            };

    assertIterableEquals(keySet, expected);
    assertIterableEquals(valueSet, expected);
  }

  @Test
  void testIteratorAndGrow() {
    LongLongHashTable table = new LongLongHashTable(5, "Test-Table");
    table.put(1, 1);
    table.put(2, 2);
    table.put(3, 3);
    table.put(4, 4);
    table.put(5, 5);

    LongLongIterator it = table.iterator();
    assertTrue(it.hasNext());
    it.next();
    assertEquals(5, it.key());
    assertEquals(5, it.value());

    assertTrue(it.hasNext());
    it.next();
    assertEquals(4, it.key());
    assertEquals(4, it.value());

    // grow!
    table.put(6, 6);
    assertFalse(it.hasNext());
  }

  @Test
  public void testClone() throws Exception {
    LongLongHashTable table = new LongLongHashTable(5, "Test-Table");
    table.put(1, 1);
    table.put(2, 2);
    table.put(3, 3);
    table.put(4, 4);
    table.put(5, 5);
    assertEquals(5, table.size());

    LongLongHashTable clone = (LongLongHashTable) table.clone();
    assertEquals(1, clone.get(1));
    assertEquals(2, clone.get(2));
    assertEquals(3, clone.get(3));
    assertEquals(4, clone.get(4));
    assertEquals(5, clone.get(5));
    assertEquals(5, clone.size());
    assertNotEquals(table, clone);
    table.close();
    clone.close();

    // double close for coverage.
    clone.close();
  }

  @Test
  void testToStringOfEmptyTable() {
    LongLongHashTable table = new LongLongHashTable(10, "Test-Table");
    assertEquals("{}", table.toString());
  }

  @Test
  void testToString() {
    LongLongHashTable table = new LongLongHashTable(10, "Test-Table");
    table.put(1l, 1l);
    table.put(2l, 2l);
    table.put(3l, 3l);
    table.put(4l, 4l);
    table.put(5l, 5l);

    assertEquals("{1=1, 3=3, 2=2, 5=5, 4=4}", table.toString());
  }

  // TODO - figure out a way to test the rehash. One way, edit the offheap array
  // to cause a scan and reflect into the object and set the scan count super high.

}
