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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static net.opentsdb.collections.LongIntHashTable.NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestLongIntHashTable {
  private Random random = new Random();

  @Test
  void testSize() {
    LongIntHashTable table = new LongIntHashTable(5, "Test-Table");
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
            assertThrows(IllegalArgumentException.class, () -> new LongIntHashTable(-1, "Test-Table"));
    assertEquals("Illegal initial capacity: -1", e.getMessage());
  }

  @Test
  public void testCollisions() {
    LongIntHashTable table = new LongIntHashTable(5, "Test-Table");
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

    LongIntHashTable table = new LongIntHashTable(2, "Test-Table");
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
    LongIntHashTable table = new LongIntHashTable(2, "Test-Table");
    table.put(-6556139685902020887L, 2114195918);
    table.put(-2315547347192171810L, -1172410493);
    assertEquals(2, table.size());
  }

  @Test
  void testRemove() {

    long k1 = -8588842706157224800l;
    long k2 = 8262112918518821554l;
    long k3 = -3872077167354140016l;
    long k4 = -2911160090977624713l;
    long k5 = 556178903343935719l;

    int v1 = random.nextInt();
    int v2 = random.nextInt();
    int v3 = random.nextInt();
    int v4 = random.nextInt();
    int v5 = random.nextInt();

    LongIntHashTable table = new LongIntHashTable(2, "Test-Table");
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
    int v6 = random.nextInt();

    table.put(k6, v6);
    assertEquals(2, table.size());
  }

  private static Stream<Arguments> buildEntries() {
    Map<Long, Integer> entries1 =
            new HashMap() {
              {
                put(1l, 1);
                put(2l, 2);
                put(3l, 3);
                put(4l, 4);
                put(5l, 5);
              }
            };

    Map<Long, Long> entries2 =
            new HashMap() {
              {
                put(1l, 1);
                put(2l, 2);
                put(3l, 3);
                put(4l, 4);
                put(5l, 5);
                put(6l, 6);
                put(7l, 7);
                put(8l, 8);
                put(9l, 9);
                put(10l, 10);
              }
            };

    return Stream.of(
            arguments(new LongIntHashTable(10, "Test-Table"), entries1, "partially filled"),
            arguments(new LongIntHashTable(10, "Test-Table"), entries2, "full"),
            arguments(new LongIntHashTable(10, "Test-Table"), new HashMap(), "empty"));
  }

  @ParameterizedTest(name = "[{index}] {2}")
  @MethodSource("buildEntries")
  void testIteration(LongIntHashTable table, Map<Long, Integer> entries, String displayName) {

    for (Map.Entry<Long, Integer> entry : entries.entrySet()) {
      table.put(entry.getKey(), entry.getValue());
    }

    Set<Long> actualKeys = new HashSet<>();
    Set<Integer> actualValues = new HashSet<>();

    LongIntIterator itr = table.iterator();
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
    LongIntHashTable table = new LongIntHashTable(10, "Test-Table");
    table.put(1, 1);
    table.put(2, 2);
    table.put(3, 3);
    table.put(4, 4);
    table.put(5, 5);

    final LongIntIterator itr1 = table.iterator();
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
    Set<Integer> valueSet = new HashSet<>();
    LongIntIterator itr2 = table.iterator();
    while (itr2.hasNext()) {
      itr2.next();

      keySet.add(itr2.key());
      valueSet.add(itr2.value());
    }

    Set<Long> expectedKeys =
            new HashSet() {
              {
                add(1l);
                add(2l);
                add(4l);
                add(5l);
              }
            };

    Set<Integer> expectedValues =
            new HashSet() {
              {
                add(1);
                add(2);
                add(4);
                add(5);
              }
            };

    assertIterableEquals(keySet, expectedKeys);
    assertIterableEquals(valueSet, expectedValues);
  }

  @Test
  void testIteratorAndGrow() {
    LongIntHashTable table = new LongIntHashTable(5, "Test-Table");
    table.put(1, 1);
    table.put(2, 2);
    table.put(3, 3);
    table.put(4, 4);
    table.put(5, 5);

    LongIntIterator it = table.iterator();
    assertTrue(it.hasNext());
    it.next();
    assertEquals(1, it.key());
    assertEquals(1, it.value());

    assertTrue(it.hasNext());
    it.next();
    assertEquals(2, it.key());
    assertEquals(2, it.value());

    // grow!
    table.put(6, 6);
    assertFalse(it.hasNext());
  }

  @Test
  public void testClone() throws Exception {
    LongIntHashTable table = new LongIntHashTable(5, "Test-Table");
    table.put(1, 1);
    table.put(2, 2);
    table.put(3, 3);
    table.put(4, 4);
    table.put(5, 5);
    assertEquals(5, table.size());

    LongIntHashTable clone = (LongIntHashTable) table.clone();
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
    LongIntHashTable table = new LongIntHashTable(10, "Test-Table");
    assertEquals("{}", table.toString());
  }

  @Test
  void testToString() {
    LongIntHashTable table = new LongIntHashTable(10, "Test-Table");
    table.put(1l, 1);
    table.put(2l, 2);
    table.put(3l, 3);
    table.put(4l, 4);
    table.put(5l, 5);

    assertEquals("{1=1, 2=2, 3=3, 4=4, 5=5}", table.toString());
  }
}
