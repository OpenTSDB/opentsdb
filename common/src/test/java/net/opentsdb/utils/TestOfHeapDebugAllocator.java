/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  The OpenTSDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.utils;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.utils.OffHeapDebugAllocator.Tracker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOfHeapDebugAllocator {

  private static MockTSDB TSDB;

  @BeforeAll
  public static void beforeClass() {
    TSDB = new MockTSDB();
  }

  @Test
  public void properFlowNoTracking() throws Exception {
    OffHeapDebugAllocator allocator = new OffHeapDebugAllocator(TSDB, 0, "ut");

    long address = allocator.allocate(16);
    assertTrue(address > 0);
    assertEquals(1, allocator.allocatedAddresses.size());
    assertEquals(0, allocator.freedAddresses.size());

    // no throw
    allocator.read(address);

    allocator.free(this, address);
    assertEquals(0, allocator.allocatedAddresses.size());
    assertEquals(1, allocator.freedAddresses.size());
  }

  @Test
  public void properFlowTracking() throws Exception {
    OffHeapDebugAllocator allocator = new OffHeapDebugAllocator(TSDB, 5, "ut");

    long address = allocator.allocate(16);
    assertTrue(address > 0);
    assertEquals(1, allocator.allocatedAddresses.size());
    assertEquals(0, allocator.trackerFreed.size());

    // no throw
    allocator.read(address);

    allocator.free(this, address);
    assertEquals(0, allocator.allocatedAddresses.size());
    assertEquals(1, allocator.trackerFreed.size());
    Tracker tracker = allocator.trackerFreed.get(address);
    assertEquals(5, tracker.freeStack.length);
    // make sure the first stack is THIS class!
    assertTrue(tracker.freeStack[0].contains(getClass().getSimpleName()));
  }

  @Test
  public void freeErrors() throws Exception {
    OffHeapDebugAllocator allocator = new OffHeapDebugAllocator(TSDB, 0, "ut");
    long address = allocator.allocate(16);

    // not in allocMap
    assertThrows(IllegalStateException.class, () -> {
      allocator.free(this, 42);
    });

    // ok!
    allocator.free(this, address);

    // already free'd
    assertThrows(IllegalStateException.class, () -> {
      allocator.free(this, address);
    });
  }

  @Test
  public void freeErrorsTracking() throws Exception {
    OffHeapDebugAllocator allocator = new OffHeapDebugAllocator(TSDB, 5, "ut");
    long address = allocator.allocate(16);

    // not in allocMap
    assertThrows(IllegalStateException.class, () -> {
      allocator.free(this, 42);
    });

    // ok!
    allocator.free(this, address);

    // already free'd
    assertThrows(IllegalStateException.class, () -> {
      allocator.free(this, address);
    });
  }

  @Test
  public void readErrors() throws Exception {
    OffHeapDebugAllocator allocator = new OffHeapDebugAllocator(TSDB, 0, "ut");
    long address = allocator.allocate(16);

    // not in allocMap
    assertThrows(IllegalStateException.class, () -> {
      allocator.read(42);
    });

    // ok!
    allocator.read(address);
    allocator.free(this, address);

    // already free'd
    assertThrows(IllegalStateException.class, () -> {
      allocator.read(address);
    });
  }

  @Test
  public void readErrorsTracking() throws Exception {
    OffHeapDebugAllocator allocator = new OffHeapDebugAllocator(TSDB, 5, "ut");
    long address = allocator.allocate(16);

    // not in allocMap
    assertThrows(IllegalStateException.class, () -> {
      allocator.read(42);
    });

    // ok!
    allocator.read(address);
    allocator.free(this, address);

    // already free'd
    assertThrows(IllegalStateException.class, () -> {
      allocator.read(address);
    });
  }

}
