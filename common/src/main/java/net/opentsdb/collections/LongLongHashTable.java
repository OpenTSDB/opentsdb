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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.Closeable;
import java.math.BigInteger;

/**
 * A linear probing primitive Map for long keys and long values. Stores data off heap.
 *
 * @see DirectByteArray
 * @see LongLongIterator
 */
public class LongLongHashTable implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LongLongHashTable.class);

  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final int KEY_SZ = 8;
  private static final int VALUE_SZ = 8;
  public static final long NOT_FOUND = Long.MIN_VALUE;

  private final int slotSz;
  private int slots;
  private int threshold;
  private int arrayLength;
  private int sz;
  private String name;
  private long address;
  private DirectByteArray table;

  public LongLongHashTable(final int initialCapacity, final String name) {

    if (initialCapacity < 0) {
      throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
    }

    this.slotSz = KEY_SZ + VALUE_SZ;
    this.slots = initialCapacity;
    this.name = name;
    resize();
  }

  @Override
  public void close() {
    if (table != null) {
      table.free();
      table = null;
    }
  }

  private void resize() {

    int oldCap = slots;
    int newCap = oldCap;
    if (table != null) { // growing the table;
      newCap = oldCap << 1; // double the size;
    }

    BigInteger p = BigInteger.valueOf(newCap);
    newCap = p.nextProbablePrime().intValueExact();

    int newThr = (int) (newCap * DEFAULT_LOAD_FACTOR);

    this.arrayLength = newCap * slotSz;
    DirectByteArray oldTable = null;
    if (table == null) {
      this.table = new DirectByteArray(arrayLength, false);
    } else {
      long oldAddress = table.init(arrayLength, false);
      oldTable = new DirectByteArray(oldAddress, false, oldCap * slotSz);
    }
    this.address = table.getAddress();
    this.sz = 0;
    this.slots = newCap;
    this.threshold = newThr;
    if (oldTable != null) {
      for (int i = 0; i < oldCap; i++) {
        int offset = i * slotSz;
        long key = oldTable.getLong(offset);
        if (key != 0 && key != -1) {
          put(key, oldTable.getLong(offset + KEY_SZ));
        }
      }
      oldTable.free();
    }
  }

  public int put(final long key, final long value) {
    int slot = getSlot(key);
    int scanLength = 0;

    int offset = slot * slotSz;
    long target = table.getLong(offset);

    boolean emptySlot = target == 0 || target == -1;
    while (!emptySlot && key != target) {
      slot = nextSlot(slot);
      scanLength++;
      offset = slot * slotSz;
      target = table.getLong(offset);
      emptySlot = target == 0 || target == -1;
    }

    if (emptySlot) {
      sz++;
    }

    table.setLong(offset, key);
    offset += KEY_SZ;
    table.setLong(offset, value);

    if (sz > threshold) {
      long start = System.nanoTime();
      int oldLength = arrayLength;
      resize();
      long end = System.nanoTime();
      LOGGER.info("Resized {} from {} to {} in {} ns", name, oldLength, arrayLength, (end - start));
    }
    return scanLength;
  }

  public long get(final long key) {
    int slot = getSlot(key);
    int offset = slot * slotSz;
    long target = table.getLong(offset);
    boolean emptySlot = target == 0;
    while (!emptySlot) {
      if (key == target) {
        return table.getLong(offset + KEY_SZ);
      }
      slot = nextSlot(slot);
      offset = slot * slotSz;
      target = table.getLong(offset);
      emptySlot = target == 0;
    }
    return NOT_FOUND;
  }

  public long remove(final long key) {
    int slot = getSlot(key);
    int offset = slot * slotSz;
    long target = table.getLong(offset);
    boolean emptySlot = target == 0;
    while (!emptySlot) {
      if (key == target) {
        return resetSlotByOffset(offset);
      }
      slot = nextSlot(slot);
      offset = slot * slotSz;
      target = table.getLong(offset);
      emptySlot = target == 0;
    }
    return NOT_FOUND;
  }

  public int size() {
    return sz;
  }

  private int getSlot(final long key) {
    // a bit slow, another option is to use a size that is multiplier of 2 and bitmask it
    return Math.abs((int) (key % slots));
  }

  private int nextSlot(int slot) {
    slot++;
    if (slot * slotSz >= arrayLength) {
      slot = 0;
    }
    return slot;
  }

  private long resetSlotByOffset(final int offset) {
    table.setLong(offset, -1);
    int valueOffset = offset + KEY_SZ;
    long lastValue = table.getLong(valueOffset);
    table.setLong(valueOffset, -1);
    sz--;
    return lastValue;
  }

  /**
   * A proper deep clone with off-heap memory. Can be used for snapshots if the original is
   * captured in a single thread.
   * @return The clone.
   */
  public Object clone() {
    final DirectByteArray newTable = new DirectByteArray(arrayLength, false);
    final Unsafe unsafe = UnsafeHelper.unsafe;
    unsafe.copyMemory(table.getAddress(), newTable.getAddress(), arrayLength);
    LongLongHashTable newLLHT = new LongLongHashTable(slots, name);
    newLLHT.slots = slots;
    newLLHT.threshold = threshold;
    newLLHT.arrayLength = arrayLength;
    newLLHT.sz = sz;
    newLLHT.name = name;
    newLLHT.table = newTable;
    return newLLHT;
  }

  public LongLongIterator iterator() {
    return new LongLongHashIterator();
  }

  private class LongLongHashIterator implements LongLongIterator {

    int count = sz;
    int offset = 0 - slotSz;
    boolean removed = false;
    final long originalAddress;

    LongLongHashIterator() {
      originalAddress = address;
    }

    @Override
    public long key() {
      return table.getLong(offset);
    }

    @Override
    public long value() {
      return table.getLong(offset + KEY_SZ);
    }

    @Override
    public boolean hasNext() {
      return count > 0 && originalAddress == address;
    }

    @Override
    public Void next() {
      offset += slotSz;
      long target = table.getLong(offset);
      boolean emptySlot = target == 0 || target == -1;
      while (emptySlot) {
        offset += slotSz;
        target = table.getLong(offset);
        emptySlot = target == 0 || target == -1;
      }
      count--;
      removed = false;
      return null;
    }

    @Override
    public void remove() {
      if (removed) {
        throw new IllegalStateException("Entry removed already");
      }
      resetSlotByOffset(offset);
      removed = true;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    LongLongIterator iterator = iterator();
    boolean first = true;
    while (iterator.hasNext()) {
      iterator.next();
      long key = iterator.key();
      long value = iterator.value();
      if (first) {
        first = false;
      } else {
        sb.append(',').append(' ');
      }
      sb.append(key);
      sb.append('=');
      sb.append(value);
    }
    sb.append('}');
    return sb.toString();
  }
}