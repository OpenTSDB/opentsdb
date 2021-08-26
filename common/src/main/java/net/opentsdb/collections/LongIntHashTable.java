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
 * A linear probing Map for long keys and int values. Stores data off heap.
 *
 * NOTE: There is now a hacky, ugly way to rehash the map without resizing when
 * deletes start to result in too many scans for missed entries. If the average
 * number of scans per operation (any operation) exceeds the scan rehash threshold
 * then we'll pick the next prime number from the primes set to hash with. It will
 * roll over but by that time the key set should hopefully be fairly new.
 *
 * There is also a decay in growth to avoid simply doubling and finding a prime
 * for the next size. We saw an issue where we only had 5.5M entries but the table
 * grew to 192M slots! So this isn't great but it will work for our use case.
 *
 * @see DirectByteArray
 * @see LongIntIterator
 */
public class LongIntHashTable implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LongIntHashTable.class);

  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final float DEFAULT_SCAN_REHASH_THRESHOLD = 25.5f;
  private static final float DEFAULT_GROWTH_RATE = 0.75f;
  private static final float DEFAULT_GROWTH_DECAY = 0.25f;
  private static final int KEY_SZ = Long.BYTES;
  private static final int VALUE_SZ = Integer.BYTES;
  // https://planetmath.org/goodhashtableprimes. In a quick test they seem solid.
  public static final int[] PRIMES_FOR_HASHING = new int[] {
          193, 769, 1543, 3079, 6151, 12289, 24593, 49157, 98317, 196613, 393241,
          786433, 1572869, 3145739, 6291469, 12582917, 25165843, 50331653,
          100663319, 201326611, 402653189, 805306457, 1610612741};
  public static final int NOT_FOUND = Integer.MIN_VALUE;

  private final int slotSz;
  private int slots;
  private int threshold;
  private int arrayLength;
  private int sz;
  private String name;
  private long address;
  private int primeIndex;
  private int prime;
  private long scans;
  private long opCount;
  private double scanRehashThreshold;
  private float growthRate;
  private float growthDecay;
  private DirectByteArray table;

  /**
   * Default ctor with a 0.75 load, 0.75 growth factor, 0.75 growth decline and
   * scan rehash threshold of 25.5.
   * @param initialCapacity The initial capacity of the table. Must be greater
   *                        than or equal to 0.
   * @param name A descriptive name for the table used in debug logs when
   *             growing or rehashing.
   */
  public LongIntHashTable(final int initialCapacity, final String name) {
    this(initialCapacity,
            name,
            DEFAULT_GROWTH_RATE,
            DEFAULT_GROWTH_DECAY,
            DEFAULT_SCAN_REHASH_THRESHOLD);
  }

  /**
   * Ctor with all of the tuning parameters.
   * @param initialCapacity The initial capacity of the table. Must be greater
   *                        than or equal to 0.
   * @param name A descriptive name for the table used in debug logs when growing
   *             or rehashing.
   * @param growthRate The initial amount of growth when the table needs to be
   *                   resized. Must be a floating point ratio, e.g. 0.75 means
   *                   the table will grow by 75% of the existing size.
   * @param growthDecay How much the growth rate should be reduced on each resize.
   *                    This slows growth over time so it's less likely to blow
   *                    out the resident set. E.g. if the initial growth rate is
   *                    0.75 and the decay is 0.25, the amount of growth will be
   *                    reduced by 25% each time. The progression would be
   *                    0.75, 0.56, 0.42, 0.32, etc.
   * @param scanRehashThreshold A threshold on the average number of scans per
   *                            operation that is used to determine when to rehash
   *                            the table.
   */
  public LongIntHashTable(final int initialCapacity,
                          final String name,
                          final float growthRate,
                          final float growthDecay,
                          final float scanRehashThreshold) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
    }

    this.slotSz = KEY_SZ + VALUE_SZ;
    this.slots = initialCapacity;
    this.name = name;
    prime = PRIMES_FOR_HASHING[0];
    this.growthRate = growthRate;
    this.growthDecay = growthDecay;
    this.scanRehashThreshold = scanRehashThreshold;
    resize();
  }

  private LongIntHashTable(final LongIntHashTable parent,
                           final DirectByteArray table) {
    this.table = table;
    this.slotSz = KEY_SZ + VALUE_SZ;
    slots = parent.slots;
    threshold = parent.threshold;
    arrayLength = parent.arrayLength;
    sz = parent.sz;
    prime = parent.prime;
    primeIndex = parent.primeIndex;
    growthRate = parent.growthRate;;
    growthDecay = parent.growthDecay;
    scanRehashThreshold = parent.scanRehashThreshold;
    // purposely leaving scans and ops out.
    address = table.getAddress();
    name = parent.name;
  }

  @Override
  public void close() {
    if (table != null) {
      table.free();
      table = null;
    }
  }

  private void resize() {
    long start = System.nanoTime();
    int oldLength = arrayLength;
    int oldCap = slots;
    int newCap = oldCap;
    if (table != null) { // growing the table;
      newCap = (int) (oldCap + (oldCap * growthRate));
      growthRate *= (1f - growthDecay);
      if (growthRate < 0.001f) {
        growthRate = 0.001f;
      }
    } else {
      BigInteger p = BigInteger.valueOf(newCap);
      newCap = p.nextProbablePrime().intValueExact();
    }

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
          put(key, oldTable.getInt(offset + KEY_SZ));
        }
      }
      oldTable.free();
      long end = System.nanoTime();
      LOGGER.info("Resized {} from {} to {} in {} ns", name, oldLength, arrayLength, (end - start));
    }
  }

  private void rehash() {
    long start = System.nanoTime();
    if (primeIndex + 1 >= PRIMES_FOR_HASHING.length) {
      primeIndex = 0;
    } else {
      primeIndex++;
    }
    prime = PRIMES_FOR_HASHING[primeIndex];
    scans = 0;
    opCount = 0;

    DirectByteArray oldTable = null;
    if (table == null) {
      this.table = new DirectByteArray(arrayLength, false);
    } else {
      long oldAddress = table.init(arrayLength, false);
      oldTable = new DirectByteArray(oldAddress, false, slots * slotSz);
    }
    this.address = table.getAddress();
    this.sz = 0;
    if (oldTable != null) {
      for (int i = 0; i < slots; i++) {
        int offset = i * slotSz;
        long key = oldTable.getLong(offset);
        if (key != 0 && key != -1) {
          put(key, oldTable.getInt(offset + KEY_SZ));
        }
      }
      oldTable.free();
    }

    long end = System.nanoTime();
    LOGGER.info("Rehashed {} in {} ns with new prime {}", name, (end - start), prime);
  }

  public int put(final long key, final int value) {
    if (++opCount < 0) {
      opCount = 1;
      scans = 0;
    }
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
    table.setInt(offset, value);

    if (sz > threshold) {
      resize();
    } else if (scanLength > 0) {
      scans += scanLength;
      if (((double) scans / (double) opCount) > scanRehashThreshold) {
        rehash();
      }
    }
    return scanLength;
  }

  public int get(final long key) {
    if (++opCount < 0) {
      opCount = 1;
      scans = 0;
    }
    int slot = getSlot(key);
    int offset = slot * slotSz;
    long target = table.getLong(offset);
    boolean emptySlot = target == 0;
    int scanLength = 0;

    while (!emptySlot) {
      if (key == target) {
        return table.getInt(offset + KEY_SZ);
      }
      slot = nextSlot(slot);
      offset = slot * slotSz;
      target = table.getLong(offset);
      emptySlot = target == 0;
      scanLength++;
    }

    if (scanLength > 0) {
      scans += scanLength;
      if (((double) scans / (double) opCount) > scanRehashThreshold) {
        rehash();
      }
    }

    return NOT_FOUND;
  }

  public int remove(final long key) {
    if (++opCount < 0) {
      opCount = 1;
      scans = 0;
    }
    int slot = getSlot(key);
    int offset = slot * slotSz;
    long target = table.getLong(offset);
    boolean emptySlot = target == 0;
    int scanLength = 0;

    while (!emptySlot) {
      if (key == target) {
        return resetSlotByOffset(offset);
      }
      slot = nextSlot(slot);
      offset = slot * slotSz;
      target = table.getLong(offset);
      emptySlot = target == 0;
      scanLength++;
    }

    if (scanLength > 0) {
      scans += scanLength;
      if (((double) scans / (double) opCount) > scanRehashThreshold) {
        rehash();
      }
    }

    return NOT_FOUND;
  }

  public int size() {
    return sz;
  }

  private int getSlot(final long key) {
    return Math.abs(((int)(key ^ prime) % slots));
  }

  private int nextSlot(int slot) {
    slot++;
    if (slot * slotSz >= arrayLength) {
      slot = 0;
    }
    return slot;
  }

  private int resetSlotByOffset(final int offset) {
    table.setLong(offset, -1);
    int valueOffset = offset + KEY_SZ;
    int lastValue = table.getInt(valueOffset);
    table.setInt(valueOffset, -1);
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
    return new LongIntHashTable(this, newTable);
  }

  public LongIntIterator iterator() {
    return new LongIntHashIterator();
  }

  private class LongIntHashIterator implements LongIntIterator {

    int count = sz;
    int offset = 0 - slotSz;
    boolean removed = false;
    final long originalAddress;

    LongIntHashIterator() {
      originalAddress = address;
    }

    @Override
    public long key() {
      return table.getLong(offset);
    }

    @Override
    public int value() {
      return table.getInt(offset + KEY_SZ);
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
    LongIntIterator iterator = iterator();
    boolean first = true;
    while (iterator.hasNext()) {
      iterator.next();
      long key = iterator.key();
      int value = iterator.value();
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