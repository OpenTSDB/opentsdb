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

import com.google.common.collect.Maps;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.collections.LongLongHashTable;
import net.opentsdb.collections.UnsafeHelper;
import net.opentsdb.core.TSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A helper that tracks memory allocations, frees and reads. Just used for
 * debugging as, naturally, it incurs a fair bit of overhead. And it's not a
 * full memory manager, again, just a simple debugger for catching issues.
 *
 * @since 3.0
 */
public class OffHeapDebugAllocator implements TimerTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(
          OffHeapDebugAllocator.class);

  protected final TSDB tsdb;
  protected Unsafe unsafe;
  protected LongLongHashTable allocatedAddresses;
  protected LongLongHashTable freedAddresses;
  protected Map<Long, Tracker> trackerFreed;
  protected volatile Counters cntrs;
  protected Java8StackHelper stackHelper;
  protected ReadWriteLock lock;
  protected NumberFormat formatter;
  protected int stacksToTrack;
  protected String[] tags;

  /**
   * Default ctor.
   * @param tsdb The TSDB to report metrics with.
   * @param stacksToTrack An optional number of stacks to track on frees. If a
   *                      read or allocation finds a free'd address it will dump
   *                      the stack of the free call so we can trace it back.
   * @param name The name of the allocator, used for logging table increases.
   */
  public OffHeapDebugAllocator(final TSDB tsdb,
                               final int stacksToTrack,
                               final String name) {
    this.tsdb = tsdb;
    this.stacksToTrack = stacksToTrack;
    unsafe = UnsafeHelper.unsafe;
    allocatedAddresses = new LongLongHashTable(1024 * 1024, name + "_allocated");
    if (stacksToTrack > 0) {
      trackerFreed = Maps.newHashMapWithExpectedSize(1024 * 1024);
    } else {
      freedAddresses = new LongLongHashTable(1024 * 1024, name + "_freed");
    }
    cntrs = new Counters();
    stackHelper = new Java8StackHelper();
    lock = new ReentrantReadWriteLock();
    formatter = NumberFormat.getInstance();
    tags = new String[] { "allocator", name };
    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
  }

  @Override
  public void run(Timeout timeout) throws Exception {
    // swap in a new one to start our count over. Could do a sliding window but... meh
    final Counters counters = cntrs;
    cntrs = new Counters();

    LOGGER.debug("Allocations: {}  Frees: {}  Reads: {}  Reused: {}",
            formatter.format(counters.allocs.get()),
            formatter.format(counters.frees.get()),
            formatter.format(counters.reads.get()),
            formatter.format(counters.reused.get()));
    tsdb.getStatsCollector().setGauge("offheap.allocator.allocations",
            counters.allocs.get(), tags);
    tsdb.getStatsCollector().setGauge("offheap.allocator.freed",
            counters.frees.get(), tags);
    tsdb.getStatsCollector().setGauge("offheap.allocator.reads",
            counters.reads.get(), tags);
    tsdb.getStatsCollector().setGauge("offheap.allocator.reused",
            counters.reused.get(), tags);

    tsdb.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
  }

  /**
   * Allocates a chunk of memory for the given size. This will add it to the
   * allocation map.
   * @param size The amount of memory to allocate.
   * @return The allocated block address.
   * @throws IllegalStateException if the address is already allocated! That just
   * indicates a bug in this implementation or the block was free'd outside of
   * this allocator. Unless something is
   * really, really wrong with the JVM or host.
   */
  public long allocate(long size) {
    // not worrying about size checks right now.
    long address = unsafe.allocateMemory(size);
    unsafe.setMemory(address, size, (byte) 0);

    Lock write = lock.writeLock();
    write.lock();
    try {
      if (freedAddresses != null) {
        // no stack tracing
        long extant = freedAddresses.remove(address);
        if (extant != LongLongHashTable.NOT_FOUND) {
          cntrs.reused.incrementAndGet();
        }
      } else {
        Tracker tracker = trackerFreed.remove(address);
        if (tracker != null) {
          cntrs.reused.incrementAndGet();
        }
      }

      long tid = allocatedAddresses.get(address);
      if (tid != LongLongHashTable.NOT_FOUND) {
        IllegalStateException e = new IllegalStateException("The address " + address
                + " was re-used but we hadn't removed it from our allocated " +
                "list. Thread ID: " + tid + "\nCurrent Thread: "
                + Thread.currentThread().getName());
        LOGGER.error("Bad allocation", e);
        throw e;
      }

      allocatedAddresses.put(address, Thread.currentThread().getId());
    } finally {
      write.unlock();
    }
    cntrs.allocs.incrementAndGet();
    return address;
  }

  /**
   * Frees the memory. Checks to make sure it was in our allocation array first,
   * then moves it to the freed array. Trying to free an invalid address (0) will
   * log a warning, won't throw.
   * @param caller The caller object. Used for logging.
   * @param address The address to free.
   * @throws IllegalStateException if the address was NOT in the allocation map
   * or it was already free'd.
   */
  public void free(Object caller, long address) {
    if (address <= 0) {
      // no-op
      LOGGER.warn("Attempt to free a zero address from", new RuntimeException());
      return;
    }

    // lock BEFORE the free so we avoid a race in allocating again before
    // we remove it from the outstanding map
    Lock write = lock.writeLock();
    write.lock();
    try {
      long out = allocatedAddresses.remove(address);
      if (out == LongLongHashTable.NOT_FOUND) {
        IllegalStateException e = new IllegalStateException("The address " + address
                + " was NOT in our allocated pool " +
                "list. Current Thread: " + Thread.currentThread().getName() );
        LOGGER.error("Failed to free", e);
        throw e;
      }

      if (freedAddresses != null) {
        long extant = freedAddresses.get(address);
        if (extant != LongLongHashTable.NOT_FOUND) {
          IllegalStateException e = new IllegalStateException("The address " + address
                  + " was ALREADY in our freed pool " +
                  "list with TID: " + extant + "\nCurrent Thread: "
                  + Thread.currentThread().getName());
          LOGGER.error("Failed to free", e);
          throw e;
        }
        freedAddresses.put(address, Thread.currentThread().getId());
      } else {
        Tracker tracker = trackerFreed.remove(address);
        if (tracker != null) {
          IllegalStateException e = new IllegalStateException("The address " + address
                  + " was ALREADY in our freed pool " +
                  "list from tracker: " + tracker + "\nCurrent Thread: "
                  + Thread.currentThread().getName());
          LOGGER.error("Failed to free", e);
          throw e;
        }

        tracker = new Tracker();
        tracker.caller = caller.toString() + "_" + System.identityHashCode(caller);
        int index = stackHelper.getStackTrace(); // skip this stack and the helper stack.
        int limit = Math.min(stacksToTrack, stackHelper.elements().length - index);
        tracker.freeStack = new String[limit];
        for (int i = 0; i < limit; i++) {
          StackTraceElement element = stackHelper.elements()[index++];
          tracker.freeStack[i] = element.toString();
        }

        trackerFreed.put(address, tracker);
        unsafe.freeMemory(address);
      }
    } finally {
      write.unlock();
    }
    cntrs.frees.incrementAndGet();
  }

  /**
   * Checks the address to make sure it's in the allocation map and NOT in the
   * free'd map. Checks free first. Always call this to validate an address before
   * reading. Trying to read an invalid address (0) will log a warning, won't throw.
   * @param address The address that will be read.
   * @throws IllegalStateException if the address WAS in the free map or NOT in
   * the allocated map. In this case it could just be the address was allocated
   * without the allocator. So watch out for that.
   */
  public void read(long address) {
    if (address <= 0) {
      // no-op
      LOGGER.warn("Attempt to read a zero address from", new RuntimeException());
      return;
    }

    Lock read = lock.readLock();
    read.lock();
    try {
      if (freedAddresses != null) {
        long tid = freedAddresses.get(address);
        if (tid != LongLongHashTable.NOT_FOUND) {
          IllegalStateException e = new IllegalStateException("Trying to read address " + address +
                  " that was in the freed list!!! from thread " + tid);
          LOGGER.error("Failed to read", e);
          throw e;
        }
      } else {
        Tracker tracker = trackerFreed.get(address);
        if (tracker != null) {
          IllegalStateException e = new IllegalStateException("Trying to read address " + address +
                  " that was in the freed list!!! from previous tracker " + tracker);
          LOGGER.error("Failed to read", e);
          throw e;
        }
      }

      long tid = allocatedAddresses.get(address);
      if (tid == LongLongHashTable.NOT_FOUND) {
        IllegalStateException e = new IllegalStateException("Trying to read address " + address +
                " that was NOT in the allocated list!!");
        LOGGER.error("Failed to read", e);
        throw e;
      }
    } finally {
      read.unlock();
    }
    cntrs.reads.incrementAndGet();
  }

  /**
   * Funky, brittle little helper for pre-9 versions (maybe later) that should
   * compute the stack trace with as little object creation as possible. It'll
   * reflect into the Throwable and call the native method for the JVM to populate
   * the stack.
   *
   * Use it by calling {@link #getStackTrace()} and capturing the index. This will
   * be the first call _after_ the Java8StackHelper class. Then start iterating
   * the traces at from {@link #elements()}.
   */
  private class Java8StackHelper {
    private final Throwable t;
    private final Field stackTrace;
    private final Method fillInStack;
    private final Method getStackTrace;
    private int idx;

    Java8StackHelper() {
      t = new Throwable();
      try {
        stackTrace = t.getClass().getDeclaredField("stackTrace");
        stackTrace.setAccessible(true);

        fillInStack = t.getClass().getDeclaredMethod("fillInStackTrace", int.class);
        fillInStack.setAccessible(true);

        getStackTrace = t.getClass().getDeclaredMethod("getOurStackTrace", null);
        getStackTrace.setAccessible(true);

      } catch (NoSuchFieldException | NoSuchMethodException e) {
        throw new RuntimeException("Failed to initialize the stack helper", e);
      }
    }

    int getStackTrace() {
      try {
        stackTrace.set(t, null);
        fillInStack.invoke(t, 0);
        final StackTraceElement[] stack = (StackTraceElement[]) getStackTrace.invoke(t);
        for (idx = 0; idx < stack.length; idx++) {
          StackTraceElement element = stack[idx];
          if (element.getClassName().startsWith("sun.") ||
              element.getClassName().startsWith("java.") ||
              element.getClassName().contains("Java8StackHelper") ||
              element.getClassName().contains("OffHeapDebugAllocator")) {
            continue;
          }
          return idx;
        }
        return idx;
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException("Failed to set the new stack trace", e);
      }
    }

    StackTraceElement[] elements() {
      try {
        return (StackTraceElement[]) stackTrace.get(t);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to read the stack trace elements", e);
      }
    }
  }

  protected class Tracker {
    String caller;
    String[] freeStack;

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
              .append("caller=")
              .append(caller)
              .append("stack=[");
      for (int i = 0 ; i < freeStack.length; i++) {
        if (i > 0) {
          buf.append(", ");
        }
        buf.append(freeStack[i]);
      }
      buf.append("]");
      return buf.toString();
    }
  }

  private class Counters {
    public AtomicLong reused = new AtomicLong();
    public AtomicLong allocs = new AtomicLong();
    public AtomicLong frees = new AtomicLong();
    public AtomicLong reads = new AtomicLong();
  }
}
