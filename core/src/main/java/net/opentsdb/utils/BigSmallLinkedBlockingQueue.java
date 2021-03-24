// This file is part of OpenTSDB.
// Copyright (C) 2019-2021  The OpenTSDB Authors.
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

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * An unbounded thread safe blocking queue based on two {@linkplain ConcurrentLinkedQueue linked
 * queues}, one big and one small. User will have to provide a {@linkplain Predicate predicate
 * function} in the constructor to evaluate if the element should go to the big queue or small. This
 * evaluation happens while {@linkplain #put(Object) adding} the element to the queue.
 *
 * <p>While {@linkplain #take() reading} the queue, it will read alternatively from the underlying
 * small and big queue. If one of the underlying queue is empty, it will continuously read form the
 * other queue. When both the underlying queues are empty, the read call is blocked till a new
 * element is added.
 *
 * @param <T>
 */
public class BigSmallLinkedBlockingQueue<T> implements TSDBQueryQueue<T>, TimerTask {

  private static final String BIG_QUEUE_SIZE = "queue.big.size";
  private static final String BIG_QUEUE_WAIT = "queue.big.wait.time";
  private static final String SMALL_QUEUE_SIZE = "queue.small.size";
  private static final String SMALL_QUEUE_WAIT = "queue.small.wait.time";
  private static final String JOB_QUEUE_SIZE = "queue.size";
  private static final String JOB_HWM = "queue.highWaterMark";

  private final TSDB tsdb;
  private final String big_queue_size;
  private final String big_queue_wait;
  private final String small_queue_size;
  private final String small_queue_wait;
  private final String job_queue_size;
  private final String job_hwm;

  private final ConcurrentLinkedQueue<T> bigQ;
  private final ConcurrentLinkedQueue<T> smallQ;

  private volatile boolean takeFromBigQ;

  private Predicate<T> bigPredicate;

  private final AtomicInteger count = new AtomicInteger();
  private volatile int high_water_mark = 0;

  /** Lock held by take */
  private final ReentrantLock readLock = new ReentrantLock();

  private final Condition readSignal = readLock.newCondition();

  public BigSmallLinkedBlockingQueue(final TSDB tsdb,
                                     final String id,
                                     final Predicate<T> bigPredicate) {
    this.tsdb = tsdb;
    this.bigPredicate = bigPredicate;
    this.bigQ = new ConcurrentLinkedQueue();
    this.smallQ = new ConcurrentLinkedQueue();
    big_queue_size = id + "." + BIG_QUEUE_SIZE;
    big_queue_wait = id + "." + BIG_QUEUE_WAIT;
    small_queue_size = id + "." + SMALL_QUEUE_SIZE;
    small_queue_wait = id + "." + SMALL_QUEUE_WAIT;
    job_queue_size = id + "." + JOB_QUEUE_SIZE;
    job_hwm = id + "." + JOB_HWM;
    tsdb.getMaintenanceTimer().newTimeout(this, 1, TimeUnit.MINUTES);
  }

  public int size() {
    return count.get();
  }

  @Override
  public void shutdown() {
  }

  public void put(T t) {
    boolean isBig = bigPredicate.test(t);
    ConcurrentLinkedQueue<T> q = isBig ? bigQ : smallQ;

    q.offer(t);
    int c = count.getAndIncrement();
    // don't really care about locking and accuracy here, just need an estimate.
    if (c > high_water_mark) {
      high_water_mark = c;
    }

    if (c == 0) {
      notifyReaders();
    }
  }

  public T take() throws InterruptedException {
    T next;
    try {
      readLock.lockInterruptibly();
      while (count.get() == 0) {
        readSignal.await();
      }
      next = doPoll();
      int c = count.getAndDecrement();
      if (c > 1) {
        readSignal.signal();
      }

    } finally {
      readLock.unlock();
    }
    return next;
  }

  private void notifyReaders() {
    readLock.lock();
    try {
      readSignal.signal();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Retrieves and removes the head of the {@code smallQ} and {@code bigQ}. Should be called from
   * the synchronized construct
   *
   * @return head of the queue
   */
  private T doPoll() {
    ConcurrentLinkedQueue<T> nextQ = takeFromBigQ ? bigQ : smallQ;
    T next = nextQ.poll();
    if (next == null) {
      ConcurrentLinkedQueue<T> otherQ = takeFromBigQ ? smallQ : bigQ;
      next = otherQ.poll();
    }
    takeFromBigQ = !takeFromBigQ;
    return next;
  }

  public void recordBigQueueWaitTime(long startTimeInNanos) {
    tsdb.getStatsCollector().addTime(big_queue_wait,
            DateTime.nanoTime() - startTimeInNanos,
            ChronoUnit.NANOS);
  }

  public void recordSmallQueueWaitTime(long startTimeInNanos) {
    tsdb.getStatsCollector().addTime(small_queue_wait,
            DateTime.nanoTime() - startTimeInNanos,
            ChronoUnit.NANOS);
  }

//  public int bigQSize() {
//    return bigQ.size();
//  }
//
//  public int smallQSize() {
//    return smallQ.size();
//  }

  @Override
  public void run(final Timeout ignored) throws Exception {
    try {
      final StatsCollector statsCollector = tsdb.getStatsCollector();
      statsCollector.setGauge(big_queue_size, bigQ.size());
      statsCollector.setGauge(small_queue_size, smallQ.size());
      statsCollector.setGauge(job_queue_size, count.get());
      statsCollector.setGauge(job_hwm, high_water_mark);
    } finally {
      tsdb.getMaintenanceTimer().newTimeout(this, 1, TimeUnit.MINUTES);
    }
  }
}
