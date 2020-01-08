// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.groupby;

import java.util.concurrent.ConcurrentLinkedQueue;
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
public class BigSmallLinkedBlockingQueue<T> {

  private final ConcurrentLinkedQueue<T> bigQ;
  private final ConcurrentLinkedQueue<T> smallQ;

  private volatile boolean takeFromBigQ;

  private Predicate<T> bigPredicate;

  private final AtomicInteger count = new AtomicInteger();

  /** Lock held by take */
  private final ReentrantLock readLock = new ReentrantLock();

  private final Condition readSignal = readLock.newCondition();

  public BigSmallLinkedBlockingQueue(Predicate<T> bigPredicate) {
    this.bigPredicate = bigPredicate;
    this.bigQ = new ConcurrentLinkedQueue();
    this.smallQ = new ConcurrentLinkedQueue();
  }

  public int size() {
    return count.get();
  }

  public void put(T t) {
    boolean isBig = bigPredicate.test(t);
    ConcurrentLinkedQueue<T> q = isBig ? bigQ : smallQ;

    q.offer(t);
    int c = count.getAndIncrement();

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

  public int bigQSize() {
    return bigQ.size();
  }

  public int smallQSize() {
    return smallQ.size();
  }

}
