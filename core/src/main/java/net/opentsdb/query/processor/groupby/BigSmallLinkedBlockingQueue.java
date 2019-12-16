package net.opentsdb.query.processor.groupby;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

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
}
