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
package net.opentsdb.utils;

import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBigSmallLinkedBlockingQueue {

  @Test
  public void testSmallJob() throws InterruptedException {
    BigSmallLinkedBlockingQueue<Integer> q = new BigSmallLinkedBlockingQueue<>((i) -> i > 10);

    q.put(5);
    assertEquals(1, q.size());
    q.put(1);
    q.put(9);
    assertEquals(3, q.size());

    assertEquals(5, (int) q.take());
    assertEquals(2, q.size());
    assertEquals(1, (int) q.take());
    assertEquals(9, (int) q.take());
    assertEquals(0, q.size());
  }

  @Test
  public void testBigJob() throws InterruptedException {
    BigSmallLinkedBlockingQueue<Integer> q = new BigSmallLinkedBlockingQueue<>((i) -> i > 10);

    q.put(15);
    assertEquals(1, q.size());
    q.put(11);
    q.put(99);
    q.put(532);
    assertEquals(4, q.size());

    assertEquals(15, (int) q.take());
    assertEquals(3, q.size());
    assertEquals(11, (int) q.take());
    assertEquals(99, (int) q.take());
    assertEquals(532, (int) q.take());
    assertEquals(0, q.size());
  }

  @Test
  public void testBigAndSmallJobs() throws InterruptedException {
    BigSmallLinkedBlockingQueue<Integer> q = new BigSmallLinkedBlockingQueue<>((i) -> i > 10);
    q.put(11);
    q.put(12);
    q.put(13);

    q.put(1);
    q.put(2);
    q.put(3);
    q.put(4);
    q.put(5);

    assertEquals(8, q.size());

    assertEquals(1, (int) q.take());
    assertEquals(11, (int) q.take());
    assertEquals(2, (int) q.take());
    assertEquals(12, (int) q.take());
    assertEquals(3, (int) q.take());
    assertEquals(13, (int) q.take());

    assertEquals(4, (int) q.take());
    assertEquals(5, (int) q.take());
  }

  @Test
  public void testReadsAreBlockedWhenQueueIsEmpty() throws InterruptedException {
    BigSmallLinkedBlockingQueue<Integer> q = new BigSmallLinkedBlockingQueue<>((i) -> i > 10);

    final AtomicBoolean readStarted = new AtomicBoolean(false);
    final AtomicInteger readCount = new AtomicInteger(0);
    Thread t =
        new Thread(
            () -> {
              readStarted.set(true);
              while (true) {
                try {
                  q.take();
                  readCount.getAndIncrement();
                } catch (InterruptedException ignored) {
                }
              }
            });
    t.setDaemon(true);
    t.start();

    // Ensure read thread is blocked
    Thread.sleep(100);

    assertTrue(readStarted.get());
    assertEquals(0, readCount.get());

    // write should unblock the reader
    q.put(1);

    Thread.sleep(100);

    // read the value
    assertEquals(1, readCount.get());

    // Ensure the reader is again blocked
    Thread.sleep(100);
    assertEquals(1, readCount.get());

    q.put(12);
    q.put(3);
    q.put(14);

    // read the value
    Thread.sleep(100);
    assertEquals(4, readCount.get());
  }

  @Test
  public void testHighConcurrency() throws InterruptedException {

    BigSmallLinkedBlockingQueue<Integer> q = new BigSmallLinkedBlockingQueue<>((i) -> i > 10);

    Semaphore semaphore = new Semaphore(0);
    final AtomicInteger v1 = new AtomicInteger();
    final AtomicInteger v2 = new AtomicInteger();
    Thread t1 =
        new Thread(
            () -> {
              try {
                v1.set(q.take());
              } catch (InterruptedException ignored) {
              }
              semaphore.release();
            });
    Thread t2 =
        new Thread(
            () -> {
              try {
                v2.set(q.take());
              } catch (InterruptedException ignored) {
              }
              semaphore.release();
            });
    t1.start();
    t2.start();

    // Ensure both reader threads are waiting
    Thread.sleep(100);

    // Suspend the reader threads till two writes are done
    t1.suspend();
    t2.suspend();

    q.put(1);
    q.put(2);

    t1.resume();
    t2.resume();

    semaphore.acquire(2);

    assertTrue(v1.get() == 1 ^ v2.get() == 1);
    assertTrue(v1.get() == 2 ^ v2.get() == 2);
  }
}
