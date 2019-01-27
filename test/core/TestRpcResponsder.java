// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;


import net.opentsdb.utils.Config;
import org.jboss.netty.util.internal.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TestRpcResponsder {

  private final AtomicInteger complete_counter = new AtomicInteger(0);

  @Test(timeout = 60000)
  public void testGracefulShutdown() throws InterruptedException {
    RpcResponder rpcResponder = new RpcResponder(new Config());

    final int n = 100;
    for (int i = 0; i < n; i++) {
      rpcResponder.response(new MockResponseProcess());
    }

    Thread.sleep(500);
    rpcResponder.close();

    try {
      rpcResponder.response(new MockResponseProcess());
      Assert.fail("Expect an IllegalStateException");
    } catch (IllegalStateException ignore) {
    }

    Assert.assertEquals(n, complete_counter.get());
  }

  private class MockResponseProcess implements Runnable {

    @Override
    public void run() {
      long duration = ThreadLocalRandom.current().nextInt(5000);
      while (duration > 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignore) {
        }
        duration -= 100;
      }
      complete_counter.incrementAndGet();
    }
  }


}
