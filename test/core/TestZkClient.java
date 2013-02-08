// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import junit.framework.TestCase;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public final class TestZkClient extends TestCase {

  @Mock
  ZooKeeper zk;
  final String zkPath = "/zk/locks";
  final String zkPrefix = "lock-";
  int seq;

  @Override
  public void setUp() throws Exception {
    seq = 1;
  }

  /**
   * Test normal lock operation
   */
  public void testLock() throws Exception, KeeperException {
    final ZkClient zkClient = new ZkClient(zk);
    // create one 'preexist' node
    final String otherNode = addNode();
    final String otherNode2 = addNode();
    final String ourNode = addNode();
    String[] nodes = new String[]{
            otherNode,
            otherNode2,
            ourNode
    };

    when(
            zk.create(
                    Matchers.<String>any(),
                    Matchers.<byte[]>any(),
                    Matchers.<List<ACL>>any(),
                    Matchers.<CreateMode>any())
    ).thenReturn(ourNode);

    when(
            zk.getChildren(zkPath, false)
    ).thenReturn(Arrays.asList(nodes))
            .thenReturn(Arrays.asList(nodes).subList(1, 2));

    when(
            zk.getData(eq(otherNode2), (Watcher) anyObject(), any(Stat.class))
    ).thenThrow(new KeeperException.NoNodeException(otherNode));

    final AtomicReference<Watcher> watcher = new AtomicReference<Watcher>(null);
    when(
            zk.getData(eq(otherNode), (Watcher) anyObject(), any(Stat.class))
    ).then(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) throws Throwable {
        watcher.set((Watcher) invocation.getArguments()[1]);
        return "some-junk".getBytes();
      }
    });


    final Thread thread = new Thread(new Runnable() {
      public void run() {
        while (watcher.get() == null) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
          }
        }
        watcher.get().process(new WatchedEvent(
                Watcher.Event.EventType.NodeDeleted,
                Watcher.Event.KeeperState.SyncConnected,
                otherNode));
      }
    });
    thread.start();

    final ZkClient.Lock lock = zkClient.newLock(zkPath, zkPrefix);
    try {
      try {
        lock.lock(2, TimeUnit.SECONDS);
      } catch (Exception e) {
        fail(e.getMessage());
      }
    } finally {
      lock.unlock();
    }

    verify(zk, times(1)).delete(eq(ourNode), anyInt());

  }

  private String addNode() {
    return String.format("%s/%s%08d", zkPath, zkPrefix, seq++);
  }

}
