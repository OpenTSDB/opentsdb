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

import com.google.common.base.Throwables;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Manages ZK connection.
 * Has simple implementation of distributed locks
 * Lock implementation uses idea of fair queue. Each created node
 * contains hostname and thread id for lock owner.
 */
public final class ZkClient implements Watcher, Closeable {

  private final Logger logger = LoggerFactory.getLogger(ZkClient.class);

  private final String hostName;
  private volatile ZooKeeper zk = null;
  private final String connectString;
  private final List<Lock> locks = new ArrayList<Lock>();

  public ZkClient(ZooKeeper zk) {
    this.connectString = "provided";
    this.zk = zk;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new ZkException("unable to find our hostname", e);
    }
  }

  public ZkClient(String connectString) throws IOException {
    this(connectString, 10000);
  }

  public ZkClient(String connectString, int timeout) {
    this.connectString = connectString;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new ZkException("unable to find our hostname", e);
    }
    try {
      zk = new ZooKeeper(connectString, timeout, this);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private ZooKeeper zk() {
    if (zk == null)
      throw new ZkException("Not connected to " + connectString);
    return zk;
  }


  public Lock newLock(String path, String prefix) {
    return new Lock(path, prefix);
  }

  public void process(WatchedEvent watchedEvent) {
    final Event.KeeperState state = watchedEvent.getState();
    switch (state) {
      case Expired:
        cancelAllLocks();
    }
  }

  private void cancelAllLocks() {
    synchronized (this) {
      for (Lock lock : locks) {
        lock.cancel();
      }
    }
  }

  /**
   * Disconnect from zk.
   * All further operations will fail on closed client.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    synchronized (this) {
      cancelAllLocks();
      if (zk != null) {
        try {
          zk.close();
        } catch (InterruptedException e) {
        }
      }
    }
  }

  /**
   * Chech node existance
   *
   * @param zkPath  path
   * @param watcher watcher to be installed, if node exists
   * @return true, if exists
   */
  public boolean exists(String zkPath, Watcher watcher) {
    try {
      return zk().exists(zkPath, watcher) != null;
    } catch (KeeperException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  byte[] BYTES = new byte[]{};

  /**
   * Create node path recursively.
   *
   * @param zkLockPath
   */
  public void create(String zkLockPath) {
    try {
      final String[] split = zkLockPath.split("/");
      final StringBuilder sb = new StringBuilder();
      for (String s : split) {
        sb.append('/').append(s);
        if (zk().exists(s, false) == null)
          zk().create(sb.toString(), BYTES, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      throw Throwables.propagate(e);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Class implements locks in zk.
   * Maintains queue as ephemeral child nodes in given path.
   * Less node (in lexicographic order) is lock owner
   */
  public class Lock implements Watcher {
    private final String prefix;
    private final String path;

    private boolean canceled = false;
    private String lockPath;
    private volatile String watchPath;

    public Lock(String path, String prefix) {
      this.path = path;
      this.prefix = prefix;
    }

    public String getPath() {
      return path;
    }

    public boolean isValid() {
      return !canceled;
    }

    public void lock() {
      try {
        lock(0, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        throw new IllegalStateException(e); // nearly impossible, due of unconditional loop
      }
    }

    /**
     * Aquire lock.
     * Lock created as ephemeral znode with sequential name.
     * If in lexicographic order our node is first, then
     * we lock considered held.
     * In case of another nodes before created znode, watcher
     * will be set on previous znode. When it will be deleted,
     * lock will be considered held.
     *
     * @throws InterruptedException
     */
    public void lock(long amount, TimeUnit timeUnits) throws TimeoutException {
      synchronized (this) {
        if (canceled)
          throw new ZkException("Zk connection canceled");
        try {
          registerLock(this);
          final byte[] lockId = String.format("%d-%s", Thread.currentThread().getId(), hostName).getBytes();
          lockPath = zk().create(path + "/" + prefix, lockId,
                  ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
          final List<String> children = zk().getChildren(path, false);
          //final List<String> children = findChildren();
          // lookup, where we in queue
          final int idx = Collections.binarySearch(children, lockPath);
          if (idx < 0) {
            throw new ZkException("Our node disappeared: " + lockPath);
          } else if (idx > 0) {
            int widx;
            // try each preceding node for it existance and install watcher on it
            for (widx = idx - 1; widx >= 0; widx--) {
              if ((watchPath = tryWatchPath(children.get(widx))) != null)
                break;
            }
            final long maxWait = timeUnits.toMillis(amount);
            final long started = System.currentTimeMillis();
            final long tick = amount > 0 ? Math.min(100, amount) : 100;
            // if we find some node to watch for, wait until it disappears
            while (watchPath != null && !canceled) {
              this.wait(tick);
              if (amount > 0 && System.currentTimeMillis() > started + maxWait)
                throw new TimeoutException("Lock wait failed");
            }
            if (canceled)
              throw new ZkException("Zk connection canceled");
          }
          // it is okay, we are the first znode, so we holding lock now.
        } catch (KeeperException e) {
          cancel();
          throw new ZkException("Lock znode creation failed", e, e.code());
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    public void unlock() {
      synchronized (this) {
        unregisterLock(this);
        cancel();
      }
    }

    /**
     * Lookup for node, and if it exists, this lock instance will watch
     * given node until it deleted. Upon deletion this instance considers,
     * that lock is held.
     *
     * @param path what to look at
     * @return true, if watch installed
     * @throws KeeperException
     * @throws InterruptedException
     */
    private String tryWatchPath(String path) throws KeeperException, InterruptedException {
      try {
        zk().getData(path, this, new Stat());
        return path;
      } catch (KeeperException ke) {
        if (ke.code().equals(KeeperException.Code.NONODE))
          return null;
        else
          throw ke;
      }
    }

    private void cancel() {
      synchronized (this) {
        if (!canceled) {
          if (lockPath != null) {
            try {
              zk().delete(lockPath, -1);
            } catch (IllegalStateException e) {
              logger.debug("Canceling lock exception:", e);
            } catch (InterruptedException e) {
              logger.debug("Canceling lock exception:", e);
            } catch (KeeperException e) {
              logger.debug("Canceling lock exception:", e);
            }
            lockPath = null;
          }
          canceled = true;
        }
      }
    }

    public void process(WatchedEvent event) {
      if (watchPath != null && event.getPath().equals(watchPath)) {
        if (event.getType().equals(Event.EventType.NodeDeleted)) {
          synchronized (this) {
            watchPath = null;
            this.notifyAll();
          }
        }
      }
    }
  }

  private void registerLock(Lock lock) {
    synchronized (this) {
      locks.add(lock);
    }
  }

  private void unregisterLock(Lock lock) {
    synchronized (this) {
      locks.remove(lock);
    }
  }
}
