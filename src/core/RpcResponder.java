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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.opentsdb.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * This class is responsible for building result of requests and
 * respond to clients asynchronously.
 *
 * It can reduce requests that stacking in AsyncHBase, especially put requests.
 * When a HBase's RPC has completed, the "AsyncHBase I/O worker" just decodes
 * the response, and then do callback by this class asynchronously. We should
 * take up workers as short as possible time so that workers can remove RPCs
 * from in-flight state more quickly.
 *
 */
public class RpcResponder {

  private static final Logger LOG = LoggerFactory.getLogger(RpcResponder.class);

  public static final String TSD_RESPONSE_ASYNC_KEY = "tsd.core.response.async";
  public static final boolean TSD_RESPONSE_ASYNC_DEFAULT = true;

  public static final String TSD_RESPONSE_WORKER_NUM_KEY =
          "tsd.core.response.worker.num";
  public static final int TSD_RESPONSE_WORKER_NUM_DEFAULT = 10;

  private final boolean async;
  private ExecutorService responders;
  private volatile boolean running = true;

  RpcResponder(final Config config) {
    async = config.getBoolean(TSD_RESPONSE_ASYNC_KEY,
            TSD_RESPONSE_ASYNC_DEFAULT);

    if (async) {
      int threads = config.getInt(TSD_RESPONSE_WORKER_NUM_KEY,
              TSD_RESPONSE_WORKER_NUM_DEFAULT);
      responders = Executors.newFixedThreadPool(threads,
        new ThreadFactoryBuilder()
          .setNameFormat("OpenTSDB Responder #%d")
          .setDaemon(true)
          .setUncaughtExceptionHandler(new ExceptionHandler())
          .build());
    }

    LOG.info("RpcResponder mode: {}", async ? "async" : "sync");
  }

  public void response(Runnable run) {
    if (async) {
      if (running) {
        responders.execute(run);
      } else {
        throw new IllegalStateException("RpcResponder is closing or closed.");
      }
    } else {
      run.run();
    }
  }

  public void close() {
    if (running) {
      running = false;
      responders.shutdown();
    }

    boolean completed;
    try {
      completed = responders.awaitTermination(5, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      completed = false;
    }

    if (!completed) {
      LOG.warn(
          "There are still some results that are not returned to the clients.");
    }
  }

  public boolean isAsync() {
    return async;
  }

  private class ExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.error("Run into an uncaught exception in thread: " + t.getName(), e);
    }
  }
}
