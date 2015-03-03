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
package net.opentsdb.tsd;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import com.typesafe.config.ConfigException;
import dagger.ObjectGraph;
import net.opentsdb.core.InvalidConfigException;
import net.opentsdb.tools.ArgP;
import net.opentsdb.tools.CliOptions;
import net.opentsdb.tools.ToolsModule;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import net.opentsdb.BuildData;
import net.opentsdb.core.TSDB;
import com.typesafe.config.Config;

/**
 * Main class of the TSD, the Time Series Daemon.
 */
final class TSDMain {
  private static final Logger LOG = LoggerFactory.getLogger(TSDMain.class);

  private static final short DEFAULT_FLUSH_INTERVAL = 1000;

  public static void main(String[] args) throws IOException {
    LOG.info("Starting {} {}", BuildData.name(), BuildData.version());
    LOG.info("Built by {}@{} on {}", BuildData.user(), BuildData.host(),
        BuildData.date());

    try {
      System.in.close();  // Release a FD we don't need.
    } catch (Exception e) {
      LOG.warn("Failed to close stdin", e);
    }

    final ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--port", "NUM", "TCP port to listen on.");
    argp.addOption("--bind", "ADDR", "Address to bind to (default: 0.0.0.0).");
    argp.addOption("--staticroot", "PATH",
                   "Web root from which to serve static files (/s URLs).");
    argp.addOption("--cachedir", "PATH",
                   "Directory under which to cache result of requests.");
    argp.addOption("--worker-threads", "NUM",
                   "Number for async io workers (default: cpu * 2).");
    argp.addOption("--async-io", "true|false",
                   "Use async NIO (default true) or traditional blocking io");
    argp.addOption("--backlog", "NUM",
                   "Size of connection attempt queue (default: 3072 or kernel"
                   + " somaxconn.");
    argp.addOption("--flush-interval", "MSEC",
                   "Maximum time for which a new data point can be buffered"
                   + " (default: " + DEFAULT_FLUSH_INTERVAL + ").");
    CliOptions.addAutoMetricFlag(argp);
    args = CliOptions.parse(argp, args);
    args = null; // free().

    ObjectGraph objectGraph = ObjectGraph.create(new ToolsModule(argp));
    // get a config object
    Config config = objectGraph.get(Config.class);

    ServerSocketChannelFactory factory = null;
    TSDB tsdb = null;

    try {
      tsdb = objectGraph.get(TSDB.class);
      registerShutdownHook(tsdb);

      factory = getServerSocketChannelFactory(config);
      final ServerBootstrap server = new ServerBootstrap(factory);
      server.shutdown();

      server.setPipelineFactory(new PipelineFactory(tsdb));
      if (config.hasPath("tsd.network.backlog")) {
        server.setOption("backlog", config.getInt("tsd.network.backlog"));
      }
      server.setOption("child.tcpNoDelay",
          config.getBoolean("tsd.network.tcp_no_delay"));
      server.setOption("child.keepAlive",
          config.getBoolean("tsd.network.keep_alive"));
      server.setOption("reuseAddress",
          config.getBoolean("tsd.network.reuse_address"));

      final InetSocketAddress addr = getBindSocketAddress(config);
      server.bind(addr);
      LOG.info("Ready to serve on {}", addr);
    } catch (InvalidConfigException e) {
      shutdown(LOG, factory, tsdb);
      System.err.println(e.getMessage());
      LOG.error(e.getMessage());
      System.exit(1);
    } catch (ConfigException e) {
      shutdown(LOG, factory, tsdb);
      System.err.println(e.getMessage());
      LOG.error(e.getMessage());
      System.exit(1);
    } catch (Throwable e) {
      shutdown(LOG, factory, tsdb);
      throw new RuntimeException("Initialization failed", e);
    }
    // The server is now running in separate threads, we can exit main.
  }

  /**
   * Get a {@link org.jboss.netty.channel.socket.ServerSocketChannelFactory}
   * that has been configured based on the provided {@link
   * com.typesafe.config.Config}.
   *
   * @param config The config to base the returned instance on
   * @return A configured {@link org.jboss.netty.channel.socket.ServerSocketChannelFactory}
   * @throws com.typesafe.config.ConfigException if the provided config does not
   *                                             have the key {@code tsd.network.async_io}
   *                                             or it is not parsable into a
   *                                             {@code boolean}
   */
  private static ServerSocketChannelFactory getServerSocketChannelFactory(final Config config) {
    if (config.getBoolean("tsd.network.async_io")) {
      return new NioServerSocketChannelFactory(
              Executors.newCachedThreadPool(),
              Executors.newCachedThreadPool(),
              getNumberOfWorkers(config));
    } else {
      return new OioServerSocketChannelFactory(
              Executors.newCachedThreadPool(),
              Executors.newCachedThreadPool());
    }
  }

  /**
   * Get the number of I/O threads to use when using async I/O.
   *
   * @param config The configuration to read from
   * @return The number of threads to use
   * @throws com.typesafe.config.ConfigException If there is a configuration
   *                                             variable for the number of
   *                                             worker threads but it is
   *                                             malformed
   */
  private static int getNumberOfWorkers(final Config config) {
    if (config.hasPath("tsd.network.worker_threads")) {
      return config.getInt("tsd.network.worker_threads");
    } else {
      return Runtime.getRuntime().availableProcessors() * 2;
    }
  }

  /**
   * Get the {@link java.net.InetSocketAddress} to bind to based on the provided
   * config.
   *
   * @param config The config that specifies what socket address to bind to
   * @return A configured and validated {@link java.net.InetSocketAddress}
   * instance
   * @throws net.opentsdb.core.InvalidConfigException if the address in the
   *                                                  config could not be
   *                                                  resolved or if the port
   *                                                  was not within the valid
   *                                                  range
   * @throws com.typesafe.config.ConfigException      if the port or host
   *                                                  address in the config was
   *                                                  missing or malformed
   */
  private static InetSocketAddress getBindSocketAddress(final Config config) {
    int bindPort = config.getInt("tsd.network.port");
    try {
      return new InetSocketAddress(getBindAddress(config), bindPort);
    } catch (IllegalArgumentException e) {
      throw new InvalidConfigException(config.getValue("tsd.network.port"),
              "The configured bind port (" + bindPort + ") is not within the valid range");
    }
  }

  /**
   * Get the {@link java.net.InetAddress} to bind to based on the provided
   * config.
   *
   * @param config The config that specifies what address to bind to
   * @return A configured and validated {@link java.net.InetAddress} instance
   * @throws net.opentsdb.core.InvalidConfigException if the address in the
   *                                                  config could not be
   *                                                  resolved
   * @throws com.typesafe.config.ConfigException      if the address in the
   *                                                  config was missing or
   *                                                  malformed
   */
  private static InetAddress getBindAddress(final Config config) {
    String bindAddress = config.getString("tsd.network.bind");
    try {
      return InetAddress.getByName(bindAddress);
    } catch (UnknownHostException e) {
      throw new InvalidConfigException(config.getValue("tsd.network.bind"),
              "Unable to resolve the configured bind address (" +bindAddress + ")");
    }
  }

  private static void shutdown(final Logger log,
                               final ServerSocketChannelFactory factory,
                               final TSDB tsdb) {
    if (factory != null)
      factory.releaseExternalResources();

    try {
      if (tsdb != null)
        tsdb.shutdown().joinUninterruptibly();
    } catch (Exception e2) {
      log.error("Failed to shutdown HBase client", e2);
    }
  }

  private static void registerShutdownHook(final TSDB tsdb) {
    final class TSDBShutdown extends Thread {
      public TSDBShutdown() {
        super("TSDBShutdown");
      }
      @Override
      public void run() {
        try {
          tsdb.shutdown().join();
        } catch (Exception e) {
          LoggerFactory.getLogger(TSDBShutdown.class)
            .error("Uncaught exception during shutdown", e);
        }
      }
    }
    Runtime.getRuntime().addShutdownHook(new TSDBShutdown());
  }
}
