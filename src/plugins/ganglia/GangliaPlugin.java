// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
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
package net.opentsdb.plugins.ganglia;

import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.BuildData;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RpcPlugin;


/**
 * RpcPlugin to process messages in the Ganglia v30x and v31x protocol.
 * To enable, add this class name to the configuration "tsd.rpc.plugins".
 */
public class GangliaPlugin extends RpcPlugin {

  private static final Logger LOG =
      LoggerFactory.getLogger(GangliaPlugin.class);

  public ChannelFactory ganglia_factory;

  @Override
  public void initialize(final TSDB tsdb) {
    Preconditions.checkState(ganglia_factory == null);
    GangliaConfig config = new GangliaConfig(tsdb.getConfig());
    ganglia_factory = setUpChannelFactory(config, tsdb);
  }

  /**
   * Sets a netty server to receive Ganglia v30x and v31x messages.
   * @param config An initialized configuration object
   * @param tsdb The TSDB to use
   * @return A channel factory for Ganglia v30x and v31x messages.
   */
  private ChannelFactory setUpChannelFactory(final GangliaConfig config,
                                             final TSDB tsdb) {
    // TODO: Configure the number of threads.
    final NioDatagramChannelFactory factory = new NioDatagramChannelFactory();
    ConnectionlessBootstrap server = new ConnectionlessBootstrap(factory);
    server.setPipelineFactory(new GangliaPipelineFactory(config, tsdb));
    // Allow packets as large as up to 2048 bytes.
    server.setOption("receiveBufferSizePredictorFactory",
                     new FixedReceiveBufferSizePredictorFactory(2048));
    server.setOption("reuseAddress", true);
    int port = config.ganglia_udp_port();
    InetSocketAddress addr = new InetSocketAddress(port);
    server.bind(addr);
    LOG.info("Ready to receive ganglia v30x and v31x metrics on " + addr);
    return factory;
  }

  @Override
  public Deferred<Object> shutdown() {
    if (ganglia_factory != null) {
      ganglia_factory.releaseExternalResources();
    }
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return BuildData.version;
  }

  @Override
  public void collectStats(StatsCollector collector) {
    GangliaHandler.collectStats(collector);
  }
}
