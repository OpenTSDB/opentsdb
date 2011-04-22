// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import net.opentsdb.stats.StatsCollector;

/**
 * Keeps track of all existing connections.
 */
final class ConnectionManager extends SimpleChannelHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

  private static final AtomicInteger connections_established
    = new AtomicInteger();
  private static final AtomicInteger exceptions_caught
    = new AtomicInteger();

  private static final DefaultChannelGroup channels =
    new DefaultChannelGroup("all-channels");

  static void closeAllConnections() {
    channels.close().awaitUninterruptibly();
  }

  /** Constructor. */
  public ConnectionManager() {
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("connectionmgr.connections", connections_established);
    collector.record("connectionmgr.exceptions", exceptions_caught);
  }

  @Override
  public void channelOpen(final ChannelHandlerContext ctx,
                          final ChannelStateEvent e) {
    channels.add(e.getChannel());
    connections_established.incrementAndGet();
  }

  @Override
  public void handleUpstream(final ChannelHandlerContext ctx,
                             final ChannelEvent e) throws Exception {
    if (e instanceof ChannelStateEvent) {
      LOG.info(e.toString());
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx,
                              final ExceptionEvent e) {
    final Throwable cause = e.getCause();
    final Channel chan = ctx.getChannel();
    if (cause instanceof ClosedChannelException) {
      LOG.warn("Attempt to write to closed channel " + chan);
    } else {
      LOG.error("Unexpected exception from downstream for " + chan, cause);
      e.getChannel().close();
    }
    exceptions_caught.incrementAndGet();
  }

}
