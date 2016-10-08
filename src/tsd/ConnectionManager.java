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
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.embedder.CodecEmbedderException;

import net.opentsdb.stats.StatsCollector;

/**
 * Keeps track of all existing connections.
 */
final class ConnectionManager extends SimpleChannelHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

  private static final AtomicLong connections_established = new AtomicLong();
  private static final AtomicLong connections_rejected = new AtomicLong();
  private static final AtomicLong exceptions_unknown = new AtomicLong();
  private static final AtomicLong exceptions_closed = new AtomicLong();
  private static final AtomicLong exceptions_reset = new AtomicLong();
  private static final AtomicLong exceptions_timeout = new AtomicLong();
  
  /** Max connections can be serviced by tsd, if over limit, tsd will refuse 
   * new connections. */
  private final int connections_limit;
  
  /** A counter used for determining how many channels are open. Something odd
   * happens with the DefaultChannelGroup in that .size() doesn't return the
   * actual number of open connections. TODO - find out why. */
  private final AtomicInteger open_connections = new AtomicInteger();

  private static final DefaultChannelGroup channels =
    new DefaultChannelGroup("all-channels");

  static void closeAllConnections() {
    channels.close().awaitUninterruptibly();
  }

  /**
   * Default Ctor with no concurrent connection limit.
   */
  public ConnectionManager() {
    connections_limit = 0;
  }
  
  /**
   * CTor for setting a limit on concurrent connections.
   * @param connections_limit The maximum number of concurrent connections allowed.
   * @since 2.3
   */
  public ConnectionManager(final int connections_limit) {
    LOG.info("TSD concurrent connection limit set to: " + connections_limit);
    this.connections_limit = connections_limit;
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("connectionmgr.connections", channels.size(), "type=open");
    collector.record("connectionmgr.connections", connections_rejected,
        "type=rejected");
    collector.record("connectionmgr.connections", connections_established, 
        "type=total");
    collector.record("connectionmgr.exceptions", exceptions_closed, 
        "type=closed");
    collector.record("connectionmgr.exceptions", exceptions_reset, 
        "type=reset");
    collector.record("connectionmgr.exceptions", exceptions_timeout, 
        "type=timeout");
    collector.record("connectionmgr.exceptions", exceptions_unknown, 
        "type=unknown");
  }

  @Override
  public void channelOpen(final ChannelHandlerContext ctx,
                          final ChannelStateEvent e) throws IOException {
    if (connections_limit > 0) {
      final int channel_size = open_connections.incrementAndGet();
      if (channel_size > connections_limit) {
        throw new ConnectionRefusedException("Channel size (" + channel_size + ") exceeds total "
            + "connection limit (" + connections_limit + ")");
        // exceptionCaught will close the connection and increment the counter.
      }
    }
    channels.add(e.getChannel());
    connections_established.incrementAndGet();
  }

  @Override
  public void channelClosed(final ChannelHandlerContext ctx,
                          final ChannelStateEvent e) throws IOException {
    open_connections.decrementAndGet();
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
      exceptions_closed.incrementAndGet();
      LOG.warn("Attempt to write to closed channel " + chan);
      return;
    }
    if (cause instanceof IOException) {
      final String message = cause.getMessage();
      if ("Connection reset by peer".equals(message)) {
        exceptions_reset.incrementAndGet();
        return;
      } else if ("Connection timed out".equals(message)) {
        exceptions_timeout.incrementAndGet();
        // Do nothing.  A client disconnecting isn't really our problem.  Oh,
        // and I'm not kidding you, there's no better way to detect ECONNRESET
        // in Java.  Like, people have been bitching about errno for years,
        // and Java managed to do something *far* worse.  That's quite a feat.
        return;
      } else if (cause instanceof ConnectionRefusedException) {
        connections_rejected.incrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Refusing connection from " + chan, e.getCause());
        }
        chan.close();
        return;
      }
    }
    if (cause instanceof CodecEmbedderException) {
    	// payload was not compressed as it was announced to be
    	LOG.warn("Http codec error : " + cause.getMessage());
    	e.getChannel().close();
    	return;
    }
    exceptions_unknown.incrementAndGet();
    LOG.error("Unexpected exception from downstream for " + chan, cause);
    e.getChannel().close();
  }

  /** Simple exception for refusing a connection. */
  private static class ConnectionRefusedException extends ChannelException {
    
    /**
     * Default ctor with a message.
     * @param message A descriptive message for the exception.
     */
    public ConnectionRefusedException(final String message) {
      super(message);
    }

    private static final long serialVersionUID = 5348377149312597939L;    
  }
}
