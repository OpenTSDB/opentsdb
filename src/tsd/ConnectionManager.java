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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.embedder.CodecEmbedderException;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateAwareChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;

/**
 * Keeps track of all existing connections.
 */
final class ConnectionManager extends IdleStateAwareChannelHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

  static final DefaultChannelGroup channels =
    new DefaultChannelGroup("all-channels");
  private final TsdStats.ConnectionManagerStats stats;

  static void closeAllConnections() {
    channels.close().awaitUninterruptibly();
  }

  /**
   * Constructor.
   */
  public ConnectionManager(final TsdStats tsdStats) {
    this.stats = tsdStats.getConnectionManagerStats();
  }

  @Override
  public void channelOpen(final ChannelHandlerContext ctx,
                          final ChannelStateEvent e) {
    channels.add(e.getChannel());
    stats.getConnections_established().inc();
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
      stats.getExceptions_closed().inc();
      LOG.warn("Attempt to write to closed channel {}", chan);
      return;
    }
    if (cause instanceof IOException) {
      final String message = cause.getMessage();
      if ("Connection reset by peer".equals(message)) {
        stats.getExceptions_reset().inc();
        return;
      } else if ("Connection timed out".equals(message)) {
        stats.getExceptions_timeout().inc();
        // Do nothing.  A client disconnecting isn't really our problem.  Oh,
        // and I'm not kidding you, there's no better way to detect ECONNRESET
        // in Java.  Like, people have been bitching about errno for years,
        // and Java managed to do something *far* worse.  That's quite a feat.
        return;
      }
    }
    if (cause instanceof CodecEmbedderException) {
    	// payload was not compressed as it was announced to be
      LOG.warn("Http codec error : {}", cause.getMessage());
    	e.getChannel().close();
    	return;
    }
    stats.getExceptions_unknown().inc();
    LOG.error("Unexpected exception from downstream for {}", chan, cause);
    e.getChannel().close();
  }

  @Override
  public void channelIdle(ChannelHandlerContext ctx, IdleStateEvent e) {
    if (e.getState() == IdleState.ALL_IDLE) {
      LOG.debug("Closed idle socket.");
      e.getChannel().close();
    }
  }
  
}
