package net.opentsdb.auth;
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

import net.opentsdb.core.TSDB;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * @since 2.3
 */
public class AuthenticationChannelHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationChannelHandler.class);
  private TSDB tsdb = null;
  private AuthenticationPlugin authentication = null;

  public AuthenticationChannelHandler(String type, TSDB tsdb) {
    LOG.info("Setting up AuthenticationChannelHandler");
    this.authentication = tsdb.getAuth();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    e.getCause().printStackTrace();
    e.getChannel().close();
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent authEvent) {
    String authResponse = "AUT_HFAIL\r\n";
    try {
      final Object authCommand = authEvent.getMessage();
      if (authCommand instanceof String[]) {
        LOG.debug("Passing auth command to Authentication Plugin");
        if (authentication.authenticate((String[]) authCommand)) {
          LOG.info("Authentication Completed");
          authResponse = "AUTH_SUCCESS.\r\n";
          ctx.getPipeline().remove(this);
        }
      } else if (message instanceof HttpRequest) {
        handleHttpQuery(tsdb, msgevent.getChannel(), (HttpRequest) message);
      } else {
        LOG.error("Unexpected message type "
                + authCommand.getClass() + ": " + authCommand);
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception caught"
              + " while serving: " + e);
    } finally {
      ChannelFuture future = authEvent.getChannel().write(authResponse);
    }
  }
}
