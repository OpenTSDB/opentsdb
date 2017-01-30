package net.opentsdb.auth;
// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * @since 2.4
 */
public class AuthenticationChannelHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationChannelHandler.class);
  private TSDB tsdb = null;
  private AuthenticationPlugin authentication = null;

  public AuthenticationChannelHandler(TSDB tsdb) {
    LOG.info("Setting up AuthenticationChannelHandler");
    this.authentication = tsdb.getAuth();
    if (this.authentication == null) {
      LOG.info("No Authentication Plugin Configured");
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    e.getCause().printStackTrace();
    e.getChannel().close();
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent authEvent) {
    if (this.authentication == null) {
      LOG.info("Attempted to use null authentication plugin. This should not happen");
      LOG.debug("Removing Authentication Handler from Connection");
      ctx.getPipeline().remove(this);
    }
    try {
      final Object authCommand = authEvent.getMessage();
      String authResponse = "AUTH_FAIL\r\n";
      // Telnet Auth
      if (authCommand instanceof String[]) {
        LOG.debug("Passing auth command to Authentication Plugin");
        if (this.authentication.authenticateTelnet((String[]) authCommand)) {
          LOG.debug("Authentication Completed");
          authResponse = "AUTH_SUCCESS.\r\n";
          LOG.debug("Removing Authentication Handler from Connection");
          ctx.getPipeline().remove(this);
        }
        ChannelFuture future = authEvent.getChannel().write(authResponse);

      // HTTTP Auth
      } else if (authCommand instanceof HttpRequest) {
        HttpResponseStatus status;
        if (this.authentication.authenticateHTTP((HttpRequest) authCommand)) {
          LOG.debug("Authentication Completed");
          ctx.getPipeline().remove(this);
        } else {
          LOG.debug("Authentication Failed");
          status = HttpResponseStatus.FORBIDDEN;
          HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
          ChannelFuture future = authEvent.getChannel().write(response);
        }
      // Unknown Authentication
      } else {
        LOG.error("Unexpected message type "
                + authCommand.getClass() + ": " + authCommand);
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception caught"
              + " while serving: " + e);
    }
  }
}
