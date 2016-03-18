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
import org.jboss.netty.handler.codec.http.HttpRequest;
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

/**
 * @since 2.3
 */
public class AuthenticationChannelHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationChannelHandler.class);
  private TSDB tsdb = null;
  private AuthenticationPlugin authentication = null;

  public AuthenticationChannelHandler(TSDB tsdb) {
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
    String authResponse = "AUTH_FAIL\r\n";
    try {
      final Object authCommand = authEvent.getMessage();
      if (authCommand instanceof String[]) {
        LOG.debug("Passing auth command to Authentication Plugin");
        if (handleTelnetAuth((String[]) authCommand)) {
          LOG.debug("Authentication Completed");
          authResponse = "AUTH_SUCCESS.\r\n";
          ctx.getPipeline().remove(this);
        }
      } else if (authCommand instanceof HttpRequest) {
        handleHTTPAuth((HttpRequest) authCommand);
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

  private Boolean handleTelnetAuth(String[] command) {
    Boolean ret = false;
    if (command.length  < 3 || command.length > 4) {
      LOG.error("Invalid Authentication Command Length: " + Integer.toString(command.length));
    } else if (command[0].equals("auth")) {
      if (command[1].equals(AuthenticationUtil.algo.trim().toLowerCase())) {
        // Command should be 'auth hmacsha256 accessKey:digest:epoch:nonce'
        Map<String, String> fields = AuthenticationUtil.stringToMap(command[2], ":");
        LOG.debug("Validating Digest Credentials");
        ret = authentication.authenticate((String) fields.get("accessKey"), fields);
      } else if (command[1].equals("basic")) {
        // Command should be 'auth basic accessKey secretAccessKey'
        LOG.debug("Validating Basic Credentials");
        ret = authentication.authenticate(command[2], command[3]);
      } else {
        LOG.error("Command not understood: " + command[0] + " " + command[1]);
      }
    } else {
      LOG.error("Command is not auth: " + command[0]);
    }
    return ret;
  }


  //Authorization: OpenTSDB accessKey:digest:epoch:nonce
  private Boolean handleHTTPAuth(final HttpRequest req) {
    Iterable<Map.Entry<String,String>> headers = req.headers();
    Iterator entries = headers.iterator();
    while (entries.hasNext()) {
      Map.Entry thisEntry = (Map.Entry) entries.next();
      String key = (String) thisEntry.getKey();
      String value = (String) thisEntry.getValue();
      if (key.trim().toLowerCase().equals("authorization")) {
        String[] fieldsRaw = value.split(" ");
        if (fieldsRaw.length == 2 && fieldsRaw[0].trim().toLowerCase().equals("opentsdb")) {
          String[] fieldsArray = fieldsRaw[1].trim().toLowerCase().split(":");
          Map<String, String> fields = AuthenticationUtil.stringToMap(fieldsRaw[1], ":");
          LOG.debug("Validating Digest Credentials");
          return authentication.authenticate((String) fields.get("accessKey"), fields);
        } else {
          throw new IllegalArgumentException("Improperly formatted Authorization Header: " + value);
        }
      }
    }
    LOG.info("No Authorization Header Found");
    return false;
  }
}
