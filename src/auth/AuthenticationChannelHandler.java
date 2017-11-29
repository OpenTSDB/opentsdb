// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

package net.opentsdb.auth;

import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import org.jboss.netty.channel.ExceptionEvent;

import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.jboss.netty.buffer.ChannelBuffers;

/**
 * This is a simple authentication handler that intercepts the initial calls
 * for a new channel and attempts to authenticate the caller.
 * <p>
 * On a successful authentication, the authentication state is attached to the
 * Netty channel for other components to use and the handler is removed so that
 * subsequent calls across the channel do not have to go through authentication.
 * At any time, the pipeline can check the auth state to determine if actions 
 * are allowed or if the state should be revoked (e.g. token timing out)
 * <p>
 * On a failed auth, an error is returned to the user (message via Telnet or 
 * 401, 403 or 500 for HTTP). The channel is left open so callers can try again
 * but if an unknown error occurs then the channel is closed.
 * 
 * @since 2.4
 */
public class AuthenticationChannelHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
      AuthenticationChannelHandler.class);
  
  public static final String TELNET_AUTH_FAILURE = "AUTH_FAIL\r\n";
  public static final String TELNET_AUTH_SUCCESS = "AUTH_SUCCESS\r\n";
  
  /** The authentication object, pulled from TSDB. */
  private final Authentication authentication;

  /**
   * Default ctor.
   * @param tsdb Non-null TSDB object from which to fetch the auth plugin.
   * @throws IllegalArgumentException if the TSDB object is null or the auth
   * object the TSDB contains is null.
   */
  public AuthenticationChannelHandler(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB object cannot be null");
    }
    if (tsdb.getAuth() == null) {
      throw new IllegalArgumentException("Attempted to instantiate an "
          + "authentication handler but it was not configured in TSDB.");
    }
    authentication = tsdb.getAuth();
    LOG.info("Set up AuthenticationChannelHandler: " + authentication.getClass());
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, 
      final ExceptionEvent e) {
    LOG.error("Unexpected exception in AuthenticationChannelHandler for "
        + "channel: " + e.getChannel(), e.getCause());
    e.getChannel().close();
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, 
      final MessageEvent authEvent) {
    try {
      final Object authCommand = authEvent.getMessage();
      
      // Telnet Auth
      if (authCommand instanceof String[]) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Authenticating Telnet command from channel: " 
              + authEvent.getChannel());
        }
        String auth_response = TELNET_AUTH_FAILURE;
        final AuthState state = authentication.authenticateTelnet(
            authEvent.getChannel(), (String[]) authCommand);
        if (state.getStatus() == AuthStatus.SUCCESS) {
          auth_response = TELNET_AUTH_SUCCESS;
          ctx.getPipeline().remove(this);
          authEvent.getChannel().setAttachment(state);
          state.setChannel(authEvent.getChannel());
        }
        authEvent.getChannel().write(auth_response);

      // HTTTP Auth
      } else if (authCommand instanceof HttpRequest) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Authenticating HTTP request from channel: " 
              + authEvent.getChannel());
        }
        HttpResponseStatus status;
        final AuthState state = authentication.authenticateHTTP(
            authEvent.getChannel(), (HttpRequest) authCommand); 
        if (state.getStatus() == AuthStatus.SUCCESS) {
          ctx.getPipeline().remove(this);
          authEvent.getChannel().setAttachment(state);
          state.setChannel(authEvent.getChannel());
          // pass it down!
          super.messageReceived(ctx, authEvent);
        } else if (state.getStatus() == AuthStatus.REDIRECTED) {
          // do nothing here as the plugin sent the redirect answer. We want to
          // keep auth inline so the next call can process the authentication.
        } else {
          switch (state.getStatus()) {
          case UNAUTHORIZED:
            status = HttpResponseStatus.UNAUTHORIZED;
            break;
          case FORBIDDEN:
            status = HttpResponseStatus.FORBIDDEN;
            break;
          default:
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            break;
          }
          
          final HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
          if (!Strings.isNullOrEmpty(state.getMessage())) {
            // TODO - JSONify or something
            response.setContent(ChannelBuffers.copiedBuffer(
                state.getMessage(), Const.UTF8_CHARSET));
          }
          authEvent.getChannel().write(response);
        }
      // Unknown Authentication. Log and close the connection.
      } else {
        LOG.error("Unexpected message type " + authCommand.getClass() + ": " 
            + authCommand + " from channel: " + authEvent.getChannel());
        authEvent.getChannel().close();
      }
    } catch (Throwable t) {
      LOG.error("Unexpected exception caught while serving channel: " 
          + authEvent.getChannel(), t);
      authEvent.getChannel().close();
    }
  }
}