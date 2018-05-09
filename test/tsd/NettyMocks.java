// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.net.SocketAddress;
import java.nio.charset.Charset;

import com.stumbleupon.async.Deferred;
import net.opentsdb.auth.AllowAllAuthenticatingAuthorizer;
import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authentication;
import net.opentsdb.auth.Authorization;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.utils.Config;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Ignore;

/**
 * Helper class that provides mockups for testing any OpenTSDB processes that
 * deal with Netty.
 */
@Ignore
public final class NettyMocks {

  /**
   * Sets up a TSDB object for HTTP RPC tests that has a Config object
   * @return A TSDB mock
   */
  public static TSDB getMockedHTTPTSDB() throws Exception {
    final TSDB tsdb = mock(TSDB.class);

    final Config config = new Config(false);
    config.overrideConfig("tsd.http.show_stack_trace", "true");
    config.overrideConfig("tsd.core.authentication.enable", "false");
    when(tsdb.getConfig()).thenReturn(config);

    final Authentication authentication = mock(Authentication.class);
    when(authentication.isReady(any(TSDB.class), any(Channel.class))).thenReturn(false);
    when(tsdb.getAuth()).thenReturn(authentication);

    return tsdb;
  }

  /**
   * Sets up a TSDB object for HTTP RPC tests that has a Config object
   * @return A TSDB mock
   */
  public static TSDB getMockedHTTPTSDBWithAuthEnabled(final AuthState.AuthStatus authStatus) throws Exception {
    final TSDB tsdb = mock(TSDB.class);

    final Config config = new Config(false);
    config.overrideConfig("tsd.http.show_stack_trace", "true");
    config.overrideConfig("tsd.core.authentication.enable", "true");
    when(tsdb.getConfig()).thenReturn(config);

    final AuthState state = mock(AuthState.class);
    when(state.getStatus()).thenReturn(authStatus);

    final Authorization authorization = mock(Authorization.class);
    when(authorization.allowQuery(any(AuthState.class), any(TSQuery.class))).thenReturn(state);

    final Authentication authentication = mock(Authentication.class);
    when(authentication.authorization()).thenReturn(authorization);
    when(authentication.isReady(any(TSDB.class), any(Channel.class))).thenReturn(true);
    when(tsdb.getAuth()).thenReturn(authentication);
    return tsdb;
  }

  /**
   * Returns a mocked Channel object that simply sets the name to
   * [fake channel]
   * @return A Channel mock
   */
  public static Channel fakeChannel() {
    final Channel chan = mock(Channel.class);
    when(chan.toString()).thenReturn("[fake channel]");
    when(chan.isConnected()).thenReturn(true);
    when(chan.isWritable()).thenReturn(true);
    
    final SocketAddress socket = mock(SocketAddress.class);
    when(socket.toString()).thenReturn("192.168.1.1:4243");
    when(chan.getRemoteAddress()).thenReturn(socket);
    return chan;
  }
  
  /**
   * Returns an HttpQuery object with the given URI and the following parameters:
   * Method = GET
   * Content = null
   * Content-Type = null
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @return an HttpQuery object
   */
  public static HttpQuery getQuery(final TSDB tsdb, final String uri) {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, uri);
    return new HttpQuery(tsdb, req, channelMock);
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = POST
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @return an HttpQuery object
   */
  public static HttpQuery postQuery(final TSDB tsdb, final String uri, 
      final String content) {
    return postQuery(tsdb, uri, content, "application/json; charset=UTF-8");
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = POST
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @param type Content-Type value
   * @return an HttpQuery object
   */
  public static HttpQuery postQuery(final TSDB tsdb, final String uri, 
      final String content, final String type) {
    return contentQuery(tsdb, uri, content, type, HttpMethod.POST);
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = PUT
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @return an HttpQuery object
   */
  public static HttpQuery putQuery(final TSDB tsdb, final String uri, 
      final String content) {
    return putQuery(tsdb, uri, content, "application/json; charset=UTF-8");
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = PUT
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @param type Content-Type value
   * @return an HttpQuery object
   */
  public static HttpQuery putQuery(final TSDB tsdb, final String uri, 
      final String content, final String type) {
    return contentQuery(tsdb, uri, content, type, HttpMethod.PUT);
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = DELETE
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @return an HttpQuery object
   */
  public static HttpQuery deleteQuery(final TSDB tsdb, final String uri, 
      final String content) {
    return deleteQuery(tsdb, uri, content, "application/json; charset=UTF-8");
  }
  
  /**
   * Returns an HttpQuery object with the given uri, content and type
   * Method = DELETE
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @param type Content-Type value
   * @return an HttpQuery object
   */
  public static HttpQuery deleteQuery(final TSDB tsdb, final String uri, 
      final String content, final String type) {
    return contentQuery(tsdb, uri, content, type, HttpMethod.DELETE);
  }
  
  /**
   * Returns an HttpQuery object with the given settings
   * @param tsdb The TSDB to associate with, needs to be mocked with the Config
   * object set
   * @param uri A URI to use
   * @param content Content to POST (UTF-8 encoding)
   * @param type Content-Type value
   * @param method The HTTP method to use, GET, POST, etc.
   * @return an HttpQuery object
   */
  public static HttpQuery contentQuery(final TSDB tsdb, final String uri, 
      final String content, final String type, final HttpMethod method) {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        method, uri);
    if (content != null) {
      req.setContent(ChannelBuffers.copiedBuffer(content, 
          Charset.forName("UTF-8")));
    }
    req.headers().set("Content-Type", type);
    return new HttpQuery(tsdb, req, channelMock);
  }

  /**
   * @param query the query to mock a future callback for
   */
  public static void mockChannelFuture(final HttpQuery query) {
    final ChannelFuture future = new DefaultChannelFuture(query.channel(), false);
    when(query.channel().write(any(ChannelBuffer.class))).thenReturn(future);
    future.setSuccess();
  }
  
  /**
   * Returns a simple pipeline with an HttpRequestDecoder and an 
   * HttpResponseEncoder. No mocking, returns an actual pipeline
   * @return The pipeline
   */
  private DefaultChannelPipeline createHttpPipeline() {
    DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
    pipeline.addLast("requestDecoder", new HttpRequestDecoder());
    pipeline.addLast("responseEncoder", new HttpResponseEncoder());
    return pipeline;
  }
}
