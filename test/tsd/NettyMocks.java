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

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;
import java.util.HashMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Ignore;
import org.powermock.reflect.Whitebox;

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
  public static TSDB getMockedHTTPTSDB() {
    final TSDB tsdb = mock(TSDB.class);
    final Config config = mock(Config.class);
    HashMap<String, String> properties = new HashMap<String, String>();
    properties.put("tsd.http.show_stack_trace", "true");
    Whitebox.setInternalState(config, "properties", properties);
    when(tsdb.getConfig()).thenReturn(config);
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
