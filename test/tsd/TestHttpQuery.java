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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(HttpQuery.class)
public class TestHttpQuery {
 
  @Test
  public void getQueryString() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
    assertTrue(params.get("param").get(0).equals("value"));
    assertTrue(params.get("param2").get(0).equals("value2"));
  }
  
  @Test
  public void getQueryStringEmpty() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
    assertTrue(params.size() == 0);
  }
  
  @Test
  public void getQueryStringMulti() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=v1&param=v2&param=v3");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
    assertTrue(params.size() == 1);
    assertTrue(params.get("param").size() == 3);
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryStringNULL() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, null);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
  }
  
  @Test
  public void getQueryStringParam() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getQueryStringParam("param").equals("value"));
  }
  
  @Test
  public void getQueryStringParamNull() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertNull(query.getQueryStringParam("nothere"));
  }
  
  @Test
  public void getRequiredQueryStringParam() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getRequiredQueryStringParam("param").equals("value"));
  }
  
  @Test (expected = BadRequestException.class)
  public void getRequiredQueryStringParamMissing() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    query.getRequiredQueryStringParam("nothere");
  }
  
  @Test
  public void hasQueryStringParam() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.hasQueryStringParam("param"));
  }
  
  @Test
  public void hasQueryStringMissing() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertFalse(query.hasQueryStringParam("nothere"));
  }
  
  @Test
  public void getQueryStringParams() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=v1&param=v2&param=v3");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    List<String> params = query.getQueryStringParams("param");
    assertNotNull(params);
    assertTrue(params.size() == 3);
  }
  
  @Test
  public void getQueryStringParamsNull() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=v1&param=v2&param=v3");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    List<String> params = query.getQueryStringParams("nothere");
    assertNull(params);
  }
  
  @Test
  public void getQueryPathA() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getQueryPath().equals("/api/v1/put"));
  }
  
  @Test
  public void getQueryPathB() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getQueryPath().equals("/"));
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryPathNull() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, null);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getQueryPath().equals("/"));
  }
  
  @Test
  public void explodePath() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    final String[] path = query.explodePath();
    assertNotNull(path);
    assertTrue(path.length == 3);
    assertTrue(path[0].equals("api"));
    assertTrue(path[1].equals("v1"));
    assertTrue(path[2].equals("put"));
  }
  
  @Test
  public void explodePathEmpty() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    final String[] path = query.explodePath();
    assertNotNull(path);
    assertTrue(path.length == 0);
  }
  
  @Test (expected = NullPointerException.class)
  public void explodePathNull() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, null);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    @SuppressWarnings("unused")
    final String[] path = query.explodePath();
  }
  
  @Test
  public void getCharsetDefault() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.addHeader("Content-Type", "text/plain");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getCharset().equals(Charset.forName("UTF-8")));
  }
  
  @Test
  public void getCharsetDefaultNoHeader() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getCharset().equals(Charset.forName("UTF-8")));
  }
  
  @Test
  public void getCharsetSupplied() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.addHeader("Content-Type", "text/plain; charset=UTF-16");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getCharset().equals(Charset.forName("UTF-16")));
  }
  
  @Test (expected = UnsupportedCharsetException.class)
  public void getCharsetInvalid() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.addHeader("Content-Type", "text/plain; charset=foobar");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getCharset().equals(Charset.forName("UTF-16")));
  }
  
  @Test
  public void getContentEncoding() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.addHeader("Content-Type", "text/plain; charset=UTF-16");
    final ChannelBuffer buf = ChannelBuffers.copiedBuffer("S\u00ED Se\u00F1or", 
        CharsetUtil.UTF_16);
    req.setContent(buf);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getContent().equals("S\u00ED Se\u00F1or"));
  }
  
  @Test
  public void getContentDefault() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final ChannelBuffer buf = ChannelBuffers.copiedBuffer("S\u00ED Se\u00F1or", 
        CharsetUtil.UTF_8);
    req.setContent(buf);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getContent().equals("S\u00ED Se\u00F1or"));
  }
  
  @Test
  public void getContentBadEncoding() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final ChannelBuffer buf = ChannelBuffers.copiedBuffer("S\u00ED Se\u00F1or", 
        CharsetUtil.ISO_8859_1);
    req.setContent(buf);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertFalse(query.getContent().equals("S\u00ED Se\u00F1or"));
  }
  
  @Test
  public void getContentEmpty() {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getContent().isEmpty());
  }
  
  @Test
  public void guessMimeTypeFromUriPNG() throws Exception {
    assertEquals(ReflectguessMimeTypeFromUri().invoke(null, "abcd.png"), 
        "image/png");
  }
  
  @Test
  public void guessMimeTypeFromUriHTML() throws Exception {
    assertEquals(ReflectguessMimeTypeFromUri().invoke(null, "abcd.html"), 
        "text/html; charset=UTF-8");
  }
  
  @Test
  public void guessMimeTypeFromUriCSS() throws Exception {
    assertEquals(ReflectguessMimeTypeFromUri().invoke(null, "abcd.css"), 
        "text/css");
  }
  
  @Test
  public void guessMimeTypeFromUriJS() throws Exception {
    assertEquals(ReflectguessMimeTypeFromUri().invoke(null, "abcd.js"), 
        "text/javascript");
  }
  
  @Test
  public void guessMimeTypeFromUriGIF() throws Exception {
    assertEquals(ReflectguessMimeTypeFromUri().invoke(null, "abcd.gif"), 
        "image/gif");
  }
  
  @Test
  public void guessMimeTypeFromUriICO() throws Exception {
    assertEquals(ReflectguessMimeTypeFromUri().invoke(null, "abcd.ico"), 
        "image/x-icon");
  }
  
  @Test
  public void guessMimeTypeFromUriOther() throws Exception {
    assertNull(ReflectguessMimeTypeFromUri().invoke(null, "abcd.jpg"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void guessMimeTypeFromUriNull() throws Exception {
    ReflectguessMimeTypeFromUri().invoke(null, (Object[])null);
  }
  
  @Test 
  public void guessMimeTypeFromUriEmpty() throws Exception {
    assertNull(ReflectguessMimeTypeFromUri().invoke(null, ""));
  }

  @Test
  public void guessMimeTypeFromContentsHTML() throws Exception {
    assertEquals(ReflectguessMimeTypeFromContents().invoke(
        new HttpQuery(null, null, NettyMocks.fakeChannel()),
        ChannelBuffers.copiedBuffer(
            "<HTML>...", Charset.forName("UTF-8"))), 
        "text/html; charset=UTF-8");
  }
  
  @Test
  public void guessMimeTypeFromContentsJSONObj() throws Exception {
    assertEquals(ReflectguessMimeTypeFromContents().invoke(
        new HttpQuery(null, null, NettyMocks.fakeChannel()),
        ChannelBuffers.copiedBuffer(
            "{\"hello\":\"world\"}", Charset.forName("UTF-8"))), 
        "application/json");
  }
  
  @Test
  public void guessMimeTypeFromContentsJSONArray() throws Exception {
    assertEquals(ReflectguessMimeTypeFromContents().invoke(
        new HttpQuery(null, null, NettyMocks.fakeChannel()),
        ChannelBuffers.copiedBuffer(
            "[\"hello\",\"world\"]", Charset.forName("UTF-8"))), 
        "application/json");
  }
  
  @Test
  public void guessMimeTypeFromContentsPNG() throws Exception {
    assertEquals(ReflectguessMimeTypeFromContents().invoke(
        new HttpQuery(null, null, NettyMocks.fakeChannel()),
        ChannelBuffers.copiedBuffer(
            new byte[] {(byte) 0x89, 0x00})), 
        "image/png");
  }
  
  @Test
  public void guessMimeTypeFromContentsText() throws Exception {
    assertEquals(ReflectguessMimeTypeFromContents().invoke(
        new HttpQuery(null, null, NettyMocks.fakeChannel()),
        ChannelBuffers.copiedBuffer(
            "Just plain text", Charset.forName("UTF-8"))), 
        "text/plain");
  }
  
  @Test 
  public void guessMimeTypeFromContentsEmpty() throws Exception {
    assertEquals(ReflectguessMimeTypeFromContents().invoke(
        new HttpQuery(null, null, NettyMocks.fakeChannel()),
        ChannelBuffers.copiedBuffer(
            "", Charset.forName("UTF-8"))), 
        "text/plain");
  }
  
  @Test (expected = NullPointerException.class)
  public void guessMimeTypeFromContentsNull() throws Exception {
    ChannelBuffer buf = null;
    ReflectguessMimeTypeFromContents().invoke(
        new HttpQuery(null, null, NettyMocks.fakeChannel()), buf);
  }
  
  /** 
   * Reflection for the guessMimeTypeFromURI(final String uri) method
   * @return The method if it was detected
   * @throws Exception If the method was not found
   */
  private Method ReflectguessMimeTypeFromUri() throws Exception {
    Method guessMimeTypeFromUri = HttpQuery.class.getDeclaredMethod(
        "guessMimeTypeFromUri", String.class);
    guessMimeTypeFromUri.setAccessible(true);
    return guessMimeTypeFromUri;
  }

  /**
   * Reflection for the ReflectguessMimeTypeFromContents(final ChannelBuffer)
   * method
   * @return The method if it was detected
   * @throws Exception if the method was not found
   */
  private Method ReflectguessMimeTypeFromContents() throws Exception {
    Method guessMimeTypeFromContents = HttpQuery.class.getDeclaredMethod(
        "guessMimeTypeFromContents", ChannelBuffer.class);
    guessMimeTypeFromContents.setAccessible(true);
    return guessMimeTypeFromContents;
  }
}
