package net.opentsdb.tsd;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.net.*;
import java.io.*;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;
import net.opentsdb.core.BaseTsdbTest.FakeTaskTimer;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.uid.NoSuchUniqueName;


import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)

@PrepareForTest({ TSDB.class, Config.class, HttpQuery.class})
public final class TestHeader {
  private TSDB tsdb = null;
  private static final Map<String, String> TAGS = new HashMap<String, String>(1);

  static {
    TAGS.put("host", "web01");
  }
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, 42, TAGS))
      .thenReturn(Deferred.fromResult(new Object()));

  }


/*
 * Test without header, test il method enable_header_tag return false
 */
 @Test
 public void testNoHeader() {
   final Channel channelMock = NettyMocks.fakeChannel();
   final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
       HttpMethod.GET, "/");

   final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
   assertFalse(tsdb.getConfig().enable_header_tag());

 }


 /*
  * Test with header and compare with attent result
  */
  @Test
  public void testHeaderTagResult() throws Exception {
    tsdb.getConfig().overrideConfig("tsd.http.header_tag", "XXXTEST");
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
        HttpMethod.POST, "/");
    req.headers().add("XXXTEST", "test");

    HttpQuery headerQuery = new HttpQuery(tsdb, req, channelMock);
    assertEquals("test", headerQuery.getHeaderValue("XXXTEST"));

  }
}
