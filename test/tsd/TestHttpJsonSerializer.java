// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNotNull;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginJARFactory;

import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for the JSON serializer.
 * <b>Note:</b> Tests for the default error handlers are in the TestHttpQuery
 * class
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class})
public final class TestHttpJsonSerializer {
  private TSDB tsdb = null;
  
  /**
   * Creates the plugin jar
   */
  @BeforeClass
  public static void initPluginJar() {
	  PluginJARFactory.newBuilder("plugin_test.jar")
	  	.service("net.opentsdb.plugin.DummyPlugin", "net.opentsdb.plugin.DummyPluginA", "net.opentsdb.plugin.DummyPluginB")
	  	.service("net.opentsdb.search.SearchPlugin", "net.opentsdb.search.DummySearchPlugin")
	  	.service("net.opentsdb.tsd.HttpSerializer", "net.opentsdb.tsd.DummyHttpSerializer")
	  	.service("net.opentsdb.tsd.RpcPlugin", "net.opentsdb.tsd.DummyRpcPlugin")
	  	.service("net.opentsdb.tsd.RTPublisher", "net.opentsdb.tsd.DummyRTPublisher")
	  	.build();
  }
  
  /**
   * Clears the created plugin jar
   */
  @AfterClass
  public static void clearPluginJar() {
	  PluginJARFactory.purge();
  }
  

  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
  }
  
  @Test
  public void constructorDefault() {
    assertNotNull(new HttpJsonSerializer());
  }
  
  @Test
  public void constructorQuery() {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    assertNotNull(new HttpJsonSerializer(query));
  }
  
  @Test
  public void shutdown() {
    assertNotNull(new HttpJsonSerializer().shutdown());
  }
  
  @Test
  public void version() {
    assertEquals("2.0.0", new HttpJsonSerializer().version());
  }
  
  @Test
  public void shortName() {
    assertEquals("json", new HttpJsonSerializer().shortName());
  }
  
  @Test
  public void requestContentType() {
    HttpJsonSerializer serdes = new HttpJsonSerializer();
    assertEquals("application/json", serdes.requestContentType());
  }
  
  @Test
  public void responseContentType() {
    HttpJsonSerializer serdes = new HttpJsonSerializer();
    assertEquals("application/json; charset=UTF-8", serdes.responseContentType());
  }
  
  @Test
  public void parseSuggestV1() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", 
        "{\"type\":\"metrics\",\"q\":\"\"}", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    HashMap<String, String> map = serdes.parseSuggestV1();
    assertNotNull(map);
    assertEquals("metrics", map.get("type"));
  }
  
  @Test (expected = BadRequestException.class)
  public void parseSuggestV1NoContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", 
        null, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseSuggestV1();
  }
  
  @Test (expected = BadRequestException.class)
  public void parseSuggestV1EmptyContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", 
        "", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseSuggestV1();
  }
  
  @Test (expected = BadRequestException.class)
  public void parseSuggestV1NotJSON() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "", 
        "This is unparsable", "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.parseSuggestV1();
  }
  
  @Test
  public void formatSuggestV1() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final List<String> metrics = new ArrayList<String>();
    metrics.add("sys.cpu.0.system"); 
    ChannelBuffer cb = serdes.formatSuggestV1(metrics);
    assertNotNull(cb);
    assertEquals("[\"sys.cpu.0.system\"]", 
        cb.toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void formatSuggestV1JSONP() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "?jsonp=func");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    final List<String> metrics = new ArrayList<String>();
    metrics.add("sys.cpu.0.system"); 
    ChannelBuffer cb = serdes.formatSuggestV1(metrics);
    assertNotNull(cb);
    assertEquals("func([\"sys.cpu.0.system\"])", 
        cb.toString(Charset.forName("UTF-8")));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void formatSuggestV1Null() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    serdes.formatSuggestV1(null);
  }
  
  @Test
  public void formatSerializersV1() throws Exception {
    HttpQuery.initializeSerializerMaps(tsdb);
    HttpQuery query = NettyMocks.getQuery(tsdb, "");
    HttpJsonSerializer serdes = new HttpJsonSerializer(query);
    assertEquals("[{\"formatters\":",
        serdes.formatSerializersV1().toString(Charset.forName("UTF-8"))
        .substring(0, 15));
  }
}
