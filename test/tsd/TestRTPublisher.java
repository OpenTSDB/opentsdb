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
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginJARFactory;
import net.opentsdb.utils.PluginLoader;

import org.hbase.async.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class})
public final class TestRTPublisher {
  private TSDB tsdb= mock(TSDB.class);
  private Config config = mock(Config.class);
  private RTPublisher rt_publisher;
  
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
    // setups a good default for the config
    when(config.hasProperty("tsd.rtpublisher.DummyRTPublisher.hosts"))
      .thenReturn(true);
    when(config.getString("tsd.rtpublisher.DummyRTPublisher.hosts"))
      .thenReturn("localhost");
    when(config.getInt("tsd.rtpublisher.DummyRTPublisher.port")).thenReturn(42);
    when(tsdb.getConfig()).thenReturn(config);
    PluginLoader.loadJAR("plugin_test.jar");
    rt_publisher = PluginLoader.loadSpecificPlugin(
        "net.opentsdb.tsd.DummyRTPublisher", RTPublisher.class);
  }
  
  @Test
  public void initialize() throws Exception {
    rt_publisher.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeMissingHost() throws Exception {
    when(config.hasProperty("tsd.rtpublisher.DummyRTPublisher.hosts"))
      .thenReturn(false);
    rt_publisher.initialize(tsdb);
  }
  
  public void initializeEmptyHost() throws Exception {
    when(config.getString("tsd.rtpublisher.DummyRTPublisher.hosts"))
      .thenReturn("");
    rt_publisher.initialize(tsdb);
  }
  
  @Test (expected = NullPointerException.class)
  public void initializeMissingPort() throws Exception {
    when(config.getInt("tsd.rtpublisher.DummyRTPublisher.port"))
      .thenThrow(new NullPointerException());
    rt_publisher.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeInvalidPort() throws Exception {
    when(config.getInt("tsd.rtpublisher.DummyRTPublisher.port"))
    .thenThrow(new NumberFormatException());
    rt_publisher.initialize(tsdb);
  }
  
  @Test
  public void shutdown() throws Exception  {
    assertNotNull(rt_publisher.shutdown());
  }
  
  @Test
  public void version() throws Exception  {
    assertEquals("2.0.0", rt_publisher.version());
  }
  
  @Test
  public void sinkDataPoint() throws Exception {
    assertNotNull(rt_publisher.sinkDataPoint("sys.cpu.user", 
        System.currentTimeMillis(), new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }, 
        null, null, (short)0x7));
  }
  
  @Test
  @Ignore
  public void sinkDataPoint2() throws Exception {
    assertNotNull(rt_publisher.sinkDataPoint("sys.cpu.user", 
        System.currentTimeMillis(), new byte[] { 75 }, 
        null, null, (short)0x7));
  }
  
  @Test
  public void publish75() throws Exception {
	  HashMap<String, String> tags = new HashMap<String, String>(2);
	  tags.put("host", "myhost");
	  tags.put("vol", "tmp");
	  //tsdb.addPoint("sys.fs.free", System.currentTimeMillis(), 75L, tags);
	  addPoint("sys.fs.free", System.currentTimeMillis(), (long)(Short.MAX_VALUE), tags);
  }
  
  
  @Test
  public void publishAnnotation() throws Exception {
	  Annotation ann = new Annotation();
	  HashMap<String, String> customMap = new HashMap<String, String>(1);
	  customMap.put("test-custom-key", "test-custom-value");
	  ann.setCustom(customMap);
	  ann.setDescription("A test annotation");
	  ann.setNotes("Test annotation notes");
	  ann.setStartTime(System.currentTimeMillis());	  
	  assertNotNull(rt_publisher.publishAnnotation(ann));
  }
  
  
  public void addPoint(final String metric,
          final long timestamp,
          final long value,
          final Map<String, String> tags) {
	final byte[] v;
	if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
	v = new byte[] { (byte) value };
	} else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
	v = Bytes.fromShort((short) value);
	} else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
	v = Bytes.fromInt((int) value);
	} else {
	v = Bytes.fromLong(value);
	}

	final short flags = (short) (v.length - 1);  // Just the length.
	//return addPointInternal(metric, timestamp, v, tags, flags);
	
	rt_publisher.sinkDataPoint(metric, timestamp, v, tags, null, flags);
}
  
}
