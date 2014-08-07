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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

import org.junit.Before;
import org.junit.Test;

public final class TestRTPublisher {
  private TSDB tsdb;
  private Config config;
  private RTPublisher rt_publisher;

  @Before
  public void before() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rtpublisher.DummyRTPublisher.hosts", "localhost");
    overrides.put("tsd.rtpublisher.DummyRTPublisher.port", "42");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

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
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rtpublisher.DummyRTPublisher.port", "42");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

    rt_publisher.initialize(tsdb);
  }

  @Test (expected = IllegalArgumentException.class)
  public void initializeEmptyHost() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rtpublisher.DummyRTPublisher.hosts", "");
    overrides.put("tsd.rtpublisher.DummyRTPublisher.port", "42");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

    rt_publisher.initialize(tsdb);
  }
  
  @Test (expected = NumberFormatException.class)
  public void initializeMissingPort() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rtpublisher.DummyRTPublisher.hosts", "localhost");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

    rt_publisher.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeInvalidPort() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.rtpublisher.DummyRTPublisher.hosts", "localhost");
    overrides.put("tsd.rtpublisher.DummyRTPublisher.port", "not a number");
    config = new Config(false, overrides);
    tsdb = new TSDB(new MemoryStore(), config);

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
  
}
