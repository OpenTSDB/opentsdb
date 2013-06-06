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
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class})
public final class TestRTPublisher {
  private TSDB tsdb= mock(TSDB.class);
  private Config config = mock(Config.class);
  private RTPublisher rt_publisher;

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
}
