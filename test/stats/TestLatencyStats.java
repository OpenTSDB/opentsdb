/*
 * Copyright 2015, Simon Matić Langford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.stats;

import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Config.class, LatencyStatsPlugin.class})
public class TestLatencyStats {
  
  private Config config;
  
  @Before
  public void before() {
    config = mock(Config.class);
    LatencyStats.clear();
  }
  
  @Test
  public void defaultPlugin() throws IOException {
    when(config.hasProperty("tsd.latency_stats.plugin.default_plugin")).thenReturn(false);
    when(config.hasProperty("tsd.latency_stats.plugin")).thenReturn(false);
    
    LatencyStatsPlugin plugin = LatencyStats.getInstance(config, "default_plugin", "some.stat", "a=b");
    assertTrue("Default plugin implementation should be Histogram", plugin instanceof Histogram);
  }
  
  @Test
  public void globalPlugin() {
    when(config.hasProperty("tsd.latency_stats.plugin.global_plugin")).thenReturn(false);
    when(config.hasProperty("tsd.latency_stats.plugin")).thenReturn(true);
    when(config.getString("tsd.latency_stats.plugin")).thenReturn("net.opentsdb.stats.DummyLatencyStatsPlugin");

    LatencyStatsPlugin plugin = LatencyStats.getInstance(config, "global_plugin", "some.stat", "a=b");
    assertTrue("Global plugin implementation should be DummyLatencyStatsPlugin", plugin instanceof DummyLatencyStatsPlugin);
  }
  
  @Test
  public void globalPluginNotFound() {
    when(config.hasProperty("tsd.latency_stats.plugin.global_plugin_not_found")).thenReturn(false);
    when(config.hasProperty("tsd.latency_stats.plugin")).thenReturn(true);
    when(config.getString("tsd.latency_stats.plugin")).thenReturn("net.opentsdb.stats.MissingPlugin");

    try {
      LatencyStats.getInstance(config, "global_plugin_not_found", "some.stat", "a=b");
      fail("Getting a missing plugin should have failed");
    }
    catch (Exception e) {
      assertTrue("Expected message should contain the missing plugin class name", e.getMessage().contains("net.opentsdb.stats.MissingPlugin"));
    }
  }
  
  @Test
  public void specificPlugin() {
    when(config.hasProperty("tsd.latency_stats.plugin.specific_plugin")).thenReturn(true);
    when(config.hasProperty("tsd.latency_stats.plugin")).thenReturn(false);
    when(config.getString("tsd.latency_stats.plugin.specific_plugin")).thenReturn("net.opentsdb.stats.DummyLatencyStatsPlugin");

    LatencyStatsPlugin plugin = LatencyStats.getInstance(config, "specific_plugin", "some.stat", "a=b");
    assertTrue("Specific plugin implementation should be DummyLatencyStatsPlugin", plugin instanceof DummyLatencyStatsPlugin);
  }
  
  @Test
  public void specificPluginNotFound() {
    when(config.hasProperty("tsd.latency_stats.plugin.specific_plugin_not_found")).thenReturn(true);
    when(config.hasProperty("tsd.latency_stats.plugin")).thenReturn(false);
    when(config.getString("tsd.latency_stats.plugin.specific_plugin_not_found")).thenReturn("net.opentsdb.stats.MissingPlugin");

    try {
      LatencyStats.getInstance(config, "specific_plugin_not_found", "some.stat", "a=b");
      fail("Getting a missing plugin should have failed");
    }
    catch (Exception e) {
      assertTrue("Expected message should contain the missing plugin class name", e.getMessage().contains("net.opentsdb.stats.MissingPlugin"));
    }
    
  }
  
  @Test
  public void defaultExtraTags() {
    LatencyStatsPlugin mockPlugin = mock(LatencyStatsPlugin.class);
    DummyLatencyStatsPlugin.setMock(mockPlugin);
    try {
      when(config.hasProperty("tsd.latency_stats.plugin.plugin_lifecycle")).thenReturn(true);
      when(config.getString("tsd.latency_stats.plugin.plugin_lifecycle")).thenReturn("net.opentsdb.stats.DummyLatencyStatsPlugin");
      when(mockPlugin.shutdown()).thenReturn(Deferred.fromResult(null));

      LatencyStats.getInstance(config, "plugin_lifecycle", "some.stat");
      LatencyStats.shutdownAll();

      verify(mockPlugin).initialize(any(Config.class), eq("some.stat"), isNull(String.class));
    }
    finally {
      DummyLatencyStatsPlugin.setMock(null);
    }
  }
  
  @Test
  public void pluginLifecycle() {
    LatencyStatsPlugin mockPlugin = mock(LatencyStatsPlugin.class);
    DummyLatencyStatsPlugin.setMock(mockPlugin);
    try {
      when(config.hasProperty("tsd.latency_stats.plugin.plugin_lifecycle")).thenReturn(true);
      when(config.getString("tsd.latency_stats.plugin.plugin_lifecycle")).thenReturn("net.opentsdb.stats.DummyLatencyStatsPlugin");
      when(mockPlugin.shutdown()).thenReturn(Deferred.fromResult(null));

      LatencyStats.getInstance(config, "plugin_lifecycle", "some.stat", "a=b");
      LatencyStats.shutdownAll();
      
      InOrder inOrder = inOrder(mockPlugin);
      inOrder.verify(mockPlugin).initialize(any(Config.class), eq("some.stat"), eq("a=b"));
      inOrder.verify(mockPlugin).version();
      inOrder.verify(mockPlugin).start();
      inOrder.verify(mockPlugin).shutdown();
    }
    finally {
      DummyLatencyStatsPlugin.setMock(null);
    }
  }
  
  
  
}
