// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.PluginsConfig.PluginConfig;
import net.opentsdb.exceptions.PluginLoadException;
import net.opentsdb.query.execution.cache.GuavaLRUCache;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cluster.ClusterConfigPlugin;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.PluginLoader;

/**
 * <B>NOTE:</b> This class depends on the behavior of {@link PluginLoader} from
 * the TSDB common class as well as
 * {@link DefaultRegistry#registerPlugin(Class, String, BaseTSDBPlugin)}'s behavior.
 */
public class TestPluginsConfig {
  private static int ORDER = 0;
  private TSDB tsdb;
  private Config tsd_config;
  private PluginsConfig config;
  
  @Before
  public void before() throws Exception {
    ORDER = 0;
    tsdb = mock(TSDB.class);
    tsd_config = new Config(false);
    
    when(tsdb.getConfig()).thenReturn(tsd_config);
    config = spy(new PluginsConfig());
  }
  
  @Test
  public void serdes() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockB");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config = new PluginsConfig();
    config.setPluginLocations(Lists.newArrayList("nosuchpath", "nosuch.jar"));
    config.setConfigs(configs);
    
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"configs\":[{"));
    assertTrue(json.contains("\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean\""));
    assertTrue(json.contains("\"plugin\":\"net.opentsdb.core.TestPluginsConfig$MockPluginA\""));
    assertTrue(json.contains("\"id\":\"MockTest\""));
    assertTrue(json.contains("\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\""));
    assertTrue(json.contains("\"id\":\"MockB\""));
    assertTrue(json.contains("\"continueOnError\":false"));
    assertTrue(json.contains("\"shutdownReverse\":true"));
    assertTrue(json.contains("\"default\":true"));
    assertTrue(json.contains("\"pluginLocations\":["));
    assertTrue(json.contains("\"nosuch.jar"));
    
    json = "{\"configs\":[{\"comment\":\"ignored\",\"type\":\""
        + "net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean\"},{\"plugin\":"
        + "\"net.opentsdb.core.TestPluginsConfig$MockPluginA\",\"id\":"
        + "\"MockTest\",\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\"},"
        + "{\"plugin\":\"net.opentsdb.core.TestPluginsConfig$MockPluginB\","
        + "\"id\":\"MockB\",\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\"}"
        + ",{\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\"},"
        + "{\"plugin\":\"net.opentsdb.core.TestPluginsConfig$MockPluginA\","
        + "\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\","
        + "\"default\":true},{\"plugin\":"
        + "\"net.opentsdb.core.TestPluginsConfig$MockPluginC\",\"type\":"
        + "\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\",\"default\":true}],"
        + "\"pluginLocations\":[\"/tmp\"],"
        + "\"continueOnError\":false,\"shutdownReverse\":true}";
    
    config = JSON.parseToObject(json, PluginsConfig.class);
    
    assertEquals(6, config.getConfigs().size());
    assertFalse(config.getContinueOnError());
    assertTrue(config.getShutdownReverse());
    assertTrue(config.getConfigs().get(4).isDefault());
    assertEquals(1, config.getPluginLocations().size());
    assertEquals("/tmp", config.getPluginLocations().get(0));
  }

  @Test
  public void validateOK() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockB");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("GonnaThrow");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateDuplicateID() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest"); // <-- BAD
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("GonnaThrow");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test
  public void validateDuplicateIDDiffType() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginCleanA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");  // <-- OK
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("GonnaThrow");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateSpecificNullID() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    //c.setId("MockTest"); // <-- BAD
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("GonnaThrow");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateSpecificEmptyID() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId(""); // <-- BAD
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("GonnaThrow");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateDefaultHasId() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockB");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setId("NotAllowed"); // <-- BAD
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("GonnaThrow");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateMultipleDefaults() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockB");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    config.validate();
  }
  
  @Test
  public void getAndSetPlugins() throws Exception {
    final GuavaLRUCache guava_plugin = mock(GuavaLRUCache.class);
    
    // as default
    config.registerPlugin(QueryCachePlugin.class, null, 
        guava_plugin);
    assertSame(guava_plugin, config.getPlugin(
        QueryCachePlugin.class, null));
    
    // with ID
    config.registerPlugin(QueryCachePlugin.class, "GuavaLRU", 
        guava_plugin);
    assertSame(guava_plugin, config.getPlugin(
        QueryCachePlugin.class, "GuavaLRU"));
    
    // empty ID, allowed for now
    config.registerPlugin(QueryCachePlugin.class, "", 
        guava_plugin);
    assertSame(guava_plugin, config.getPlugin(
        QueryCachePlugin.class, ""));
    
    try {
      config.registerPlugin(null, "GuavaLRU", guava_plugin);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config.registerPlugin(QueryCachePlugin.class, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // wrong type
    try {
      config.registerPlugin(ClusterConfigPlugin.class, null, guava_plugin);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // already present
    try {
      config.registerPlugin(QueryCachePlugin.class, null, 
          guava_plugin);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config.getPlugin(null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initializeNullList() throws Exception {
    assertNull(config.initialize(tsdb).join(1));
  }
  
  @Test
  public void initializeEmptyList() throws Exception {
    config.setConfigs(Lists.<PluginConfig>newArrayList());
    assertNull(config.initialize(tsdb).join(1));
  }
  
  @Test
  public void initializeSingleWithId() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    assertNull(config.initialize(tsdb).join(1));
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, times(1)).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginBase.class));
    
    final MockPluginA plugin = (MockPluginA) config.getPlugin(clazz, "MockTest");
    assertEquals(0, plugin.order);
  }
  
  @Test
  public void initializeSingleDefault() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    assertNull(config.initialize(tsdb).join(1));
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, times(1)).registerPlugin(eq(clazz), eq(null), 
        any(MockPluginBase.class));
    
    final MockPluginA plugin = (MockPluginA) config.getPlugin(clazz, null);
    assertEquals(0, plugin.order);
  }
  
  @Test
  public void initializeSingleCtorException() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) {
      assertTrue(e.getCause() instanceof UnsupportedOperationException);
    }
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, never()).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void initializeSingleInitException() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) {
      assertTrue(e.getCause() instanceof UnsupportedOperationException);
    }
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, never()).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void initializeSingleInitExceptionContinueOnError() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    config.setContinueOnError(true);
    
    assertNull(config.initialize(tsdb).join(1));
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, never()).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void initializeSingleNoSuchPlugin() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginD");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) { }
    
    verify(config, never()).registerPlugin(any(Class.class), anyString(), 
        any(BaseTSDBPlugin.class));
  }
  
  @Test
  public void initializeSingleNoSuchType() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$NoSuchMockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) { }
    
    verify(config, never()).registerPlugin(any(Class.class), anyString(), 
        any(BaseTSDBPlugin.class));
  }
  
  @Test
  public void initializeSinglesChain() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("DifferentID");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    assertNull(config.initialize(tsdb).join(1));
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, times(1)).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginBase.class));
    verify(config, times(1)).registerPlugin(eq(clazz), eq("DifferentID"), 
        any(MockPluginBase.class));
    verify(config, times(1)).registerPlugin(eq(clazz), eq(null), 
        any(MockPluginBase.class));
    
    MockPluginA plugin = (MockPluginA) config.getPlugin(clazz, "MockTest");
    assertEquals(0, plugin.order);
    
    plugin = (MockPluginA) config.getPlugin(clazz, "DifferentID");
    assertEquals(1, plugin.order);
    
    plugin = (MockPluginA) config.getPlugin(clazz, null);
    assertEquals(2, plugin.order);
  }
  
  @Test
  public void initializeSinglesChainExceptionInMiddle() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("ThrowingException");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) {
      assertTrue(e.getCause() instanceof UnsupportedOperationException);
    }
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, times(1)).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), eq("ThrowingException"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), eq(null), 
        any(MockPluginBase.class));
    
    MockPluginA plugin = (MockPluginA) config.getPlugin(clazz, "MockTest");
    assertEquals(0, plugin.order);
  }
  
  @Test
  public void initializeSinglesChainExceptionInMiddleContinue() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("ThrowingException");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    config.setContinueOnError(true);
    
    assertNull(config.initialize(tsdb).join(1));
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, times(1)).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), eq("ThrowingException"), 
        any(MockPluginBase.class));
    verify(config, times(1)).registerPlugin(eq(clazz), eq(null), 
        any(MockPluginBase.class));
    
    MockPluginA plugin = (MockPluginA) config.getPlugin(clazz, "MockTest");
    assertEquals(0, plugin.order);
    
    plugin = (MockPluginA) config.getPlugin(clazz, null);
    assertEquals(2, plugin.order);
  }
  
  @Test
  public void initializeSingleWrongType() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseWrongType");
    configs.add(c);
    
    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, never()).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void initializeManyOfType() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    config.setConfigs(configs);
    
    assertNull(config.initialize(tsdb).join(1));
    
    final Class<?> clazz = MockPluginBaseClean.class;
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanC"), 
        any(MockPluginCleanC.class));
  }
  
  @Test
  public void initializeManyOfTypeExceptionInInitOrCtor() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) {
      assertTrue(e.getCause() instanceof UnsupportedOperationException);
    }
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginA"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginB"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginC"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void initializeManyOfTypeExceptionInInitOrCtorContinue() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    config.setContinueOnError(true);
    
    assertNull(config.initialize(tsdb).join(1));
    
    final Class<?> clazz = MockPluginBase.class;
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginA"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginB"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginC"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void initializeManyOfTypeChainWithException() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    

    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) {
      assertTrue(e.getCause() instanceof UnsupportedOperationException);
    }
    
    Class<?> clazz = MockPluginBaseClean.class;
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanC"), 
        any(MockPluginCleanC.class));
    
    clazz = MockPluginBase.class;
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginA"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginB"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginC"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void initializeManyOfTypeChainWithExceptionContinue() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    config.setContinueOnError(true);
    
    assertNull(config.initialize(tsdb).join(1));
    
    Class<?> clazz = MockPluginBaseClean.class;
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanC"), 
        any(MockPluginCleanC.class));
    
    clazz = MockPluginBase.class;
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginA"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.MockPluginBase.core.TestPluginsConfig.MockPluginB"), 
        any(BaseTSDBPlugin.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.MockPluginBase.core.TestPluginsConfig.MockPluginC"), 
        any(BaseTSDBPlugin.class));
  }
  
  @Test
  public void initializeMixChain() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockB");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    config.setConfigs(configs);
    
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) {
      assertTrue(e.getCause() instanceof UnsupportedOperationException);
    }
    
    Class<?> clazz = MockPluginBaseClean.class;
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanC"), 
        any(MockPluginCleanC.class));
    
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginA"), 
        any(MockPluginBaseClean.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginB"), 
        any(MockPluginBaseClean.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginC"), 
        any(MockPluginBaseClean.class));
    
    clazz = MockPluginBase.class;
    verify(config, times(1)).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginA.class));
    verify(config, never()).registerPlugin(eq(clazz), eq("MockB"), 
        any(MockPluginBase.class));
    verify(config, never()).registerPlugin(eq(clazz), eq(null), 
        any(MockPluginA.class));
    verify(config, never()).registerPlugin(eq(clazz), eq("MockC"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void initializeMixChainContinue() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockTest");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setId("MockB");
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    
    c = new PluginConfig();
    c.setDefault(true);
    c.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC");
    c.setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase");
    configs.add(c);
    

    config.setConfigs(configs);
    config.setContinueOnError(true);
    
    assertNull(config.initialize(tsdb).join(1));
    
    Class<?> clazz = MockPluginBaseClean.class;
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginCleanC"), 
        any(MockPluginCleanC.class));
    
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginA"), 
        any(MockPluginBaseClean.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginB"), 
        any(MockPluginBaseClean.class));
    verify(config, never()).registerPlugin(eq(clazz), 
        eq("net.opentsdb.core.TestPluginsConfig.MockPluginC"), 
        any(MockPluginBaseClean.class));
    
    clazz = MockPluginBase.class;
    verify(config, times(1)).registerPlugin(eq(clazz), eq("MockTest"), 
        any(MockPluginA.class));
    verify(config, never()).registerPlugin(eq(clazz), eq("MockB"), 
        any(MockPluginBase.class));
    verify(config, times(1)).registerPlugin(eq(clazz), eq(null), 
        any(MockPluginA.class));
    verify(config, never()).registerPlugin(eq(clazz), eq("MockC"), 
        any(MockPluginBase.class));
  }
  
  @Test
  public void shutdownEmpty() throws Exception {
    assertNull(config.shutdown().join());
  }
  
  @Test
  public void shutdownCleanReverse() throws Exception {
    config.instantiatedPlugins().add(new MockPluginA());
    config.instantiatedPlugins().add(new MockPluginCleanA());
    config.instantiatedPlugins().add(new MockPluginCleanB());
    
    assertNull(config.shutdown().join());
    assertEquals(2, ((MockPluginCleanB) config.instantiatedPlugins().get(2)).order);
    assertEquals(1, ((MockPluginCleanA) config.instantiatedPlugins().get(1)).order);
    assertEquals(0, ((MockPluginA) config.instantiatedPlugins().get(0)).order);
  }
  
  @Test
  public void shutdownClean() throws Exception {
    config.setShutdownReverse(false);
    config.instantiatedPlugins().add(new MockPluginA());
    config.instantiatedPlugins().add(new MockPluginCleanA());
    
    final MockPluginCleanB mock_b = new MockPluginCleanB();
    config.instantiatedPlugins().add(mock_b);
    config.registerPlugin(MockPluginBaseClean.class, "Sneaky", mock_b);
    
    final MockPluginCleanC registered = new MockPluginCleanC();
    config.registerPlugin(MockPluginBaseClean.class, "Sneaky2", registered);
    
    assertNull(config.shutdown().join());
    assertEquals(0, registered.order);
    assertEquals(1, ((MockPluginCleanB) config.instantiatedPlugins().get(2)).order);
    assertEquals(1, ((MockPluginCleanB) config.instantiatedPlugins().get(2)).shutdown_count);
    assertEquals(2, ((MockPluginCleanA) config.instantiatedPlugins().get(1)).order);
    assertEquals(3, ((MockPluginA) config.instantiatedPlugins().get(0)).order);
  }
  
  @Test
  public void shutdownExceptionReverse() throws Exception {
    config.instantiatedPlugins().add(new MockPluginA());
    config.instantiatedPlugins().add(new MockPluginB());
    config.instantiatedPlugins().add(new MockPluginCleanB());
    
    assertNull(config.shutdown().join());
    assertEquals(2, ((MockPluginCleanB) config.instantiatedPlugins().get(2)).order);
    assertEquals(1, ((MockPluginB) config.instantiatedPlugins().get(1)).order);
    assertEquals(0, ((MockPluginA) config.instantiatedPlugins().get(0)).order);
  }
  
  @Test
  public void shutdownException() throws Exception {
    config.setShutdownReverse(false);
    config.instantiatedPlugins().add(new MockPluginA());
    config.instantiatedPlugins().add(new MockPluginB());
    config.instantiatedPlugins().add(new MockPluginCleanB());
    
    assertNull(config.shutdown().join());
    assertEquals(0, ((MockPluginCleanB) config.instantiatedPlugins().get(2)).order);
    assertEquals(1, ((MockPluginB) config.instantiatedPlugins().get(1)).order);
    assertEquals(2, ((MockPluginA) config.instantiatedPlugins().get(0)).order);
  }
  
  /** -------- MOCKS ---------- **/
  
  public static abstract class MockPluginBase extends BaseTSDBPlugin { }
  
  public static class MockPluginA extends MockPluginBase {
    public int order = -1;
    
    @Override
    public String id() {
      return "MockA";
    }

    @Override
    public String version() {
      return "1";
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb) {
      order = ORDER++;
      return Deferred.fromResult(null);
    }
    
    @Override
    public Deferred<Object> shutdown() {
      order = ORDER++;
      return Deferred.fromResult(null);
    }
  }
  
  public static class MockPluginB extends MockPluginBase {
    public int order = -1;
    
    @Override
    public String id() {
      return "MockB";
    }

    @Override
    public String version() {
      return "1";
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb) {
      order = ORDER++;
      return Deferred.fromError(new UnsupportedOperationException("Boo!"));
    }
    
    @Override
    public Deferred<Object> shutdown() {
      order = ORDER++;
      return Deferred.fromError(new UnsupportedOperationException("Boo!"));
    }
  }

  public static class MockPluginC extends MockPluginBase {
    public int order = -1;
    
    public MockPluginC() {
      throw new UnsupportedOperationException("Boo!");
    }
    
    @Override
    public String id() {
      return "MockC";
    }

    @Override
    public String version() {
      return "1";
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb) {
      order = ORDER++;
      return Deferred.fromResult(null);
    }
  }
  
  public static abstract class MockPluginBaseClean extends BaseTSDBPlugin { }
  
  public static class MockPluginCleanA extends MockPluginBaseClean {
    public int order = -1;
    
    @Override
    public String id() {
      return "MockPluginCleanA";
    }

    @Override
    public String version() {
      return "1";
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb) {
      order = ORDER++;
      return Deferred.fromResult(null);
    }
  
    @Override
    public Deferred<Object> shutdown() {
      order = ORDER++;
      return Deferred.fromResult(null);
    }
  }
  
  public static class MockPluginCleanB extends MockPluginBaseClean {
    public int order = -1;
    public int shutdown_count = 0;
    
    @Override
    public String id() {
      return "MockPluginCleanB";
    }

    @Override
    public String version() {
      return "1";
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb) {
      order = ORDER++;
      return Deferred.fromResult(null);
    }
  
    @Override
    public Deferred<Object> shutdown() {
      order = ORDER++;
      ++shutdown_count;
      return Deferred.fromResult(null);
    }
  }
  
  public static class MockPluginCleanC extends MockPluginBaseClean {
    public int order = -1;
    
    @Override
    public String id() {
      return "MockPluginCleanC";
    }

    @Override
    public String version() {
      return "1";
    }
    
    @Override
    public Deferred<Object> initialize(final TSDB tsdb) {
      order = ORDER++;
      return Deferred.fromResult(null);
    }
  
    @Override
    public Deferred<Object> shutdown() {
      order = ORDER++;
      return Deferred.fromResult(null);
    }
  }

  public static abstract class MockPluginBaseWrongType extends BaseTSDBPlugin { }
}
