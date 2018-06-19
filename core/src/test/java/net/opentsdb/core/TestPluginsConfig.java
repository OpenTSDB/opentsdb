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
import static org.junit.Assert.assertNotEquals;
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

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.PluginsConfig.PluginConfig;
import net.opentsdb.exceptions.PluginLoadException;
import net.opentsdb.query.execution.cache.GuavaLRUCache;
import net.opentsdb.query.execution.cache.QueryCachePlugin;
import net.opentsdb.query.execution.cluster.ClusterConfigPlugin;
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
  private Configuration tsd_config;
  private PluginsConfig config;
  
  @Before
  public void before() throws Exception {
    ORDER = 0;
    tsdb = mock(TSDB.class);
    tsd_config = UnitTestConfiguration.getConfiguration();
    when(tsdb.getConfig()).thenReturn(tsd_config);
    config = spy(new PluginsConfig());
  }
  
  @Test
  public void serdes() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
      .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
      .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockB")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config = new PluginsConfig();
    config.setPluginLocations(Lists.newArrayList("nosuchpath", "nosuch.jar"));
    config.setConfigs(configs);
    
    String json = JSON.serializeToString(config);
    System.out.println("JSON: " + json);
    assertTrue(json.contains("\"configs\":[{"));
    assertTrue(json.contains("\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean\""));
    assertTrue(json.contains("\"plugin\":\"net.opentsdb.core.TestPluginsConfig$MockPluginA\""));
    assertTrue(json.contains("\"id\":\"MockTest\""));
    assertTrue(json.contains("\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\""));
    assertTrue(json.contains("\"id\":\"MockB\""));
    assertTrue(json.contains("\"continueOnError\":false"));
    assertTrue(json.contains("\"shutdownReverse\":true"));
    assertTrue(json.contains("\"isDefault\":true"));
    assertTrue(json.contains("\"pluginLocations\":["));
    assertTrue(json.contains("\"nosuch.jar"));
    assertTrue(json.contains("\"loadDefaultTypes\":true"));
    assertTrue(json.contains("\"loadDefaultInstances\":true"));
    
    json = "{\"configs\":[{\"comment\":\"ignored\",\"type\":\""
        + "net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean\"},{\"plugin\":"
        + "\"net.opentsdb.core.TestPluginsConfig$MockPluginA\",\"id\":"
        + "\"MockTest\",\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\"},"
        + "{\"plugin\":\"net.opentsdb.core.TestPluginsConfig$MockPluginB\","
        + "\"id\":\"MockB\",\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\"}"
        + ",{\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\"},"
        + "{\"plugin\":\"net.opentsdb.core.TestPluginsConfig$MockPluginA\","
        + "\"type\":\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\","
        + "\"isDefault\":true},{\"plugin\":"
        + "\"net.opentsdb.core.TestPluginsConfig$MockPluginC\",\"type\":"
        + "\"net.opentsdb.core.TestPluginsConfig$MockPluginBase\",\"default\":true}],"
        + "\"pluginLocations\":[\"/tmp\"],"
        + "\"continueOnError\":false,\"shutdownReverse\":true,"
        + "\"loadDefaultTypes\":false,\"loadDefaultInstances\":false}";
    
    config = JSON.parseToObject(json, PluginsConfig.class);
    
    assertEquals(6, config.getConfigs().size());
    assertFalse(config.getContinueOnError());
    assertTrue(config.getShutdownReverse());
    assertTrue(config.getConfigs().get(4).getIsDefault());
    assertEquals(1, config.getPluginLocations().size());
    assertEquals("/tmp", config.getPluginLocations().get(0));
    assertFalse(config.getLoadDefaultInstances());
    assertFalse(config.getLoadDefaultTypes());
  }

  @Test
  public void validateOK() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockB")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("GonnaThrow")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateDuplicateID() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest") // <-- BAD
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("GonnaThrow")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test
  public void validateDuplicateIDDiffType() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginCleanA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")  // <-- OK
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("GonnaThrow")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateSpecificNullID() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        //.setId("MockTest") // <-- BAD
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("GonnaThrow")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateSpecificEmptyID() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("") // <-- BAD
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("GonnaThrow")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateDefaultHasId() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockB")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setId("NotAllowed") // <-- BAD
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("GonnaThrow")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    
    config.validate();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void validateMultipleDefaults() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockB")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
  public void initializeNoDefaultImplementationsFound() throws Exception {
    try {
      config.initialize(tsdb).join(1);
      fail("Expected PluginLoadException");
    } catch (PluginLoadException e) { }
  }
  
  @Test
  public void initializeNullList() throws Exception {
    config.setLoadDefaultInstances(false);
    assertNull(config.initialize(tsdb).join(1));
    assertEquals(PluginsConfig.DEFAULT_TYPES.size(), config.configs.size());
  }
  
  @Test
  public void initializeNullListNoDefaults() throws Exception {
    config.setLoadDefaultTypes(false);
    config.setLoadDefaultInstances(false);
    assertNull(config.initialize(tsdb).join(1));
    assertNull(config.configs);
  }
  
  @Test
  public void initializeEmptyList() throws Exception {
    config.setConfigs(Lists.<PluginConfig>newArrayList());
    config.setLoadDefaultInstances(false);
    assertNull(config.initialize(tsdb).join(1));
    assertEquals(PluginsConfig.DEFAULT_TYPES.size(), config.configs.size());
  }
  
  @Test
  public void initializeEmptyListNoDefaults() throws Exception {
    config.setConfigs(Lists.<PluginConfig>newArrayList());
    config.setLoadDefaultTypes(false);
    config.setLoadDefaultInstances(false);
    assertNull(config.initialize(tsdb).join(1));
    assertTrue(config.configs.isEmpty());
  }
  
  @Test
  public void initializeSingleWithId() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setLoadDefaultInstances(false);
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
    PluginConfig c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    config.setLoadDefaultInstances(false);
    
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
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginD")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$NoSuchMockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("DifferentID")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setLoadDefaultInstances(false);
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
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("ThrowingException")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("ThrowingException")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
       .setId("MockTest")
       .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
       .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseWrongType")
       .build();
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
  public void initializeDupeTypesWithDefault() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.query.interpolation.QueryInterpolatorFactory")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.uid.UniqueIdFactory")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    config.setLoadDefaultInstances(false);
    
    assertNull(config.initialize(tsdb).join(1));
    assertEquals(PluginsConfig.DEFAULT_TYPES.size(), config.configs.size());
  }
  
  @Test
  public void initializeManyOfType() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    config.setLoadDefaultInstances(false);
    config.setConfigs(configs);
    
    assertNull(config.initialize(tsdb).join(1));
    
    final Class<?> clazz = MockPluginBaseClean.class;
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanC"), 
        any(MockPluginCleanC.class));
  }
  
  @Test
  public void initializeManyOfTypeExceptionInInitOrCtor() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
        eq("MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanC"), 
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
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    config.setContinueOnError(true);
    
    assertNull(config.initialize(tsdb).join(1));
    
    Class<?> clazz = MockPluginBaseClean.class;
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanC"), 
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
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockB")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
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
        eq("MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanC"), 
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
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockTest")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setId("MockB")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginB")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginA")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    c = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginC")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBase")
        .build();
    configs.add(c);
    
    config.setConfigs(configs);
    config.setContinueOnError(true);
    
    assertNull(config.initialize(tsdb).join(1));
    
    Class<?> clazz = MockPluginBaseClean.class;
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanA"), 
        any(MockPluginCleanA.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanB"), 
        any(MockPluginCleanB.class));
    verify(config, times(1)).registerPlugin(eq(clazz), 
        eq("MockPluginCleanC"), 
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
  public void initializeDupeWithDefaults() throws Exception {
    final List<PluginConfig> configs = Lists.newArrayList();
    PluginConfig c = PluginConfig.newBuilder()
        .setType("net.opentsdb.stats.StatsCollector")
        .setId("BlackholeStatsCollector")
        .setPlugin("net.opentsdb.stats.BlackholeStatsCollector")
        .build();
    configs.add(c);
    
    config.setLoadDefaultInstances(false);
    config.setConfigs(configs);
    
    assertNull(config.initialize(tsdb).join(1));
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
  
  @Test
  public void pluginConfigEquals() throws Exception {
    PluginConfig c1 = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setId("MyPlugin")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginBaseCleanA")
        .build();
    PluginConfig c2 = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setId("MyPlugin")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginBaseCleanA")
        .build();
    
    assertEquals(c1, c2);
    assertFalse(c1.equals(null));
    
    c2 = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setId("MyPlugin2") // diff
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginBaseCleanA")
        .build();
    assertNotEquals(c1, c2);
    
    c2 = PluginConfig.newBuilder()
        .setIsDefault(true)
        //.setId("MyPlugin") // diff
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginBaseCleanA")
        .build();
    assertNotEquals(c1, c2);
    
    c2 = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setId("MyPlugin")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseCleanA") // diff
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginBaseCleanA")
        .build();
    assertNotEquals(c1, c2);
    
    c2 = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setId("MyPlugin")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        .setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginBaseCleanB") // diff
        .build();
    assertNotEquals(c1, c2);
    
    c2 = PluginConfig.newBuilder()
        .setIsDefault(true)
        .setId("MyPlugin")
        .setType("net.opentsdb.core.TestPluginsConfig$MockPluginBaseClean")
        //.setPlugin("net.opentsdb.core.TestPluginsConfig$MockPluginBaseCleanA") // diff
        .build();
    assertNotEquals(c1, c2);
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
