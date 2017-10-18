// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.TimedQueryExecutor;
import net.opentsdb.query.execution.cluster.TestClusterConfigPlugin.UTPlugin.Config;
import net.opentsdb.utils.JSON;

public class TestClusterConfigPlugin {

  private DefaultTSDB tsdb;
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(DefaultTSDB.class);
    context = mock(QueryContext.class);
  }
  
  @Test
  public void defaultPluginMethods() throws Exception {
    final UTPlugin plugin = new UTPlugin();
    assertNull(plugin.initialize(tsdb).join());
    assertEquals("UTPlugin", plugin.id());
    assertEquals("3.0.0", plugin.version());
    assertNull(plugin.shutdown().join());
    
    final Config config = (Config) new Config.Builder()
        .setId("UTPlugin")
        .setImplementation("UTPlugin")
        .addCluster(mock(ClusterDescriptor.class))
        .build();
    plugin.setConfig(config);
    assertSame(config, plugin.config);
    
    plugin.setupQuery(context);
    assertSame(context, plugin.context);
    assertNull(plugin.override);
    
    plugin.setupQuery(context, "Override");
    assertSame(context, plugin.context);
    assertEquals("Override", plugin.override);
  }
  
  @Test
  public void builder() throws Exception {
    ClusterConfigPlugin.Config config = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addOverride(ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build())))
        .build();
    
    assertEquals("MyPlugin", config.getId());
    assertEquals("StaticClusterConfig", config.implementation());
    assertEquals(1, config.getClusters().size());
    assertEquals("Primary", config.getClusters().get(0).getCluster());
    assertEquals(1, config.getOverrides().size());
    assertEquals("ShorterTimeout", config.getOverrides().get(0).getId());
    
    String json = "{\"implementation\":\"StaticClusterConfig\","
        + "\"id\":\"MyPlugin\","
        + "\"clusters\":[{\"cluster\":\"Primary\",\"description\":"
        + "\"Most popular\",\"executorConfigs\":[{\"executorType\":"
        + "\"TimedQueryExecutor\",\"timeout\":60000,\"executorId\":"
        + "\"Primary_Timer\"}]}],\"overrides\":[{\"id\":\"ShorterTimeout\","
        + "\"clusters\":[{\"cluster\":\"Primary\",\"executorConfigs\":"
        + "[{\"executorType\":\"TimedQueryExecutor\",\"timeout\":30000,"
        + "\"executorId\":\"Primary_Timer\"}]}]}]}";
    
    StaticClusterConfig.Config static_config = 
        (StaticClusterConfig.Config) JSON.parseToObject(json, 
        ClusterConfigPlugin.Config.class);
    assertEquals("MyPlugin", static_config.getId());
    assertEquals("StaticClusterConfig", static_config.implementation());
    assertEquals(1, static_config.getClusters().size());
    assertEquals("Primary", static_config.getClusters().get(0).getCluster());
    assertEquals(1, static_config.getOverrides().size());
    assertEquals("ShorterTimeout", static_config.getOverrides().get(0).getId());

    json = JSON.serializeToString(static_config);
    assertTrue(json.contains("\"implementation\":\"StaticClusterConfig\""));
    assertTrue(json.contains("\"id\":\"MyPlugin\""));
    assertTrue(json.contains("\"clusters\":["));
    assertTrue(json.contains("\"cluster\":\"Primary\""));
    assertTrue(json.contains("\"overrides\":["));
    assertTrue(json.contains("\"id\":\"ShorterTimeout\""));
    
    json = "{\"implementation\":\"UTPlugin\"}";
    try {
      JSON.parseToObject(json, ClusterConfigPlugin.Config.class);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    try {
      UTPlugin.Config.newBuilder()
        //.setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      UTPlugin.Config.newBuilder()
        .setId("")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        //.setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        //.addCluster(ClusterDescriptor.newBuilder()
        //  .setCluster("Primary")
        //  .setDescription("Most popular")
        //  .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
        //      .setTimeout(60000)
        //      .setExecutorId("Primary_Timer")
        //      .setExecutorType("TimedQueryExecutor")
        //      .build()))
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .setClusters(Collections.<ClusterDescriptor>emptyList())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final ClusterConfigPlugin.Config c1 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    
    ClusterConfigPlugin.Config c2 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = UTPlugin.Config.newBuilder()
        .setId("WhatPlugin?")  // <-- Diff
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("SomeOtherImp")  // <-- Diff
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary2")  // <-- Diff
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        //.addCluster(ClusterDescriptor.newBuilder()  // <-- Diff
        //  .setCluster("Primary")
        //  .setDescription("Most popular")
        //  .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
        //      .setTimeout(60000)
        //      .setExecutorId("Primary_Timer")
        //      .setExecutorType("TimedQueryExecutor")
        //      .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        // <- Diff order is OK
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .setDescription("Most popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout")
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addOverride(ClusterOverride.newBuilder()
          .setId("ShorterTimeout2")  // <-- Diff
          .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Primary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Primary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        //.addOverride(ClusterOverride.newBuilder()  // <-- Diff
        //  .setId("ShorterTimeout")
        //  .addCluster(ClusterDescriptor.newBuilder()
        //    .setCluster("Primary")
        //    .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
        //        .setTimeout(30000)
        //        .setExecutorId("Primary_Timer")
        //        .setExecutorType("TimedQueryExecutor")
        //        .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = UTPlugin.Config.newBuilder()
        .setId("MyPlugin")
        .setImplementation("StaticClusterConfig")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .setDescription("Most popular")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(60000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .setDescription("Least popular")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(60000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        // <-- Diff order is OK
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterSecondaryTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Secondary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Secondary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .addOverride(ClusterOverride.newBuilder()
            .setId("ShorterTimeout")
            .addCluster(ClusterDescriptor.newBuilder()
              .setCluster("Primary")
              .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                  .setTimeout(30000)
                  .setExecutorId("Primary_Timer")
                  .setExecutorType("TimedQueryExecutor")
                  .build())))
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
  }
  
  @JsonInclude(Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class UTPlugin extends ClusterConfigPlugin {
    ClusterConfigPlugin.Config config;
    QueryContext context;
    String override;
    
    @Override
    public void setConfig(final ClusterConfigPlugin.Config config) {
      this.config = config;
    }

    @Override
    public Map<String, ClusterDescriptor> clusters() {
      return null;
    }

    @Override
    public List<String> setupQuery(final QueryContext context, 
        final String override) {
      this.context = context;
      this.override = override;
      return null;
    }

    @Override
    public String id() {
      return "UTPlugin";
    }

    @Override
    public String version() {
      return "3.0.0";
    }
    
    @JsonInclude(Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonDeserialize(builder = Config.Builder.class)
    public static class Config extends ClusterConfigPlugin.Config {
      protected Config(final Builder builder) {
        super(builder);
      }
      
      /** @return A builder to work with. */
      public static Builder newBuilder() {
        return new Builder();
      }
      
      /**
       * Clones the given config. 
       * @param config A non-null config to clone.
       * @return The cloned config as a builder.
       */
      public static Builder newBuilder(final Config config) {
        final ClusterConfigPlugin.Config.Builder builder = new Builder()
            .setId(config.id);
        if (config.clusters != null) {
          for (final ClusterDescriptor cluster : builder.clusters) {
            builder.addCluster(ClusterDescriptor.newBuilder(cluster));
          }
        }
        if (config.overrides != null) {
          for (final ClusterOverride override : builder.overrides) {
            builder.addOverride(ClusterOverride.newBuilder(override));
          }
        }
        return (Builder) builder;
      }
      
      public static class Builder extends ClusterConfigPlugin.Config.Builder {
        @Override
        public Config build() {
          return new Config(this);
        }
      }
    }
    
  }
}
