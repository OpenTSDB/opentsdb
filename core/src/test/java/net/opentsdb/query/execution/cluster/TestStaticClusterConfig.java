//// This file is part of OpenTSDB.
//// Copyright (C) 2017  The OpenTSDB Authors.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package net.opentsdb.query.execution.cluster;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotSame;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertSame;
//import static org.junit.Assert.fail;
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.doAnswer;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import java.util.List;
//
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.invocation.InvocationOnMock;
//import org.mockito.stubbing.Answer;
//
//import net.opentsdb.query.context.QueryContext;
//import net.opentsdb.query.execution.QueryExecutorConfig;
//import net.opentsdb.query.execution.TimedQueryExecutor;
//import net.opentsdb.query.execution.cluster.StaticClusterConfig.Config;
//
//public class TestStaticClusterConfig {
//
//  private QueryContext context;
//  private Config config;
//  private QueryExecutorConfig caught;
//  
//  @Before
//  public void before() throws Exception {
//    context = mock(QueryContext.class);
//    config = (Config) Config.newBuilder()
//        .setId("MyPlugin")
//        .setImplementation("StaticClusterConfig")
//        .addCluster(ClusterDescriptor.newBuilder()
//          .setCluster("Primary")
//          .setDescription("Most popular")
//          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//              .setTimeout(60000)
//              .setExecutorId("Primary_Timer")
//              .setExecutorType("TimedQueryExecutor")))
//        .addOverride(ClusterOverride.newBuilder()
//          .setId("ShorterTimeout")
//          .addCluster(ClusterDescriptor.newBuilder()
//            .setCluster("Primary")
//            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//                .setTimeout(30000)
//                .setExecutorId("Primary_Timer")
//                .setExecutorType("TimedQueryExecutor"))))
//        .addOverride(ClusterOverride.newBuilder()
//            .setId("JustPrimary")
//            .addCluster(ClusterDescriptor.newBuilder()
//              .setCluster("Primary")))
//        .build();
//    doAnswer(new Answer<Void>() {
//      @Override
//      public Void answer(InvocationOnMock invocation) throws Throwable {
//        caught = (QueryExecutorConfig) invocation.getArguments()[0];
//        return null;
//      }
//    }).when(context).addConfigOverride(any(QueryExecutorConfig.class));
//  }
//  
//  @Test
//  public void builder() throws Exception {
//    // other tests in TestClusterConfigPlugin class.
//    Config clone = Config.newBuilder(config).build();
//    assertNotSame(clone, config);
//    assertNotSame(clone.clusters, config.clusters);
//    assertNotSame(clone.clusters.get(0), config.clusters.get(0));
//    assertNotSame(clone.overrides, config.overrides);
//    assertNotSame(clone.overrides.get(0), config.overrides.get(0));
//    assertEquals("MyPlugin", clone.getId());
//    assertEquals("StaticClusterConfig", clone.implementation());
//    assertEquals(1, clone.getClusters().size());
//    assertEquals("Primary", clone.getClusters().get(0).getCluster());
//    assertEquals(2, clone.getOverrides().size());
//    assertEquals("JustPrimary", clone.getOverrides().get(0).getId());
//    assertEquals("ShorterTimeout", clone.getOverrides().get(1).getId());
//  }
//
//  @Test
//  public void setConfig() throws Exception {
//    final StaticClusterConfig implementation = new StaticClusterConfig();
//    implementation.setConfig(config);
//    
//    assertEquals(1, implementation.clusters().size());
//    assertEquals("Primary", implementation.clusters().get("Primary")
//        .getCluster());
//    assertEquals("Most popular", implementation.clusters().get("Primary")
//        .getDescription());
//    assertEquals(2, implementation.overrides().size());
//    assertEquals("ShorterTimeout", implementation.overrides()
//        .get("ShorterTimeout").getId());
//    assertEquals("JustPrimary", implementation.overrides()
//        .get("JustPrimary").getId());
//    
//    try {
//      implementation.setConfig(config);
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException e) { }
//    
//    try {
//      implementation.setConfig(null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//
//  @Test
//  public void setupQueryDefault() throws Exception {
//    final StaticClusterConfig implementation = new StaticClusterConfig();
//    implementation.setConfig(config);
//    
//    final List<String> clusters = implementation.setupQuery(context);
//    assertEquals(1, clusters.size());
//    assertEquals("Primary", clusters.get(0));
//    assertSame(config.getClusters().get(0).getExecutorConfigs().get(0), caught);
//  }
//  
//  @Test
//  public void setupQueryDefaultContextHasOverride() throws Exception {
//    when(context.getConfigOverride("Primary_Timer"))
//      .thenReturn(mock(QueryExecutorConfig.class));
//    final StaticClusterConfig implementation = new StaticClusterConfig();
//    implementation.setConfig(config);
//    
//    final List<String> clusters = implementation.setupQuery(context);
//    assertEquals(1, clusters.size());
//    assertEquals("Primary", clusters.get(0));
//    verify(context, never()).addConfigOverride(any(QueryExecutorConfig.class));
//    assertNull(caught);
//  }
//  
//  @Test
//  public void setupQueryOverride() throws Exception {
//    final StaticClusterConfig implementation = new StaticClusterConfig();
//    implementation.setConfig(config);
//    
//    final List<String> clusters = implementation.setupQuery(context, 
//        "ShorterTimeout");
//    assertEquals(1, clusters.size());
//    assertEquals("Primary", clusters.get(0));
//    assertSame(config.getOverrides().get(1).getClusters()
//        .get(0).getExecutorConfigs().get(0), caught);
//  }
//  
//  @Test
//  public void setupQueryOverrideNoExecutorConfig() throws Exception {
//    final StaticClusterConfig implementation = new StaticClusterConfig();
//    implementation.setConfig(config);
//    
//    final List<String> clusters = implementation.setupQuery(context, 
//        "JustPrimary");
//    assertEquals(1, clusters.size());
//    assertEquals("Primary", clusters.get(0));
//    assertEquals("Primary_Timer", caught.getExecutorId());
//  }
//  
//  @Test
//  public void setupQueryOverrideNotConfigured() throws Exception {
//    final StaticClusterConfig implementation = new StaticClusterConfig();
//    implementation.setConfig(config);
//    
//    try {
//      implementation.setupQuery(context, "NoSuchOverride");
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    assertNull(caught);
//    verify(context, never()).addConfigOverride(any(QueryExecutorConfig.class));
//  }
//  
//  @Test
//  public void setupQueryOverrideContextHasOverride() throws Exception {
//    when(context.getConfigOverride("Primary_Timer"))
//      .thenReturn(mock(QueryExecutorConfig.class));
//    final StaticClusterConfig implementation = new StaticClusterConfig();
//    implementation.setConfig(config);
//    
//    final List<String> clusters = implementation.setupQuery(context, 
//        "ShorterTimeout");
//    assertEquals(1, clusters.size());
//    assertEquals("Primary", clusters.get(0));
//    verify(context, never()).addConfigOverride(any(QueryExecutorConfig.class));
//    assertNull(caught);
//  }
//}
