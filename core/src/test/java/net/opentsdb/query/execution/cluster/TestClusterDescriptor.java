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
//import static org.junit.Assert.assertNotEquals;
//import static org.junit.Assert.assertNotSame;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//
//import org.junit.Test;
//
//import net.opentsdb.query.execution.TimedQueryExecutor;
//import net.opentsdb.utils.JSON;
//
//public class TestClusterDescriptor {
//
//  @Test
//  public void builder() throws Exception {
//    ClusterDescriptor cluster = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .setDescription("Most popular")
//        .build();
//    assertEquals("Primary", cluster.getCluster());
//    assertEquals("Most popular", cluster.getDescription());
//    assertTrue(cluster.getExecutorConfigs().isEmpty());
//    
//    cluster = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .setDescription("Most popular")
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(60000)
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(30000)
//            .setExecutorId("Primary_NotherEx")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .build();
//    
//    assertEquals("Primary", cluster.getCluster());
//    assertEquals("Most popular", cluster.getDescription());
//    assertEquals(2, cluster.getExecutorConfigs().size());
//    assertEquals("Primary_NotherEx", 
//        cluster.getExecutorConfigs().get(0).getExecutorId());
//    assertEquals("Primary_Timer", 
//        cluster.getExecutorConfigs().get(1).getExecutorId());
//    
//    String json = "{\"cluster\":\"Primary\",\"description\":\"Most popular\","
//        + "\"executorConfigs\":[{\"executorType\":\"TimedQueryExecutor\","
//        + "\"timeout\":30000,\"executorId\":\"Primary_NotherEx\"},"
//        + "{\"executorType\":\"TimedQueryExecutor\",\"timeout\":60000,"
//        + "\"executorId\":\"Primary_Timer\"}]}";
//    cluster = JSON.parseToObject(json, ClusterDescriptor.class);
//    assertEquals("Primary", cluster.getCluster());
//    assertEquals("Most popular", cluster.getDescription());
//    assertEquals(2, cluster.getExecutorConfigs().size());
//    assertEquals("Primary_NotherEx", 
//        cluster.getExecutorConfigs().get(0).getExecutorId());
//    assertEquals("Primary_Timer", 
//        cluster.getExecutorConfigs().get(1).getExecutorId());
//    
//    json = JSON.serializeToString(cluster);
//    assertTrue(json.contains("\"cluster\":\"Primary\""));
//    assertTrue(json.contains("\"description\":\"Most popular\""));
//    assertTrue(json.contains("\"executorConfigs\":["));
//    assertTrue(json.contains("\"executorType\":\"TimedQueryExecutor\""));
//    assertTrue(json.contains("\"executorId\":\"Primary_NotherEx\""));
//    assertTrue(json.contains("\"executorId\":\"Primary_Timer\""));
//    
//    ClusterDescriptor clone = ClusterDescriptor.newBuilder(cluster).build();
//    assertNotSame(clone, cluster);
//    assertNotSame(clone.executor_configs, cluster.executor_configs);
//    assertEquals("Primary", clone.getCluster());
//    assertEquals("Most popular", clone.getDescription());
//    assertEquals(2, clone.getExecutorConfigs().size());
//    assertEquals("Primary_NotherEx", 
//        clone.getExecutorConfigs().get(0).getExecutorId());
//    assertEquals("Primary_Timer", 
//        clone.getExecutorConfigs().get(1).getExecutorId());
//    
//    // minimum
//    cluster = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(60000)
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .build();
//    assertEquals("Primary", cluster.getCluster());
//    assertNull(cluster.getDescription());
//    assertEquals(1, cluster.getExecutorConfigs().size());
//    assertEquals("Primary_Timer", 
//        cluster.getExecutorConfigs().get(0).getExecutorId());
//    
//    try {
//      ClusterDescriptor.newBuilder()
//          //.setCluster("Primary")
//          .setDescription("Most popular")
//          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//              .setTimeout(60000)
//              .setExecutorId("Primary_Timer")
//              .setExecutorType("TimedQueryExecutor")
//              .build())
//          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//              .setTimeout(30000)
//              .setExecutorId("Primary_NotherEx")
//              .setExecutorType("TimedQueryExecutor")
//              .build())
//          .build();
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      ClusterDescriptor.newBuilder()
//          .setCluster("")
//          .setDescription("Most popular")
//          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//              .setTimeout(60000)
//              .setExecutorId("Primary_Timer")
//              .setExecutorType("TimedQueryExecutor")
//              .build())
//          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//              .setTimeout(30000)
//              .setExecutorId("Primary_NotherEx")
//              .setExecutorType("TimedQueryExecutor")
//              .build())
//          .build();
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//  }
//
//  @Test
//  public void hashCodeEqualsCompareTo() throws Exception {
//    final ClusterDescriptor c1 = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .setDescription("Most popular")
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(60000)
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(30000)
//            .setExecutorId("Primary_NotherEx")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .build();
//    
//    ClusterDescriptor c2 = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .setDescription("Most popular")
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(60000)
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(30000)
//            .setExecutorId("Primary_NotherEx")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .build();
//    assertEquals(c1.hashCode(), c2.hashCode());
//    assertEquals(c1, c2);
//    assertEquals(0, c1.compareTo(c2));
//    
//    c2 = ClusterDescriptor.newBuilder()
//        .setCluster("Secondary")  // <-- Diff
//        .setDescription("Most popular")
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(60000)
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(30000)
//            .setExecutorId("Primary_NotherEx")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .setDescription("Least popular")  // <-- Diff
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(60000)
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(30000)
//            .setExecutorId("Primary_NotherEx")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .setDescription("Most popular") 
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(15000) // <-- Diff
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(30000)
//            .setExecutorId("Primary_NotherEx")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(1, c1.compareTo(c2));
//    
//    c2 = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .setDescription("Most popular") 
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(60000)
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        //.addExecutorConfig(TimedQueryExecutor.Config.newBuilder() // <-- Diff
//        //    .setTimeout(30000)
//        //    .setExecutorId("Primary_NotherEx")
//        //    .setExecutorType("TimedQueryExecutor")
//        //    .build())
//        .build();
//    assertNotEquals(c1.hashCode(), c2.hashCode());
//    assertNotEquals(c1, c2);
//    assertEquals(-1, c1.compareTo(c2));
//    
//    c2 = ClusterDescriptor.newBuilder()
//        .setCluster("Primary")
//        .setDescription("Most popular") 
//        // Diff order but should be ok.
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(30000)
//            .setExecutorId("Primary_NotherEx")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
//            .setTimeout(60000)
//            .setExecutorId("Primary_Timer")
//            .setExecutorType("TimedQueryExecutor")
//            .build())
//        .build();
//    assertEquals(c1.hashCode(), c2.hashCode());
//    assertEquals(c1, c2);
//    assertEquals(0, c1.compareTo(c2));
//  }
//}
