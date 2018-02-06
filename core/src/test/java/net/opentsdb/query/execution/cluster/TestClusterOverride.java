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
package net.opentsdb.query.execution.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;

import org.junit.Test;

import net.opentsdb.query.execution.TimedQueryExecutor;
import net.opentsdb.utils.JSON;

public class TestClusterOverride {

  @Test
  public void builder() throws Exception {
    ClusterOverride override = ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .build();
    assertEquals("ShorterTimeout", override.getId());
    assertEquals(2, override.getClusters().size());
    assertEquals("Primary", override.getClusters().get(0).getCluster());
    assertEquals("Primary_Timer", override.getClusters().get(0)
        .getExecutorConfigs().get(0).getExecutorId());
    assertEquals("Secondary", override.getClusters().get(1).getCluster());
    assertEquals("Secondary_Timer", override.getClusters().get(1)
        .getExecutorConfigs().get(0).getExecutorId());
    
    String json = "{\"id\":\"ShorterTimeout\",\"clusters\":[{\"cluster\":"
        + "\"Primary\",\"executorConfigs\":[{\"executorType\":"
        + "\"TimedQueryExecutor\",\"timeout\":30000,\"executorId\":"
        + "\"Primary_Timer\"}]},{\"cluster\":\"Secondary\",\"executorConfigs\":"
        + "[{\"executorType\":\"TimedQueryExecutor\",\"timeout\":30000,"
        + "\"executorId\":\"Secondary_Timer\"}]}]}";
    override = JSON.parseToObject(json, ClusterOverride.class);
    assertEquals("ShorterTimeout", override.getId());
    assertEquals(2, override.getClusters().size());
    assertEquals("Primary", override.getClusters().get(0).getCluster());
    assertEquals("Primary_Timer", override.getClusters().get(0)
        .getExecutorConfigs().get(0).getExecutorId());
    assertEquals("Secondary", override.getClusters().get(1).getCluster());
    assertEquals("Secondary_Timer", override.getClusters().get(1)
        .getExecutorConfigs().get(0).getExecutorId());
    
    json = JSON.serializeToString(override);
    assertTrue(json.contains("\"id\":\"ShorterTimeout\""));
    assertTrue(json.contains("\"cluster\":\"Primary\""));
    assertTrue(json.contains("\"executorId\":\"Primary_Timer\""));
    assertTrue(json.contains("\"cluster\":\"Secondary\""));
    assertTrue(json.contains("\"executorId\":\"Secondary_Timer\""));
    
    ClusterOverride clone = ClusterOverride.newBuilder(override).build();
    assertNotSame(clone, override);
    assertNotSame(clone.clusters, override.clusters);
    assertNotSame(clone.getClusters().get(0), override.getClusters().get(0));
    assertNotSame(clone.getClusters().get(1), override.getClusters().get(1));
    assertEquals("ShorterTimeout", clone.getId());
    assertEquals(2, clone.getClusters().size());
    assertEquals("Primary", clone.getClusters().get(0).getCluster());
    assertEquals("Primary_Timer", clone.getClusters().get(0)
        .getExecutorConfigs().get(0).getExecutorId());
    assertEquals("Secondary", clone.getClusters().get(1).getCluster());
    assertEquals("Secondary_Timer", clone.getClusters().get(1)
        .getExecutorConfigs().get(0).getExecutorId());
    
    try {
      ClusterOverride.newBuilder()
        //.setId("ShorterTimeout")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ClusterOverride.newBuilder()
        .setId("")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        .setClusters(Collections.<ClusterDescriptor>emptyList())
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final ClusterOverride o1 = ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .build();
    
    ClusterOverride o2 = ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .build();
    assertEquals(o1.hashCode(), o2.hashCode());
    assertEquals(o1, o2);
    assertEquals(0, o1.compareTo(o2));
    
    o2 = ClusterOverride.newBuilder()
        .setId("Timeout")  // <-- Diff
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .build();
    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertNotEquals(o1, o2);
    assertEquals(-1, o1.compareTo(o2));
    
    o2 = ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("P2")  // <-- Diff
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .build();
    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertNotEquals(o1, o2);
    assertEquals(1, o1.compareTo(o2));
    
    o2 = ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        //.addCluster(ClusterDescriptor.newBuilder()   // <-- Diff
        //    .setCluster("Secondary")
        //    .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
        //        .setTimeout(30000)
        //        .setExecutorId("Secondary_Timer")
        //        .setExecutorType("TimedQueryExecutor")
        //        .build()))
        .build();
    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertNotEquals(o1, o2);
    assertEquals(1, o1.compareTo(o2));
    
    o2 = ClusterOverride.newBuilder()
        .setId("ShorterTimeout")
        // <-- Diff order NOT OK in this case (ordering is important)
        .addCluster(ClusterDescriptor.newBuilder()
            .setCluster("Secondary")
            .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
                .setTimeout(30000)
                .setExecutorId("Secondary_Timer")
                .setExecutorType("TimedQueryExecutor")
                .build()))
        .addCluster(ClusterDescriptor.newBuilder()
          .setCluster("Primary")
          .addExecutorConfig(TimedQueryExecutor.Config.newBuilder()
              .setTimeout(30000)
              .setExecutorId("Primary_Timer")
              .setExecutorType("TimedQueryExecutor")
              .build()))
        .build();
    assertNotEquals(o1.hashCode(), o2.hashCode());
    assertNotEquals(o1, o2);
    assertEquals(-1, o1.compareTo(o2));
  }
}
