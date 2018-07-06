// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.joins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.JSON;

public class TestJoinConfig {

  @Test
  public void builder() throws Exception {
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "Hostname")
        .addJoins("owner", "owner")
        .setId("join1")
        .build();
    
    assertEquals(JoinType.INNER, config.getType());
    assertEquals(2, config.getJoins().size());
    assertEquals("Hostname", config.getJoins().get("host"));
    assertEquals("owner", config.getJoins().get("owner"));
    assertFalse(config.getExplicitTags());
    
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"join1\""));
    assertTrue(json.contains("\"type\":\"INNER\""));
    assertTrue(json.contains("\"joins\":{"));
    assertTrue(json.contains("\"host\":\"Hostname\""));
    assertTrue(json.contains("\"owner\":\"owner\""));
    assertTrue(json.contains("\"explicitTags\":false"));
    
    config = JSON.parseToObject("{\"id\":\"join1\",\"type\":\"INNER\","
        + "\"joins\":{\"host\":\"Hostname\",\"owner\":\"owner\"},"
        + "\"explicitTags\":true}", 
        JoinConfig.class);
    assertEquals(JoinType.INNER, config.getType());
    assertEquals(2, config.getJoins().size());
    assertEquals("Hostname", config.getJoins().get("host"));
    assertEquals("owner", config.getJoins().get("owner"));
    assertTrue(config.getExplicitTags());
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .setId("join1")
        .build();
    assertEquals(JoinType.NATURAL, config.getType());
    assertTrue(config.getJoins().isEmpty());
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.CROSS)
        .setId("join1")
        .build();
    assertEquals(JoinType.CROSS, config.getType());
    assertTrue(config.getJoins().isEmpty());
    
    try {
      JoinConfig.newBuilder()
        //.setType(JoinType.INNER)
        .addJoins("host", "Hostname")
        .addJoins("owner", "owner")
        .setId("join1")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        //.addJoins("host", "Hostname")
        //.addJoins("owner", "owner")
        .setId("join1")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void compareEqualsHash() throws Exception {
    final JoinConfig c1 = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "Hostname")
        .addJoins("owner", "owner")
        .setExplicitTags(true)
        .setId("join1")
        .build();
    
    JoinConfig c2 = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "Hostname")
        .addJoins("owner", "owner")
        .setExplicitTags(true)
        .setId("join1")
        .build();
    assertEquals(c1, c2);
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.OUTER) // <-- Diff
        .addJoins("host", "Hostname")
        .addJoins("owner", "owner")
        .setExplicitTags(true)
        .setId("join1")
        .build();
    assertNotEquals(c1, c2);
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("owner", "owner") // <-- Diff order OK!
        .addJoins("host", "Hostname")
        .setExplicitTags(true)
        .setId("join1")
        .build();
    assertEquals(c1, c2);
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "hostname") // <-- Diff
        .addJoins("owner", "owner")
        .setExplicitTags(true)
        .setId("join1")
        .build();
    assertNotEquals(c1, c2);
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL) // <-- Diff
        //.addJoins("host", "Hostname")
        //.addJoins("owner", "owner")
        .setExplicitTags(true)
        .setId("join1")
        .build();
    assertNotEquals(c1, c2);
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertEquals(-1, c1.compareTo(c2));

    c2 = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "Hostname")
        .addJoins("owner", "owner")
        .setExplicitTags(false) // <-- Diff
        .setId("join1")
        .build();
    assertNotEquals(c1, c2);
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "Hostname")
        .addJoins("owner", "owner")
        .setExplicitTags(true)
        .setId("jc") // <-- Diff
        .build();
    assertNotEquals(c1, c2);
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertEquals(1, c1.compareTo(c2));

  }
}
