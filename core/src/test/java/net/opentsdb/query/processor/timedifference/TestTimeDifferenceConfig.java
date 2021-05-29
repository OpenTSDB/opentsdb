// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timedifference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.time.temporal.ChronoUnit;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

public class TestTimeDifferenceConfig {
  
  @Test
  public void build() throws Exception {
    TimeDifferenceConfig config = TimeDifferenceConfig.newBuilder()
        .setResolution(ChronoUnit.SECONDS)
        .setId("diff")
        .build();
    assertEquals(ChronoUnit.SECONDS, config.getResolution());
    assertEquals("diff", config.getId());
    
    try {
      TimeDifferenceConfig.newBuilder()
          .setResolution(ChronoUnit.YEARS)
          .setId("diff")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TimeDifferenceConfig.newBuilder()
          .setResolution(ChronoUnit.SECONDS)
          //.setId("diff")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void serdes() throws Exception {
    TimeDifferenceConfig config = TimeDifferenceConfig.newBuilder()
        .setResolution(ChronoUnit.SECONDS)
        .setId("diff")
        .build();
    
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"diff\""));
    assertTrue(json.contains("\"resolution\":\"SECONDS\""));
    
    JsonNode node = JSON.getMapper().readTree(json);
    config = new TimeDifferenceFactory().parseConfig(JSON.getMapper(), mock(TSDB.class), node);
    assertEquals(ChronoUnit.SECONDS, config.getResolution());
    assertEquals("diff", config.getId());
  }
  
  @Test
  public void equalsAndHashCode() throws Exception {
    TimeDifferenceConfig config = TimeDifferenceConfig.newBuilder()
        .setResolution(ChronoUnit.SECONDS)
        .setId("diff")
        .build();
    
    TimeDifferenceConfig config2 = TimeDifferenceConfig.newBuilder()
        .setResolution(ChronoUnit.SECONDS)
        .setId("diff")
        .build();
    assertEquals(config, config2);
    assertEquals(config.hashCode(), config2.hashCode());
    
    config2 = TimeDifferenceConfig.newBuilder()
        .setResolution(ChronoUnit.MINUTES) // <-- diff
        .setId("diff")
        .build();
    assertNotEquals(config, config2);
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = TimeDifferenceConfig.newBuilder()
        .setResolution(ChronoUnit.SECONDS)
        .setId("diff2") // <-- diff
        .build();
    assertNotEquals(config, config2);
    assertNotEquals(config.hashCode(), config2.hashCode());
  }
}
