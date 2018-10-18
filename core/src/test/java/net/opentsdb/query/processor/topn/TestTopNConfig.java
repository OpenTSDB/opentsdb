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
package net.opentsdb.query.processor.topn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

public class TestTopNConfig {

  @Test
  public void build() throws Exception {
    TopNConfig config = (TopNConfig) TopNConfig.newBuilder()
        .setTop(true)
        .setCount(10)
        .setInfectiousNan(true)
        .setId("Toppy")
        .build();
    
    assertTrue(config.getTop());
    assertEquals(10, config.getCount());
    assertTrue(config.getInfectiousNan());
    assertEquals("Toppy", config.getId());
  }
  
  @Test
  public void serdes() throws Exception {
    TopNConfig config = (TopNConfig) TopNConfig.newBuilder()
        .setTop(true)
        .setCount(10)
        .setInfectiousNan(true)
        .setId("Toppy")
        .addSource("m1")
        .build();
    
    final String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"Toppy\""));
    assertTrue(json.contains("\"count\":10"));
    assertTrue(json.contains("\"top\":true"));
    assertTrue(json.contains("\"infectiousNan\":true"));
    assertTrue(json.contains("\"type\":\"TopN\""));
    assertTrue(json.contains("\"sources\":[\"m1\"]"));
    
    JsonNode node = JSON.getMapper().readTree(json);
    config = (TopNConfig) new TopNFactory()
        .parseConfig(JSON.getMapper(), mock(TSDB.class), node);
    assertTrue(config.getTop());
    assertEquals(10, config.getCount());
    assertTrue(config.getInfectiousNan());
    assertEquals("Toppy", config.getId());
    assertEquals(1, config.getSources().size());
    assertEquals("m1", config.getSources().get(0));
    assertEquals(TopNFactory.TYPE, config.getType());
  }
  
}
