// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import net.opentsdb.utils.JSON;

public class TestJsonV2QuerySerdesOptions {

  @Test
  public void builder() throws Exception {
    JsonV2QuerySerdesOptions config = (JsonV2QuerySerdesOptions) 
        JsonV2QuerySerdesOptions.newBuilder()
        .setShowTsuids(true)
        .setMsResolution(true)
        .setShowQuery(true)
        .setShowStats(true)
        .setShowSummary(true)
        .setParallelThreshold(42)
        .addFilter("gb:m1")
        .setId("json")
        .setType("JsonV2")
        .build();
    assertTrue(config.getShowTsuids());
    assertTrue(config.getMsResolution());
    assertTrue(config.getShowQuery());
    assertTrue(config.getShowStats());
    assertTrue(config.getShowSummary());
    assertEquals(42, config.getParallelThreshold());
    assertEquals(1, config.getFilter().size());
    assertEquals("gb:m1", config.getFilter().get(0));
    assertEquals("json", config.getId());
    assertEquals("JsonV2", config.getType());
  }
  
  @Test
  public void serdes() throws Exception {
    JsonV2QuerySerdesOptions config = (JsonV2QuerySerdesOptions) 
        JsonV2QuerySerdesOptions.newBuilder()
        .setShowTsuids(true)
        .setMsResolution(true)
        .setShowQuery(true)
        .setShowStats(true)
        .setShowSummary(true)
        .setParallelThreshold(42)
        .addFilter("gb:m1")
        .setId("json")
        .setType("JsonV2")
        .build();
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"id\":\"json\""));
    assertTrue(json.contains("\"type\":\"JsonV2\""));
    assertTrue(json.contains("\"filter\":[\"gb:m1\"]"));
    assertTrue(json.contains("\"msResolution\":true"));
    assertTrue(json.contains("\"showTsuids\":true"));
    assertTrue(json.contains("\"showQuery\":true"));
    assertTrue(json.contains("\"showStats\":true"));
    assertTrue(json.contains("\"showSummary\":true"));
    assertTrue(json.contains("\"parallelThreshold\":42"));
    
    config = JSON.parseToObject(json, JsonV2QuerySerdesOptions.class);
    assertTrue(config.getShowTsuids());
    assertTrue(config.getMsResolution());
    assertTrue(config.getShowQuery());
    assertTrue(config.getShowStats());
    assertTrue(config.getShowSummary());
    assertEquals(42, config.getParallelThreshold());
    assertEquals(1, config.getFilter().size());
    assertEquals("gb:m1", config.getFilter().get(0));
    assertEquals("json", config.getId());
    assertEquals("JsonV2", config.getType());
  }
}
