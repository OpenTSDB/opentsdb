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
package net.opentsdb.query.filter;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Maps;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

import java.util.Map;

public class TestMetricLiteralFilter {

  @Test
  public void parse() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    MetricLiteralFactory factory = new MetricLiteralFactory();
    String json = "{\"type\":\"MetricLiteral\",\"metric\":\"sys.cpu.user\"}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    MetricLiteralFilter filter = (MetricLiteralFilter) 
        factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("sys.cpu.user", filter.getMetric());
    
    try {
      factory.parse(tsdb, JSON.getMapper(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builder() throws Exception {
    MetricLiteralFilter filter = MetricLiteralFilter.newBuilder()
        .setMetric("system.cpu.user")
        .build();
    assertEquals("system.cpu.user", filter.getMetric());
    
    // trim
    filter = MetricLiteralFilter.newBuilder()
        .setMetric("  system.cpu.user ")
        .build();
    assertEquals("system.cpu.user", filter.getMetric());
    
    try {
      MetricLiteralFilter.newBuilder()
      .setMetric(null)
      .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      MetricLiteralFilter.newBuilder()
      .setMetric("")
      .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
  }
  
  @Test
  public void serialize() throws Exception {
    MetricLiteralFilter filter = MetricLiteralFilter.newBuilder()
        .setMetric("system.cpu.user")
        .build();
    
    final String json = JSON.serializeToString(filter);
    assertTrue(json.contains("\"type\":\"MetricLiteral\""));
    assertTrue(json.contains("\"metric\":\"system.cpu.user\""));
  }

  @Test
  public void initialize() throws Exception {
    MetricLiteralFilter filter = MetricLiteralFilter.newBuilder()
        .setMetric("system.cpu.user")
        .build();
    assertNull(filter.initialize(null).join());
  }


  @Test
  public void equality() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    MetricLiteralFactory factory = new MetricLiteralFactory();
    String json = "{\"type\":\"MetricLiteral\",\"metric\":\"sys.cpu.user\"}";

    JsonNode node = JSON.getMapper().readTree(json);
    MetricLiteralFilter filter = (MetricLiteralFilter)
            factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("sys.cpu.user", filter.getMetric());

    MetricLiteralFilter filter2 = (MetricLiteralFilter)
            factory.parse(tsdb, JSON.getMapper(), node);

    String json2 = "{\"type\":\"MetricLiteral\",\"metric\":\"sys.cpu.users\"}";

    JsonNode node2 = JSON.getMapper().readTree(json2);
    MetricLiteralFilter filter3 = (MetricLiteralFilter)
            factory.parse(tsdb, JSON.getMapper(), node2);
    assertEquals("sys.cpu.user", filter.getMetric());


    assertTrue(filter.equals(filter2));
    assertTrue(!filter.equals(filter3));
    assertEquals(filter.hashCode(), filter2.hashCode());
    assertNotEquals(filter.hashCode(), filter3.hashCode());

    filter = MetricLiteralFilter.newBuilder()
            .setMetric("system.cpu.user")
            .build();

    // trim
    filter2 = MetricLiteralFilter.newBuilder()
            .setMetric("  system.cpu.user ")
            .build();

    filter3 = MetricLiteralFilter.newBuilder()
            .setMetric("  system.cpu.pct ")
            .build();

    assertTrue(filter.equals(filter2));
    assertTrue(!filter.equals(filter3));
    assertEquals(filter.hashCode(), filter2.hashCode());
    assertNotEquals(filter.hashCode(), filter3.hashCode());

  }

  
}
