// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

public class TestPassThroughFilter {

  @Test
  public void parse() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    PassThroughFilterFactory factory = new PassThroughFilterFactory();
    String json = "{\"type\":\"PassThrough\",\"filter\":\"A AND B\"}";

    JsonNode node = JSON.getMapper().readTree(json);
    PassThroughStringFilter filter = (PassThroughStringFilter)
        factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("A AND B", filter.getFilter());

    try {
      factory.parse(tsdb, JSON.getMapper(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void builder() throws Exception {
    PassThroughStringFilter filter = PassThroughStringFilter.newBuilder()
        .setFilter("A AND B")
        .build();
    assertEquals("A AND B", filter.getFilter());

    // trim
    filter = PassThroughStringFilter.newBuilder()
        .setFilter("  A AND B ")
        .build();
    assertEquals("A AND B", filter.getFilter());

    try {
      PassThroughStringFilter.newBuilder()
          .setFilter(null)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      PassThroughStringFilter.newBuilder()
          .setFilter("")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

  }

  @Test
  public void serialize() throws Exception {
    PassThroughFilter filter = PassThroughStringFilter.newBuilder()
        .setFilter("A AND B")
        .build();

    final String json = JSON.serializeToString(filter);
    assertTrue(json.contains("\"type\":\"PassThrough\""));
    assertTrue(json.contains("\"filter\":\"A AND B\""));
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
    PassThroughFilterFactory factory = new PassThroughFilterFactory();
    String json = "{\"type\":\"PassThrough\",\"filter\":\"A AND B\"}";

    JsonNode node = JSON.getMapper().readTree(json);
    PassThroughFilter filter = (PassThroughFilter)
        factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("A AND B", filter.getFilter());

  }


}
