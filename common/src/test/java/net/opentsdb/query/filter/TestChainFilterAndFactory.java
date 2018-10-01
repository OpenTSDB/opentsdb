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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.query.filter.UTFilterFactory.UTQueryFilter;

public class TestChainFilterAndFactory {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  @Test
  public void parse() throws Exception {
    String json = "{\"type\":\"chain\",\"op\":\"OR\",\"filters\":["
        + "{\"type\":\"UTQueryFilter\",\"tag\":\"host\",\"filter\":\"web01|web02\"},"
        + "{\"type\":\"UTQueryFilter\",\"tag\":\"host\",\"filter\":\"web03\"}]}";
    
    MockTSDB tsdb = new MockTSDB();
    UTFilterFactory tv_factory = new UTFilterFactory();
    ChainFilterFactory chain_factory = new ChainFilterFactory();
    when(tsdb.registry.getPlugin(QueryFilterFactory.class, "UTQueryFilter"))
      .thenReturn(tv_factory);
    when(tsdb.registry.getPlugin(QueryFilterFactory.class, "Chain"))
      .thenReturn(chain_factory);
    
    JsonNode node = MAPPER.readTree(json);
    ChainFilter filter = (ChainFilter) chain_factory.parse(tsdb, MAPPER, node);
    assertEquals(FilterOp.OR, filter.getOp());
    assertEquals(2, filter.getFilters().size());
    assertEquals("web01|web02", ((UTQueryFilter) 
        filter.getFilters().get(0)).filter);
    assertEquals("web03", ((UTQueryFilter) 
        filter.getFilters().get(1)).filter);
    
    // default operation
    json = "{\"type\":\"chain\",\"filters\":["
        + "{\"type\":\"UTQueryFilter\",\"tag\":\"host\",\"filter\":\"web01|web02\"},"
        + "{\"type\":\"UTQueryFilter\",\"tag\":\"host\",\"filter\":\"web03\"}]}";
    node = MAPPER.readTree(json);
    filter = (ChainFilter) chain_factory.parse(tsdb, MAPPER, node);
    assertEquals(FilterOp.AND, filter.getOp());
    assertEquals(2, filter.getFilters().size());
    
    try {
      chain_factory.parse(tsdb, MAPPER, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no type
    json = "{\"type\":\"chain\",\"filters\":["
        + "{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"host\",\"filter\":\"web01|web02\"},"
        + "{\"tagKey\":\"host\",\"filter\":\"web03\"}]}";
    node = MAPPER.readTree(json);
    try {
      chain_factory.parse(tsdb, MAPPER, node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builder() throws Exception {
    ChainFilter filter = ChainFilter.newBuilder()
        .setOp(FilterOp.OR)
        .addFilter(new UTQueryFilter("host", "web01|web02"))
        .addFilter(new UTQueryFilter("owner", "tyrion"))
        .build();
    assertEquals(FilterOp.OR, filter.getOp());
    assertEquals(2, filter.getFilters().size());
    assertEquals("web01|web02", ((UTQueryFilter) 
        filter.getFilters().get(0)).filter);
    assertEquals("tyrion", ((UTQueryFilter) 
        filter.getFilters().get(1)).filter);
    
    filter = ChainFilter.newBuilder()
        //.setOp(FilterOp.OR) // DEFAULT
        .addFilter(new UTQueryFilter("host", "web01|web02"))
        .addFilter(new UTQueryFilter("owner", "tyrion"))
        .build();
    assertEquals(FilterOp.AND, filter.getOp());
    assertEquals(2, filter.getFilters().size());
    assertEquals("web01|web02", ((UTQueryFilter) 
        filter.getFilters().get(0)).filter);
    assertEquals("tyrion", ((UTQueryFilter) 
        filter.getFilters().get(1)).filter);
    
    try {
      ChainFilter.newBuilder()
          .setOp(FilterOp.OR)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serialize() throws Exception {
    ChainFilter filter = ChainFilter.newBuilder()
        .setOp(FilterOp.OR)
        .addFilter(new UTQueryFilter("host", "web01|web02"))
        .addFilter(new UTQueryFilter("owner", "tyrion"))
        .build();
    
    final String json = MAPPER.writeValueAsString(filter);
    assertTrue(json.contains("\"filters\":["));
    assertTrue(json.contains("\"type\":\"UTFilter\""));
    assertTrue(json.contains("\"tag\":\"host\""));
    assertTrue(json.contains("\"filter\":\"web01|web02\""));
    assertTrue(json.contains("\"tag\":\"owner\""));
    assertTrue(json.contains("\"filter\":\"tyrion\""));
    assertTrue(json.contains("\"op\":\"OR\""));
    assertTrue(json.contains("\"type\":\"Chain\""));
  }
  
  @Test
  public void initialize() throws Exception {
    UTQueryFilter filter_a = spy(new UTQueryFilter("host", "web01|web02"));
    UTQueryFilter filter_b = spy(new UTQueryFilter("owner", "tyrion"));
    
    ChainFilter filter = ChainFilter.newBuilder()
        .setOp(FilterOp.OR)
        .addFilter(filter_a)
        .addFilter(filter_b)
        .build();
    
    assertNull(filter.initialize(null).join());
    verify(filter_a, times(1)).initialize(null);
    verify(filter_b, times(1)).initialize(null);
  }
  
}
