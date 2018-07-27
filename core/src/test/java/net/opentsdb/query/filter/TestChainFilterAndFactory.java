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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.utils.JSON;

public class TestChainFilterAndFactory {

  @Test
  public void parse() throws Exception {
    String json = "{\"type\":\"chain\",\"op\":\"OR\",\"filters\":["
        + "{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"host\",\"filter\":\"web01|web02\"},"
        + "{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"host\",\"filter\":\"web03\"}]}";
    
    MockTSDB tsdb = new MockTSDB();
    TagValueLiteralOrFactory tv_factory = new TagValueLiteralOrFactory();
    ChainFilterFactory chain_factory = new ChainFilterFactory();
    when(tsdb.registry.getPlugin(QueryFilterFactory.class, "TagValueLiteralOr"))
      .thenReturn(tv_factory);
    when(tsdb.registry.getPlugin(QueryFilterFactory.class, "Chain"))
      .thenReturn(chain_factory);
    
    JsonNode node = JSON.getMapper().readTree(json);
    ChainFilter filter = (ChainFilter) chain_factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals(FilterOp.OR, filter.getOp());
    assertEquals(2, filter.getFilters().size());
    assertEquals("web01|web02", ((TagValueLiteralOrFilter) 
        filter.getFilters().get(0)).filter());
    assertEquals("web03", ((TagValueLiteralOrFilter) 
        filter.getFilters().get(1)).filter());
    
    // default operation
    json = "{\"type\":\"chain\",\"filters\":["
        + "{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"host\",\"filter\":\"web01|web02\"},"
        + "{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"host\",\"filter\":\"web03\"}]}";
    node = JSON.getMapper().readTree(json);
    filter = (ChainFilter) chain_factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals(FilterOp.AND, filter.getOp());
    assertEquals(2, filter.getFilters().size());
    
    try {
      chain_factory.parse(tsdb, JSON.getMapper(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // only one filter.
    json = "{\"type\":\"chain\",\"filters\":["
        + "{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"host\",\"filter\":\"web01|web02\"}]}";
    node = JSON.getMapper().readTree(json);
    try {
      chain_factory.parse(tsdb, JSON.getMapper(), node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no type
    json = "{\"type\":\"chain\",\"filters\":["
        + "{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"host\",\"filter\":\"web01|web02\"},"
        + "{\"tagKey\":\"host\",\"filter\":\"web03\"}]}";
    node = JSON.getMapper().readTree(json);
    try {
      chain_factory.parse(tsdb, JSON.getMapper(), node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builder() throws Exception {
    ChainFilter filter = ChainFilter.newBuilder()
        .setOp(FilterOp.OR)
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey("host")
            .setFilter("web01|web02")
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey("owner")
            .setFilter("tyrion")
            .build())
        .build();
    assertEquals(FilterOp.OR, filter.getOp());
    assertEquals(2, filter.getFilters().size());
    assertEquals("web01|web02", ((TagValueLiteralOrFilter) 
        filter.getFilters().get(0)).filter());
    assertEquals("tyrion", ((TagValueLiteralOrFilter) 
        filter.getFilters().get(1)).filter());
    
    filter = ChainFilter.newBuilder()
        //.setOp(FilterOp.OR) // DEFAULT
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey("host")
            .setFilter("web01|web02")
            .build())
        .addFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey("owner")
            .setFilter("tyrion")
            .build())
        .build();
    assertEquals(FilterOp.AND, filter.getOp());
    assertEquals(2, filter.getFilters().size());
    assertEquals("web01|web02", ((TagValueLiteralOrFilter) 
        filter.getFilters().get(0)).filter());
    assertEquals("tyrion", ((TagValueLiteralOrFilter) 
        filter.getFilters().get(1)).filter());
    
    try {
      ChainFilter.newBuilder()
          .setOp(FilterOp.OR)
          .addFilter(TagValueLiteralOrFilter.newBuilder()
              .setTagKey("host")
              .setFilter("web01|web02")
              .build())
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ChainFilter.newBuilder()
          .setOp(FilterOp.OR)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
