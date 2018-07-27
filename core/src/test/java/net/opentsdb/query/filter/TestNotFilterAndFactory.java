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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.utils.JSON;

public class TestNotFilterAndFactory {
  
  @Test
  public void parse() throws Exception {
    String json = "{\"type\":\"Not\",\"filter\":"
        + "{\"type\":\"TagValueLiteralOr\",\"tagKey\":\"host\",\"filter\":\"web01|web02\"}}";
    
    MockTSDB tsdb = new MockTSDB();
    TagValueLiteralOrFactory tv_factory = new TagValueLiteralOrFactory();
    NotFilterFactory not_factory = new NotFilterFactory();
    when(tsdb.registry.getPlugin(QueryFilterFactory.class, "TagValueLiteralOr"))
      .thenReturn(tv_factory);
    when(tsdb.registry.getPlugin(QueryFilterFactory.class, "Not"))
      .thenReturn(not_factory);
    
    JsonNode node = JSON.getMapper().readTree(json);
    NotFilter filter = (NotFilter) not_factory.parse(tsdb, JSON.getMapper(), node);
    assertTrue(filter.getFilter() instanceof TagValueLiteralOrFilter);
    assertEquals("web01|web02", ((TagValueLiteralOrFilter) 
        filter.getFilter()).filter());
    
    // no type
    json = "{\"type\":\"Not\",\"filter\":"
        + "{\"tagKey\":\"host\",\"filter\":\"web01|web02\"}}";
    node = JSON.getMapper().readTree(json);
    try {
      not_factory.parse(tsdb, JSON.getMapper(), node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no filter
    json = "{\"type\":\"Not\",\"filter\":null}";
    node = JSON.getMapper().readTree(json);
    try {
      not_factory.parse(tsdb, JSON.getMapper(), node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builder() throws Exception {
    NotFilter filter = NotFilter.newBuilder()
        .setFilter(TagValueLiteralOrFilter.newBuilder()
            .setTagKey("host")
            .setFilter("web01|web02")
            .build())
        .build();
    assertEquals("web01|web02", ((TagValueLiteralOrFilter) 
        filter.getFilter()).filter());
    
    try {
      NotFilter.newBuilder().build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
}
