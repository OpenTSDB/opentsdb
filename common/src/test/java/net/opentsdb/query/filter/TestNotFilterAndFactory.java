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
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.filter.UTFilterFactory.UTQueryFilter;

public class TestNotFilterAndFactory {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  
  @Test
  public void parse() throws Exception {
    String json = "{\"type\":\"Not\",\"filter\":"
        + "{\"type\":\"TagValueLiteralOr\",\"tag\":\"host\",\"filter\":\"web01|web02\"}}";
    
    MockTSDB tsdb = new MockTSDB();
    UTFilterFactory tv_factory = new UTFilterFactory();
    NotFilterFactory not_factory = new NotFilterFactory();
    when(tsdb.registry.getPlugin(QueryFilterFactory.class, "TagValueLiteralOr"))
      .thenReturn(tv_factory);
    when(tsdb.registry.getPlugin(QueryFilterFactory.class, "Not"))
      .thenReturn(not_factory);
    
    JsonNode node = MAPPER.readTree(json);
    NotFilter filter = (NotFilter) not_factory.parse(tsdb, MAPPER, node);
    assertTrue(filter.getFilter() instanceof UTQueryFilter);
    assertEquals("web01|web02", ((UTQueryFilter) filter.getFilter()).filter);
    
    // no type
    json = "{\"type\":\"Not\",\"filter\":"
        + "{\"tag\":\"host\",\"filter\":\"web01|web02\"}}";
    node = MAPPER.readTree(json);
    try {
      not_factory.parse(tsdb, MAPPER, node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no filter
    json = "{\"type\":\"Not\",\"filter\":null}";
    node = MAPPER.readTree(json);
    try {
      not_factory.parse(tsdb, MAPPER, node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builder() throws Exception {
    NotFilter filter = NotFilter.newBuilder()
        .setFilter(new UTQueryFilter("host", "web01|web02"))
        .build();
    assertEquals("web01|web02", ((UTQueryFilter) 
        filter.getFilter()).filter);
    
    try {
      NotFilter.newBuilder().build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void serialize() throws Exception {
    NotFilter filter = NotFilter.newBuilder()
        .setFilter(new UTQueryFilter("host", "web01|web02"))
        .build();
    
    final String json = MAPPER.writeValueAsString(filter);
    assertTrue(json.contains("\"filter\":{"));
    assertTrue(json.contains("\"type\":\"UTFilter\""));
    assertTrue(json.contains("\"tag\":\"host\""));
    assertTrue(json.contains("\"filter\":\"web01|web02\""));
    assertTrue(json.contains("\"type\":\"Not\""));
  }
  
  @Test
  public void initialize() throws Exception {
    UTQueryFilter filter_a = spy(new UTQueryFilter("host", "web01|web02"));
    
    NotFilter filter = NotFilter.newBuilder()
        .setFilter(filter_a)
        .build();
    
    assertNull(filter.initialize(null).join());
    verify(filter_a, times(1)).initialize(null);
  }

  @Test
  public void equality() throws Exception {
    NotFilter filter = NotFilter.newBuilder()
            .setFilter(new UTQueryFilter("host", "web01|web02"))
            .build();

    NotFilter filter2 = NotFilter.newBuilder()
            .setFilter(new UTQueryFilter("host", "web01|web02"))
            .build();

    NotFilter filter3 = NotFilter.newBuilder()
            .setFilter(new UTQueryFilter("hostname", "web01|web02"))
            .build();



    assertTrue(filter.equals(filter2));
    assertTrue(!filter.equals(filter3));
    assertEquals(filter.hashCode(), filter2.hashCode());
    assertNotEquals(filter.hashCode(), filter3.hashCode());

    filter3 = NotFilter.newBuilder()
            .setFilter(new UTQueryFilter("host", "web01"))
            .build();

    assertTrue(!filter.equals(filter3));
    assertNotEquals(filter.hashCode(), filter3.hashCode());

    assertNotEquals(filter, filter.getFilter());
  }
  
}
