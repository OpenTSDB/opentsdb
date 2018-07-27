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

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.utils.JSON;

public class TestTagValueLiteralOrFilterAndFactory {

  @Test
  public void parse() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    TagValueLiteralOrFactory factory = new TagValueLiteralOrFactory();
    String json = "{\"tagKey\":\"host\",\"filter\":\"web01|web02\"}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    TagValueLiteralOrFilter filter = (TagValueLiteralOrFilter) 
        factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("host", filter.tagKey());
    assertEquals("web01|web02", filter.filter());
    assertEquals(2, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    assertTrue(filter.literals().contains("web02"));
    
    try {
      factory.parse(tsdb, JSON.getMapper(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no filter
    json = "{\"tagKey\":\"host\"}";
    node = JSON.getMapper().readTree(json);
    try {
      factory.parse(tsdb, JSON.getMapper(), node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no tag key
    json = "{\"filter\":\"web01|web02\"}";
    node = JSON.getMapper().readTree(json);
    try {
      factory.parse(tsdb, JSON.getMapper(), node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void builder() throws Exception {
    TagValueLiteralOrFilter filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("web01")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("web01", filter.filter());
    assertEquals(1, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    
    filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("web01|web02")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("web01|web02", filter.filter());
    assertEquals(2, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    assertTrue(filter.literals().contains("web02"));
    
    // trim
    filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter(" web01 | web02 ")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals(" web01 | web02 ", filter.filter());
    assertEquals(2, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    assertTrue(filter.literals().contains("web02"));
    
    // leading and trailing pipes
    filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("| web01 | web02 | ")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("| web01 | web02 | ", filter.filter());
    assertEquals(2, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    assertTrue(filter.literals().contains("web02"));
    
    try {
      TagValueLiteralOrFilter.newBuilder()
        //.setTagKey("host")
        .setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueLiteralOrFilter.newBuilder()
        .setTagKey("")
        .setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        //.setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("|")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
