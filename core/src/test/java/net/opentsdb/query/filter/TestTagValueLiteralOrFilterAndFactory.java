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
import static org.mockito.Mockito.mock;

import net.opentsdb.core.TSDB;
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
    assertEquals("host", filter.getTagKey());
    assertEquals("web01|web02", filter.getFilter());
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
    assertEquals("host", filter.getTagKey());
    assertEquals("web01", filter.getFilter());
    assertEquals(1, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    
    filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("web01|web02")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals("web01|web02", filter.getFilter());
    assertEquals(2, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    assertTrue(filter.literals().contains("web02"));
    
    // trim
    filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter(" web01 | web02 ")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals(" web01 | web02 ", filter.getFilter());
    assertEquals(2, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    assertTrue(filter.literals().contains("web02"));
    
    // leading and trailing pipes
    filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("| web01 | web02 | ")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals("| web01 | web02 | ", filter.getFilter());
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

  @Test
  public void serialize() throws Exception {
    TagValueLiteralOrFilter filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("web01|web02")
        .build();
    
    final String json = JSON.serializeToString(filter);
    assertTrue(json.contains("\"filter\":\"web01|web02\""));
    assertTrue(json.contains("\"tagKey\":\"host"));
    assertTrue(json.contains("\"type\":\"TagValueLiteralOr"));
  }
  
  @Test
  public void initialize() throws Exception {
    TagValueLiteralOrFilter filter = TagValueLiteralOrFilter.newBuilder()
        .setTagKey("host")
        .setFilter("web01")
        .build();
    assertNull(filter.initialize(null).join());
  }

  @Test
  public void tagkeyLiteralEquality() throws Exception {
    TagKeyLiteralOrFilter filter = TagKeyLiteralOrFilter.newBuilder()
            .setFilter("web01|web02")
            .build();

    TagKeyLiteralOrFilter filter2 = TagKeyLiteralOrFilter.newBuilder()
            .setFilter("web01|web02")
            .build();


    TagKeyLiteralOrFilter filter3 = TagKeyLiteralOrFilter.newBuilder()
            .setFilter("web01|web02|web03")
            .build();

    assertTrue(filter.equals(filter2));
    assertTrue(!filter.equals(filter3));
    assertEquals(filter.hashCode(), filter2.hashCode());
    assertNotEquals(filter.hashCode(), filter3.hashCode());

  }


  @Test
  public void tagkeyRegexEquality() throws Exception {
    TagKeyRegexFilter filter = TagKeyRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();

    TagKeyRegexFilter filter2 = TagKeyRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();


    TagKeyRegexFilter filter3 = TagKeyRegexFilter.newBuilder()
            .setFilter("ogg-02.ops.ankh.morpork.com")
            .build();

    assertTrue(filter.equals(filter2));
    assertTrue(!filter.equals(filter3));
    assertEquals(filter.hashCode(), filter2.hashCode());
    assertNotEquals(filter.hashCode(), filter3.hashCode());

  }


  @Test
  public void tagvalueLiteralEquality() throws Exception {
    TagValueLiteralOrFilter filter = TagValueLiteralOrFilter.newBuilder()
            .setTagKey("host")
            .setFilter("web01|web02")
            .build();

    TagValueLiteralOrFilter filter2 = TagValueLiteralOrFilter.newBuilder()
            .setTagKey("host")
            .setFilter("web01 | web02")
            .build();


    TagValueLiteralOrFilter filter3 = TagValueLiteralOrFilter.newBuilder()
            .setTagKey("host")
            .setFilter("web01")
            .build();

    TagValueLiteralOrFilter filter4 = TagValueLiteralOrFilter.newBuilder()
            .setTagKey("host2")
            .setFilter("web01 | web02")
            .build();

    assertTrue(filter.equals(filter2));
    assertTrue(!filter.equals(filter3));
    assertTrue(!filter.equals(filter4));
    assertEquals(filter.hashCode(), filter2.hashCode());
    assertNotEquals(filter.hashCode(), filter3.hashCode());

  }


}
