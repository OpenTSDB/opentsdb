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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.utils.JSON;

import static org.junit.Assert.*;

public class TestTagValueRegexFilterAndFactory {
  private static final String TAGK = "host";
  
  @Test
  public void parse() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    TagValueRegexFactory factory = new TagValueRegexFactory();
    String json = "{\"tagKey\":\"host\",\"filter\":\"web.*\"}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    TagValueRegexFilter filter = (TagValueRegexFilter) 
        factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("host", filter.getTagKey());
    assertEquals("web.*", filter.getFilter());
    assertFalse(filter.matchesAll());
    
    json = "{\"key\":\"host\",\"filter\":\"web.*\"}";
    node = JSON.getMapper().readTree(json);
    filter = (TagValueRegexFilter) 
        factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("host", filter.getTagKey());
    assertEquals("web.*", filter.getFilter());
    assertFalse(filter.matchesAll());
    
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
  public void builderAndMAtches() throws Exception {
    Map<String, String> tags = new HashMap<String, String>(1);
    tags.put(TAGK, "ogg-01.ops.ankh.morpork.com");
    
    TagValueRegexFilter filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("ogg-01.ops.ankh.morpork.com")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals("ogg-01.ops.ankh.morpork.com", filter.getFilter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("ogg-01.ops.ankh.*")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals("ogg-01.ops.ankh.*", filter.getFilter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter(".*")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals(".*", filter.getFilter());
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("^.*")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals("^.*", filter.getFilter());
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter(".*$")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals(".*$", filter.getFilter());
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("^.*$")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals("^.*$", filter.getFilter());
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    // trim
    filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter(" ogg-01.ops.ankh.* ")
        .build();
    assertEquals("host", filter.getTagKey());
    assertEquals(" ogg-01.ops.ankh.* ", filter.getFilter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
        
    try {
      TagValueRegexFilter.newBuilder()
        //.setKey("host")
        .setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueRegexFilter.newBuilder()
        .setKey("")
        .setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueRegexFilter.newBuilder()
        .setKey("host")
        //.setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // bad pattern
    try {
      TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("*noprefix")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serialize() throws Exception {
    TagValueRegexFilter filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("ogg-01.ops.ankh.*")
        .build();
    
    final String json = JSON.serializeToString(filter);
    assertTrue(json.contains("\"filter\":\"ogg-01.ops.ankh.*\""));
    assertTrue(json.contains("\"tagKey\":\"host"));
    assertTrue(json.contains("\"type\":\"TagValueRegex"));
  }
  
  @Test
  public void initialize() throws Exception {
    TagValueRegexFilter filter = TagValueRegexFilter.newBuilder()
        .setKey("host")
        .setFilter("ogg-01.ops.ankh.morpork.com")
        .build();
    assertNull(filter.initialize(null).join());
  }

  @Test
  public void equality() throws Exception {
    TagValueRegexFilter filter = TagValueRegexFilter.newBuilder()
            .setKey("host")
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();

    TagValueRegexFilter filter2 = TagValueRegexFilter.newBuilder()
            .setKey("host")
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();


    TagValueRegexFilter filter3 = TagValueRegexFilter.newBuilder()
            .setKey("host2")
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();

    assertTrue(filter.equals(filter2));
    assertTrue(!filter.equals(filter3));
    assertEquals(filter.hashCode(), filter2.hashCode());
    assertNotEquals(filter.hashCode(), filter3.hashCode());

  }
  
}
