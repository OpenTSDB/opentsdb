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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.utils.JSON;

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
    assertEquals("host", filter.tagKey());
    assertEquals("web.*", filter.filter());
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
        .setTagKey("host")
        .setFilter("ogg-01.ops.ankh.morpork.com")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("ogg-01.ops.ankh.morpork.com", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        .setFilter("ogg-01.ops.ankh.*")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("ogg-01.ops.ankh.*", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        .setFilter(".*")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals(".*", filter.filter());
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        .setFilter("^.*")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("^.*", filter.filter());
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        .setFilter(".*$")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals(".*$", filter.filter());
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        .setFilter("^.*$")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("^.*$", filter.filter());
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    // trim
    filter = TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        .setFilter(" ogg-01.ops.ankh.* ")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals(" ogg-01.ops.ankh.* ", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
        
    try {
      TagValueRegexFilter.newBuilder()
        //.setTagKey("host")
        .setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueRegexFilter.newBuilder()
        .setTagKey("")
        .setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        //.setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        .setFilter("")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // bad pattern
    try {
      TagValueRegexFilter.newBuilder()
        .setTagKey("host")
        .setFilter("*noprefix")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

}
