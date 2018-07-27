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

public class TestTagValueWildcardFilterAndFactory {
  private static final String TAGK = "host";
  
  @Test
  public void parse() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    TagValueWildcardFactory factory = new TagValueWildcardFactory();
    String json = "{\"tagKey\":\"host\",\"filter\":\"web*\"}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    TagValueWildcardFilter filter = (TagValueWildcardFilter) 
        factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("host", filter.tagKey());
    assertEquals("web*", filter.filter());
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
  public void builderAndMatches() throws Exception {
    Map<String, String> tags = new HashMap<String, String>(1);
    tags.put(TAGK, "ogg-01.ops.ankh.morpork.com");
    
    TagValueWildcardFilter filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("*.morpork.com")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("*.morpork.com", filter.filter());
    assertEquals(1, filter.components().length);
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("ogg*")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("ogg*", filter.filter());
    assertEquals(1, filter.components().length);
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("*")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("*", filter.filter());
    assertEquals(1, filter.components().length);
    assertTrue(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("ogg*com")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("ogg*com", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("ogg*ops*ank*com")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("ogg*ops*ank*com", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("ogg*ops*com")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("ogg*ops*com", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("*morpork*")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("*morpork*", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("*ops*com")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("*ops*com", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("ogg***com")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals("ogg***com", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
    
    // trim
    filter = TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter(" ogg*ops*com ")
        .build();
    assertEquals("host", filter.tagKey());
    assertEquals(" ogg*ops*com ", filter.filter());
    assertFalse(filter.matchesAll());
    assertTrue(filter.matches(tags));
        
    try {
      TagValueWildcardFilter.newBuilder()
        //.setTagKey("host")
        .setFilter("ogg*ops*com")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueWildcardFilter.newBuilder()
        .setTagKey("")
        .setFilter("ogg*ops*com")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        //.setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no wildcard
    try {
      TagValueWildcardFilter.newBuilder()
        .setTagKey("host")
        .setFilter("web01")
        .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

}
