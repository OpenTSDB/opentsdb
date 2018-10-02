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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.utils.JSON;

public class TestTagValueRangeFilterAndFactory {
  private static final String TAGK = "host";
  
  @Test
  public void parse() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    TagValueRangeFilterFactory factory = new TagValueRangeFilterFactory();
    String json = "{\"tagKey\":\"host\",\"filter\":\"web{01-04}\"}";
    
    JsonNode node = JSON.getMapper().readTree(json);
    TagValueRangeFilter filter = (TagValueRangeFilter) 
        factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals("host", filter.getTagKey());
    assertEquals("web{01-04}", filter.getFilter());
    
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
    json = "{\"filter\":\"web{01-04}\"}";
    node = JSON.getMapper().readTree(json);
    try {
      factory.parse(tsdb, JSON.getMapper(), node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void builderAndMAtches() throws Exception {
    TagValueRangeFilter filter = 
        (TagValueRangeFilter) TagValueRangeFilter.newBuilder()
        .setTagKey(TAGK)
        .setFilter("web{01-04}")
        .build();
    assertEquals(4, filter.literals().size());
    assertTrue(filter.literals().contains("web01"));
    assertTrue(filter.literals().contains("web02"));
    assertTrue(filter.literals().contains("web03"));
    assertTrue(filter.literals().contains("web04"));
    
    assertTrue(filter.matches(ImmutableMap.
        <String, String>builder().put(TAGK, "web01")
        .build()));
    assertTrue(filter.matches(ImmutableMap.
        <String, String>builder().put(TAGK, "web02")
        .build()));
    assertFalse(filter.matches(ImmutableMap.
        <String, String>builder().put(TAGK, "web05")
        .build()));
  }

  @Test
  public void serialize() throws Exception {
    TagValueRangeFilter filter = 
        (TagValueRangeFilter) TagValueRangeFilter.newBuilder()
        .setTagKey(TAGK)
        .setFilter("web{01-04}")
        .build();
    
    final String json = JSON.serializeToString(filter);
    assertTrue(json.contains("\"filter\":\"web{01-04}\""));
    assertTrue(json.contains("\"tagKey\":\"host"));
    assertTrue(json.contains("\"type\":\"TagValueRange"));
  }
  
  @Test
  public void initialize() throws Exception {
    TagValueRangeFilter filter = 
        (TagValueRangeFilter) TagValueRangeFilter.newBuilder()
        .setTagKey("host")
        .setFilter("web{01-04}")
        .build();
    assertNull(filter.initialize(null).join());
  }
  
  @Test
  public void testParseRangeExpressionOne() {
    final String exp = "web{01-03}.tmp.yahoo.com";
    Set<String> result = TagValueRangeFilter.parseRangeExpression(exp);
    assertEquals(3, result.size());
    assertTrue(result.contains("web01.tmp.yahoo.com"));
    assertTrue(result.contains("web02.tmp.yahoo.com"));
    assertTrue(result.contains("web03.tmp.yahoo.com"));
    
    final String exp2 = "web{001-06}.tmp.yahoo.com";
    Set<String> result2 = TagValueRangeFilter.parseRangeExpression(exp2);
    assertEquals(0, result2.size());
    
    final String exp3 = "web001.tmp.{dc1,cd1,sg3}.yahoo.com";
    Set<String> result3 = TagValueRangeFilter.parseRangeExpression(exp3);
    assertEquals(3, result3.size());
    assertTrue(result3.contains("web001.tmp.dc1.yahoo.com"));
    assertTrue(result3.contains("web001.tmp.cd1.yahoo.com"));
    assertTrue(result3.contains("web001.tmp.sg3.yahoo.com"));
    
    final String exp4 = "web001{}.tmp.dc1.yahoo.com";
    Set<String> result4 = TagValueRangeFilter.parseRangeExpression(exp4);
    assertEquals(1, result4.size());
    assertTrue(result4.contains("web001.tmp.dc1.yahoo.com"));
    
    final String exp5 = "web001.tmp.{dc1}.yahoo.com";
    Set<String> result5 = TagValueRangeFilter.parseRangeExpression(exp5);
    assertEquals(0, result5.size());
  }
  
  @Test
  public void testParseRangeExpressionOneNumericNoPrefixZero() {
    final String exp = "web{0-10}.tmp.yahoo.com";
    Set<String> result = TagValueRangeFilter.parseRangeExpression(exp);
    assertEquals(11, result.size());
    
    for (int i = 0; i <= 10; ++i) {
      StringBuilder sb = new StringBuilder();
      sb.append("web").append(i).append(".tmp.yahoo.com");
      assertTrue(result.contains(sb.toString()));
    }
  }
  
  @Test
  public void testParseRangeExpressionOneNumericPrefixZero() {
    final String exp = "web{000-100}.tmp.yahoo.com";
    Set<String> result = TagValueRangeFilter.parseRangeExpression(exp);
    assertEquals(101, result.size());
    
    for (int i = 0; i <= 100; ++i) {
      StringBuilder sb = new StringBuilder();
      sb.append("web");
      String numStr = Integer.toString(i);
      int j = 0;
      while (numStr.length() + j < 3) {
        sb.append("0");
        ++j;
      }
      sb.append(numStr).append(".tmp.yahoo.com");
      assertTrue(result.contains(sb.toString()));
    }
  }
  
  @Test
  public void testParseRangeExpressionTwo() {
    final String exp = "web{01-03}.tmp.{dc1,cd1}.yahoo.com";
    Set<String> result = TagValueRangeFilter.parseRangeExpression(exp);
    assertEquals(6, result.size());
    assertTrue(result.contains("web01.tmp.dc1.yahoo.com"));
    assertTrue(result.contains("web02.tmp.dc1.yahoo.com"));
    assertTrue(result.contains("web03.tmp.dc1.yahoo.com"));
    assertTrue(result.contains("web01.tmp.cd1.yahoo.com"));
    assertTrue(result.contains("web02.tmp.cd1.yahoo.com"));
    assertTrue(result.contains("web03.tmp.cd1.yahoo.com"));
  }
  
}
