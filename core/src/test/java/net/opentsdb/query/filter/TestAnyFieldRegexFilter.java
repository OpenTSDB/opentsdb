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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.utils.JSON;

public class TestAnyFieldRegexFilter {

  @Test
  public void parse() throws Exception {
    MockTSDB tsdb = new MockTSDB();
    AnyFieldRegexFactory factory = new AnyFieldRegexFactory();
    String json = "{\"filter\":\"web.*\"}";

    JsonNode node = JSON.getMapper().readTree(json);
    AnyFieldRegexFilter filter = (AnyFieldRegexFilter)
            factory.parse(tsdb, JSON.getMapper(), node);
    assertEquals(".*", filter.getTagKey());
    assertEquals("web.*", filter.getFilter());

    try {
      factory.parse(tsdb, JSON.getMapper(), null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    // no filter
    json = "{}";
    node = JSON.getMapper().readTree(json);
    try {
      factory.parse(tsdb, JSON.getMapper(), node);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

//    // no tag key
//    json = "{\"filter\":\"web01|web02\"}";
//    node = JSON.getMapper().readTree(json);
//    try {
//      factory.parse(tsdb, JSON.getMapper(), node);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void builderAndMAtches() throws Exception {
    Map<String, String> tags = new HashMap<String, String>(1);
    tags.put(".*", "ogg-01.ops.ankh.morpork.com");

    AnyFieldRegexFilter filter = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();
    assertEquals("ogg-01.ops.ankh.morpork.com", filter.getFilter());
    assertTrue(filter.matches(tags));

    filter = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.*")
            .build();
    assertEquals(".*", filter.getTagKey());
    assertEquals("ogg-01.ops.ankh.*", filter.getFilter());
    assertTrue(filter.matches(tags));

    filter = AnyFieldRegexFilter.newBuilder()
            .setFilter(".*")
            .build();
    assertEquals(".*", filter.getTagKey());
    assertEquals(".*", filter.getFilter());
    assertTrue(filter.matches(tags));

    filter = AnyFieldRegexFilter.newBuilder()
            .setFilter("^.*")
            .build();
    assertEquals(".*", filter.getTagKey());
    assertEquals("^.*", filter.getFilter());
    assertTrue(filter.matches(tags));

    filter = AnyFieldRegexFilter.newBuilder()
            .setFilter(".*$")
            .build();
    assertEquals(".*", filter.getTagKey());
    assertEquals(".*$", filter.getFilter());
    assertTrue(filter.matches(tags));

    filter = AnyFieldRegexFilter.newBuilder()
            .setFilter("^.*$")
            .build();
    assertEquals(".*", filter.getTagKey());
    assertEquals("^.*$", filter.getFilter());
    assertTrue(filter.matches(tags));

    // trim
    filter = AnyFieldRegexFilter.newBuilder()
            .setFilter(" ogg-01.ops.ankh.* ")
            .build();
    assertEquals(".*", filter.getTagKey());
    assertEquals(" ogg-01.ops.ankh.* ", filter.getFilter());
    assertTrue(filter.matches(tags));



    try {
      AnyFieldRegexFilter.newBuilder()
              //.setFilter("web01")
              .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }


    // bad pattern
    try {
      AnyFieldRegexFilter.newBuilder()
              .setFilter("*noprefix")
              .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void serialize() throws Exception {
    AnyFieldRegexFilter filter = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.*")
            .build();

    final String json = JSON.serializeToString(filter);
    assertTrue(json.contains("\"filter\":\"ogg-01.ops.ankh.*\""));
    assertTrue(json.contains("\"tagKey\":\".*"));
    assertTrue(json.contains("\"type\":\"AnyFieldRegex"));
  }

  @Test
  public void initialize() throws Exception {
    AnyFieldRegexFilter filter = AnyFieldRegexFilter.newBuilder()
            .setFilter("ogg-01.ops.ankh.morpork.com")
            .build();
    assertNull(filter.initialize(null).join());
  }

}
