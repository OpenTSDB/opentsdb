// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.pojo;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.utils.JSON;

import org.junit.Test;

import com.google.common.collect.Lists;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TestFilter {

  @Test(expected = IllegalArgumentException.class)
  public void validationErrorWhenIdIsNull() throws Exception {
    String json = "{\"id\":null}";
    Filter filter = JSON.parseToObject(json, Filter.class);
    filter.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void validationBadId() throws Exception {
    String json = "{\"id\":\"bad.Id\",\"tags\":[]}";
    Filter filter = JSON.parseToObject(json, Filter.class);
    filter.validate();
  }
  
  @Test
  public void deserialize() throws Exception {
    String json = "{\"id\":\"f1\",\"tags\":[{\"tagk\":\"host\","
        + "\"filter\":\"*\",\"type\":\"iwildcard\",\"groupBy\":false}],"
        + "\"explicitTags\":\"true\"}";

    TagVFilter tag = new TagVFilter.Builder().setFilter("*").setGroupBy(
        false)
        .setTagk("host").setType("iwildcard").build();

    Filter expectedFilter = Filter.newBuilder().setId("f1")
        .setTags(Arrays.asList(tag)).setExplicitTags(true).build();

    Filter filter = JSON.parseToObject(json, Filter.class);
    filter.validate();
    assertEquals(expectedFilter, filter);
  }

  @Test
  public void serialize() throws Exception {
    TagVFilter tag = new TagVFilter.Builder().setFilter("*").setGroupBy(false)
        .setTagk("host").setType("iwildcard").build();

    Filter filter = Filter.newBuilder().setId("f1")
        .setTags(Arrays.asList(tag)).setExplicitTags(true).build();

    String actual = JSON.serializeToString(filter);
    assertTrue(actual.contains("\"id\":\"f1\""));
    assertTrue(actual.contains("\"tags\":["));
    assertTrue(actual.contains("\"tagk\":\"host\""));
    assertTrue(actual.contains("\"explicitTags\":true"));
  }

  @Test
  public void unknownShouldBeIgnored() throws Exception {
    String json = "{\"id\":\"1\",\"unknown\":\"yo\"}";
    JSON.parseToObject(json, Filter.class);
    // pass if no unexpected exception
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidTags() throws Exception {
    String json = "{\"id\":\"1\",\"tags\":[{\"tagk\":\"\","
        + "\"filter\":\"*\",\"type\":\"iwildcard\",\"group_by\":false}],"
        + "\"aggregation\":{\"tags\":[\"appid\"],\"aggregator\":\"sum\"}}";

    Filter filter = JSON.parseToObject(json, Filter.class);
    filter.validate();
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidAggregation() throws Exception {
    String json = "{\"id\":\"1\",\"tags\":[{\"tagk\":\"\","
        + "\"filter\":\"*\",\"type\":\"iwildcard\",\"group_by\":false}],"
        + "\"aggregator\":\"what\"}";
    Filter filter = JSON.parseToObject(json, Filter.class);
    filter.validate();
  }

  @Test
  public void build() throws Exception{
    final Filter filter = Filter.newBuilder()
        .setId("f1")
        .addFilter(new TagVFilter.Builder()
            .setFilter("*")
            .setGroupBy(false)
            .setTagk("host")
            .setType("iwildcard"))
        .addFilter(new TagVFilter.Builder()
            .setFilter("*")
            .setGroupBy(true)
            .setTagk("datacenter")
            .setType("wildcard"))
        .setExplicitTags(true)
        .build();
    
    final Filter clone = Filter.newBuilder(filter).build();
    assertNotSame(clone, filter);
    assertEquals("f1", clone.getId());
    assertTrue(clone.getExplicitTags());
    assertEquals("host", clone.getTags().get(0).getTagk());
    assertEquals("datacenter", clone.getTags().get(1).getTagk());
  }
  
  public void hashCodeEqualsCompareTo() throws Exception {
    final Filter f1 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(false)
              .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    
    Filter f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(false)
              .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    assertEquals(f1.hashCode(), f2.hashCode());
    assertEquals(f1, f2);
    assertEquals(0, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        // use add method
        .addFilter(new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(false))
        .addFilter(new TagVFilter.Builder()
            .setFilter("phx*")
            .setTagk("dc")
            .setType("wildcard")
            .setGroupBy(true))
        .build();
    assertEquals(f1.hashCode(), f2.hashCode());
    assertEquals(f1, f2);
    assertEquals(0, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        // use add method. Order is different!
        .addFilter(new TagVFilter.Builder()
            .setFilter("phx*")
            .setTagk("dc")
            .setType("wildcard")
            .setGroupBy(true))
        .addFilter(new TagVFilter.Builder()
            .setFilter("web01")
            .setTagk("host")
            .setType("literal_or")
            .setGroupBy(false))
        .build();
    assertEquals(f1.hashCode(), f2.hashCode());
    assertEquals(f1, f2);
    assertEquals(0, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f2")  // <-- diff
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(false)
              .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    assertNotEquals(f1.hashCode(), f2.hashCode());
    assertNotEquals(f1, f2);
    assertEquals(-1, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(true)  // <-- diff
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(false)
              .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    assertNotEquals(f1.hashCode(), f2.hashCode());
    assertNotEquals(f1, f2);
    assertEquals(1, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()
              .setFilter("web02")  // <-- diff
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(false)
              .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    assertNotEquals(f1.hashCode(), f2.hashCode());
    assertNotEquals(f1, f2);
    assertEquals(-1, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(true)  // <-- diff
              .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    assertNotEquals(f1.hashCode(), f2.hashCode());
    assertNotEquals(f1, f2);
    assertEquals(1, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()  // <-- order changes, should be good!
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build(),
            new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(false)
              .build())
            )
        .build();
    assertEquals(f1.hashCode(), f2.hashCode());
    assertEquals(f1, f2);
    assertEquals(0, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            new TagVFilter.Builder()
              .setFilter("web01")
              .setTagk("host")
              .setType("literal_or")
              .setGroupBy(true)  // <-- diff
              .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    assertNotEquals(f1.hashCode(), f2.hashCode());
    assertNotEquals(f1, f2);
    assertEquals(1, f1.compareTo(f2));
    
    f2 = new Filter.Builder()
        .setId("f1")
        .setExplicitTags(false)
        .setTags(Lists.newArrayList(
            //new TagVFilter.Builder()  // <-- diff
            //  .setFilter("web01")
            //  .setTagk("host")
            //  .setType("literal_or")
            //  .setGroupBy(false)
            //  .build(),
            new TagVFilter.Builder()
              .setFilter("phx*")
              .setTagk("dc")
              .setType("wildcard")
              .setGroupBy(true)
              .build()))
        .build();
    assertNotEquals(f1.hashCode(), f2.hashCode());
    assertNotEquals(f1, f2);
    assertEquals(-1, f1.compareTo(f2));
  }
  
}
