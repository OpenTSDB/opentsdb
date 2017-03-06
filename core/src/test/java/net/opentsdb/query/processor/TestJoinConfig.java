// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.processor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.common.Const;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.query.pojo.Join.SetOperator;

public class TestJoinConfig {
  
  @Test
  public void builder() throws Exception {
    JoinConfig config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setTags(Lists.newArrayList("host", "colo"))
              .build())
          .build();
    assertEquals(SetOperator.UNION, config.getJoin().getOperator());
    assertEquals(2, config.getJoin().getTags().size());
    assertTrue(config.getJoin().getTags().contains("host"));
    assertTrue(config.getJoin().getTags().contains("colo"));
    assertEquals(2, config.getTagKeys().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        config.getTagKeys().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        config.getTagKeys().get(1));
    
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.INTERSECTION)
              .setUseQueryTags(true)
              .build())
          .setFilters(Filter.newBuilder()
              .setId("f1")
              .setTags(Lists.newArrayList(
                  TagVFilter.Builder()
                  .setFilter("*")
                  .setType("wildcard")
                  .setTagk("host")
                  .build()))
              .build())
          .build();
    assertEquals(SetOperator.INTERSECTION, config.getJoin().getOperator());
    assertEquals("f1", config.getFilters().getId());
    assertEquals(1, config.getTagKeys().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        config.getTagKeys().get(0));
    
    // missing filters
    try {
      config = (JoinConfig) 
          JoinConfig.newBuilder()
            .setJoin(Join.newBuilder()
                .setOperator(SetOperator.INTERSECTION)
                .setUseQueryTags(true)
                .build())
            .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config = (JoinConfig) 
          JoinConfig.newBuilder()
            .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final JoinConfig c1 = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.INTERSECTION)
              .setUseQueryTags(true)
              .build())
          .setFilters(Filter.newBuilder()
              .setId("f1")
              .setTags(Lists.newArrayList(
                  TagVFilter.Builder()
                  .setFilter("*")
                  .setType("wildcard")
                  .setTagk("host")
                  .build()))
              .build())
          .build();
    
    JoinConfig c2 = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.INTERSECTION)
              .setUseQueryTags(true)
              .build())
          .setFilters(Filter.newBuilder()
              .setId("f1")
              .setTags(Lists.newArrayList(
                  TagVFilter.Builder()
                  .setFilter("*")
                  .setType("wildcard")
                  .setTagk("host")
                  .build()))
              .build())
          .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION) // <-- Diff
              .setUseQueryTags(true)
              .build())
          .setFilters(Filter.newBuilder()
              .setId("f1")
              .setTags(Lists.newArrayList(
                  TagVFilter.Builder()
                  .setFilter("*")
                  .setType("wildcard")
                  .setTagk("host")
                  .build()))
              .build())
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.INTERSECTION)
              .setUseQueryTags(true)
              .build())
          .setFilters(Filter.newBuilder()
              .setId("f2") // <-- Diff
              .setTags(Lists.newArrayList(
                  TagVFilter.Builder()
                  .setFilter("*")
                  .setType("wildcard")
                  .setTagk("host")
                  .build()))
              .build())
          .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    // more detailed tests are in the Join and Filter classes.
  }
}
