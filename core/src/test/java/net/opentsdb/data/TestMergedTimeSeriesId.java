// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestMergedTimeSeriesId {

  @Test
  public void alias() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .setAlias("Series A")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .setAlias("Series B")
        .setMetric("ice.dragon")
        .build();
    
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertNull(merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("Merged!")
        .build();
    assertEquals("Merged!", merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("")
        .build();
    assertEquals("", merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias((String) null)
        .build();
    assertNull(merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("Merged!")
        .build();
    assertEquals("Merged!", merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("000001")
        .build();
    assertEquals("000001", merged.alias());
  }
  
  @Test
  public void mergeNameSpaces() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .setNamespace("Tyrell")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .setNamespace("Lanister")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals("Tyrell", merged.namespace());

    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setNamespace("Dorne")
        .build();
    assertEquals("Dorne", merged.namespace());
  }

  @Test
  public void mergeMetrics() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .setMetric("Tyrell")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .setMetric("Lanister")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals("Tyrell", merged.metric());

    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setMetric("Stark")
        .build();
    assertEquals("Stark", merged.metric());
  }
  
  @Test
  public void mergeTagsSame() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(2, merged.tags().size());
    assertEquals("web01", 
        merged.tags().get("host"));
    assertEquals("lax", 
        merged.tags().get("colo"));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAgg1() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    
    assertEquals(1, merged.tags().size());
    assertEquals("lax", 
        merged.tags().get("colo"));
    assertEquals(1, merged.aggregatedTags().size());
    assertEquals("host", 
        merged.aggregatedTags().get(0));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAgg2() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lga")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertEquals(2, merged.aggregatedTags().size());
    assertEquals("colo", 
        merged.aggregatedTags().get(0));
    assertEquals("host", 
        merged.aggregatedTags().get(1));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsExistingAgg() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertEquals("lax", 
        merged.tags().get("colo"));
    assertEquals(1, merged.aggregatedTags().size());
    assertEquals("host", 
        merged.aggregatedTags().get(0));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsIncomingAgg() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertEquals("lax", 
        merged.tags().get("colo"));
    assertEquals(1, merged.aggregatedTags().size());
    assertEquals("host", 
        merged.aggregatedTags().get(0));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsExistingDisjoint() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addDisjointTag("host")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertEquals("lax", 
        merged.tags().get("colo"));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(1, merged.disjointTags().size());
    assertEquals("host", 
        merged.disjointTags().get(0));
  }
  
  @Test
  public void mergeTagsIncomingDisjoint() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addDisjointTag("host")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertEquals("lax", 
        merged.tags().get("colo"));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(1, merged.disjointTags().size());
    assertEquals("host", 
        merged.disjointTags().get(0));
  }
  
  @Test
  public void mergeTagsDisjoint1() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("owner", "Lanister")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertEquals("web01", 
        merged.tags().get("host"));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(2, merged.disjointTags().size());
    assertEquals("colo", 
        merged.disjointTags().get(0));
    assertEquals("owner", 
        merged.disjointTags().get(1));
  }
  
  @Test
  public void mergeTagsDisjoint2() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("dept", "KingsGaurd")
        .addTags("owner", "Lanister")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertTrue(merged.aggregatedTags().isEmpty());
    
    assertEquals(4, merged.disjointTags().size());
    assertEquals("colo", 
        merged.disjointTags().get(0));
    assertEquals("dept", 
        merged.disjointTags().get(1));
    assertEquals("host", 
        merged.disjointTags().get(2));
    assertEquals("owner", 
        merged.disjointTags().get(3));
  }

  @Test
  public void mergeTagsAlreadyAgged() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertEquals("web01", 
        merged.tags().get("host"));
    assertEquals(1, merged.aggregatedTags().size());
    assertEquals("colo", 
        merged.aggregatedTags().get(0));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAlreadyDisjoint() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addDisjointTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertEquals("web01", 
        merged.tags().get("host"));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(1, merged.disjointTags().size());
    assertEquals("colo", 
        merged.disjointTags().get(0));
  }

  @Test
  public void mergeTagsAlreadyAggedToDisjoint() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId c = BaseTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("dept", "KingsGaurd")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .addSeries(c)
        .build();
    assertEquals(1, merged.tags().size());
    assertEquals("web01", 
        merged.tags().get("host"));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(2, merged.disjointTags().size());
    assertEquals("colo", 
        merged.disjointTags().get(0));
    assertEquals("dept", 
        merged.disjointTags().get(1));
  }
  
  @Test
  public void mergeAggTagsSame() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertEquals(2, merged.aggregatedTags().size());
    assertEquals("colo", 
        merged.aggregatedTags().get(0));
    assertEquals("host", 
        merged.aggregatedTags().get(1));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeAggTagsDisjoint1() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("owner")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertEquals(1, merged.aggregatedTags().size());
    assertEquals("host", 
        merged.aggregatedTags().get(0));
    assertEquals(2, merged.disjointTags().size());
    assertEquals("colo", 
        merged.disjointTags().get(0));
    assertEquals("owner", 
        merged.disjointTags().get(1));
  }
  
  @Test
  public void mergeAggTagsDisjoint2() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addAggregatedTag("dept")
        .addAggregatedTag("owner")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(4, merged.disjointTags().size());
    assertEquals("colo", 
        merged.disjointTags().get(0));
    assertEquals("dept", 
        merged.disjointTags().get(1));
    assertEquals("host", 
        merged.disjointTags().get(2));
    assertEquals("owner", 
        merged.disjointTags().get(3));
  }

  @Test
  public void mergeDisjointTags() throws Exception {
    TimeSeriesId a = BaseTimeSeriesId.newBuilder()
        .addDisjointTag("host")
        .addDisjointTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId b = BaseTimeSeriesId.newBuilder()
        .addDisjointTag("host")
        .addDisjointTag("owner")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(3, merged.disjointTags().size());
    assertEquals("colo", 
        merged.disjointTags().get(0));
    assertEquals("host", 
        merged.disjointTags().get(1));
    assertEquals("owner", 
        merged.disjointTags().get(2));
  }
}
