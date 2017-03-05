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
package net.opentsdb.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.common.Const;

public class TestMergedTimeSeriesId {

  @Test
  public void alias() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("Series A")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .setAlias("Series B")
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
    assertArrayEquals("Merged!".getBytes(Const.UTF8_CHARSET), merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("")
        .build();
    assertNull(merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias((String) null)
        .build();
    assertNull(merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("Merged!".getBytes(Const.UTF8_CHARSET))
        .build();
    assertArrayEquals("Merged!".getBytes(Const.UTF8_CHARSET), merged.alias());
    
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias(new byte[] { 0, 0, 1 })
        .build();
    assertArrayEquals(new byte[] { 0, 0, 1 }, merged.alias());
  }
  
  @Test
  public void mergeNameSpaces() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(Lists.newArrayList("Tyrell", "Frey", "Dorne"))
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(Lists.newArrayList("Lanister", "Frey", "Dorne"))
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(4, merged.namespaces().size());
    assertArrayEquals("Dorne".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(1));
    assertArrayEquals("Lanister".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(2));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(3));

    b = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(new ArrayList<String>())
        .build();
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(3, merged.namespaces().size());
    assertArrayEquals("Dorne".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(2));
    
    b = SimpleStringTimeSeriesId.newBuilder()
        .build();
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(3, merged.namespaces().size());
    assertArrayEquals("Dorne".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), merged.namespaces().get(2));
    
    a = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(new ArrayList<String>())
        .build();
    b = SimpleStringTimeSeriesId.newBuilder()
        .setNamespaces(new ArrayList<String>())
        .build();
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.namespaces().isEmpty());
  }

  @Test
  public void mergeMetrics() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("Tyrell", "Frey", "Dorne"))
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("Lanister", "Frey", "Dorne"))
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(4, merged.metrics().size());
    assertArrayEquals("Dorne".getBytes(Const.UTF8_CHARSET), merged.metrics().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), merged.metrics().get(1));
    assertArrayEquals("Lanister".getBytes(Const.UTF8_CHARSET), merged.metrics().get(2));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), merged.metrics().get(3));

    b = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(new ArrayList<String>())
        .build();
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(3, merged.metrics().size());
    assertArrayEquals("Dorne".getBytes(Const.UTF8_CHARSET), merged.metrics().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), merged.metrics().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), merged.metrics().get(2));
    
    b = SimpleStringTimeSeriesId.newBuilder()
        .build();
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(3, merged.metrics().size());
    assertArrayEquals("Dorne".getBytes(Const.UTF8_CHARSET), merged.metrics().get(0));
    assertArrayEquals("Frey".getBytes(Const.UTF8_CHARSET), merged.metrics().get(1));
    assertArrayEquals("Tyrell".getBytes(Const.UTF8_CHARSET), merged.metrics().get(2));
    
    a = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(new ArrayList<String>())
        .build();
    b = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(new ArrayList<String>())
        .build();
    merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.metrics().isEmpty());
  }
  
  @Test
  public void mergeTagsSame() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(2, merged.tags().size());
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAgg1() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    System.out.println(merged);
    assertEquals(1, merged.tags().size());
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    assertEquals(1, merged.aggregatedTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(0));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAgg2() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lga")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertEquals(2, merged.aggregatedTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(1));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsExistingAgg() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    assertEquals(1, merged.aggregatedTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(0));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsIncomingAgg() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    assertEquals(1, merged.aggregatedTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(0));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsExistingDisjoint() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addDisjointTag("host")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(1, merged.disjointTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
  }
  
  @Test
  public void mergeTagsIncomingDisjoint() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addDisjointTag("host")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals("lax".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("colo".getBytes(Const.UTF8_CHARSET)));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(1, merged.disjointTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
  }
  
  @Test
  public void mergeTagsDisjoint1() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("owner", "Lanister")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(2, merged.disjointTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
    assertArrayEquals("owner".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(1));
  }
  
  @Test
  public void mergeTagsDisjoint2() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("dept", "KingsGaurd")
        .addTags("owner", "Lanister")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertTrue(merged.aggregatedTags().isEmpty());
    
    assertEquals(4, merged.disjointTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
    assertArrayEquals("dept".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(1));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(2));
    assertArrayEquals("owner".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(3));
  }

  @Test
  public void mergeTagsAlreadyAgged() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertEquals(1, merged.aggregatedTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(0));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAlreadyDisjoint() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addDisjointTag("colo")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(1, merged.disjointTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
  }

  @Test
  public void mergeTagsAlreadyAggedToDisjoint() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    TimeSeriesId c = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("dept", "KingsGaurd")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .addSeries(c)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals("web01".getBytes(Const.UTF8_CHARSET), 
        merged.tags().get("host".getBytes(Const.UTF8_CHARSET)));
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(2, merged.disjointTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
    assertArrayEquals("dept".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(1));
  }
  
  @Test
  public void mergeAggTagsSame() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertEquals(2, merged.aggregatedTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(1));
    assertTrue(merged.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeAggTagsDisjoint1() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("owner")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertEquals(1, merged.aggregatedTags().size());
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.aggregatedTags().get(0));
    assertEquals(2, merged.disjointTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
    assertArrayEquals("owner".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(1));
  }
  
  @Test
  public void mergeAggTagsDisjoint2() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addAggregatedTag("dept")
        .addAggregatedTag("owner")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(4, merged.disjointTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
    assertArrayEquals("dept".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(1));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(2));
    assertArrayEquals("owner".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(3));
  }

  @Test
  public void mergeDisjointTags() throws Exception {
    TimeSeriesId a = SimpleStringTimeSeriesId.newBuilder()
        .addDisjointTag("host")
        .addDisjointTag("colo")
        .build();
    TimeSeriesId b = SimpleStringTimeSeriesId.newBuilder()
        .addDisjointTag("host")
        .addDisjointTag("owner")
        .build();
    TimeSeriesId merged = MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertTrue(merged.tags().isEmpty());
    assertTrue(merged.aggregatedTags().isEmpty());
    assertEquals(3, merged.disjointTags().size());
    assertArrayEquals("colo".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(0));
    assertArrayEquals("host".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(1));
    assertArrayEquals("owner".getBytes(Const.UTF8_CHARSET), 
        merged.disjointTags().get(2));
  }
}
