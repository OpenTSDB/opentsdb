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
package net.opentsdb.query.joins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesStringId;

public class TestStringIdOverride {

  private static TimeSeriesStringId BASE;

  @BeforeClass
  public static void beforeClass() throws Exception {
    BASE = BaseTimeSeriesStringId.newBuilder()
        .setAlias("orig")
        .setNamespace("ns")
        .setMetric("metric")
        .addTags("host", "web01")
        .addAggregatedTag("sassenach")
        .addDisjointTag("owner")
        .addUniqueId("uid")
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    StringIdOverride id = new StringIdOverride(BASE, "slànge y va");
    
    assertEquals("slànge y va", id.alias());
    assertEquals(BASE.namespace(), id.namespace());
    assertEquals("slànge y va", id.metric());
    assertEquals(1, id.tags().size());
    assertEquals(BASE.tags().get("host"), id.tags().get("host"));
    assertEquals(1, id.aggregatedTags().size());
    assertEquals(BASE.aggregatedTags().get(0), 
        id.aggregatedTags().get(0));
    assertEquals(1, id.disjointTags().size());
    assertEquals(BASE.disjointTags().get(0), 
        id.disjointTags().get(0));
    assertEquals(1, id.uniqueIds().size());
    assertEquals(BASE.uniqueIds().iterator().next(), 
        id.uniqueIds().iterator().next());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final StringIdOverride id1 = new StringIdOverride(BASE, "slànge y va");
    StringIdOverride id2 = new StringIdOverride(BASE, "slànge y va");
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = new StringIdOverride(BASE, "slànge y");
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    // this is ok, metric is overidden.
    TimeSeriesStringId base2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("orig")
        .setNamespace("ns")
        .setMetric("woops") // <-- DIFF
        .addTags("host", "web01")
        .addAggregatedTag("sassenach")
        .addDisjointTag("owner")
        .addUniqueId("uid")
        .build();
    id2 = new StringIdOverride(base2, "slànge y va");
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    base2 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("orig")
        .setNamespace("ns")
        .setMetric("metric")
        .addTags("host", "web02") // <-- DIFF
        .addAggregatedTag("sassenach")
        .addDisjointTag("owner")
        .addUniqueId("uid")
        .build();
    id2 = new StringIdOverride(base2, "slànge y va");
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
  }
}
