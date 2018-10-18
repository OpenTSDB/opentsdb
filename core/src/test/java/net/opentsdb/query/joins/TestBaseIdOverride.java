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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;

public class TestBaseIdOverride {
  
  private static TimeSeriesByteId BASE;

  @BeforeClass
  public static void beforeClass() throws Exception {
    BASE = BaseTimeSeriesByteId
        .newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setAlias("orig".getBytes())
        .setNamespace("ns".getBytes())
        .setMetric("metric".getBytes())
        .addTags("host".getBytes(), "web01".getBytes())
        .addAggregatedTag("sassenach".getBytes())
        .addDisjointTag("owner".getBytes())
        .addUniqueId("uid".getBytes())
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    ByteIdOverride id = new ByteIdOverride(BASE, "slànge y va");
    
    assertArrayEquals("slànge y va".getBytes(Const.UTF8_CHARSET),
        id.alias());
    assertArrayEquals(BASE.namespace(), id.namespace());
    assertArrayEquals("slànge y va".getBytes(Const.UTF8_CHARSET), 
        id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals(BASE.tags().get("host".getBytes()), 
        id.tags().get("host".getBytes()));
    assertEquals(1, id.aggregatedTags().size());
    assertArrayEquals(BASE.aggregatedTags().get(0), 
        id.aggregatedTags().get(0));
    assertEquals(1, id.disjointTags().size());
    assertArrayEquals(BASE.disjointTags().get(0), 
        id.disjointTags().get(0));
    assertEquals(1, id.uniqueIds().size());
    assertArrayEquals(BASE.uniqueIds().iterator().next(), 
        id.uniqueIds().iterator().next());
    assertTrue(id.skipMetric());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final ByteIdOverride id1 = new ByteIdOverride(BASE, "slànge y va");
    ByteIdOverride id2 = new ByteIdOverride(BASE, "slànge y va");
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = new ByteIdOverride(BASE, "slànge y");
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    // this is ok, metric is overidden.
    TimeSeriesByteId base2 = BaseTimeSeriesByteId
        .newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setAlias("orig".getBytes())
        .setNamespace("ns".getBytes())
        .setMetric("metric2".getBytes()) // <-- DIFF
        .addTags("host".getBytes(), "web01".getBytes())
        .addAggregatedTag("sassenach".getBytes())
        .addDisjointTag("owner".getBytes())
        .addUniqueId("uid".getBytes())
        .build();
    id2 = new ByteIdOverride(base2, "slànge y va");
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    base2 = BaseTimeSeriesByteId
        .newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setAlias("orig".getBytes())
        .setNamespace("ns".getBytes())
        .setMetric("metric".getBytes())
        .addTags("host".getBytes(), "web02".getBytes()) // <-- DIFF
        .addAggregatedTag("sassenach".getBytes())
        .addDisjointTag("owner".getBytes())
        .addUniqueId("uid".getBytes())
        .build();
    id2 = new ByteIdOverride(base2, "slànge y va");
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
  }
}
