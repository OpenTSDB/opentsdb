// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.common.Const;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;

public class TestMergedTimeSeriesId {
  private static final byte[] BYTES_1 = "Tyrell".getBytes();
  private static final byte[] BYTES_1_ALT = "Tyrell".getBytes();
  private static final byte[] BYTES_2 = "Lanister".getBytes();
  private static final byte[] BYTES_2_ALT = "Lanister".getBytes();
  private static final byte[] BYTES_3 = "Medici".getBytes();
  private static final byte[] BYTES_3_ALT = "Medici".getBytes();
  private static final byte[] FAMILY = "Family".getBytes();
  private static final byte[] DRAGON = "Dragon".getBytes();
  private static final byte[] DRAGON_ALT = "Dragon".getBytes();
  private static final byte[] DRAGON_1 = "Drogon".getBytes();
  private static final byte[] DRAGON_1_ALT = "Drogon".getBytes();
  private static final byte[] DRAGON_2 = "Rhaegal".getBytes();
  private static final byte[] DRAGON_2_ALT = "Rhaegal".getBytes();
  private static final byte[] METRIC = "ice.dragon".getBytes();
  private static final byte[] METRIC_ALT = "ice.dragon".getBytes();
  
  private ReadableTimeSeriesDataStore data_store;
  
  @Before
  public void before() throws Exception {
    data_store = mock(ReadableTimeSeriesDataStore.class);
  }
  
  @Test
  public void setAlias() throws Exception {
    MergedTimeSeriesId.Builder builder = MergedTimeSeriesId.newBuilder();
    assertNull(builder.alias);
    
    builder.setAlias("alias");
    assertArrayEquals("alias".getBytes(Const.UTF8_CHARSET), builder.alias);
    
    builder.setAlias((String) null);
    assertNull(builder.alias);
    
    builder.setAlias(new byte[] { 'h', 'i' });
    assertArrayEquals("hi".getBytes(Const.UTF8_CHARSET), builder.alias);
  }
  
  @Test
  public void setNamespace() throws Exception {
    MergedTimeSeriesId.Builder builder = MergedTimeSeriesId.newBuilder();
    assertNull(builder.namespace);
    
    builder.setNamespace("namespace");
    assertArrayEquals("namespace".getBytes(Const.UTF8_CHARSET), builder.namespace);
    
    builder.setNamespace((String) null);
    assertNull(builder.namespace);
    
    builder.setNamespace(new byte[] { 'n', 's' });
    assertArrayEquals("ns".getBytes(Const.UTF8_CHARSET), builder.namespace);
  }
  
  @Test
  public void setMetric() throws Exception {
    MergedTimeSeriesId.Builder builder = MergedTimeSeriesId.newBuilder();
    assertNull(builder.metric);
    
    builder.setMetric("metric");
    assertArrayEquals("metric".getBytes(Const.UTF8_CHARSET), builder.metric);
    
    builder.setMetric((String) null);
    assertNull(builder.metric);
    
    builder.setMetric(new byte[] { 'm', 'e', 't' });
    assertArrayEquals("met".getBytes(Const.UTF8_CHARSET), builder.metric);
  }
  
  @Test
  public void addSeries() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .setNamespace("Tyrell")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .setNamespace("Lanister")
        .setMetric("ice.dragon")
        .build();
    
    MergedTimeSeriesId.Builder builder = MergedTimeSeriesId.newBuilder()
      .addSeries(a)
      .addSeries(b);
    assertEquals(2, builder.ids.size());
    assertEquals(Const.TS_STRING_ID, builder.type);
    
    try {
      builder.addSeries(mock(TimeSeriesByteId.class));
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
    assertEquals(2, builder.ids.size());
    assertEquals(Const.TS_STRING_ID, builder.type);
    
    try {
      builder.addSeries(null);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) { }
    assertEquals(2, builder.ids.size());
    assertEquals(Const.TS_STRING_ID, builder.type);
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .setNamespace(BYTES_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .setNamespace(BYTES_2)
        .setMetric(METRIC_ALT)
        .build();
    builder = MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d);
      assertEquals(2, builder.ids.size());
      assertEquals(Const.TS_BYTE_ID, builder.type);
      
      d = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
          .setNamespace(BYTES_2_ALT)
          .setMetric(METRIC)
          .build();
      try {
        MergedTimeSeriesId.newBuilder()
          .addSeries(c)
          .addSeries(d)
          .build();
        fail("Expected RuntimeException");
      } catch (RuntimeException e) { }
  }
  
  @Test
  public void alias() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .setAlias("Series A")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .setAlias("Series B")
        .setMetric("ice.dragon")
        .build();
    
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertNull(merged.alias());
    
    merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("Merged!")
        .build();
    assertEquals("Merged!", merged.alias());
    
    merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("")
        .build();
    assertNull(merged.alias());
    
    merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias((String) null)
        .build();
    assertNull(merged.alias());
    
    merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("Merged!")
        .build();
    assertEquals("Merged!", merged.alias());
    
    merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setAlias("000001")
        .build();
    assertEquals("000001", merged.alias());
  }
  
  @Test
  public void mergeNameSpaces() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .setNamespace("Tyrell")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .setNamespace("Lanister")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals("Tyrell", merged.namespace());

    merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setNamespace("Dorne")
        .build();
    assertEquals("Dorne", merged.namespace());
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .setNamespace(BYTES_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .setNamespace(BYTES_2)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertArrayEquals(BYTES_1, merged_bytes.namespace());
    
    merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .setNamespace(BYTES_3)
        .addSeries(c)
        .addSeries(d)
        .build();
    assertArrayEquals(BYTES_3, merged_bytes.namespace());
  }

  @Test
  public void mergeMetrics() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .setMetric("Tyrell")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .setMetric("Lanister")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .build();
    assertEquals("Tyrell", merged.metric());

    merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
        .addSeries(a)
        .addSeries(b)
        .setMetric("Stark")
        .build();
    assertEquals("Stark", merged.metric());
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .setMetric(BYTES_1)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .setMetric(BYTES_2)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertArrayEquals(BYTES_1, merged_bytes.metric());
    
    merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .setMetric(BYTES_3)
        .addSeries(c)
        .addSeries(d)
        .build();
    assertArrayEquals(BYTES_3, merged_bytes.metric());
  }
  
  @Test
  public void mergeTagsSame() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1_ALT)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(2, merged_bytes.tags().size());
    assertArrayEquals(BYTES_1, merged_bytes.tags().get(FAMILY));
    assertArrayEquals(DRAGON_1, merged_bytes.tags().get(DRAGON));
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertTrue(merged_bytes.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAgg1() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_2)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(1, merged_bytes.tags().size());
    assertArrayEquals(DRAGON_1, merged_bytes.tags().get(DRAGON));
    assertEquals(1, merged_bytes.aggregatedTags().size());
    assertArrayEquals(FAMILY, merged_bytes.aggregatedTags().get(0));
    assertTrue(merged_bytes.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAgg2() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lga")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON, DRAGON_2)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_2_ALT)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertTrue(merged_bytes.tags().isEmpty());
    assertEquals(2, merged_bytes.aggregatedTags().size());
    assertArrayEquals(DRAGON, merged_bytes.aggregatedTags().get(0));
    assertArrayEquals(FAMILY, merged_bytes.aggregatedTags().get(1));
    assertTrue(merged_bytes.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsExistingAgg() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("host")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addAggregatedTag(FAMILY)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_2)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals(DRAGON_1, merged_bytes.tags().get(DRAGON));
    assertEquals(1, merged.aggregatedTags().size());
    assertArrayEquals(FAMILY, merged_bytes.aggregatedTags().get(0));
    assertTrue(merged_bytes.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsIncomingAgg() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("host")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addAggregatedTag(FAMILY)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals(DRAGON_1, merged_bytes.tags().get(DRAGON));
    assertEquals(1, merged.aggregatedTags().size());
    assertArrayEquals(FAMILY, merged_bytes.aggregatedTags().get(0));
    assertTrue(merged_bytes.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsExistingDisjoint() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addDisjointTag("host")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addDisjointTag(FAMILY)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals(DRAGON_1, merged_bytes.tags().get(DRAGON));
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertEquals(1, merged.disjointTags().size());
    assertArrayEquals(FAMILY, merged_bytes.disjointTags().get(0));
  }
  
  @Test
  public void mergeTagsIncomingDisjoint() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addDisjointTag("host")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addDisjointTag(FAMILY)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals(DRAGON_1, merged_bytes.tags().get(DRAGON));
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertEquals(1, merged.disjointTags().size());
    assertArrayEquals(FAMILY, merged_bytes.disjointTags().get(0));
  }
  
  @Test
  public void mergeTagsDisjoint1() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("owner", "Lanister")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1_ALT)
        .addTags(BYTES_2_ALT, BYTES_3_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(1, merged.tags().size());
    assertArrayEquals(BYTES_1, merged_bytes.tags().get(FAMILY));
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertEquals(2, merged.disjointTags().size());
    assertArrayEquals(DRAGON, merged_bytes.disjointTags().get(0));
    assertArrayEquals(BYTES_2, merged_bytes.disjointTags().get(1));
  }
  
  @Test
  public void mergeTagsDisjoint2() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("dept", "KingsGaurd")
        .addTags("owner", "Lanister")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(BYTES_3, DRAGON_2)
        .addTags(BYTES_2_ALT, BYTES_3_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertTrue(merged_bytes.tags().isEmpty());
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertEquals(4, merged.disjointTags().size());
    assertArrayEquals(DRAGON, merged_bytes.disjointTags().get(0));
    assertArrayEquals(FAMILY, merged_bytes.disjointTags().get(1));
    assertArrayEquals(BYTES_2, merged_bytes.disjointTags().get(2));
    assertArrayEquals(BYTES_3, merged_bytes.disjointTags().get(3));
  }

  @Test
  public void mergeTagsAlreadyAgged() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON, DRAGON_1)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1_ALT)
        .addAggregatedTag(DRAGON_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(1, merged_bytes.tags().size());
    assertArrayEquals(BYTES_1, merged_bytes.tags().get(FAMILY));
    assertEquals(1, merged_bytes.aggregatedTags().size());
    assertArrayEquals(DRAGON, merged_bytes.aggregatedTags().get(0));
    assertTrue(merged_bytes.disjointTags().isEmpty());
  }
  
  @Test
  public void mergeTagsAlreadyDisjoint() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addDisjointTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addDisjointTag(DRAGON)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1_ALT)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertEquals(1, merged_bytes.tags().size());
    assertArrayEquals(BYTES_1, merged_bytes.tags().get(FAMILY));
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertEquals(1, merged_bytes.disjointTags().size());
    assertEquals(DRAGON, merged_bytes.disjointTags().get(0));
  }

  @Test
  public void mergeTagsAlreadyAggedToDisjoint() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId c = BaseTimeSeriesStringId.newBuilder()
        .addTags("host", "web01")
        .addTags("dept", "KingsGaurd")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addDisjointTag(DRAGON)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId e = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(DRAGON_ALT, DRAGON_1_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId f = BaseTimeSeriesByteId.newBuilder(data_store)
        .addTags(FAMILY, BYTES_1)
        .addTags(BYTES_3, DRAGON_2_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(d)
        .addSeries(e)
        .addSeries(f)
        .build();
    assertEquals(1, merged_bytes.tags().size());
    assertArrayEquals(BYTES_1, merged_bytes.tags().get(FAMILY));
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertEquals(2, merged_bytes.disjointTags().size());
    assertEquals(DRAGON, merged_bytes.disjointTags().get(0));
    assertEquals(BYTES_3, merged_bytes.disjointTags().get(1));
  }
  
  @Test
  public void mergeAggTagsSame() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addAggregatedTag(FAMILY)
        .addAggregatedTag(DRAGON)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addAggregatedTag(FAMILY)
        .addAggregatedTag(DRAGON_ALT)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertTrue(merged_bytes.tags().isEmpty());
    assertEquals(2, merged_bytes.aggregatedTags().size());
    assertArrayEquals(DRAGON, merged_bytes.aggregatedTags().get(0));
    assertArrayEquals(FAMILY, merged_bytes.aggregatedTags().get(1));
  }
  
  @Test
  public void mergeAggTagsDisjoint1() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("owner")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addAggregatedTag(FAMILY)
        .addAggregatedTag(DRAGON)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addAggregatedTag(FAMILY)
        .addAggregatedTag(BYTES_3)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertTrue(merged_bytes.tags().isEmpty());
    assertEquals(1, merged_bytes.aggregatedTags().size());
    assertArrayEquals(FAMILY, merged_bytes.aggregatedTags().get(0));
    assertEquals(2, merged_bytes.disjointTags().size());
    assertArrayEquals(DRAGON, merged_bytes.disjointTags().get(0));
    assertArrayEquals(BYTES_3, merged_bytes.disjointTags().get(1));
  }
  
  @Test
  public void mergeAggTagsDisjoint2() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("host")
        .addAggregatedTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addAggregatedTag("dept")
        .addAggregatedTag("owner")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addAggregatedTag(FAMILY)
        .addAggregatedTag(DRAGON)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addAggregatedTag(BYTES_1)
        .addAggregatedTag(BYTES_3)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertTrue(merged_bytes.tags().isEmpty());
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertEquals(4, merged_bytes.disjointTags().size());
    assertArrayEquals(DRAGON, merged_bytes.disjointTags().get(0));
    assertArrayEquals(FAMILY, merged_bytes.disjointTags().get(1));
    assertArrayEquals(BYTES_3, merged_bytes.disjointTags().get(2));
    assertArrayEquals(BYTES_1, merged_bytes.disjointTags().get(3));
  }

  @Test
  public void mergeDisjointTags() throws Exception {
    TimeSeriesStringId a = BaseTimeSeriesStringId.newBuilder()
        .addDisjointTag("host")
        .addDisjointTag("colo")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId b = BaseTimeSeriesStringId.newBuilder()
        .addDisjointTag("host")
        .addDisjointTag("owner")
        .setMetric("ice.dragon")
        .build();
    TimeSeriesStringId merged = (TimeSeriesStringId) MergedTimeSeriesId.newBuilder()
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
    
    TimeSeriesByteId c = BaseTimeSeriesByteId.newBuilder(data_store)
        .addDisjointTag(FAMILY)
        .addDisjointTag(DRAGON)
        .setMetric(METRIC)
        .build();
    TimeSeriesByteId d = BaseTimeSeriesByteId.newBuilder(data_store)
        .addDisjointTag(FAMILY)
        .addDisjointTag(BYTES_3)
        .setMetric(METRIC_ALT)
        .build();
    TimeSeriesByteId merged_bytes = (TimeSeriesByteId) MergedTimeSeriesId.newBuilder()
        .addSeries(c)
        .addSeries(d)
        .build();
    assertTrue(merged_bytes.tags().isEmpty());
    assertTrue(merged_bytes.aggregatedTags().isEmpty());
    assertEquals(3, merged_bytes.disjointTags().size());
    assertArrayEquals(DRAGON, merged_bytes.disjointTags().get(0));
    assertArrayEquals(FAMILY, merged_bytes.disjointTags().get(1));
    assertArrayEquals(BYTES_3, merged_bytes.disjointTags().get(2));
  }
}
