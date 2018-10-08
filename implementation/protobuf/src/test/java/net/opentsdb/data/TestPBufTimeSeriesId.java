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
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.data.pbuf.TimeSeriesIdPB;

public class TestPBufTimeSeriesId {

  @Test
  public void newBuilderFromID() throws Exception {
    PBufTimeSeriesId id = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
          .setAlias("alias")
          .setNamespace("oath")
          .setMetric("metric.foo")
          .addTags("host", "web01")
          .addTags("danger", "will")
          .addAggregatedTag("owner")
          .addDisjointTag("colo")
          .addUniqueId("tyrion")
          .build()
        ).build();
    
    assertEquals("alias", id.alias());
    assertEquals("oath", id.namespace());
    assertEquals("metric.foo", id.metric());
    assertEquals(2, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals("will", id.tags().get("danger"));
    assertEquals(1, id.aggregatedTags().size());
    assertTrue(id.aggregatedTags().contains("owner"));
    assertEquals(1, id.disjointTags().size());
    assertTrue(id.disjointTags().contains("colo"));
    assertEquals(1, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains("tyrion"));
  }
  
  @Test
  public void newBuilderFromProtobuf() throws Exception {
    TimeSeriesIdPB.TimeSeriesId source = 
        TimeSeriesIdPB.TimeSeriesId.newBuilder()
          .setAlias("alias")
          .setNamespace("oath")
          .setMetric("metric.foo")
          .putTags("host", "web01")
          .putTags("danger", "will")
          .addAggregatedTags("owner")
          .addDisjointTags("colo")
          .addUniqueIds("tyrion")
          .build();
    
    PBufTimeSeriesId id = new PBufTimeSeriesId(source);
    assertEquals("alias", id.alias());
    assertEquals("oath", id.namespace());
    assertEquals("metric.foo", id.metric());
    assertEquals(2, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals("will", id.tags().get("danger"));
    assertEquals(1, id.aggregatedTags().size());
    assertTrue(id.aggregatedTags().contains("owner"));
    assertEquals(1, id.disjointTags().size());
    assertTrue(id.disjointTags().contains("colo"));
    assertEquals(1, id.uniqueIds().size());
    assertTrue(id.uniqueIds().contains("tyrion"));
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    Map<String, String> tags = Maps.newHashMap();
    tags.put("host", "web01");
    tags.put("colo", "lax");
    
    final BaseTimeSeriesStringId id1 = BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build();
    
    PBufTimeSeriesId id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID2") // <-- DIFF
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("Yahoo") // <-- DIFF
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    
    PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        //.setNamespace("OpenTSDB") // <-- IDFF
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.idle") // <-- DIFF
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    Map<String, String> tags2 = Maps.newHashMap();
    tags.put("host", "web02"); // <-- Diff
    tags.put("colo", "lax");
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags2)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    tags2 = Maps.newHashMap();
    tags.put("host", "web01");
    //tags.put("colo", "lax"); // <-- Diff
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags2)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        //.setTags(tags) // <-- DIFF
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner")) // <-- DIFF
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(-1, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        //.setAggregatedTags(Lists.newArrayList("owner", "role")) // <-- DIFF
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("role", "owner")) // <-- DIFF order ok
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery")) // <-- DIFF
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        //.setDisjointTags(Lists.newArrayList("propery", "type")) // <-- DIFF
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("type", "propery")) // <-- DIFF Order ok
        .addUniqueId("000001")
        .addUniqueId("000002")
        .build()
      ).build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000001")
        //.addUniqueId("000002") // <-- DIFF
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        //.addUniqueId("000001") // <-- DIFF
        //.addUniqueId("000002")
        .build()
      ).build();
    assertNotEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id2);
    assertEquals(1, id1.compareTo(id2));
    
    id2 = PBufTimeSeriesId.newBuilder(
        BaseTimeSeriesStringId.newBuilder()
        .setAlias("FakeID")
        .setNamespace("OpenTSDB")
        .setMetric("sys.cpu.user")
        .setTags(tags)
        .setAggregatedTags(Lists.newArrayList("owner", "role"))
        .setDisjointTags(Lists.newArrayList("propery", "type"))
        .addUniqueId("000002") // <-- DIFF order ok
        .addUniqueId("000001")
        .build()
      ).build();
    assertEquals(id1.hashCode(), id2.hashCode());
    assertEquals(id1, id2);
    assertEquals(0, id1.compareTo(id2));
  }
}
