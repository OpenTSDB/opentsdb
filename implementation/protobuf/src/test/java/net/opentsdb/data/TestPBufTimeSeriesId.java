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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

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
  
}
