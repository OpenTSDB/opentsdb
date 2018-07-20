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

import org.junit.Test;

import net.opentsdb.data.pbuf.TimeSeriesDatumIdPB;

public class TestPBufTimeSeriesDatumId {

  @Test
  public void newBuilderFromID() throws Exception {
    PBufTimeSeriesDatumId id = PBufTimeSeriesDatumId.newBuilder(
        BaseTimeSeriesDatumStringId.newBuilder()
          .setNamespace("oath")
          .setMetric("metric.foo")
          .addTags("host", "web01")
          .addTags("danger", "will")
          .build()
        ).build();
    
    assertEquals("oath", id.namespace());
    assertEquals("metric.foo", id.metric());
    assertEquals(2, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals("will", id.tags().get("danger"));
  }
  
  @Test
  public void newBuilderFromProtobuf() throws Exception {
    TimeSeriesDatumIdPB.TimeSeriesDatumId source = 
        TimeSeriesDatumIdPB.TimeSeriesDatumId.newBuilder()
          .setNamespace("oath")
          .setMetric("metric.foo")
          .putTags("host", "web01")
          .putTags("danger", "will")
          .build();
    
    PBufTimeSeriesDatumId id = new PBufTimeSeriesDatumId(source);
    assertEquals("oath", id.namespace());
    assertEquals("metric.foo", id.metric());
    assertEquals(2, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals("will", id.tags().get("danger"));
  }
}
