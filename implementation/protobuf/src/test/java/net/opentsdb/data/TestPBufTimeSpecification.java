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

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import net.opentsdb.data.pbuf.TimeSpecificationPB;
import net.opentsdb.data.pbuf.TimeStampPB;

public class TestPBufTimeSpecification {

  @Test
  public void newBuilderFromProtobuf() throws Exception {
    TimeSpecificationPB.TimeSpecification source = 
        TimeSpecificationPB.TimeSpecification.newBuilder()
        .setStart(TimeStampPB.TimeStamp.newBuilder()
            .setEpoch(1514764800)
            .setNanos(500)
            .setZoneId("UTC")
            .build())
        .setEnd(TimeStampPB.TimeStamp.newBuilder()
            .setEpoch(1514768400)
            .setNanos(250)
            .setZoneId("UTC")
            .build())
        .setTimeZone("America/Denver")
        .setInterval("1h")
        .build();
    
    PBufTimeSpecification spec = new PBufTimeSpecification(source);
    assertEquals(1514764800, spec.start().epoch());
    assertEquals(500, spec.start().nanos());
    assertEquals(ZoneId.of("America/Denver"), spec.start().timezone());
    assertEquals(1514768400, spec.end().epoch());
    assertEquals(250, spec.end().nanos());
    assertEquals(ZoneId.of("America/Denver"), spec.end().timezone());
    assertEquals(Duration.ofSeconds(3600), spec.interval());
    assertEquals("1h", spec.stringInterval());
    assertEquals(ChronoUnit.HOURS, spec.units());
    assertEquals(ZoneId.of("America/Denver"), spec.timezone());
    
    TimeStamp ts = spec.start().getCopy();
    spec.updateTimestamp(1, ts);
    assertEquals(1514768400, ts.epoch());
    
    spec.updateTimestamp(2, ts);
    assertEquals(1514772000, ts.epoch());
    
    spec.nextTimestamp(ts);
    assertEquals(1514775600, ts.epoch());
  }
}
