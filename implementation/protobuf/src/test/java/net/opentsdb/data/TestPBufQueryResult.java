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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import net.opentsdb.common.Const;
import net.opentsdb.data.pbuf.TimeSeriesPB;
import net.opentsdb.data.pbuf.QueryResultPB.QueryResult;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.serdes.PBufIteratorSerdesFactory;

public class TestPBufQueryResult {

  @Test
  public void ctorWithTimeSeries() throws Exception {
    QueryResult pbuf = QueryResult.newBuilder()
        .setResolution(2)
        .setSequenceId(42)
        .addTimeseries(TimeSeriesPB.TimeSeries.newBuilder())
        .addTimeseries(TimeSeriesPB.TimeSeries.newBuilder())
        .build();
    PBufIteratorSerdesFactory factory = new PBufIteratorSerdesFactory();
    QueryNode node = mock(QueryNode.class);
    ByteArrayInputStream bais = new ByteArrayInputStream(pbuf.toByteArray());
    
    PBufQueryResult result = new PBufQueryResult(factory, node, null, bais);
    assertNull(result.timeSpecification());
    assertEquals(2, result.timeSeries().size());
    assertEquals(42, result.sequenceId());
    assertSame(node, result.source());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertEquals(ChronoUnit.MILLIS, result.resolution());
  }
  
  @Test
  public void ctorEmpty() throws Exception {
    QueryResult pbuf = QueryResult.newBuilder()
        .setResolution(2)
        .setSequenceId(42)
        .build();
    PBufIteratorSerdesFactory factory = new PBufIteratorSerdesFactory();
    QueryNode node = mock(QueryNode.class);
    ByteArrayInputStream bais = new ByteArrayInputStream(pbuf.toByteArray());
    
    PBufQueryResult result = new PBufQueryResult(factory, node, null, bais);
    assertNull(result.timeSpecification());
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(42, result.sequenceId());
    assertSame(node, result.source());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertEquals(ChronoUnit.MILLIS, result.resolution());
  }
  
  @Test
  public void ctorBadData() throws Exception {
    PBufIteratorSerdesFactory factory = new PBufIteratorSerdesFactory();
    QueryNode node = mock(QueryNode.class);
    ByteArrayInputStream bais = new ByteArrayInputStream(
        new byte[] { 42, 3, 0 });
    
    try {
      new PBufQueryResult(factory, node, null, bais);
      fail("Expected SerdesException");
    } catch (SerdesException e) { }
  }
}
