// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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
package net.opentsdb.query.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import net.opentsdb.exceptions.QueryExecutionException;
import org.junit.Before;
import org.junit.Test;

public class TestTimeRouterFactory extends BaseTestTimeRouterFactorySplit {

  @Before
  public void beforeLocal() {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.SPLIT_KEY), false);
  }

  @Test
  public void matchFirstFuture() throws Exception {
    setupQuery(baseMinus("15m"), basePlus("15m"));
    setCurrentQuery3FixOverlappingSources();
    run();

    //assertEquals(2, planner.graph().nodes().size());
    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), basePlus("15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void matchFirstTip() throws Exception {
    setupQuery(baseMinus("15m"), BASE_TS);
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void matchFirstStartOverlap() throws Exception {
    setupQuery(baseMinus("2h"), baseMinus("2h", Op.MINUS, "15m"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("2h"),
            baseMinus("2h", Op.MINUS, "15m")});
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void matchSecondEndOverlap() throws Exception {
    setupQuery(baseMinus("3h"), baseMinus("2h", Op.MINUS, "15m"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("3h"),
            baseMinus("2h", Op.MINUS, "15m")});
    assertEdgeToContextNode("m1");
    assertSources("m1", "s2");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void matchThirdStartEndOverlap() throws Exception {
    setupQuery(baseMinus("48h"), baseMinus("24h", Op.MINUS, "15m"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("48h"),
            baseMinus("24h", Op.MINUS, "15m")});
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void overLappingTimeShift() throws Exception {
    setupQuery(baseMinus("15m"), basePlus("15m"), null, "15m");
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), basePlus("15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void overLappingTimeShiftSetupGraph() throws Exception {
    setupQuery(baseMinus("15m"), basePlus("15m"), null, "15m");
    setCurrentQuery3FixOverlappingSources();
    S1.setupGraph = true;
    run();

    assertNodesAndEdges(3, 2);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), basePlus("15m") });
    assertEdgeToContextNode("m1_timeShift");
    assertEdge("m1_timeShift", "m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_timeShift", "m1_timeShift", "m1");
    assertPushdowns("m1", 0);
  }

  @Test
  public void overLappingBeyondStart() throws Exception {
    setupQuery(baseMinus("2w"), baseMinus("24h", Op.MINUS, "15m"));
    setCurrentQuery3FixOverlappingSources();
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void overLappingTimeShiftTooEarly() throws Exception {
    setupQuery(baseMinus("15m"), basePlus("15m"), null, "1n");
    setCurrentQuery3FixOverlappingSources();
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void zeroStartTimeShift() throws Exception {
    setupQuery(baseMinus("15m"), basePlus("15m"), null, "1n");
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry(Integer.toString(baseMinus("24h")), "1h-ago", BASE_TS, S2),
            mockEntry(null, Integer.toString(baseMinus("22h")), BASE_TS, S3)
    );
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), basePlus("15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertResultIds("m1", "m1", "m1");
    assertPushdowns("m1", 0);
  }
}
