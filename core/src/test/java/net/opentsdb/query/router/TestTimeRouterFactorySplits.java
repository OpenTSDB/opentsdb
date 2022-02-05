/*
 * // This file is part of OpenTSDB.
 * // Copyright (C) 2021  The OpenTSDB Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //   http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package net.opentsdb.query.router;

import com.google.common.collect.Lists;
import net.opentsdb.common.Const;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.timedifference.TimeDifferenceConfig;
import net.opentsdb.rollup.DefaultRollupInterval;
import net.opentsdb.rollup.RollupConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CASES:
 * - pushdowns
 *   - serialization source
 *   - some factories support, others don't
 *
 * - raw time series
 *   - butt-to-butt coverage
 *   - one of 3 satisfies (1, 2 and 3)
 *   - two of 3 satisfies (1 and 2, 2 and 3)
 *   - overlap coverage
 *   - gap in coverage
 *   - one doesn't have type
 *   - order of sources in config
 *   - possible duplicates (ignore for now, tell users not to do it)
 *   - unsatisfied rollup in one
 *   - pushdown GB
 *   - pushdown uneven pds
 *   - FIXED split time (e.g. hbase cutover for UIDs)
 *   - different data types
 *   - TODO - ID converter
 *
 */
public class TestTimeRouterFactorySplits extends BaseTestTimeRouterFactorySplit {

  @Test
  public void relativeSources3OverlappingSources() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesButtJoints() throws Exception {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.OVERLAP_DURATION_KEY), null);
    setupQuery(baseMinus("48h"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesEndFuture() throws Exception {
    setupQuery(baseMinus("48h"), basePlus("5m"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), basePlus("5m") });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesEndPast() throws Exception {
    setupQuery(baseMinus("48h"), baseMinus("5m"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), baseMinus("5m") });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesOverlapEqualsPrevEnd() throws Exception {
    setupQuery(baseMinus("48h"), baseMinus("2h", Op.MINUS, "5m"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") });
    assertEdge("m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesOverlapTooSmall() throws Exception {
    setupQuery(baseMinus("48h"), baseMinus("2h", Op.MINUS, "15m"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdge("m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesFirstOnly() throws Exception {
    setupQuery(baseMinus("15m"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void relativeSources3OverlappingSourcesFirstAndSecond() throws Exception {
    setupQuery(baseMinus("5h"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s2", baseMinus("5h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2");
    assertEdgeToContextNode("m1");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
  }

  @Test
  public void relativeSources3OverlappingSourcesSecondOnly() throws Exception {
    setupQuery(baseMinus("5h"), baseMinus("2h", Op.MINUS, "15m"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("5h"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s2");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void relativeSources3OverlappingSourcesThirdOnly() throws Exception {
    setupQuery(baseMinus("36h"), baseMinus("24h", Op.MINUS, "15m"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("36h"), baseMinus("24h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void relativeSources3SourcesGapBetween1And2() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry("24h-ago", "3h-ago", BASE_TS, S2),
            mockEntry("1w-ago", "22h-ago", BASE_TS, S3)
    );
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void relativeSources3SourcesGapBetween2And3() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry("24h-ago", "1h-ago", BASE_TS, S2),
            mockEntry("1w-ago", "36h-ago", BASE_TS, S3)
    );
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void relativeSources3SourcesButtJoints() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry("24h-ago", "2h-ago", BASE_TS, S2),
            mockEntry("1w-ago", "24h-ago", BASE_TS, S3)
    );
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertIsMerger("m1");
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.OVERLAP_DURATION_KEY), "5m");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesToEarly() throws Exception {
    setupQuery(baseMinus("3w"), baseMinus("2w"));
    setCurrentQuery3OverlappingSources();
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) {}
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftAll3() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "30m");
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftLast2() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "2h");
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), BASE_TS });
    assertEdge("m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftLastOne() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "26h");
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("48h"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftOOB() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "1w");
    setCurrentQuery3OverlappingSources();
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) {}
  }

  @Test
  public void relativeAndFixed3SourcesOverlap() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesButtJoints() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry(Integer.toString(baseMinus("24h")), "2h-ago", BASE_TS, S2),
            mockEntry("1w-ago", Integer.toString(baseMinus("24h")),
                    BASE_TS, S3)
    );
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesFirstOnly() throws Exception {
    setupQuery(baseMinus("15m"), BASE_TS);
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesFirstAndSecond() throws Exception {
    setupQuery(baseMinus("5h"), BASE_TS);
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s2", baseMinus("5h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
  }

  @Test
  public void relativeAndFixed3SourcesSecondOnly() throws Exception {
    setupQuery(baseMinus("5h"), baseMinus("2h", Op.MINUS, "15m"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("5h"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s2");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesThirdOnly() throws Exception {
    setupQuery(baseMinus("36h"), baseMinus("24h", Op.MINUS, "15m"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("36h"), baseMinus("24h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesTimeShiftAll3() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "30m");
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesTimeShiftLast2() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "2h");
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), BASE_TS });
    assertEdge("m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesTimeShiftLastOne() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "26h");
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("48h"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesTimeShiftOOP() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "1w");
    setCurrentQuery3FixOverlappingSources();
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) {}
  }

  @Test
  public void zeroStart3Sources() throws Exception {
    setupQuery(baseMinus("2w"), BASE_TS);
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry(Integer.toString(baseMinus("24h")), "1h-ago", BASE_TS, S2),
            mockEntry(null, Integer.toString(baseMinus("22h")), BASE_TS, S3)
    );
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("2w"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void zeroStart3SourcesTimeShiftLastOne() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "1w");
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry(Integer.toString(baseMinus("24h")), "1h-ago", BASE_TS, S2),
            mockEntry(null, Integer.toString(baseMinus("22h")), BASE_TS, S3)
    );
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("48h"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertResultIds("m1", "m1", "m1");
  }

  @Test
  public void firstSourceRollupsOnly() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    RollupConfig rollupConfig = defaultRollupBuilder()
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1h")
                    .setRowSpan("1d")
                    .setTable("foo3")
                    .setPreAggregationTable("foo4")
                    .build())
            .build();
    S1.rollupConfig = rollupConfig;

    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void secondSourceRollupsOnly() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    RollupConfig rollupConfig = defaultRollupBuilder()
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1h")
                    .setRowSpan("1d")
                    .setTable("foo3")
                    .setPreAggregationTable("foo4")
                    .build())
            .build();
    S2.rollupConfig = rollupConfig;

    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void thirdSourceRollupsOnly() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    RollupConfig rollupConfig = defaultRollupBuilder()
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1h")
                    .setRowSpan("1d")
                    .setTable("foo3")
                    .setPreAggregationTable("foo4")
                    .build())
            .build();
    S3.rollupConfig = rollupConfig;

    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void firstSourceFullOnly() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1, null, true),
            mockEntry("24h-ago", "1h-ago", BASE_TS, S2),
            mockEntry("1w-ago", "22h-ago", BASE_TS, S3)
    );

    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void noTimeSeriesInSource() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS);
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry(Integer.toString(baseMinus("24h")), "2h-ago", BASE_TS, S2, "events"),
            mockEntry("1w-ago", Integer.toString(baseMinus("24h")),
                    BASE_TS, S3)
    );

    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void groupbyPushdownAll() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("gb", "m1_s1",
            "gb", "m1_s2",
            "gb", "m1_s3");
    assertEdgeToContextNode("gb");
    assertIsMerger("gb");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            GroupByConfig.class, "gb", "m1_s1", "gb", "m1_s1");
    assertPushdowns("m1_s2", 1,
            GroupByConfig.class, "gb", "m1_s2", "gb", "m1_s2");
    assertPushdowns("m1_s3", 1,
            GroupByConfig.class, "gb", "m1_s3", "gb", "m1_s3");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupbyPushdownAllButOne() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.pushdowns = Lists.newArrayList();
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("gb", "m1_s1",
            "gb", "m1_gb",
            "m1_gb", "m1_s2",
            "gb", "m1_s3");
    assertEdgeToContextNode("gb");
    assertMergerExpecting("gb", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            GroupByConfig.class, "gb", "m1_s1", "gb", "m1_s1");
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 1,
            GroupByConfig.class, "gb", "m1_s3", "gb", "m1_s3");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1_gb", "m1_gb", "m1_s2");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupbyPushdownButSerdes() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, new String[] {"m1", "gb"}, null, gbConfig("gb", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(6, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3",
            "gb", "m1");
    assertEdgeToContextNode("gb", "m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 0);
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupbyPushdownNonPushdownBetween() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "tdiff"),
            TimeDifferenceConfig.newBuilder()
                    .addSource("m1")
                    .setId("tdiff")
                    .build());
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(7, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3",
            "tdiff", "m1",
            "gb", "tdiff");
    assertEdgeToContextNode("gb");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 0);
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("tdiff", "tdiff", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void idConverterOnAll() throws Exception {
    S1.idType = Const.TS_BYTE_ID;
    S2.idType = Const.TS_BYTE_ID;
    S3.idType = Const.TS_BYTE_ID;
    setupQuery(baseMinus("48h"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "IdConverter_m1",
            "IdConverter_m1", "m1_s1",
            "IdConverter_m1", "m1_s2",
            "IdConverter_m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("IdConverter_m1", "m1_s1", "m1_s1",
            "m1_s2", "m1_s2",
            "m1_s3", "m1_s3");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void idConverterForOne() throws Exception {
    S2.idType = Const.TS_BYTE_ID;
    setupQuery(baseMinus("48h"), BASE_TS);
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "IdConverter_m1",
            "IdConverter_m1", "m1_s1",
            "IdConverter_m1", "m1_s2",
            "IdConverter_m1", "m1_s3");
    assertEdgeToContextNode("m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("IdConverter_m1", "m1_s1", "m1_s1",
            "m1_s2", "m1_s2",
            "m1_s3", "m1_s3");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void idConverterPushdownAllButOne() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.pushdowns = Lists.newArrayList();
    S2.idType = Const.TS_BYTE_ID;
    run();
debug();
    assertNodesAndEdges(7, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("gb", "IdConverter_m1",
            "IdConverter_m1", "m1_s1",
            "IdConverter_m1", "m1_gb",
            "m1_gb", "m1_s2",
            "IdConverter_m1", "m1_s3");
    assertEdgeToContextNode("gb");
    assertMergerExpecting("gb", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            GroupByConfig.class, "gb", "m1_s1", "gb", "m1_s1");
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 1,
            GroupByConfig.class, "gb", "m1_s3", "gb", "m1_s3");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("IdConverter_m1", "m1_s1", "m1_s1",
            "m1_s2", "m1_s2",
            "m1_s3", "m1_s3");
    assertResultIds("m1_gb", "m1_gb", "m1_s2");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }
}
