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
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.query.processor.slidingwindow.SlidingWindowConfig;
import net.opentsdb.query.processor.timedifference.TimeDifferenceConfig;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
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
 * - rate only
 *   - butt-joints
 *   - overlap
 *   - best effort vs fail
 *
 * - rate + DS
 *   - interval > overlap
 *   - interval < overlap
 *   - interval == overlap
 *   - rate BEFORE ds (may ignore for now and mark as known)
 *   - rate AFTER ds
 *
 * - window + DS
 *   - above
 *
 * - rate + DS + window
 *   - rate BEFORE ds (may ignore for now and mark as known)
 *   - rate AFTER ds
 *
 */
public class TestTimeRouterFactorySplitsWindow extends BaseTestTimeRouterFactorySplit {

  @Test
  public void relativeSources3OverlappingSources() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"), baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesDisabledOverlap() throws Exception {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.OVERLAP_DURATION_KEY), null);
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"), baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3NonOverlappingSources() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry("24h-ago", "2h-ago", BASE_TS, S2),
            mockEntry("1w-ago", "24h-ago", BASE_TS, S3)
    );
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"), baseMinus("24h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3NonOverlappingSourcesFailOnOverlap() throws Exception {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), true);
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry("24h-ago", "2h-ago", BASE_TS, S2),
            mockEntry("1w-ago", "24h-ago", BASE_TS, S3)
    );
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void relativeSources3OverlappingSourcesWindowPastStartofOldestStore() throws Exception {
    setupQuery(baseMinus("1w"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    try {
      run();
      fail("expected QueryExecutionException");
    } catch (QueryExecutionException e) { };
  }

  @Test
  public void relativeSources3OverlappingSourcesOverlapEqualsPrevEnd() throws Exception {
    setupQuery(baseMinus("48h"),
             baseMinus("2h", Op.MINUS, "5m"),
             slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") });
    assertEdge("w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesOverlapTooSmall() throws Exception {
    setupQuery(baseMinus("48h"),
             baseMinus("2h", Op.MINUS, "15m"),
             slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdge("w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesWindowTooBig() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "65m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "65m"),
                    baseMinus("24h", Op.MINUS, "65m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");

    assertResultIds("m1_s1", "m1_s1", "m1_s1");
  }

  @Test
  public void relativeSources3OverlappingSourcesWindowTooBigFailOnOverlap() throws Exception {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), true);
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "65m", "m1"));
    setCurrentQuery3OverlappingSources();
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), false);
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftAll3() throws Exception {
    S1.setupGraph = true;
    S2.setupGraph = true;
    S3.setupGraph = true;
    setupQuery(baseMinus("48h"), BASE_TS, null, "30m", slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(8, 7);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"), baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1_timeShift",
            "w", "m1_s2_timeShift",
            "w", "m1_s3_timeShift",
            "m1_s1_timeShift", "m1_s1",
            "m1_s2_timeShift", "m1_s2",
            "m1_s3_timeShift", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
    assertResultIds("m1_s1_timeShift", "m1_s1_timeShift", "m1_s1");
    assertResultIds("m1_s2_timeShift", "m1_s2_timeShift", "m1_s2");
    assertResultIds("m1_s3_timeShift", "m1_s3_timeShift", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftAll3PushDown() throws Exception {
    S1.pushdowns = Lists.newArrayList(TimeShiftConfig.class, SlidingWindowConfig.class);
    S2.pushdowns = Lists.newArrayList(TimeShiftConfig.class, SlidingWindowConfig.class);
    S3.pushdowns = Lists.newArrayList(TimeShiftConfig.class, SlidingWindowConfig.class);
    S1.setupGraph = true;
    S2.setupGraph = true;
    S3.setupGraph = true;
    setupQuery(baseMinus("48h"), BASE_TS, null, "30m", slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"), baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            TimeShiftConfig.class, "m1_s1_timeShift", "m1_s1", "m1_s1_timeShift", "m1_s1",
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1_timeShift", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            TimeShiftConfig.class, "m1_s2_timeShift", "m1_s2", "m1_s2_timeShift", "m1_s2",
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2_timeShift", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            TimeShiftConfig.class, "m1_s3_timeShift", "m1_s3", "m1_s3_timeShift", "m1_s3",
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3_timeShift", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftLast2() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "2h", slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"), baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"), BASE_TS });
    assertEdge("w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftLast1() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "26h", slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("48h", Op.PLUS, "15m"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertPushdowns("m1", 1,
            SlidingWindowConfig.class, "w", "m1", "w", "m1");
    assertResultIds("m1", "w", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesOverlap() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesButtJoints() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry(Integer.toString(baseMinus("24h")), "2h-ago", BASE_TS, S2),
            mockEntry("1w-ago", Integer.toString(baseMinus("24h")),
                    BASE_TS, S3)
    );
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"), baseMinus("24h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesFirstOnly() throws Exception {
    setupQuery(baseMinus("15m"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("30m"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertPushdowns("m1", 1,
            SlidingWindowConfig.class, "w", "m1", "w", "m1");
    assertResultIds("m1", "w", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesFirstAndSecond() throws Exception {
    setupQuery(baseMinus("5h"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s2", baseMinus("5h", Op.PLUS, "15m"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertPushdowns("m1_s1", 1,
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
  }

  @Test
  public void relativeAndFixed3SourcesSecondOnly() throws Exception {
    setupQuery(baseMinus("5h"),
            baseMinus("2h", Op.MINUS, "15m"),
            slidingConfig("w", "15m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("5h", Op.PLUS, "15m"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s2");
    assertPushdowns("m1", 1,
            SlidingWindowConfig.class, "w", "m1", "w", "m1");
    assertResultIds("m1", "w", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesThirdOnly() throws Exception {
    setupQuery(baseMinus("36h"),
            baseMinus("24h", Op.MINUS, "15m"),
            slidingConfig("w", "15m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("36h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertPushdowns("m1", 1,
            SlidingWindowConfig.class, "w", "m1", "w", "m1");
    assertResultIds("m1", "w", "m1");
  }

  @Test
  public void pushdownAllButOne() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.pushdowns = Lists.newArrayList();
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_w",
            "m1_w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            SlidingWindowConfig.class, "w_m1_s1", "m1_s1", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 1,
            SlidingWindowConfig.class, "w_m1_s3", "m1_s3", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_w", "m1_w", "m1_s2");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void pushdownButSerdes() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS,
            new String[] {"m1", "w"}, null,
            slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(6, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3",
            "w", "m1");
    assertEdgeToContextNode("w", "m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 0);
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void pushdownNonPushdownBetween() throws Exception {
    // NOTE: The tdiff here is just for testing a non-pushdown, not to test the
    // actual order of operations.
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "tdiff"),
            TimeDifferenceConfig.newBuilder()
                    .addSource("m1")
                    .setId("tdiff")
                    .build());
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(7, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3",
            "tdiff", "m1",
            "w", "tdiff");
    assertEdgeToContextNode("w");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 0);
    assertResultIds("w", "w", "m1");
    assertResultIds("tdiff", "tdiff", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdown() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "w"),
             slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("gb", "m1_s1",
            "gb", "m1_s2",
            "gb", "m1_s3");
    assertEdgeToContextNode("gb");
    assertMergerExpecting("gb", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            SlidingWindowConfig.class, "w", "m1_s1", "w", "m1_s1",
            GroupByConfig.class, "gb_m1_s1", "w", "gb_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            SlidingWindowConfig.class, "w", "m1_s2", "w", "m1_s2",
            GroupByConfig.class, "gb_m1_s2", "w", "gb_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            SlidingWindowConfig.class, "w", "m1_s3", "w", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "w", "gb_m1_s3", "m1_s3");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdownOneNoGB() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "w"),
             slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.pushdowns = Lists.newArrayList(SlidingWindowConfig.class);
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "m1_gb", "m1_s2",
            "w", "m1_gb",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            SlidingWindowConfig.class, "w", "m1_s1", "w", "m1_s1",
            GroupByConfig.class, "gb_m1_s1", "w", "gb_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            SlidingWindowConfig.class, "w", "m1_s3", "w", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "w", "gb_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_gb", "m1_gb", "m1_s2");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdownOneNoGBAnotherNoRate() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "w"),
             slidingConfig("w", "15m", "m1"));
    setCurrentQuery3OverlappingSources();
    S1.pushdowns = Lists.newArrayList(GroupByConfig.class);
    S2.pushdowns = Lists.newArrayList(SlidingWindowConfig.class);
    run();

    assertNodesAndEdges(7, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1_gb", "m1_s2",
            "m1_gb", "m1_w",
            "m1_w", "m1_s1",
            "w", "m1_gb",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 1,
            SlidingWindowConfig.class, "w_m1_s2", "m1_s2", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            SlidingWindowConfig.class, "w", "m1_s3", "w", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "w", "gb_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_gb", "m1_gb", "m1_s1", "m1_gb", "m1_s2");
    assertResultIds("m1_w", "m1_w", "m1_s1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void dsThenslidingConfig() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"),
            dsConfig("ds", "1m", "w"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            SlidingWindowConfig.class, "w", "m1_s1", "w", "m1_s1",
            DownsampleConfig.class, "ds_m1_s1", "w", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            SlidingWindowConfig.class, "w", "m1_s2", "w", "m1_s2",
            DownsampleConfig.class, "ds_m1_s2", "w", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            SlidingWindowConfig.class, "w", "m1_s3", "w", "m1_s3",
            DownsampleConfig.class, "ds_m1_s3", "w", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void windowThenDs() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m","ds"),
            dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            DownsampleConfig.class, "ds", "m1_s1", "ds", "m1_s1",
            SlidingWindowConfig.class, "w_m1_s1", "ds", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            DownsampleConfig.class, "ds", "m1_s2", "ds", "m1_s2",
            SlidingWindowConfig.class, "w_m1_s2", "ds", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            DownsampleConfig.class, "ds", "m1_s3", "ds", "m1_s3",
            SlidingWindowConfig.class, "w_m1_s3", "ds", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void dsBiggerThenslidingConfig() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"),
            dsConfig("ds", "30m", "w"));
    setCurrentQuery3OverlappingSources();
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
  }

  @Test
  public void rateThenslidingConfig() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "m1"),
            rate("rate", "w"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            SlidingWindowConfig.class, "w", "m1_s1", "w", "m1_s1",
            RateConfig.class, "rate_m1_s1", "w", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            SlidingWindowConfig.class, "w", "m1_s2", "w", "m1_s2",
            RateConfig.class, "rate_m1_s2", "w", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            SlidingWindowConfig.class, "w", "m1_s3", "w", "m1_s3",
            RateConfig.class, "rate_m1_s3", "w", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void windowThenRate() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, slidingConfig("w", "15m", "rate"),
            rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            RateConfig.class, "rate", "m1_s1", "rate", "m1_s1",
            SlidingWindowConfig.class, "w_m1_s1", "rate", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            RateConfig.class, "rate", "m1_s2", "rate", "m1_s2",
            SlidingWindowConfig.class, "w_m1_s2", "rate", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            RateConfig.class, "rate", "m1_s3", "rate", "m1_s3",
            SlidingWindowConfig.class, "w_m1_s3", "rate", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void rateWindowDs() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS,
            rate("rate", "m1"),
            dsConfig("ds", "1m", "rate"),
            slidingConfig("w", "15m", "ds"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h", Op.PLUS, "15m"),
                    baseMinus("24h", Op.MINUS, "15m") },
            new Object[] { "m1_s2", baseMinus("24h"),
                    baseMinus("2h", Op.MINUS, "15m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("w", "m1_s1",
            "w", "m1_s2",
            "w", "m1_s3");
    assertEdgeToContextNode("w");
    assertMergerExpecting("w", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 3,
            RateConfig.class, "rate", "m1_s1", "rate", "m1_s1",
            DownsampleConfig.class, "ds", "rate", "ds", "m1_s1",
            SlidingWindowConfig.class, "w_m1_s1", "ds", "w_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 3,
            RateConfig.class, "rate", "m1_s2", "rate", "m1_s2",
            DownsampleConfig.class, "ds", "rate", "ds", "m1_s2",
            SlidingWindowConfig.class, "w_m1_s2", "ds", "w_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 3,
            RateConfig.class, "rate", "m1_s3", "rate", "m1_s3",
            DownsampleConfig.class, "ds", "rate", "ds", "m1_s3",
            SlidingWindowConfig.class, "w_m1_s3", "ds", "w_m1_s3", "m1_s3");
    assertResultIds("w", "w", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }
}
