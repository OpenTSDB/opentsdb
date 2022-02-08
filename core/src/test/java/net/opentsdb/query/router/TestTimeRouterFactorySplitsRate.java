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
 * *
 * - rate + DS + window
 *   - rate BEFORE ds (may ignore for now and mark as known)
 *   - rate AFTER ds
 *
 */
public class TestTimeRouterFactorySplitsRate extends BaseTestTimeRouterFactorySplit {

  @Test
  public void relativeSources3OverlappingSources() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            RateConfig.class, "rate_m1_s1", "m1_s1", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesDisabledOverlap() throws Exception {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.OVERLAP_DURATION_KEY), null);
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            RateConfig.class, "rate_m1_s1", "m1_s1", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3NonOverlappingSources() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"));
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
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            RateConfig.class, "rate_m1_s1", "m1_s1", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3NonOverlappingSourcesFailOnOverlap() throws Exception {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), true);
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"));
    setEntryConfigs(
            mockEntry("2h-ago", null, BASE_TS, S1),
            mockEntry("24h-ago", "2h-ago", BASE_TS, S2),
            mockEntry("1w-ago", "24h-ago", BASE_TS, S3)
    );
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), false);
  }

  @Test
  public void relativeSources3OverlappingSourcesOverlapEqualsPrevEnd() throws Exception {
    setupQuery(baseMinus("48h"),
             baseMinus("2h", Op.MINUS, "5m"),
             rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") });
    assertEdge("rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesOverlapTooSmall() throws Exception {
    setupQuery(baseMinus("48h"),
             baseMinus("2h", Op.MINUS, "15m"),
             rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdge("rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftAll3() throws Exception {
    S1.setupGraph = true;
    S2.setupGraph = true;
    S3.setupGraph = true;
    setupQuery(baseMinus("48h"), BASE_TS, null, "30m", rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(8, 7);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1_timeShift",
            "rate", "m1_s2_timeShift",
            "rate", "m1_s3_timeShift",
            "m1_s1_timeShift", "m1_s1",
            "m1_s2_timeShift", "m1_s2",
            "m1_s3_timeShift", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            RateConfig.class, "rate_m1_s1", "m1_s1", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
    assertResultIds("m1_s1_timeShift", "m1_s1_timeShift", "m1_s1");
    assertResultIds("m1_s2_timeShift", "m1_s2_timeShift", "m1_s2");
    assertResultIds("m1_s3_timeShift", "m1_s3_timeShift", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftAll3Pushdown() throws Exception {
    S1.pushdowns = Lists.newArrayList(TimeShiftConfig.class, RateConfig.class);
    S2.pushdowns = Lists.newArrayList(TimeShiftConfig.class, RateConfig.class);
    S3.pushdowns = Lists.newArrayList(TimeShiftConfig.class, RateConfig.class);
    S1.setupGraph = true;
    S2.setupGraph = true;
    S3.setupGraph = true;
    setupQuery(baseMinus("48h"), BASE_TS, null, "30m", rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            TimeShiftConfig.class, "m1_s1_timeShift", "m1_s1", "m1_s1_timeShift", "m1_s1",
            RateConfig.class, "rate_m1_s1", "m1_s1_timeShift", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            TimeShiftConfig.class, "m1_s2_timeShift", "m1_s2", "m1_s2_timeShift", "m1_s2",
            RateConfig.class, "rate_m1_s2", "m1_s2_timeShift", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            TimeShiftConfig.class, "m1_s3_timeShift", "m1_s3", "m1_s3_timeShift", "m1_s3",
            RateConfig.class, "rate_m1_s3", "m1_s3_timeShift", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftLast2() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "2h", rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), BASE_TS });
    assertEdge("rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesTimeShiftLast1() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "26h", rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("48h"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertPushdowns("m1", 1,
            RateConfig.class, "rate", "m1", "rate", "m1");
    assertResultIds("m1", "rate", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesOverlap() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            RateConfig.class, "rate_m1_s1", "m1_s1", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesButtJoints() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"));
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
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            RateConfig.class, "rate_m1_s1", "m1_s1", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesFirstOnly() throws Exception {
    setupQuery(baseMinus("15m"), BASE_TS, rate("rate", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertPushdowns("m1", 1,
            RateConfig.class, "rate", "m1", "rate", "m1");
    assertResultIds("m1", "rate", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesFirstAndSecond() throws Exception {
    setupQuery(baseMinus("5h"), BASE_TS, rate("rate", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s2", baseMinus("5h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertPushdowns("m1_s1", 1,
            RateConfig.class, "rate_m1_s1", "m1_s1", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
  }

  @Test
  public void relativeAndFixed3SourcesSecondOnly() throws Exception {
    setupQuery(baseMinus("5h"),
            baseMinus("2h", Op.MINUS, "15m"),
            rate("rate", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("5h"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s2");
    assertPushdowns("m1", 1,
            RateConfig.class, "rate", "m1", "rate", "m1");
    assertResultIds("m1", "rate", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesThirdOnly() throws Exception {
    setupQuery(baseMinus("36h"),
            baseMinus("24h", Op.MINUS, "15m"),
            rate("rate", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("36h"), baseMinus("24h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertPushdowns("m1", 1,
            RateConfig.class, "rate", "m1", "rate", "m1");
    assertResultIds("m1", "rate", "m1");
  }

  @Test
  public void pushdownAllButOne() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.pushdowns = Lists.newArrayList();
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "rate", "m1_rate",
            "m1_rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            RateConfig.class, "rate_m1_s1", "m1_s1", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 1,
            RateConfig.class, "rate_m1_s3", "m1_s3", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_rate", "m1_rate", "m1_s2");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void pushdownButSerdes() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS,
            new String[] {"m1", "rate"}, null,
            rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(6, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3",
            "rate", "m1");
    assertEdgeToContextNode("rate", "m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 0);
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void pushdownNonPushdownBetween() throws Exception {
    // NOTE: The tdiff here is just for testing a non-pushdown, not to test the
    // actual order of operations.
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "tdiff"),
            TimeDifferenceConfig.newBuilder()
                    .addSource("m1")
                    .setId("tdiff")
                    .build());
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(7, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3",
            "tdiff", "m1",
            "rate", "tdiff");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 0);
    assertResultIds("tdiff", "tdiff", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdown() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "rate"),
            rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("gb", "m1_s1",
            "gb", "m1_s2",
            "gb", "m1_s3");
    assertEdgeToContextNode("gb");
    assertMergerExpecting("gb", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            RateConfig.class, "rate", "m1_s1", "rate", "m1_s1",
            GroupByConfig.class, "gb_m1_s1", "rate", "gb_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            RateConfig.class, "rate", "m1_s2", "rate", "m1_s2",
            GroupByConfig.class, "gb_m1_s2", "rate", "gb_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            RateConfig.class, "rate", "m1_s3", "rate", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "rate", "gb_m1_s3", "m1_s3");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdownOneNoGB() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "rate"),
            rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.pushdowns = Lists.newArrayList(RateConfig.class);
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "m1_gb", "m1_s2",
            "rate", "m1_gb",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            RateConfig.class, "rate", "m1_s1", "rate", "m1_s1",
            GroupByConfig.class, "gb_m1_s1", "rate", "gb_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            RateConfig.class, "rate", "m1_s3", "rate", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "rate", "gb_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_gb", "m1_gb", "m1_s2");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdownOneNoGBAnotherNoRate() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "rate"),
            rate("rate", "m1"));
    setCurrentQuery3OverlappingSources();
    S1.pushdowns = Lists.newArrayList(GroupByConfig.class);
    S2.pushdowns = Lists.newArrayList(RateConfig.class);
    run();

    assertNodesAndEdges(7, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1_gb", "m1_s2",
            "m1_gb", "m1_rate",
            "m1_rate", "m1_s1",
            "rate", "m1_gb",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 1,
            RateConfig.class, "rate_m1_s2", "m1_s2", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            RateConfig.class, "rate", "m1_s3", "rate", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "rate", "gb_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_gb", "m1_gb", "m1_s1", "m1_gb", "m1_s2");
    assertResultIds("m1_rate", "m1_rate", "m1_s1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void dsThenRate() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"),
            dsConfig("ds", "1m", "rate"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            RateConfig.class, "rate", "m1_s1", "rate", "m1_s1",
            DownsampleConfig.class, "ds_m1_s1", "rate", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            RateConfig.class, "rate", "m1_s2", "rate", "m1_s2",
            DownsampleConfig.class, "ds_m1_s2", "rate", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            RateConfig.class, "rate", "m1_s3", "rate", "m1_s3",
            DownsampleConfig.class, "ds_m1_s3", "rate", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void rateThenDs() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "ds"),
            dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("rate", "m1_s1",
            "rate", "m1_s2",
            "rate", "m1_s3");
    assertEdgeToContextNode("rate");
    assertMergerExpecting("rate", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            DownsampleConfig.class, "ds", "m1_s1", "ds", "m1_s1",
            RateConfig.class, "rate_m1_s1", "ds", "rate_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            DownsampleConfig.class, "ds", "m1_s2", "ds", "m1_s2",
            RateConfig.class, "rate_m1_s2", "ds", "rate_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            DownsampleConfig.class, "ds", "m1_s3", "ds", "m1_s3",
            RateConfig.class, "rate_m1_s3", "ds", "rate_m1_s3", "m1_s3");
    assertResultIds("rate", "rate", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void gbThenDsThenRate() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, rate("rate", "m1"),
            dsConfig("ds", "1m", "rate"),
            gbConfig("gb", "ds"));
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
    assertMergerExpecting("gb", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 3,
            RateConfig.class, "rate", "m1_s1", "rate", "m1_s1",
            DownsampleConfig.class, "ds", "rate", "ds", "m1_s1",
            GroupByConfig.class, "gb_m1_s1", "ds", "gb_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 3,
            RateConfig.class, "rate", "m1_s2", "rate", "m1_s2",
            DownsampleConfig.class, "ds", "rate", "ds", "m1_s2",
            GroupByConfig.class, "gb_m1_s2", "ds", "gb_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 3,
            RateConfig.class, "rate", "m1_s3", "rate", "m1_s3",
            DownsampleConfig.class, "ds", "rate", "ds", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "ds", "gb_m1_s3", "m1_s3");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }
}
