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
import net.opentsdb.query.processor.timedifference.TimeDifferenceConfig;
import net.opentsdb.query.processor.timeshift.TimeShiftConfig;
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
 * - downsample
 *   - interval > overlap
 *   - interval < overlap
 *   - interval == overlap
 *   - interval == auto - what do we do if it hasn't resolved?
 *   - interval == 0all!
 *   - unsatisfied rollup config
 *   - pushdowns
 *   - one does not support pushdown ds
 *   - best effort vs fail
 *
 */
public class TestTimeRouterFactorySplitsDownsampler extends BaseTestTimeRouterFactorySplit {

  @Test
  public void relativeSources3OverlappingSources() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
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
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalGtOverlap() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds","6h", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    // winds up as a butt join
    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalGtOverlapFailOn() throws Exception {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), true);
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "6h", "m1"));
    setCurrentQuery3OverlappingSources();
    try {
      run();
      fail("Expected QueryExecutionException");
    } catch (QueryExecutionException e) { }
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), false);
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalEqOverlap() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "2h", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    // 2hr overlap for the oldest store but butt join for the newest since 2hr > 1hr
    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalLtOverlap1h() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1h", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    // 2hr for the older, 1hr overlap since that's all we have.
    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalLtOverlap30m() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "30m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    // 1hr overlap
    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalLtOverlapTimeShiftAll3() throws Exception {
    S1.setupGraph = true;
    S2.setupGraph = true;
    S3.setupGraph = true;
    setupQuery(baseMinus("48h"), BASE_TS, null, "30m", dsConfig("ds", "30m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    // 1hr overlap
    assertNodesAndEdges(8, 7);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1_timeShift",
            "ds", "m1_s2_timeShift",
            "ds", "m1_s3_timeShift",
            "m1_s1_timeShift", "m1_s1",
            "m1_s2_timeShift", "m1_s2",
            "m1_s3_timeShift", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
    assertResultIds("m1_s1_timeShift", "m1_s1_timeShift", "m1_s1");
    assertResultIds("m1_s2_timeShift", "m1_s2_timeShift", "m1_s2");
    assertResultIds("m1_s3_timeShift", "m1_s3_timeShift", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalLtOverlapTimeShiftAll3PushDown() throws Exception {
    S1.pushdowns = Lists.newArrayList(TimeShiftConfig.class, DownsampleConfig.class);
    S2.pushdowns = Lists.newArrayList(TimeShiftConfig.class, DownsampleConfig.class);
    S3.pushdowns = Lists.newArrayList(TimeShiftConfig.class, DownsampleConfig.class);
    S1.setupGraph = true;
    S2.setupGraph = true;
    S3.setupGraph = true;
    setupQuery(baseMinus("48h"), BASE_TS, null, "30m", dsConfig("ds", "30m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    // 1hr overlap
    assertNodesAndEdges(5, 4);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("1h") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            TimeShiftConfig.class, "m1_s1_timeShift", "m1_s1", "m1_s1_timeShift", "m1_s1",
            DownsampleConfig.class, "ds_m1_s1", "m1_s1_timeShift", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            TimeShiftConfig.class, "m1_s2_timeShift", "m1_s2", "m1_s2_timeShift", "m1_s2",
            DownsampleConfig.class, "ds_m1_s2", "m1_s2_timeShift", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            TimeShiftConfig.class, "m1_s3_timeShift", "m1_s3", "m1_s3_timeShift", "m1_s3",
            DownsampleConfig.class, "ds_m1_s3", "m1_s3_timeShift", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalLtOverlapTimeShiftLast2() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "2h", dsConfig("ds", "30m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    // 1hr overlap
    assertNodesAndEdges(4, 3);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("22h") },
            new Object[] { "m1_s2", baseMinus("24h"), BASE_TS });
    assertEdge("ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesIntervalLtOverlapTimeShiftLast1() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, null, "26h", dsConfig("ds", "30m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    // 1hr overlap
    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("48h"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds", "m1", "ds", "m1");
    assertResultIds("m1", "ds", "m1");
  }

  @Test
  public void relativeSources3ONonverlappingSourcesInterval() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "30m", "m1"));
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
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3ONonverlappingSourcesIntervalFailOnOverlap() throws Exception {
    TSDB.config.override(FACTORY.getConfigKey(TimeRouterFactory.FAIL_OVERLAP_KEY), true);
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "30m", "m1"));
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
             dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") });
    assertEdge("ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeSources3OverlappingSourcesOverlapTooSmall() throws Exception {
    setupQuery(baseMinus("48h"),
             baseMinus("2h", Op.MINUS, "15m"),
             dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdge("ds", "m1_s2",
            "ds", "m1_s3");
    assertMergerExpecting("ds", "m1_s2", "m1_s3");
    assertSources("m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesOverlap() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
    setCurrentQuery3FixOverlappingSources();
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
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesButtJoints() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
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
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void relativeAndFixed3SourcesFirstOnly() throws Exception {
    setupQuery(baseMinus("15m"), BASE_TS, dsConfig("ds", "1m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(new Object[] { "m1", baseMinus("15m"), BASE_TS });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s1");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds", "m1", "ds", "m1");
    assertResultIds("m1", "ds", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesFirstAndSecond() throws Exception {
    setupQuery(baseMinus("5h"), BASE_TS, dsConfig("ds", "1m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(4, 3);
    assertTimestamps(
            new Object[] { "m1_s2", baseMinus("5h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_s2");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2");
    assertSources("m1_s1", "s1", "m1_s2", "s2");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
  }

  @Test
  public void relativeAndFixed3SourcesSecondOnly() throws Exception {
    setupQuery(baseMinus("5h"),
            baseMinus("2h", Op.MINUS, "15m"),
            dsConfig("ds", "1m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("5h"), baseMinus("2h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s2");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds", "m1", "ds", "m1");
    assertResultIds("m1", "ds", "m1");
  }

  @Test
  public void relativeAndFixed3SourcesThirdOnly() throws Exception {
    setupQuery(baseMinus("36h"),
            baseMinus("24h", Op.MINUS, "15m"),
            dsConfig("ds", "1m", "m1"));
    setCurrentQuery3FixOverlappingSources();
    run();

    assertNodesAndEdges(2, 1);
    assertTimestamps(
            new Object[] { "m1", baseMinus("36h"), baseMinus("24h", Op.MINUS, "15m") });
    assertEdgeToContextNode("m1");
    assertSources("m1", "s3");
    assertPushdowns("m1", 1,
            DownsampleConfig.class, "ds", "m1", "ds", "m1");
    assertResultIds("m1", "ds", "m1");
  }

  @Test
  public void firstSourceRollupsOnlyUnsatisfied() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
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
  public void firstSourceRollupsOnlySatisfied() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    RollupConfig rollupConfig = defaultRollupBuilder()
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1m")
                    .setRowSpan("1d")
                    .setTable("foo3")
                    .setPreAggregationTable("foo4")
                    .build())
            .build();
    S1.rollupConfig = rollupConfig;
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
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void secondSourceRollupsOnlyUnsatsified() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
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
  public void secondSourceRollupsOnlySatsified() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    RollupConfig rollupConfig = defaultRollupBuilder()
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1m")
                    .setRowSpan("1d")
                    .setTable("foo3")
                    .setPreAggregationTable("foo4")
                    .build())
            .build();
    S2.rollupConfig = rollupConfig;
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
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void thirdSourceRollupsOnlyUnsatisfied() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
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
  public void thirdSourceRollupsOnlySatisfied() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    RollupConfig rollupConfig = defaultRollupBuilder()
            .addInterval(DefaultRollupInterval.builder()
                    .setInterval("1m")
                    .setRowSpan("1d")
                    .setTable("foo3")
                    .setPreAggregationTable("foo4")
                    .build())
            .build();
    S3.rollupConfig = rollupConfig;
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
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void pushdownAllButOne() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.pushdowns = Lists.newArrayList();
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "ds", "m1_ds",
            "m1_ds", "m1_s2",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_ds", "m1_ds", "m1_s2");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void pushdownButSerdes() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS,
            new String[] {"m1", "ds"}, null,
            dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    run();

    assertNodesAndEdges(6, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1", "m1_s1",
            "m1", "m1_s2",
            "m1", "m1_s3",
            "ds", "m1");
    assertEdgeToContextNode("ds", "m1");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 0);
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1", "m1", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void pushdownNonPushdownBetween() throws Exception {
    // NOTE: The tdiff here is just for testing a non-pushdown, not to test the
    // actual order of operations.
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "tdiff"),
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
            "ds", "tdiff");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("m1", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 0);
    assertPushdowns("m1_s3", 0);
    assertResultIds("ds", "ds", "m1");
    assertResultIds("tdiff", "tdiff", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdown() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "ds"),
            dsConfig("ds", "1m", "m1"));
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
    assertPushdowns("m1_s1", 2,
            DownsampleConfig.class, "ds", "m1_s1", "ds", "m1_s1",
            GroupByConfig.class, "gb_m1_s1", "ds", "gb_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 2,
            DownsampleConfig.class, "ds", "m1_s2", "ds", "m1_s2",
            GroupByConfig.class, "gb_m1_s2", "ds", "gb_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            DownsampleConfig.class, "ds", "m1_s3", "ds", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "ds", "gb_m1_s3", "m1_s3");
    assertResultIds("gb", "gb", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdownOneNoGB() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "ds"),
            dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    run();

    assertNodesAndEdges(6, 5);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("ds", "m1_s1",
            "m1_gb", "m1_s2",
            "ds", "m1_gb",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertMergerExpecting("ds", "m1_s1", "m1_s2", "m1_s3");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 2,
            DownsampleConfig.class, "ds", "m1_s1", "ds", "m1_s1",
            GroupByConfig.class, "gb_m1_s1", "ds", "gb_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            DownsampleConfig.class, "ds", "m1_s3", "ds", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "ds", "gb_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_gb", "m1_gb", "m1_s2");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void groupByPushdownOneNoGBAnotherNoDS() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, gbConfig("gb", "ds"),
            dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    S1.pushdowns = Lists.newArrayList(GroupByConfig.class);
    S2.pushdowns = Lists.newArrayList(DownsampleConfig.class);
    run();

    assertNodesAndEdges(7, 6);
    assertTimestamps(new Object[] { "m1_s3", baseMinus("48h"), baseMinus("24h", Op.MINUS, "5m") },
            new Object[] { "m1_s2", baseMinus("24h"), baseMinus("2h", Op.MINUS, "5m") },
            new Object[] { "m1_s1", baseMinus("2h"), BASE_TS });
    assertEdge("m1_gb", "m1_s2",
            "m1_gb", "m1_ds",
            "m1_ds", "m1_s1",
            "ds", "m1_gb",
            "ds", "m1_s3");
    assertEdgeToContextNode("ds");
    assertIsMerger("ds");
    assertSources("m1_s1", "s1", "m1_s2", "s2", "m1_s3", "s3");
    assertPushdowns("m1_s1", 0);
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 2,
            DownsampleConfig.class, "ds", "m1_s3", "ds", "m1_s3",
            GroupByConfig.class, "gb_m1_s3", "ds", "gb_m1_s3", "m1_s3");
    assertResultIds("m1_gb", "m1_gb", "m1_s1", "m1_gb", "m1_s2");
    assertResultIds("m1_ds", "m1_ds", "m1_s1");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void autoDs() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "auto", "m1"));
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
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void runAll() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, DownsampleConfig.newBuilder()
            .setAggregator("avg")
            .setInterval("0all")
            .setRunAll(true)
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .addSource("m1")
            .setId("ds")
            .build());
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
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

  @Test
  public void noRollupConfig() throws Exception {
    setupQuery(baseMinus("48h"), BASE_TS, dsConfig("ds", "1m", "m1"));
    setCurrentQuery3OverlappingSources();
    S2.rollupConfig = null;
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
    assertPushdowns("m1_s1", 1,
            DownsampleConfig.class, "ds_m1_s1", "m1_s1", "ds_m1_s1", "m1_s1");
    assertPushdowns("m1_s2", 1,
            DownsampleConfig.class, "ds_m1_s2", "m1_s2", "ds_m1_s2", "m1_s2");
    assertPushdowns("m1_s3", 1,
            DownsampleConfig.class, "ds_m1_s3", "m1_s3", "ds_m1_s3", "m1_s3");
    assertResultIds("ds", "ds", "m1");
    assertResultIds("m1_s1", "m1_s1", "m1_s1");
    assertResultIds("m1_s2", "m1_s2", "m1_s2");
    assertResultIds("m1_s3", "m1_s3", "m1_s3");
  }

}
