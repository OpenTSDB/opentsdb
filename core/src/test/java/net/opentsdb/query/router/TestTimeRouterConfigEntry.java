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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.router.TimeRouterConfigEntry.ConfigSorter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.router.TimeRouterConfigEntry.MatchType;
import net.opentsdb.utils.DateTime;

import java.util.Collections;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TimeRouterConfigEntry.class, DateTime.class })
public class TestTimeRouterConfigEntry {

  private MockTSDB tsdb;
  private TimeSeriesDataSourceFactory factory;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    factory = mock(TimeSeriesDataSourceFactory.class);
    when(factory.supportsQuery(any(QueryPipelineContext.class), 
        any(TimeSeriesDataSourceConfig.class))).thenReturn(true);
  }

  @Test
  public void builder() throws Exception {
    // relative start, no end
    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
            .setStart("2h-ago")
            .setSourceId("s1")
            .build();
    assertEquals("s1", entry.getSourceId());
    assertEquals("2h-ago", entry.getStartString());
    assertNull(entry.getEndString());
    assertTrue(entry.isStartRelative());
    assertTrue(entry.isEndRelative());
    assertFalse(entry.isFullOnly());
    assertNull(entry.getDataType());
    assertNull(entry.getTimeZone());
    assertNull(entry.getTimeout());

    // relative end
    entry = TimeRouterConfigEntry.newBuilder()
            .setStart("24h-ago")
            .setEnd("2h-ago")
            .setSourceId("s1")
            .build();
    assertEquals("s1", entry.getSourceId());
    assertEquals("24h-ago", entry.getStartString());
    assertEquals("2h-ago", entry.getEndString());
    assertTrue(entry.isStartRelative());
    assertTrue(entry.isEndRelative());
    assertFalse(entry.isFullOnly());
    assertNull(entry.getDataType());
    assertNull(entry.getTimeZone());
    assertNull(entry.getTimeout());

    // fixed start, relative end
    long now = System.currentTimeMillis() / 1000;
    entry = TimeRouterConfigEntry.newBuilder()
            .setStart(Long.toString(now))
            .setSourceId("s1")
            .build();
    assertEquals("s1", entry.getSourceId());
    assertEquals(Long.toString(now), entry.getStartString());
    assertNull(entry.getEndString());
    assertFalse(entry.isStartRelative());
    assertTrue(entry.isEndRelative());
    assertFalse(entry.isFullOnly());
    assertNull(entry.getDataType());
    assertNull(entry.getTimeZone());
    assertNull(entry.getTimeout());

    // relative start, fixed end. This can be used when shifting out of a cluster
    // or source with a TTL. E.g. you turn up a new TSDB table with bigger UIDs
    // and you set the start to the retention but the end is fixed. Eventually
    // the start would be after the end so it would never be queried.
    entry = TimeRouterConfigEntry.newBuilder()
            .setStart("2h-ago")
            .setEnd(Long.toString(now))
            .setSourceId("s1")
            .build();
    assertEquals("s1", entry.getSourceId());
    assertEquals("2h-ago", entry.getStartString());
    assertEquals(Long.toString(now), entry.getEndString());
    assertTrue(entry.isStartRelative());
    assertFalse(entry.isEndRelative());
    assertFalse(entry.isFullOnly());
    assertNull(entry.getDataType());
    assertNull(entry.getTimeZone());
    assertNull(entry.getTimeout());

    // fixed start but no end. The corollary to the above where we're
    // shifting off a cluster.
    entry = TimeRouterConfigEntry.newBuilder()
            .setStart(Long.toString(now - (3600 * 24)))
            .setSourceId("s1")
            .build();
    assertEquals("s1", entry.getSourceId());
    assertEquals(Long.toString(now - (3600 * 24)), entry.getStartString());
    assertNull(entry.getEndString());
    assertFalse(entry.isStartRelative());
    assertTrue(entry.isEndRelative());
    assertFalse(entry.isFullOnly());
    assertNull(entry.getDataType());
    assertNull(entry.getTimeZone());
    assertNull(entry.getTimeout());

    // fixed start, relative end
    entry = TimeRouterConfigEntry.newBuilder()
            .setStart(Long.toString(now - (3600 * 24)))
            .setEnd("2h-ago")
            .setSourceId("s1")
            .build();
    assertEquals("s1", entry.getSourceId());
    assertEquals(Long.toString(now - (3600 * 24)), entry.getStartString());
    assertEquals("2h-ago", entry.getEndString());
    assertFalse(entry.isStartRelative());
    assertTrue(entry.isEndRelative());
    assertFalse(entry.isFullOnly());
    assertNull(entry.getDataType());
    assertNull(entry.getTimeZone());
    assertNull(entry.getTimeout());

    // start and end fixed timestamps
    entry = TimeRouterConfigEntry.newBuilder()
            .setStart(Long.toString(now - (3600 * 24)))
            .setEnd(Long.toString(now))
            .setSourceId("s1")
            .build();
    assertEquals("s1", entry.getSourceId());
    assertEquals(Long.toString(now - (3600 * 24)), entry.getStartString());
    assertEquals(Long.toString(now), entry.getEndString());
    assertFalse(entry.isStartRelative());
    assertFalse(entry.isEndRelative());
    assertFalse(entry.isFullOnly());
    assertNull(entry.getDataType());
    assertNull(entry.getTimeZone());
    assertNull(entry.getTimeout());

    // other fields
    entry = TimeRouterConfigEntry.newBuilder()
            .setStart(Long.toString(now - (3600 * 24)))
            .setEnd(Long.toString(now))
            .setFullOnly(true)
            .setTimeout("1m")
            .setDataType("event")
            .setTimeZone("America/Denver")
            .setSourceId("s1")
            .build();
    assertEquals("s1", entry.getSourceId());
    assertEquals(Long.toString(now - (3600 * 24)), entry.getStartString());
    assertEquals(Long.toString(now), entry.getEndString());
    assertFalse(entry.isStartRelative());
    assertFalse(entry.isEndRelative());
    assertTrue(entry.isFullOnly());
    assertEquals("event", entry.getDataType());
    assertEquals("America/Denver", entry.getTimeZone());
    assertEquals("1m", entry.getTimeout());
  }

  @Test
  public void configSorterRelativeOverlap() throws Exception {
    List<TimeRouterConfigEntry> sources = Lists.newArrayList(
            TimeRouterConfigEntry.newBuilder()
                    .setStart("2h-ago").setSourceId("s1").build(),
            TimeRouterConfigEntry.newBuilder()
                    .setStart("48h-ago").setEnd("22h-ago").setSourceId("s3").build(),
            TimeRouterConfigEntry.newBuilder()
                    .setStart("24h-ago").setEnd("1h-ago").setSourceId("s2").build());
    Collections.sort(sources, new ConfigSorter());
    assertEquals("s1", sources.get(0).getSourceId());
    assertEquals("s2", sources.get(1).getSourceId());
    assertEquals("s3", sources.get(2).getSourceId());
  }

  @Test
  public void configSorterRelativeButtJoints() throws Exception {
    List<TimeRouterConfigEntry> sources = Lists.newArrayList(
            TimeRouterConfigEntry.newBuilder()
                    .setStart("2h-ago").setSourceId("s1").build(),
            TimeRouterConfigEntry.newBuilder()
                    .setStart("48h-ago").setEnd("24h-ago").setSourceId("s3").build(),
            TimeRouterConfigEntry.newBuilder()
                    .setStart("24h-ago").setEnd("2h-ago").setSourceId("s2").build());
    Collections.sort(sources, new ConfigSorter());
    assertEquals("s1", sources.get(0).getSourceId());
    assertEquals("s2", sources.get(1).getSourceId());
    assertEquals("s3", sources.get(2).getSourceId());
  }

  @Test
  public void configSorterRelativeAndAbsoluteOverlap() throws Exception {
    ConfigSorter sorter = new ConfigSorter();
    List<TimeRouterConfigEntry> sources = Lists.newArrayList(
            TimeRouterConfigEntry.newBuilder()
                    .setStart("2h-ago").setSourceId("s1").build(),
            TimeRouterConfigEntry.newBuilder()
                    .setStart("48h-ago").setEnd(Integer.toString(
                            (int) sorter.now - (3600 * 22))).setSourceId("s3").build(),
            TimeRouterConfigEntry.newBuilder()
                    .setStart(Integer.toString(
                            (int) sorter.now - (3600 * 24))).setEnd("1h-ago").setSourceId("s2").build());
    Collections.sort(sources, sorter);
    assertEquals("s1", sources.get(0).getSourceId());
    assertEquals("s2", sources.get(1).getSourceId());
    assertEquals("s3", sources.get(2).getSourceId());
  }

  @Test
  public void match0Config() throws Exception {
    TimeSeriesDataSourceConfig.Builder config = (TimeSeriesDataSourceConfig.Builder)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("m1");

    TimeSeriesQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config.build())
        .build();

    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setFactory(factory)
        .build();
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.query()).thenReturn(query);
    assertEquals(MatchType.FULL, entry.match(context, config, 0));
  }

  @Test
  public void matchRelativeEndAndStart() throws Exception {
    TimeStamp start = new SecondTimeStamp(7200);
    TimeStamp end = new SecondTimeStamp(9000);
    TimeSeriesDataSourceConfig.Builder config = (TimeSeriesDataSourceConfig.Builder)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
            .setStartTimeStamp(start)
            .setEndTimeStamp(end)
        .setId("m1");

    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    when(query.startTime()).thenReturn(start);
    when(query.endTime()).thenReturn(end);

    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("1h-ago")
        .setFactory(factory)
        .build();

    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.query()).thenReturn(query);

    // ******* start == set, end == 0, e.g. 24h cache
    // full
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial due to start
    config.startOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // end in future is fine, still full since end is zero.
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(10800);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // start before end (now) is ok as end is set to 0.
    config.startOverrideTimeStamp().updateEpoch(10800);
    config.endOverrideTimeStamp().updateEpoch(12600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // ****** start == 0, end == relative (e.g warm storage)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setEnd("1h-ago")
        .setFactory(factory)
        .build();

    // full
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial due to end being on boundary
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(5400);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // partial due to end being over boundary
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(7200);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // start before end
    config.startOverrideTimeStamp().updateEpoch(5400);
    config.endOverrideTimeStamp().updateEpoch(7200);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // ******* start = relative, end == relative (e.g. transition)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("2h-ago")
        .setEnd("1h-ago")
        .setFactory(factory)
        .build();

    // full
    config.startOverrideTimeStamp().updateEpoch(1800);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial start early
    config.startOverrideTimeStamp().updateEpoch(-3600);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // partial end late
    config.startOverrideTimeStamp().updateEpoch(1800);
    config.endOverrideTimeStamp().updateEpoch(5400);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // end out of bounds
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(1799);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // start out of bounds
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(9000);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));
  }

  @Test
  public void matchAbsoluteEndAndStart() throws Exception {
    TimeStamp start = new SecondTimeStamp(7200);
    TimeStamp end = new SecondTimeStamp(9000);
    TimeSeriesDataSourceConfig.Builder config = (TimeSeriesDataSourceConfig.Builder)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setStartTimeStamp(start)
        .setEndTimeStamp(end)
        .setId("m1");

    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    when(query.startTime()).thenReturn(start);
    when(query.endTime()).thenReturn(end);

    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("5400")
        .setFactory(factory)
        .build();

    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.query()).thenReturn(query);

    // ******* start == set, end == 0, e.g. 24h cache
    // full
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial due to start
    config.startOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // end in future is fine, still full since end is zero.
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(10800);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // end before start
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // start before end (now) is ok as end is set to 0.
    config.startOverrideTimeStamp().updateEpoch(10800);
    config.endOverrideTimeStamp().updateEpoch(12600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // ****** start == 0, end == relative (e.g warm storage)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setEnd("5400")
        .setFactory(factory)
        .build();

    // full
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial due to end being on boundary
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(5400);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // partial due to end being over boundary
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(7200);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // start before end
    config.startOverrideTimeStamp().updateEpoch(5400);
    config.endOverrideTimeStamp().updateEpoch(7200);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // ******* start = relative, end == relative (e.g. transition)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("1800")
        .setEnd("5400")
        .setFactory(factory)
        .build();

    // full
    config.startOverrideTimeStamp().updateEpoch(1800);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial start early
    config.startOverrideTimeStamp().updateEpoch(-3600);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // partial end late
    config.startOverrideTimeStamp().updateEpoch(1800);
    config.endOverrideTimeStamp().updateEpoch(5400);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // end out of bounds
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(1799);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // start out of bounds
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(9000);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));
  }

  @Test
  public void matchFullOnly() throws Exception {
    TimeStamp start = new SecondTimeStamp(7200);
    TimeStamp end = new SecondTimeStamp(9000);
    TimeSeriesDataSourceConfig.Builder config = (TimeSeriesDataSourceConfig.Builder)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setStartTimeStamp(start)
        .setEndTimeStamp(end)
        .setId("m1");

    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    when(query.startTime()).thenReturn(start);
    when(query.endTime()).thenReturn(end);

    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("5400")
        .setFullOnly(true)
        .setFactory(factory)
        .build();

    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.query()).thenReturn(query);

    // ******* start == set, end == 0, e.g. 24h cache
    // full
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial due to start
    config.startOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // end in future is fine, still full since end is zero.
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(10800);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // end before start
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // start before end (now) is ok as end is set to 0.
    config.startOverrideTimeStamp().updateEpoch(10800);
    config.endOverrideTimeStamp().updateEpoch(12600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // ****** start == 0, end == relative (e.g warm storage)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setEnd("5400")
        .setFullOnly(true)
        .setFactory(factory)
        .build();

    // full
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial due to end being on boundary
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(5400);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // partial due to end being over boundary
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(7200);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // start before end
    config.startOverrideTimeStamp().updateEpoch(5400);
    config.endOverrideTimeStamp().updateEpoch(7200);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // ******* start = relative, end == relative (e.g. transition)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("1800")
        .setEnd("5400")
        .setFullOnly(true)
        .setFactory(factory)
        .build();

    // full
    config.startOverrideTimeStamp().updateEpoch(1800);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial start early
    config.startOverrideTimeStamp().updateEpoch(-3600);
    config.endOverrideTimeStamp().updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // partial end late
    config.startOverrideTimeStamp().updateEpoch(1800);
    config.endOverrideTimeStamp().updateEpoch(5400);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // end out of bounds
    config.startOverrideTimeStamp().updateEpoch(0);
    config.endOverrideTimeStamp().updateEpoch(1799);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));

    // start out of bounds
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(9000);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));
  }

  @Test
  public void matchFactory() throws Exception {
    TimeSeriesDataSourceConfig.Builder config = (TimeSeriesDataSourceConfig.Builder)
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("m1");

    TimeSeriesQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config.build())
        .build();

    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setFactory(factory)
        .build();
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.query()).thenReturn(query);

    assertEquals(MatchType.FULL, entry.match(context, config, 0));
    when(factory.supportsQuery(any(QueryPipelineContext.class),
        any(TimeSeriesDataSourceConfig.class))).thenReturn(false);
    assertEquals(MatchType.NONE, entry.match(context, config, 0));
  }

  @Test
  public void matchTimeShift() throws Exception {
    TimeStamp start = new SecondTimeStamp(7200);
    TimeStamp end = new SecondTimeStamp(9000);
    TimeSeriesDataSourceConfig.Builder config = (TimeSeriesDataSourceConfig.Builder)
            DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                            .setMetric("sys.cpu.user")
                            .build())
                    .setStartTimeStamp(start)
                    .setEndTimeStamp(end)
                    .setTimeShiftInterval("5m")
                    .setId("m1");

    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    when(query.startTime()).thenReturn(start);
    when(query.endTime()).thenReturn(end);

    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
            .setSourceId("mock")
            .setStart("1h-ago")
            .setFactory(factory)
            .build();

    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.query()).thenReturn(query);

    // full
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(9000);
    assertEquals(MatchType.FULL, entry.match(context, config, 9000));

    // partial - note same timestamps
    config = (TimeSeriesDataSourceConfig.Builder)
            DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                            .setMetric("sys.cpu.user")
                            .build())
                    .setStartTimeStamp(start)
                    .setEndTimeStamp(end)
                    .setTimeShiftInterval("45m")
                    .setId("m1");
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(9000);
    assertEquals(MatchType.PARTIAL, entry.match(context, config, 9000));

    // too early
    config = (TimeSeriesDataSourceConfig.Builder)
            DefaultTimeSeriesDataSourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                            .setMetric("sys.cpu.user")
                            .build())
                    .setStartTimeStamp(start)
                    .setEndTimeStamp(end)
                    .setTimeShiftInterval("1h")
                    .setId("m1");
    config.startOverrideTimeStamp().updateEpoch(7200);
    config.endOverrideTimeStamp().updateEpoch(9000);
    assertEquals(MatchType.NONE, entry.match(context, config, 9000));
  }
}
