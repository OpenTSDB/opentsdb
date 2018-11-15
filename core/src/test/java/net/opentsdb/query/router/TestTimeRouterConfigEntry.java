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
package net.opentsdb.query.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.router.TimeRouterConfigEntry.MatchType;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TimeRouterConfigEntry.class, DateTime.class })
public class TestTimeRouterConfigEntry {

  private MockTSDB tsdb;
  private TimeSeriesDataSourceFactory factory;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    factory = mock(TimeSeriesDataSourceFactory.class);
    when(tsdb.registry.getPlugin(eq(TimeSeriesDataSourceFactory.class), 
        anyString())).thenReturn(factory);
    when(factory.supportsQuery(any(TimeSeriesQuery.class), 
        any(TimeSeriesDataSourceConfig.class))).thenReturn(true);
  }
  
  @Test
  public void match0Config() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("m1")
        .build();
    
    TimeSeriesQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config)
        .build();
    
    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .build();
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
  }
  
  @Test
  public void matchRelativeEndAndStart() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("m1")
        .build();
    
    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    TimeStamp start = new SecondTimeStamp(7200);
    TimeStamp end = new SecondTimeStamp(9000);
    when(query.startTime()).thenReturn(start);
    when(query.endTime()).thenReturn(end);
    
    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("1h-ago")
        .build();
    
    //PowerMockito.mockStatic(DateTime.class);
    // 2.5 hours ish
    PowerMockito.stub(PowerMockito.method(DateTime.class, 
        "currentTimeMillis")).toReturn(9000 * 1000L);
    
    // ******* start == set, end == 0, e.g. 24h cache
    // full
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial due to start
    start.updateEpoch(3600);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // end in future is fine, still full since end is zero.
    start.updateEpoch(7200);
    end.updateEpoch(10800);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // end before start
    start.updateEpoch(0);
    end.updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // start before end (now) is ok as end is set to 0.
    start.updateEpoch(10800);
    end.updateEpoch(12600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // ****** start == 0, end == relative (e.g warm storage)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setEnd("1h-ago")
        .build();
    
    // full
    start.updateEpoch(0);
    end.updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial due to end being on boundary
    start.updateEpoch(0);
    end.updateEpoch(5400);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // partial due to end being over boundary
    start.updateEpoch(0);
    end.updateEpoch(7200);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // start before end
    start.updateEpoch(5400);
    end.updateEpoch(7200);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // ******* start = relative, end == relative (e.g. transition)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("2h-ago")
        .setEnd("1h-ago")
        .build();
    
    // full
    start.updateEpoch(1800);
    end.updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial start early
    start.updateEpoch(-3600);
    end.updateEpoch(3600);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // partial end late
    start.updateEpoch(1800);
    end.updateEpoch(5400);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // end out of bounds
    start.updateEpoch(0);
    end.updateEpoch(1799);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // start out of bounds
    start.updateEpoch(7200);
    end.updateEpoch(9000);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb)); 
  }

  @Test
  public void matchAbsoluteEndAndStart() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("m1")
        .build();
    
    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    TimeStamp start = new SecondTimeStamp(7200);
    TimeStamp end = new SecondTimeStamp(9000);
    when(query.startTime()).thenReturn(start);
    when(query.endTime()).thenReturn(end);
    
    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("5400")
        .build();
    
    //PowerMockito.mockStatic(DateTime.class);
    // 2.5 hours ish
    PowerMockito.stub(PowerMockito.method(DateTime.class, 
        "currentTimeMillis")).toReturn(9000 * 1000L);
    
    // ******* start == set, end == 0, e.g. 24h cache
    // full
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial due to start
    start.updateEpoch(3600);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // end in future is fine, still full since end is zero.
    start.updateEpoch(7200);
    end.updateEpoch(10800);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // end before start
    start.updateEpoch(0);
    end.updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // start before end (now) is ok as end is set to 0.
    start.updateEpoch(10800);
    end.updateEpoch(12600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // ****** start == 0, end == relative (e.g warm storage)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setEnd("5400")
        .build();
    
    // full
    start.updateEpoch(0);
    end.updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial due to end being on boundary
    start.updateEpoch(0);
    end.updateEpoch(5400);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // partial due to end being over boundary
    start.updateEpoch(0);
    end.updateEpoch(7200);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // start before end
    start.updateEpoch(5400);
    end.updateEpoch(7200);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // ******* start = relative, end == relative (e.g. transition)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("1800")
        .setEnd("5400")
        .build();
    
    // full
    start.updateEpoch(1800);
    end.updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial start early
    start.updateEpoch(-3600);
    end.updateEpoch(3600);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // partial end late
    start.updateEpoch(1800);
    end.updateEpoch(5400);
    assertEquals(MatchType.PARTIAL, entry.match(query, config, tsdb));
    
    // end out of bounds
    start.updateEpoch(0);
    end.updateEpoch(1799);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // start out of bounds
    start.updateEpoch(7200);
    end.updateEpoch(9000);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb)); 
  }
  
  @Test
  public void matchFullOnly() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("m1")
        .build();
    
    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    TimeStamp start = new SecondTimeStamp(7200);
    TimeStamp end = new SecondTimeStamp(9000);
    when(query.startTime()).thenReturn(start);
    when(query.endTime()).thenReturn(end);
    
    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("5400")
        .setFullOnly(true)
        .build();
    
    //PowerMockito.mockStatic(DateTime.class);
    // 2.5 hours ish
    PowerMockito.stub(PowerMockito.method(DateTime.class, 
        "currentTimeMillis")).toReturn(9000 * 1000L);
    
    // ******* start == set, end == 0, e.g. 24h cache
    // full
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial due to start
    start.updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // end in future is fine, still full since end is zero.
    start.updateEpoch(7200);
    end.updateEpoch(10800);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // end before start
    start.updateEpoch(0);
    end.updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // start before end (now) is ok as end is set to 0.
    start.updateEpoch(10800);
    end.updateEpoch(12600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // ****** start == 0, end == relative (e.g warm storage)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setEnd("5400")
        .setFullOnly(true)
        .build();
    
    // full
    start.updateEpoch(0);
    end.updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial due to end being on boundary
    start.updateEpoch(0);
    end.updateEpoch(5400);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // partial due to end being over boundary
    start.updateEpoch(0);
    end.updateEpoch(7200);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // start before end
    start.updateEpoch(5400);
    end.updateEpoch(7200);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // ******* start = relative, end == relative (e.g. transition)
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .setStart("1800")
        .setEnd("5400")
        .setFullOnly(true)
        .build();
    
    // full
    start.updateEpoch(1800);
    end.updateEpoch(3600);
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    
    // partial start early
    start.updateEpoch(-3600);
    end.updateEpoch(3600);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // partial end late
    start.updateEpoch(1800);
    end.updateEpoch(5400);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // end out of bounds
    start.updateEpoch(0);
    end.updateEpoch(1799);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    // start out of bounds
    start.updateEpoch(7200);
    end.updateEpoch(9000);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb)); 
  }
  
  @Test
  public void matchFactory() throws Exception {
    TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric("sys.cpu.user")
            .build())
        .setId("m1")
        .build();
    
    TimeSeriesQuery query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart("1h-ago")
        .addExecutionGraphNode(config)
        .build();
    
    TimeRouterConfigEntry entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .build();
    assertEquals(MatchType.FULL, entry.match(query, config, tsdb));
    verify(tsdb.registry, times(1)).getPlugin(
        TimeSeriesDataSourceFactory.class, "mock");
    
    when(factory.supportsQuery(any(TimeSeriesQuery.class), 
        any(TimeSeriesDataSourceConfig.class))).thenReturn(false);
    assertEquals(MatchType.NONE, entry.match(query, config, tsdb));
    
    entry = TimeRouterConfigEntry.newBuilder()
        .setSourceId("mock")
        .build();
    when(tsdb.registry.getPlugin(eq(TimeSeriesDataSourceFactory.class), 
        anyString())).thenReturn(null);
    try {
      entry.match(query, config, tsdb);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
  }
}
