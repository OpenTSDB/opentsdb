// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.readcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import com.google.common.collect.Lists;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.UnitTestException;

public class TestCombinedCachedResult {
private static final int BASE_TIME = 1546300800;
  
  private QueryPipelineContext context;
  private TimeSeriesQuery query;
  private List<QuerySink> sinks;
  private QueryNode<?> node;
  private String data_source;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    query = mock(TimeSeriesQuery.class);
    sinks = Lists.newArrayList(mock(QuerySink.class));
    node = mock(QueryNode.class);
    data_source = "m1";
    when(context.query()).thenReturn(query);
    when(query.startTime()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(query.endTime()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 4)));
  }
  
  @Test
  public void emptyResults() throws Exception {
    CombinedCachedResult result = new CombinedCachedResult(context, new QueryResult[0], 
        node, data_source, sinks, "1h");
    
    assertTrue(result.timeSeries().isEmpty());
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
    assertEquals(1, result.resultInterval());
    assertEquals(ChronoUnit.HOURS, result.resultUnits());
  }
  
  @Test
  public void resultsAllEmpty() throws Exception {
    QueryResult[] results = new QueryResult[3];
    results[0] = mockResult(Collections.emptyList(), null, null);
    results[1] = mockResult(Collections.emptyList(), null, null);
    results[2] = mockResult(Collections.emptyList(), null, null);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertTrue(result.timeSeries().isEmpty());
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
  }
  
  @Test
  public void resultsOneWithData() throws Exception {
    QueryResult[] results = new QueryResult[3];
    results[0] = mockResult(Collections.emptyList(), null, null);
    List<TimeSeries> series = Lists.newArrayList(
        mockTimeSeries("web01"),
        mockTimeSeries("web02"));
    results[1] = mockResult(series, null, null);
    results[2] = mockResult(Collections.emptyList(), null, null);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertEquals(2, result.timeSeries().size());
    TimeSeries ts = result.time_series.get(series.get(0).id().buildHashCode());
    assertSame(ts.id(), series.get(0).id());
    ts = result.time_series.get(series.get(1).id().buildHashCode());
    assertSame(ts.id(), series.get(1).id());
    
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
  }
  
  @Test
  public void resultsAllWithData() throws Exception {
    QueryResult[] results = new QueryResult[3];
    List<TimeSeries> series = Lists.newArrayList(
        mockTimeSeries("web01"),
        mockTimeSeries("web02"));
    results[0] = mockResult(series, null, null);
    results[1] = mockResult(series, null, null);
    results[2] = mockResult(series, null, null);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertEquals(2, result.timeSeries().size());
    TimeSeries ts = result.time_series.get(series.get(0).id().buildHashCode());
    assertSame(ts.id(), series.get(0).id());
    ts = result.time_series.get(series.get(1).id().buildHashCode());
    assertSame(ts.id(), series.get(1).id());
    
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
  }
  
  @Test
  public void resultsStaggeredData() throws Exception {
    QueryResult[] results = new QueryResult[3];
    List<TimeSeries> series = Lists.newArrayList(
        mockTimeSeries("web01"));
    results[0] = mockResult(series, null, null);
    series = Lists.newArrayList(
        mockTimeSeries("web02"));
    results[1] = mockResult(series, null, null);
    series = Lists.newArrayList(
        mockTimeSeries("web01"));
    results[2] = mockResult(series, null, null);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertEquals(2, result.timeSeries().size());
    TimeSeries ts = result.time_series.get(mockTimeSeries("web01").id().buildHashCode());
    assertEquals(ts.id(), mockTimeSeries("web01").id());
    ts = result.time_series.get(mockTimeSeries("web02").id().buildHashCode());
    assertEquals(ts.id(), mockTimeSeries("web02").id());
    
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
  }
  
  @Test
  public void resultsTimeSpecAligned() throws Exception {
    QueryResult[] results = new QueryResult[3];
    List<TimeSeries> series = Lists.newArrayList(
        mockTimeSeries("web01"),
        mockTimeSeries("web02"));
    results[0] = mockResult(series, mockTimeSpec(BASE_TIME), null);
    results[1] = mockResult(series, mockTimeSpec(BASE_TIME + 3600), null);
    results[2] = mockResult(series, mockTimeSpec(BASE_TIME + (3600 * 2)), null);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertEquals(2, result.timeSeries().size());
    TimeSeries ts = result.time_series.get(series.get(0).id().buildHashCode());
    assertSame(ts.id(), series.get(0).id());
    ts = result.time_series.get(series.get(1).id().buildHashCode());
    assertSame(ts.id(), series.get(1).id());
    
    TimeSpecification spec = mockTimeSpec(BASE_TIME);
    assertEquals(BASE_TIME, result.timeSpecification().start().epoch());
    assertEquals(BASE_TIME + (3600 * 4), result.timeSpecification().end().epoch());
    assertEquals(spec.interval(), result.timeSpecification().interval());
    assertEquals(spec.stringInterval(), result.timeSpecification().stringInterval());
    assertEquals(spec.timezone(), result.timeSpecification().timezone());
    assertEquals(spec.units(), result.timeSpecification().units());
    assertNull(result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
    assertTrue(result.aligned());
  }
  
  @Test
  public void resultsTimeSpecNotAligned() throws Exception {
    when(query.startTime()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(query.endTime()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 4) - 300));
    QueryResult[] results = new QueryResult[3];
    List<TimeSeries> series = Lists.newArrayList(
        mockTimeSeries("web01"),
        mockTimeSeries("web02"));
    results[0] = mockResult(series, mockTimeSpec(BASE_TIME), null);
    results[1] = mockResult(series, mockTimeSpec(BASE_TIME + 3600), null);
    results[2] = mockResult(series, mockTimeSpec(BASE_TIME + (3600 * 2)), null);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertEquals(2, result.timeSeries().size());
    TimeSeries ts = result.time_series.get(series.get(0).id().buildHashCode());
    assertSame(ts.id(), series.get(0).id());
    ts = result.time_series.get(series.get(1).id().buildHashCode());
    assertSame(ts.id(), series.get(1).id());
    
    TimeSpecification spec = mockTimeSpec(BASE_TIME);
    assertEquals(BASE_TIME + 300, result.timeSpecification().start().epoch());
    assertEquals(BASE_TIME + (3600 * 4 - 300), result.timeSpecification().end().epoch());
    assertEquals(spec.interval(), result.timeSpecification().interval());
    assertEquals(spec.stringInterval(), result.timeSpecification().stringInterval());
    assertEquals(spec.timezone(), result.timeSpecification().timezone());
    assertEquals(spec.units(), result.timeSpecification().units());
    assertNull(result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
    assertFalse(result.aligned());
  }
  
  @Test
  public void resultsRollupConfig() throws Exception {
    QueryResult[] results = new QueryResult[3];
    List<TimeSeries> series = Lists.newArrayList(
        mockTimeSeries("web01"),
        mockTimeSeries("web02"));
    RollupConfig rollup_config = mock(RollupConfig.class);
    results[0] = mockResult(series, null, rollup_config);
    results[1] = mockResult(series, null, rollup_config);
    results[2] = mockResult(series, null, rollup_config);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertEquals(2, result.timeSeries().size());
    TimeSeries ts = result.time_series.get(series.get(0).id().buildHashCode());
    assertSame(ts.id(), series.get(0).id());
    ts = result.time_series.get(series.get(1).id().buildHashCode());
    assertSame(ts.id(), series.get(1).id());
    
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertSame(rollup_config, result.rollupConfig());
  }
  
  @Test
  public void resultsError() throws Exception {
    QueryResult[] results = new QueryResult[3];
    List<TimeSeries> series = Lists.newArrayList(
        mockTimeSeries("web01"),
        mockTimeSeries("web02"));
    results[0] = mockResult(series, null, null);
    results[1] = mock(QueryResult.class);
    when(results[1].error()).thenReturn("Boo!");
    results[2] = mockResult(series, null, null);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertTrue(result.timeSeries().isEmpty());
    assertNull(result.timeSpecification());
    assertEquals("Boo!", result.error());
    assertNull(result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
  }
  
  @Test
  public void resultsException() throws Exception {
    QueryResult[] results = new QueryResult[3];
    List<TimeSeries> series = Lists.newArrayList(
        mockTimeSeries("web01"),
        mockTimeSeries("web02"));
    UnitTestException ex = new UnitTestException();
    results[0] = mockResult(series, null, null);
    results[1] = mock(QueryResult.class);
    when(results[1].exception()).thenReturn(ex);
    results[2] = mockResult(series, null, null);
    CombinedCachedResult result = new CombinedCachedResult(context, results, 
        node, data_source, sinks, "1h");
    
    assertTrue(result.timeSeries().isEmpty());
    assertNull(result.timeSpecification());
    assertNull(result.error());
    assertSame(ex, result.exception());
    assertSame(node, result.source());
    assertEquals(data_source, result.dataSource());
    assertEquals(Const.TS_STRING_ID, result.idType());
    assertNull(result.rollupConfig());
  }
  
  // TODO - figure out how to handle this
//  @Test
//  public void close() throws Exception {
//    CombinedCachedResult result = new CombinedCachedResult(context, new QueryResult[0], 
//        node, data_source, sinks, "1h");
//    result.close();
//    
//    verify(sinks.get(0), times(1)).onComplete();
//  }
  
  QueryResult mockResult(final List<TimeSeries> series, 
                         final TimeSpecification spec, 
                         final RollupConfig rollup_config) {
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(series);
    when(result.timeSpecification()).thenReturn(spec);
    when(result.rollupConfig()).thenReturn(rollup_config);
    return result;
  }
  
  TimeSeries mockTimeSeries(final String host) {
    TimeSeries series = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", host)
        .build();
    when(series.id()).thenReturn(id);
    return series;
  }

  TimeSpecification mockTimeSpec(final int start) {
    TimeSpecification spec = mock(TimeSpecification.class);
    when(spec.start()).thenReturn(new SecondTimeStamp(start));
    when(spec.end()).thenReturn(new SecondTimeStamp(start + 3600));
    when(spec.interval()).thenReturn(Duration.ofMinutes(1));
    when(spec.stringInterval()).thenReturn("1m");
    when(spec.timezone()).thenReturn(Const.UTC);
    when(spec.units()).thenReturn(ChronoUnit.MINUTES);
    return spec;
  }
}
