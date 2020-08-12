// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.TimeSeriesQuery;

public class TestCombinedCachedTimeSeries {
private static final int BASE_TIME = 1546300800;
  
  private QueryPipelineContext context;
  private TimeSeriesQuery query;
  private List<QuerySink> sinks;
  private TimeSeriesId id;
  private QueryNode<?> node;
  private QueryResultId data_source;
  
  @Before
  public void before() throws Exception {
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build();
    context = mock(QueryPipelineContext.class);
    query = mock(TimeSeriesQuery.class);
    sinks = Lists.newArrayList(mock(QuerySink.class));
    node = mock(QueryNode.class);
    data_source = new DefaultQueryResultId("m1", "m1");
    when(context.query()).thenReturn(query);
    when(query.startTime()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(query.endTime()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 4)));
  }
  
  @Test
  public void ctorAndAddAndClose() throws Exception {
    CombinedCachedResult result = mockResult(3);
    TimeSeries ts1 = mockSeries(NumericType.TYPE);
    CombinedCachedTimeSeries cts = new CombinedCachedTimeSeries(result, 0, ts1);
    
    assertEquals(3, cts.series.length);
    assertSame(ts1, cts.series[0]);
    assertNull(cts.series[1]);
    assertNull(cts.series[2]);
    
    TimeSeries ts2 = mockSeries(NumericType.TYPE);
    TimeSeries ts3 = mockSeries(NumericType.TYPE);
    cts.add(2, ts3);
    cts.add(1, ts2);
    
    assertSame(ts1, cts.series[0]);
    assertSame(ts2, cts.series[1]);
    assertSame(ts3, cts.series[2]);
    
    assertEquals(1, cts.types().size());
    assertTrue(cts.types().contains(NumericType.TYPE));
    
    cts.close();
    verify(ts1, times(1)).close();
    verify(ts2, times(1)).close();
    verify(ts3, times(1)).close();
  }
  
  @Test
  public void iterator() throws Exception {
    CombinedCachedResult result = mockResult(3);
    TimeSeries ts1 = mockSeries(NumericType.TYPE);
    TimeSeries ts2 = mockSeries(NumericType.TYPE);
    CombinedCachedTimeSeries cts = new CombinedCachedTimeSeries(result, 0, ts1);
    cts.add(1, ts2);
    
    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> it = 
        cts.iterator(NumericType.TYPE);
    assertTrue(it.isPresent());
    assertTrue(it.get() instanceof CombinedCachedNumeric);
    
    // no such type
    it = cts.iterator(NumericArrayType.TYPE);
    assertFalse(it.isPresent());
    
    // array
    ts1 = mockSeries(NumericArrayType.TYPE);
    ts2 = mockSeries(NumericArrayType.TYPE);
    cts = new CombinedCachedTimeSeries(result, 0, ts1);
    cts.add(1, ts2);
    
    it = cts.iterator(NumericArrayType.TYPE);
    assertTrue(it.isPresent());
    assertTrue(it.get() instanceof CombinedCachedNumericArray);
    
    // summary
    ts1 = mockSeries(NumericSummaryType.TYPE);
    ts2 = mockSeries(NumericSummaryType.TYPE);
    cts = new CombinedCachedTimeSeries(result, 0, ts1);
    cts.add(1, ts2);
    
    it = cts.iterator(NumericSummaryType.TYPE);
    assertTrue(it.isPresent());
    assertTrue(it.get() instanceof CombinedCachedNumericSummary);
  }
  
  @Test
  public void iterators() throws Exception {
    CombinedCachedResult result = mockResult(3);
    TimeSeries ts1 = mockSeries(NumericType.TYPE);
    TimeSeries ts2 = mockSeries(NumericType.TYPE);
    CombinedCachedTimeSeries cts = new CombinedCachedTimeSeries(result, 0, ts1);
    cts.add(1, ts2);
    
    Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators = 
        cts.iterators();
    assertEquals(1, iterators.size());
    assertTrue(iterators.iterator().next() instanceof CombinedCachedNumeric);
    
    // array
    ts1 = mockSeries(NumericArrayType.TYPE);
    ts2 = mockSeries(NumericArrayType.TYPE);
    cts = new CombinedCachedTimeSeries(result, 0, ts1);
    cts.add(1, ts2);
    
    iterators = cts.iterators();
    assertEquals(1, iterators.size());
    assertTrue(iterators.iterator().next() instanceof CombinedCachedNumericArray);
    
    // summary
    ts1 = mockSeries(NumericSummaryType.TYPE);
    ts2 = mockSeries(NumericSummaryType.TYPE);
    cts = new CombinedCachedTimeSeries(result, 0, ts1);
    cts.add(1, ts2);
    
    iterators = cts.iterators();
    assertEquals(1, iterators.size());
    assertTrue(iterators.iterator().next() instanceof CombinedCachedNumericSummary);
  }
  
  CombinedCachedResult mockResult(final int num_results) {
    QueryResult[] results = new QueryResult[num_results];
    int timestamp = BASE_TIME;
    for (int i = 0; i < num_results; i++) {
      QueryResult r = mock(QueryResult.class);
      TimeSpecification time_spec = mock(TimeSpecification.class);
      when(r.timeSpecification()).thenReturn(time_spec);
      when(time_spec.start()).thenReturn(new SecondTimeStamp(timestamp));
      when(time_spec.end()).thenReturn(new SecondTimeStamp(timestamp + 3600));
      when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
      when(time_spec.stringInterval()).thenReturn("1m");
      results[i] = r;
    }
    CombinedCachedResult result = new CombinedCachedResult(context, results, node, 
        data_source, sinks, "1h");
    return result;
  }
  
  TimeSeries mockSeries(final TypeToken<? extends TimeSeriesDataType> type) {
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(id);
    when(ts.types()).thenReturn(Lists.newArrayList(type));
    when(ts.iterator(any(TypeToken.class))).thenReturn(Optional
        .of(mock(TypedTimeSeriesIterator.class)));
    return ts;
  }
}
