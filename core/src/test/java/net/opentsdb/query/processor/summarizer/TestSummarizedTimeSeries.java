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
package net.opentsdb.query.processor.summarizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.aggregators.AverageFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.SumFactory;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;

public class TestSummarizedTimeSeries {
  private static final long BASE_TIME = 1356998400L;
  
  private QueryPipelineContext context;
  private SummarizerPassThroughResult result;
  private SummarizerConfig config;
  private Summarizer node;
  private TimeSeries source;
  private SemanticQuery query;
  private RollupConfig rollup_config;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    result = mock(SummarizerPassThroughResult.class);
    source = mock(TimeSeries.class);
    node = mock(Summarizer.class);
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Long.toString(BASE_TIME))
        .setEnd(Long.toString(BASE_TIME + 3600))
        .setExecutionGraph(Collections.emptyList())
        .build();
    when(context.query()).thenReturn(query);
    
    config = (SummarizerConfig) 
        SummarizerConfig.newBuilder()
        .setSummaries(Lists.newArrayList("sum", "avg"))
        .setInfectiousNan(true)
        .setId("summarizer")
        .build();
    
    when(result.source()).thenReturn(node);
    when(node.pipelineContext()).thenReturn(context);
    Map<String, NumericAggregator> aggs = Maps.newHashMap();
    when(node.aggregators()).thenReturn(aggs);
    aggs.put("avg", new AverageFactory().newAggregator());
    aggs.put("sum", new SumFactory().newAggregator());
    
    rollup_config = DefaultRollupConfig.newBuilder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addAggregationId("max", 2)
        .addAggregationId("min", 3)
        .addAggregationId("avg", 5)
        .addInterval(RollupInterval.builder()
            .setInterval("sum")
            .setTable("tsdb")
            .setPreAggregationTable("tsdb")
            .setInterval("1h")
            .setRowSpan("1d"))
        .build();
    when(result.rollupConfig()).thenReturn(rollup_config);
    when(result.summarizerNode()).thenReturn(node);
    when(node.config()).thenReturn(config);
  }
  
  @Test
  public void summarizeLong() throws Exception {
    SummarizedTimeSeries ts = new SummarizedTimeSeries(result, source);
    long[] values = new long[] { 42, 8, 16, -4, 23 };
    ts.summarize(values, 0, values.length);
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericSummaryType.TYPE));
    
    TypedTimeSeriesIterator<NumericSummaryType> iterator = 
        (TypedTimeSeriesIterator<NumericSummaryType>) 
            ts.iterator(NumericSummaryType.TYPE).get();
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertEquals(2, value.value().summariesAvailable().size());
    assertEquals(85, value.value().value(0).longValue());
    assertEquals(17, value.value().value(5).longValue());
  }
  
  @Test
  public void summarizeDoubles() throws Exception {
    SummarizedTimeSeries ts = new SummarizedTimeSeries(result, source);
    double[] values = new double[] { 42.5, 8.0, 16.01, -4, 23.8 };
    ts.summarize(values, 0, values.length);
    assertEquals(1, ts.types().size());
    assertTrue(ts.types().contains(NumericSummaryType.TYPE));
    
    TypedTimeSeriesIterator<NumericSummaryType> iterator = 
        (TypedTimeSeriesIterator<NumericSummaryType>) 
            ts.iterator(NumericSummaryType.TYPE).get();
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = iterator.next();
    assertEquals(0, value.timestamp().epoch());
    assertEquals(2, value.value().summariesAvailable().size());
    assertEquals(86.31, value.value().value(0).doubleValue(), 0.001);
    assertEquals(17.262, value.value().value(5).doubleValue(), 0.001);
  }
}
