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
package net.opentsdb.query.processor.groupby;

import com.google.common.collect.Lists;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGroupByNumericSummaryParallelIterator {
  public static MockTSDB TSDB;
  public static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static GroupByFactory groupByFactory;

  private GroupByConfig config;
  private GroupBy node;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;
  private List<TimeSeries> time_series;
  private NumericSummaryInterpolatorConfig interpolator_config;
  private DownsampleConfig ds_config;
  private DefaultRollupConfig rollup_config;
  private GroupByResult result;
  private int queueThreshold = 1000;
  private int threadCount = 8;
  private int timeSeriesPerJob = 512;
  
  // TODO - test floats, gaps, nans, etc.
  
  private static final long BASE_TIME = 1356998400L;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = MockTSDBDefault.getMockTSDB();
    NUMERIC_CONFIG = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();

    groupByFactory = (GroupByFactory) TSDB.registry.getQueryNodeFactory(GroupByFactory.TYPE);
  }
  
  @Before
  public void before() throws Exception {
    rollup_config = DefaultRollupConfig.newBuilder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 2)
        .addAggregationId("avg", 5)
        .addInterval(RollupInterval.builder()
            .setInterval("sum")
            .setTable("tsdb")
            .setPreAggregationTable("tsdb")
            .setInterval("1h")
            .setRowSpan("1d"))
        .build();
    interpolator_config = 
        (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setDataType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .addInterpolatorConfig(interpolator_config)
        .setId("Testing")
        .build();
    result = mock(GroupByResult.class);
    when(result.rollupConfig()).thenReturn(rollup_config);

    ds_config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("1h")
        .setStart(Long.toString(BASE_TIME))
        .setEnd(Long.toString(BASE_TIME + (24 * 8 * 3600)))
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .build();
  }
  
  @Test
  public void sum() throws Exception {
    setupDataLongs(0, false);
    setupMock("sum");
    
    GroupByNumericSummaryParallelIterator iterator = 
        new GroupByNumericSummaryParallelIterator(node, result, time_series, queueThreshold, timeSeriesPerJob, threadCount);
    long ts = BASE_TIME;
    int i = 0;
    double v = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, tsv.timestamp().epoch());
      assertEquals(v, tsv.value().value(0).doubleValue(), 0.001);
      ts += 3600;
      if (v == 0) {
        v = 64;
      } else {
        v += 64;
      }
      i++;
    }
    assertEquals(192, i);
  }
  
  @Test
  public void avg() throws Exception {
    setupDataLongs(5, false);
    setupMock("avg");
    
    GroupByNumericSummaryParallelIterator iterator = 
        new GroupByNumericSummaryParallelIterator(node, result, time_series, queueThreshold, timeSeriesPerJob, threadCount);
    long ts = BASE_TIME;
    int i = 0;
    double v = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, tsv.timestamp().epoch());
      assertEquals(v, tsv.value().value(5).doubleValue(), 0.001);
      ts += 3600;
      v++;
      i++;
    }
    assertEquals(192, i);
  }
  
  @Test
  public void max() throws Exception {
    setupDataLongs(5, false);
    setupMock("max");
    
    GroupByNumericSummaryParallelIterator iterator = 
        new GroupByNumericSummaryParallelIterator(node, result, time_series, queueThreshold, timeSeriesPerJob, threadCount);
    long ts = BASE_TIME;
    int i = 0;
    double v = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, tsv.timestamp().epoch());
      assertEquals(v, tsv.value().value(5).doubleValue(), 0.001);
      ts += 3600;
      v++;
      i++;
    }
    assertEquals(192, i);
  }
  
  @Test
  public void min() throws Exception {
    setupDataLongs(5, false);
    setupMock("min");
    
    GroupByNumericSummaryParallelIterator iterator = 
        new GroupByNumericSummaryParallelIterator(node, result, time_series, queueThreshold, timeSeriesPerJob, threadCount);
    long ts = BASE_TIME;
    int i = 0;
    double v = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, tsv.timestamp().epoch());
      assertEquals(v, tsv.value().value(5).doubleValue(), 0.001);
      ts += 3600;
      v++;
      i++;
    }
    assertEquals(192, i);
  }
  
  @Test
  public void count() throws Exception {
    setupDataLongs(5, false);
    setupMock("count");
    
    GroupByNumericSummaryParallelIterator iterator = 
        new GroupByNumericSummaryParallelIterator(node, result, time_series, queueThreshold, timeSeriesPerJob, threadCount);
    long ts = BASE_TIME;
    int i = 0;
    double v = 64;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, tsv.timestamp().epoch());
      assertEquals(v, tsv.value().value(5).doubleValue(), 0.001);
      ts += 3600;
      i++;
    }
    assertEquals(192, i);
  }
  
  @Test
  public void avgGaps() throws Exception {
    setupDataLongs(5, true);
    setupMock("avg");
    
    GroupByNumericSummaryParallelIterator iterator = 
        new GroupByNumericSummaryParallelIterator(node, result, time_series, queueThreshold, timeSeriesPerJob, threadCount);
    long ts = BASE_TIME;
    int i = 0;
    double v = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      System.out.println(tsv.timestamp().epoch() + "  " + tsv.value().value(5).toDouble());
      assertEquals(ts, tsv.timestamp().epoch());
      if (v % 2 == 0) {
        assertTrue(Double.isNaN(tsv.value().value(5).doubleValue()));
      } else {
        assertEquals(v, tsv.value().value(5).doubleValue(), 0.001);
      }
      ts += 3600;
      v++;
      i++;
    }
    assertEquals(192, i);
  }
  
  @Test
  public void twoSeries() throws Exception {
    setupDataLongs(0, false, 2);
    setupMock("sum");
    
    GroupByNumericSummaryParallelIterator iterator = 
        new GroupByNumericSummaryParallelIterator(node, result, time_series, queueThreshold, timeSeriesPerJob, threadCount);
    long ts = BASE_TIME;
    int i = 0;
    double v = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, tsv.timestamp().epoch());
      assertEquals(v, tsv.value().value(0).doubleValue(), 0.001);
      ts += 3600;
      if (v == 0) {
        v = 2;
      } else {
        v += 2;
      }
      i++;
    }
    assertEquals(192, i);
  }
  
  void setupDataLongs(final int summary, final boolean gaps) {
    setupDataLongs(summary, gaps, 64);
  }
  
  void setupDataLongs(final int summary, final boolean gaps, final int count) {
    time_series = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      MockTimeSeries series = new MockTimeSeries(
          BaseTimeSeriesStringId.newBuilder()
          .setMetric(Integer.toString(i))
          .build());
      
      long ts = BASE_TIME;
      for (int x = 0; x < 24 * 8; x++) {
        if (gaps && x % 2 == 0) {
          ts += 3600;
          continue;
        }
        
        MutableNumericSummaryValue v = new MutableNumericSummaryValue();
        v.resetTimestamp(new SecondTimeStamp(ts));
        v.resetValue(summary, x);
        series.addValue(v);
        ts += 3600;
      }
      time_series.add(series);
    }
  }
  
  private void setupMock(final String agg) {
    config = GroupByConfig.newBuilder()
        .setAggregator(agg)
        .addTagKey("dc")
        .addInterpolatorConfig(interpolator_config)
        .setId("Testing")
        .build();

    node = mock(GroupBy.class);
    when(node.config()).thenReturn(config);
    when(node.getDownsampleConfig()).thenReturn(ds_config);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(node.pipelineContext()).thenReturn(pipeline_context);
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    when(context.tsdb()).thenReturn(TSDB);
    when(result.source()).thenReturn(node);
    when(node.factory()).thenReturn(groupByFactory);
  }
}
