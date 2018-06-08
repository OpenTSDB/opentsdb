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
package net.opentsdb.query.processor.groupby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryInterpolatorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.DefaultInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestGroupByNumericSummaryIterator {
  private GroupByConfig config;
  private QueryNode node;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;
  private MockTimeSeries ts1;
  private MockTimeSeries ts2;
  private MockTimeSeries ts3;
  private Map<String, TimeSeries> source_map;
  private NumericSummaryInterpolatorConfig interpolator_config;
  private DefaultRollupConfig rollup_config;
  private QueryResult result;
  
  private static final long BASE_TIME = 1356998400000L;
  
  @Before
  public void before() throws Exception {
    rollup_config = DefaultRollupConfig.builder()
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
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    result = mock(QueryResult.class);
    when(result.rollupConfig()).thenReturn(rollup_config);
  }
  
  @Test
  public void nextAllPresent() throws Exception {
    long[] sums = new long[] { 10, 11, 12, 13, 21, 22, 23, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sum(sums, i, false), tsv.value().value(0).longValue());
      assertEquals(sum(counts, i, false), tsv.value().value(2).longValue());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextStaggeredMissing() throws Exception {
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextStaggeredNaNs() throws Exception {
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, true);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextStaggeredNaNsInfectiousNans() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, true);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesStart() throws Exception {
    long[] sums = new long[] { -1, 11, 12, 13, -1, 22, 23, 24, -1, 32, 33, 34 };
    long[] counts = new long[] { -1, 2, 3, 4, -1, 2, 3, 4, -1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesEnd() throws Exception {
    long[] sums = new long[] { 10, 11, 12, -1, 22, 22, 23, -1, 31, 32, 33, -1 };
    long[] counts = new long[] { 1, 2, 3, -1, 1, 2, 3, -1, 1, 2, 3, -1 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesMiddle() throws Exception {
    long[] sums = new long[] { 10, -1, 12, 13, 22, -1, 23, 24, 31, -1, 33, 34 };
    long[] counts = new long[] { 1, -1, 3, 4, 1, -1, 3, 4, 1, -1, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      if (sum < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, false);
      if (sum < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesStartFillInfectiousNan() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { -1, 11, 12, 13, -1, 22, 23, 24, -31, 32, 33, 34 };
    long[] counts = new long[] { -1, 2, 3, 4, 1, 2, 3, 4, -1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesEndFillInfectiousNan() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { 10, 11, 12, -1, 22, 22, 23, -1, 31, 32, 33, -1 };
    long[] counts = new long[] { 1, 2, 3, -1, 1, 2, 3, -1, 1, 2, 3, -1 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoSummariesMiddleFillInfectiousNan() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { 10, -1, 12, 13, 22, -1, 23, 24, 31, -1, 33, 34 };
    long[] counts = new long[] { 1, -1, 3, 4, 1, -1, 3, 4, 1, -1, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(0).longValue());
      }
      sum = sum(counts, i, true);
      if (sum < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(sum, tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextOneSeriesEmpty() throws Exception {
    long[] sums = new long[] { 10, 11, 12, 13, -1, -1, -1, -1, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, -1, -1, -1, 1, 2, 3, 4 };
    setupData(sums, counts, false);
    ts2.clear();
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      long sum = sum(sums, i, false);
      assertEquals(sum, tsv.value().value(0).longValue());
      sum = sum(counts, i, false);
      assertEquals(sum, tsv.value().value(2).longValue());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountAllPresent() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { 10, 11, 12, 13, 21, 22, 23, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(avg(sums, counts, i, false), tsv.value().value(5).doubleValue(), 0.0001);
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountEmpty() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { 10, 11, 12, 13, 21, 22, 23, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    ts1.clear();
    ts2.clear();
    ts3.clear();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void nextAvgSumAndCountStaggeredMissing() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(avg(sums, counts, i, false), tsv.value().value(5).doubleValue(), 0.0001);
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountStaggeredMissingStaggeredNaNs() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, true);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(avg(sums, counts, i, false), tsv.value().value(5).doubleValue(), 0.0001);
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountStaggeredMissingStaggeredNaNsInfectious() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setInfectiousNan(true)
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { -1, 11, 12, -1, 21, 22, -1, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, -1, 2, 3, -1, -1, 2, -1, 4 }; 
    setupData(sums, counts, true);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertTrue(Double.isNaN(tsv.value().value(5).doubleValue()));
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountNoSummariesStart() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { -1, 11, 12, 13, -1, 22, 23, 24, -1, 32, 33, 34 };
    long[] counts = new long[] { -1, 2, 3, 4, -1, 2, 3, 4, -1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountNoSummariesEnd() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { 10, 11, 12, -1, 22, 22, 23, -1, 31, 32, 33, -1 };
    long[] counts = new long[] { 1, 2, 3, -1, 1, 2, 3, -1, 1, 2, 3, -1 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAvgSumAndCountNoSummariesMiddle() throws Exception {
    interpolator_config = (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NONE)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setComponentAggregator(Aggregators.SUM)
        .setType(NumericSummaryType.TYPE.toString())
        .build();
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(interpolator_config)
        .build();
    
    long[] sums = new long[] { 10, -1, 12, 13, 22, -1, 23, 24, 31, -1, 33, 34 };
    long[] counts = new long[] { 1, -1, 3, 4, 1, -1, 3, 4, 1, -1, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextFromNumericInterpolatorConfig() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("sum")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setType(NumericType.TYPE.toString())
            .build())
        .build();
    
    long[] sums = new long[] { 10, 11, 12, 13, 21, 22, 23, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sum(sums, i, false), tsv.value().value(0).longValue());
      assertNull(tsv.value().value(2));
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextFromNumericInterpolatorConfigAvg() throws Exception {
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setAggregator("avg")
        .addTagKey("dc")
        .setId("Testing")
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NONE)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setType(NumericType.TYPE.toString())
            .build())
        .build();
    
    long[] sums = new long[] { 10, 11, 12, 13, 21, 22, 23, 24, 31, 32, 33, 34 };
    long[] counts = new long[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 }; 
    setupData(sums, counts, false);
    setupMock();
    
    GroupByNumericSummaryIterator iterator = new GroupByNumericSummaryIterator(node, result, source_map);
    long ts = BASE_TIME;
    int i = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> tsv = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      double avg = avg(sums, counts, i, true);
      if (avg < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(avg, tsv.value().value(5).doubleValue(), 0.0001);
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  // TODO - ints the doubles
  
  private long sum(long[] dps, int i, boolean infectious) {
    long sum = -1;
    for (int x = 0; x < 3; x++) {
      if (dps[i + (x * 4)] < 0) {
        if (infectious) {
          return -1;
        }
      } else {
        if (sum < 0) {
          sum = 0;
        }
        sum += dps[i + (x * 4)];
      }
    }
    return sum;
  }
  
  private double avg(long[] sums, long[] counts, int i, boolean infectious) {
    long sum = -1;
    long count = -1;
    for (int x = 0; x < 3; x++) {
      if (sums[i + (x * 4)] < 0) {
        if (infectious) {
          return -1;
        }
      } else {
        if (sum < 0) {
          sum = 0;
        }
        sum += sums[i + (x * 4)];
      }
      
      if (counts[i + (x * 4)] < 0) {
        if (infectious) {
          return -1;
        }
      } else {
        if (count < 0) {
          count = 0;
        }
        count += counts[i + (x * 4)];
      }
    }
    
    return (double) sum / (double) count;
  }
  
  private void setupData(long[] sums, long[] counts, boolean nans) {
    ts1 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    int sum_idx = 0;
    int counts_idx = 0;
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts1.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts1.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts1.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts1.addValue(v);
    
    ts2 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts2.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts2.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts2.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts2.addValue(v);
    
    ts3 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts3.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts3.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts3.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    if (sums[sum_idx] >= 0) {
      v.resetValue(0, sums[sum_idx]);
    } else if (nans) {
      v.resetValue(0, Double.NaN);
    }
    if (counts[counts_idx] >= 0) {
      v.resetValue(2, counts[counts_idx]);
    } else if (nans) {
      v.resetValue(2, Double.NaN);
    }
    sum_idx++;
    counts_idx++;
    ts3.addValue(v);
    
    source_map = Maps.newHashMapWithExpectedSize(3);
    source_map.put("a", ts1);
    source_map.put("b", ts2);
    source_map.put("c", ts3);
  }
  
  private void setupMock() throws Exception {
    node = mock(QueryNode.class);
    when(node.config()).thenReturn(config);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(node.pipelineContext()).thenReturn(pipeline_context);
    final QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    final TSDB tsdb = mock(TSDB.class);
    when(context.tsdb()).thenReturn(tsdb);
    final Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    final QueryInterpolatorFactory interp_factory = new DefaultInterpolatorFactory();
    interp_factory.initialize(tsdb).join();
    when(registry.getPlugin(any(Class.class), anyString())).thenReturn(interp_factory);
  }
  
  void print(final TimeSeriesValue<NumericSummaryType> tsv) {
    System.out.println("**** [UT] " + tsv.timestamp());
    if (tsv.value() == null) {
      System.out.println("**** [UT] Null value *****");
    } else {
      for (int summary : tsv.value().summariesAvailable()) {
        NumericType t = tsv.value().value(summary);
        if (t == null) {
          System.out.println("***** [UT] value for " + summary + " was null");
        } else {
          System.out.println("***** [UT] [" + summary + "] " + t.toDouble());
        }
      }
    }
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
  }
}