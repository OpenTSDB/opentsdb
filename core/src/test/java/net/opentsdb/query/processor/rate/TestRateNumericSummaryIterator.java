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
package net.opentsdb.query.processor.rate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRateNumericSummaryIterator {
  private TimeSeriesQuery query;
  private RateConfig config;
  private QueryNode node;
  private QueryResult result;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;
  private DefaultRollupConfig rollup_config;
  private int sum_id;
  private int count_id;
  
  private static final long BASE_TIME = 1356998400000L;
  
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
    sum_id = rollup_config.getIdForAggregator("sum");
    count_id = rollup_config.getIdForAggregator("count");
  }
  
  @Test
  public void counterLongSumsNoGap() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(false, false, false, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterLongSumsGap() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(true, false, false, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDoubleSumsNoGap() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(false, false, true, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDoublesSumsGap() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(true, false, true, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterAlternateDoublesLongs() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(false, false, false, true)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterAvgNoGap() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(false, true, false, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(60, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(60, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(60, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterAvgGap() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(true, true, false, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(60, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(60, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(count_id).doubleValue()));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDoublesSumsAsCount() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setRateToCount(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(true, false, true, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(240, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(360, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterAvgLongReset() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, false)));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(2.5620477880152155E15, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0205, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterAvgDoubleReset() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, true)));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(2.5620477880152155E15, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0205, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterAvgLongResetDrop() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .setDropResets(true)
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, false)));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(count_id).doubleValue()));
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0205, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterAvgDoubleResetDrop() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .setDropResets(true)
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, true)));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(count_id).doubleValue()));
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0205, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterAvgLongResetThreshold() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .setResetValue(1024)
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, false)));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(count_id).doubleValue()));
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0205, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }

  @Test
  public void counterAvgDoubleResetThreshold() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setCounter(true)
        .setId("foo")
        .setResetValue(1024)
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, true)));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(count_id).doubleValue()));
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0205, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void longResetThreshold() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .setResetValue(1)
        .build();
    
    setupMock();
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    long timestamp = BASE_TIME;
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    dp.resetValue(sum_id, 120);
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    dp.resetValue(sum_id, 1024 * 16);
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    dp.resetValue(sum_id, 1024 * 17);
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    dp.resetValue(sum_id, 1024 * 18);
    ts.addValue(dp);
    
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(ts));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.2844, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.2844, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void doubleResetThreshold() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setId("foo")
        .setResetValue(1)
        .build();
    
    setupMock();
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    long timestamp = BASE_TIME;
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    dp.resetValue(sum_id, 120.0);
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    dp.resetValue(sum_id, 1024.0 * 16.0);
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    dp.resetValue(sum_id, 1024.0 * 17.0);
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    dp.resetValue(sum_id, 1024.0 * 18.0);
    ts.addValue(dp);
    
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(ts));
    
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.2844, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.2844, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void deltaOnlyLongs() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(false, false, false, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void deltaOnlyDoubles() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildData(false, false, true, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void deltaOnlyLongsReset() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).longValue());
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(-234, v.value().value(sum_id).longValue());
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(74, v.value().value(sum_id).longValue());
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void deltaOnlyDoublesReset() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, true)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(-234, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(74, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDeltaOnlyLongsDropReset() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setCounter(true)
        .setDropResets(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, false)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).longValue());
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(count_id).doubleValue()));
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(74, v.value().value(sum_id).longValue());
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void counterDeltaOnlyDoublesDropReset() throws Exception {
    config = (RateConfig) RateConfig.newBuilder()
        .setInterval("1s")
        .setDeltaOnly(true)
        .setCounter(true)
        .setDropResets(true)
        .setId("foo")
        .build();
    
    setupMock();
    RateNumericSummaryIterator it = new RateNumericSummaryIterator(node, result,
        Lists.newArrayList(buildResetData(true, true)));
    assertTrue(it.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 3600000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(5, v.value().value(count_id).longValue());
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(sum_id).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(count_id).doubleValue()));
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(74, v.value().value(sum_id).doubleValue(), 0.0001);
    assertEquals(2, v.value().value(count_id).longValue());
    
    assertFalse(it.hasNext());
  }
  
  void setupMock() {
    node = mock(QueryNode.class);
    result = mock(QueryResult.class);
    when(result.rollupConfig()).thenReturn(rollup_config);
    when(node.config()).thenReturn(config);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(query_context.query()).thenReturn(query);
    when(node.pipelineContext()).thenReturn(pipeline_context);
  }
  
  MockTimeSeries buildData(final boolean nan_gap, 
                           final boolean avg, 
                           final boolean doubles, 
                           final boolean alt_ld) {
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    long timestamp = BASE_TIME;
    double val = 120;
    for (int i = 0; i < 4; i++) {
      MutableNumericSummaryValue v = new MutableNumericSummaryValue();
      v.resetTimestamp(new MillisecondTimeStamp(timestamp));
      if (nan_gap && i > 0 && i % 3 == 0) {
        v.resetValue(sum_id, Double.NaN);
        if (avg) {
          v.resetValue(count_id, Double.NaN);
        }
      } else {
        if (alt_ld) {
          if (i % 2 == 0) {
            v.resetValue(sum_id, val);
          } else {
            v.resetValue(sum_id, (long) val);
          }
        } else if (doubles) {
          v.resetValue(sum_id, val);
        } else {
          v.resetValue(sum_id, (long) val);
        }
        if (avg) {
          v.resetValue(count_id, 60);
        }
      }
      ts.addValue(v);
      val += 120;
      timestamp += 3600000;
    }
    return ts;
  }
  
  MockTimeSeries buildResetData(final boolean avg, final boolean doubles) {
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    long timestamp = BASE_TIME;
    MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    if (doubles) {
      dp.resetValue(sum_id, 120.0);
    } else {
      dp.resetValue(sum_id, 120);
    }
    if (avg) {
      dp.resetValue(count_id, 5);
    }
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    if (doubles) {
      dp.resetValue(sum_id, 240.0);
    } else {
      dp.resetValue(sum_id, 240);
    }
    if (avg) {
      dp.resetValue(count_id, 5);
    }
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    if (doubles) {
      dp.resetValue(sum_id, 6.0);
    } else {
      dp.resetValue(sum_id, 6);
    }
    if (avg) {
      dp.resetValue(count_id, 2);
    }
    ts.addValue(dp);
    timestamp += 3600000;
    
    dp = new MutableNumericSummaryValue();
    dp.resetTimestamp(new MillisecondTimeStamp(timestamp));
    if (doubles) {
      dp.resetValue(sum_id, 80.0);
    } else {
      dp.resetValue(sum_id, 80);
    }
    if (avg) {
      dp.resetValue(count_id, 2);
    }
    ts.addValue(dp);
    timestamp += 3600000;
    return ts;
  }
}
