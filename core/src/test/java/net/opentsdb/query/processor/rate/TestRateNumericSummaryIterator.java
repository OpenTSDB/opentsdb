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

import org.junit.Test;

import com.google.common.collect.Lists;

public class TestRateNumericSummaryIterator {
  private TimeSeriesQuery query;
  private RateConfig config;
  private QueryNode node;
  private QueryResult result;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;
  
  private static final long BASE_TIME = 1356998400000L;
  
  @Test
  public void longSumsNoGap() throws Exception {
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
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void longSumsGap() throws Exception {
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
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(0).doubleValue()));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void doubleSumsNoGap() throws Exception {
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
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void doublesSumsGap() throws Exception {
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
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(0).doubleValue()));
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void alternateDoublesLongs() throws Exception {
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
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(1, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void AvgNoGap() throws Exception {
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
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    assertEquals(0.0, v.value().value(1).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 2), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    assertEquals(0.0, v.value().value(1).doubleValue(), 0.0001);
    
    assertTrue(it.hasNext());
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600000 * 3), v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(0.0333, v.value().value(0).doubleValue(), 0.0001);
    assertEquals(0.0, v.value().value(1).doubleValue(), 0.0001);
    
    assertFalse(it.hasNext());
  }
  
  // TODO - more UTS.
  
  void setupMock() {
    node = mock(QueryNode.class);
    result = mock(QueryResult.class);
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
        v.resetValue(0, Double.NaN);
        if (avg) {
          v.resetValue(1, Double.NaN);
        }
      } else {
        if (alt_ld) {
          if (i % 2 == 0) {
            v.resetValue(0, val);
          } else {
            v.resetValue(0, (long) val);
          }
        } else if (doubles) {
          v.resetValue(0, val);
        } else {
          v.resetValue(0, (long) val);
        }
        if (avg) {
          v.resetValue(1, 60);
        }
      }
      ts.addValue(v);
      val += 120;
      timestamp += 3600000;
    }
    return ts;
  }
}
