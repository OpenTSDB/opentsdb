// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.timedifference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.common.Const;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;

public class TestTimeDifferenceNumericSummaryIterator {

  public static MockTSDB TSDB;
  
  private SemanticQuery query;
  private QueryPipelineContext context;
  private QueryResult result;
  private TimeDifference node;
  private TimeDifferenceConfig config;
  private TimeSeriesStringId id;
  
  @BeforeClass
  public static void beforeClass() {
    TSDB = MockTSDBDefault.getMockTSDB();
  }
  
  @Before
  public void before() throws Exception {
    result = mock(QueryResult.class);
    context = mock(QueryPipelineContext.class);
    query = mock(SemanticQuery.class);
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .build();
    
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(TSDB);
    
    when(query.startTime()).thenReturn(new SecondTimeStamp(60L * 5));
  }
  
  @Test
  public void longs() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(0L))
        .addValue(1, 8L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L))
        .addValue(1, 6L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(1, 3L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(1, 2L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(1, 9L).build());
    
    long timestamp = 60;
    setNode(ChronoUnit.SECONDS);
    TimeDifferenceNumericSummaryIterator iterator = 
        new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    for (int i = 0; i < 4; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericSummaryType> value = iterator.next();
      assertEquals(60, value.value().value(1).longValue());
      assertEquals(timestamp, value.timestamp().epoch());
      timestamp += 60;
    }
    assertFalse(iterator.hasNext());
    
    timestamp = 60;
    setNode(ChronoUnit.MILLIS);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    for (int i = 0; i < 4; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericSummaryType> value = iterator.next();
      assertEquals(60_000, value.value().value(1).longValue());
      assertEquals(timestamp, value.timestamp().epoch());
      timestamp += 60;
    }
    assertFalse(iterator.hasNext());
    
    timestamp = 60;
    setNode(ChronoUnit.NANOS);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    for (int i = 0; i < 4; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericSummaryType> value = iterator.next();
      assertEquals(60_000_000_000L, value.value().value(1).longValue());
      assertEquals(timestamp, value.timestamp().epoch());
      timestamp += 60;
    }
    assertFalse(iterator.hasNext());
    
    timestamp = 60;
    setNode(ChronoUnit.MINUTES);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    for (int i = 0; i < 4; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericSummaryType> value = iterator.next();
      assertEquals(1, value.value().value(1).doubleValue(), 0.001);
      assertEquals(timestamp, value.timestamp().epoch());
      timestamp += 60;
    }
    assertFalse(iterator.hasNext());
    
    timestamp = 60;
    setNode(ChronoUnit.HOURS);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    for (int i = 0; i < 4; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericSummaryType> value = iterator.next();
      assertEquals(0.016, value.value().value(1).doubleValue(), 0.001);
      assertEquals(timestamp, value.timestamp().epoch());
      timestamp += 60;
    }
    assertFalse(iterator.hasNext());
    
    // milli resolution
    ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new MillisecondTimeStamp(500))
        .addValue(1, 8L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new MillisecondTimeStamp(60_000L))
        .addValue(1, 6L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new MillisecondTimeStamp((60_000L * 2) + 500))
        .addValue(1, 3L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new MillisecondTimeStamp(60_000L * 3))
        .addValue(1, 2L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new MillisecondTimeStamp((60_000L * 4) + 500))
        .addValue(1, 0L).build());
    
    setNode(ChronoUnit.MILLIS);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = iterator.next();
    assertEquals(59_500L, value.value().value(1).longValue());
    assertEquals(60_000L, value.timestamp().msEpoch());
    
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertEquals(60_500L, value.value().value(1).longValue());
    assertEquals((60_000L * 2) + 500, value.timestamp().msEpoch());
    
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertEquals(59_500L, value.value().value(1).longValue());
    assertEquals(60_000L * 3, value.timestamp().msEpoch());
    
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertEquals(60_500L, value.value().value(1).longValue());
    assertEquals((60_000L * 4) + 500, value.timestamp().msEpoch());
    assertFalse(iterator.hasNext());
    
    // nano resolution
    ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new ZonedNanoTimeStamp(0L, 500, Const.UTC))
        .addValue(1, 8L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new ZonedNanoTimeStamp(60L, 0, Const.UTC))
        .addValue(1, 6L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new ZonedNanoTimeStamp(60L * 2, 500, Const.UTC))
        .addValue(1, 3L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new ZonedNanoTimeStamp(60L * 3, 0, Const.UTC))
        .addValue(1, 2L).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new ZonedNanoTimeStamp(60L * 4, 500, Const.UTC))
        .addValue(1, 9L).build());
    
    setNode(ChronoUnit.NANOS);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertEquals(59_999_999_500L, value.value().value(1).longValue());
    assertEquals(60L, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertEquals(60_000_000_500L, value.value().value(1).longValue());
    assertEquals(60L * 2, value.timestamp().epoch());
    assertEquals(500, value.timestamp().nanos());
    
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertEquals(59_999_999_500L, value.value().value(1).longValue());
    assertEquals(60L * 3, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertEquals(60_000_000_500L, value.value().value(1).longValue());
    assertEquals(60L * 4, value.timestamp().epoch());
    assertEquals(500, value.timestamp().nanos());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void doubles() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(0L))
        .addValue(1, 8.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L))
        .addValue(1, 6.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
            .addValue(1, 3.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(1, 2.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(1, 9.0).build());
    
    long timestamp = 60;
    setNode(ChronoUnit.SECONDS);
    TimeDifferenceNumericSummaryIterator iterator = 
        new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    for (int i = 0; i < 4; i++) {
      assertTrue(iterator.hasNext());
      TimeSeriesValue<NumericSummaryType> value = iterator.next();
      assertEquals(60, value.value().value(1).longValue());
      assertEquals(timestamp, value.timestamp().epoch());
      timestamp += 60;
    }
    assertFalse(iterator.hasNext());
    
    // test NaNs. The resolution is covered in longs.
    ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(0L))
        .addValue(1, 8.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L))
        .addValue(1, 6.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(1, Double.NaN).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(1, Double.NaN).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(1, 9.0).build());
    
    setNode(ChronoUnit.SECONDS);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> value = iterator.next();
    assertEquals(60L, value.value().value(1).longValue());
    assertEquals(60L, value.timestamp().epoch());
    
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertEquals(180L, value.value().value(1).longValue());
    assertEquals(60L * 4, value.timestamp().epoch());
    assertFalse(iterator.hasNext());
    
    // start nan
    ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(0L))
        .addValue(1, Double.NaN).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L))
        .addValue(1, Double.NaN).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(1, 3.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(1, 2.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(1, 9.0).build());
    
    timestamp = 60L * 3;
    setNode(ChronoUnit.SECONDS);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    for (int i = 0; i < 2; i++) {
      assertTrue(iterator.hasNext());
      value = iterator.next();
      assertEquals(60, value.value().value(1).longValue());
      assertEquals(timestamp, value.timestamp().epoch());
      timestamp += 60;
    }
    assertFalse(iterator.hasNext());
    
    // end Nans
    ts = new MockTimeSeries(id);
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(0L))
        .addValue(1, 8.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L))
        .addValue(1, 6.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 2))
        .addValue(1, 3.0).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 3))
        .addValue(1, Double.NaN).build());
    ts.addValue(MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(60L * 4))
        .addValue(1, Double.NaN).build());
    
    timestamp = 60L;
    setNode(ChronoUnit.SECONDS);
    iterator = new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    for (int i = 0; i < 2; i++) {
      assertTrue(iterator.hasNext());
      value = iterator.next();
      assertEquals(60, value.value().value(1).longValue());
      assertEquals(timestamp, value.timestamp().epoch());
      timestamp += 60;
    }
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void noData() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    
    setNode(ChronoUnit.SECONDS);
    TimeDifferenceNumericSummaryIterator iterator = 
        new TimeDifferenceNumericSummaryIterator(node, result, Lists.newArrayList(ts));
    assertFalse(iterator.hasNext());
  }
  
  void setNode(final ChronoUnit units) {
    node = mock(TimeDifference.class);
    config = TimeDifferenceConfig.newBuilder()
        .setId("diff")
        .setResolution(units)
        .build();
    when(node.config()).thenReturn(config);
  }
}
