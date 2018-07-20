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
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.pbuf.NumericSummarySegmentPB.NumericSummarySegment;
import net.opentsdb.data.pbuf.TimeSeriesDataPB.TimeSeriesData;
import net.opentsdb.data.pbuf.TimeSeriesDataSequencePB.TimeSeriesDataSegment;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.execution.serdes.BaseSerdesOptions;
import net.opentsdb.query.serdes.SerdesOptions;

public class TestPBufNumericSummarySerdesFactoryAndIterator {

  private SerdesOptions options;
  private QueryContext ctx;
  private QueryResult result;
  
  @Before
  public void before() throws Exception {
    options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525824000000L))
        .setEnd(new MillisecondTimeStamp(1525827600000L))
        .setId("pbuf")
        .build();
    ctx = mock(QueryContext.class);
    result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
  }
  
  @Test
  public void serializeSeconds() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824000000L));
    v.resetValue(0, 42.5);
    v.resetValue(2, 4);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824060000L));
    v.resetValue(0, 8);
    v.resetValue(2, 1);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(1525824120000L));
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824180000L));
    v.resetValue(0, Double.NaN);
    v.resetValue(2, 0);
    ts.addValue(v);
    
    TimeSeriesData data = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    
    assertEquals(1, data.getSegmentsCount());
    assertEquals(1525824000, data.getSegments(0).getStart().getEpoch());
    assertEquals(0, data.getSegments(0).getStart().getNanos());
    assertEquals("UTC", data.getSegments(0).getStart().getZoneId());
    assertEquals(1525827600, data.getSegments(0).getEnd().getEpoch());
    assertEquals(0, data.getSegments(0).getEnd().getNanos());
    assertEquals("UTC", data.getSegments(0).getEnd().getZoneId());
    assertTrue(data.getSegments(0).getData().is(NumericSummarySegment.class));
    
    PBufNumericSummaryIterator iterator = new PBufNumericSummaryIterator(data);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = iterator.next();
    assertEquals(1525824000, value.timestamp().epoch());
    assertEquals(42.5, value.value().value(0).doubleValue(), 0.001);
    assertEquals(4, value.value().value(2).longValue());
    
    value = iterator.next();
    assertEquals(1525824060, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    value = iterator.next();
    assertEquals(1525824120, value.timestamp().epoch());
    assertNull(value.value());
    
    value = iterator.next();
    assertEquals(1525824180, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(0, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void serializeMilliSeconds() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
    when(result.resolution()).thenReturn(ChronoUnit.MILLIS);
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824000500L));
    v.resetValue(0, 42.5);
    v.resetValue(2, 4);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824060250L));
    v.resetValue(0, 8);
    v.resetValue(2, 1);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(1525824120750L));
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824180001L));
    v.resetValue(0, Double.NaN);
    v.resetValue(2, 0);
    ts.addValue(v);
    
    TimeSeriesData data = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    assertEquals(1, data.getSegmentsCount());
    assertEquals(1525824000, data.getSegments(0).getStart().getEpoch());
    assertEquals(0, data.getSegments(0).getStart().getNanos());
    assertEquals("UTC", data.getSegments(0).getStart().getZoneId());
    assertEquals(1525827600, data.getSegments(0).getEnd().getEpoch());
    assertEquals(0, data.getSegments(0).getEnd().getNanos());
    assertEquals("UTC", data.getSegments(0).getEnd().getZoneId());
    assertTrue(data.getSegments(0).getData().is(NumericSummarySegment.class));
    
    PBufNumericSummaryIterator iterator = new PBufNumericSummaryIterator(data);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = iterator.next();
    assertEquals(1525824000500L, value.timestamp().msEpoch());
    assertEquals(42.5, value.value().value(0).doubleValue(), 0.001);
    assertEquals(4, value.value().value(2).longValue());
    
    value = iterator.next();
    assertEquals(1525824060250L, value.timestamp().msEpoch());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    value = iterator.next();
    assertEquals(1525824120750L, value.timestamp().msEpoch());
    assertNull(value.value());
    
    value = iterator.next();
    assertEquals(1525824180001L, value.timestamp().msEpoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(0, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void serializeNanoSeconds() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
    when(result.resolution()).thenReturn(ChronoUnit.NANOS);
    final ZoneId tz = ZoneId.of("America/Denver");
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new ZonedNanoTimeStamp(1525824000, 250, tz));
    v.resetValue(0, 42.5);
    v.resetValue(2, 4);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new ZonedNanoTimeStamp(1525824060, 123, tz));
    v.resetValue(0, 8);
    v.resetValue(2, 1);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new ZonedNanoTimeStamp(1525824120, 1234443, tz));
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new ZonedNanoTimeStamp(1525824180, 42, tz));
    v.resetValue(0, Double.NaN);
    v.resetValue(2, 0);
    ts.addValue(v);
    
    TimeSeriesData data = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    assertEquals(1, data.getSegmentsCount());
    assertEquals(1525824000, data.getSegments(0).getStart().getEpoch());
    assertEquals(0, data.getSegments(0).getStart().getNanos());
    assertEquals("UTC", data.getSegments(0).getStart().getZoneId());
    assertEquals(1525827600, data.getSegments(0).getEnd().getEpoch());
    assertEquals(0, data.getSegments(0).getEnd().getNanos());
    assertEquals("UTC", data.getSegments(0).getEnd().getZoneId());
    assertTrue(data.getSegments(0).getData().is(NumericSummarySegment.class));
    
    PBufNumericSummaryIterator iterator = new PBufNumericSummaryIterator(data);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = iterator.next();
    assertEquals(1525824000L, value.timestamp().epoch());
    assertEquals(250, value.timestamp().nanos());
    assertEquals(42.5, value.value().value(0).doubleValue(), 0.001);
    assertEquals(4, value.value().value(2).longValue());
    
    value = iterator.next();
    assertEquals(1525824060, value.timestamp().epoch());
    assertEquals(123, value.timestamp().nanos());
    assertEquals(8, value.value().value(0).longValue());
    assertEquals(1, value.value().value(2).longValue());
    
    value = iterator.next();
    assertEquals(1525824120, value.timestamp().epoch());
    assertEquals(1234443, value.timestamp().nanos());
    assertNull(value.value());
    
    value = iterator.next();
    assertEquals(1525824180, value.timestamp().epoch());
    assertEquals(42, value.timestamp().nanos());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertEquals(0, value.value().value(2).longValue());
    
    assertFalse(iterator.hasNext());
  }

  @Test
  public void serializeAllNulls() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(1525824000000L));
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(1525824060000L));
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(1525824120000L));
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(1525824180000L));
    ts.addValue(v);
    
    TimeSeriesData data = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    assertEquals(1, data.getSegmentsCount());
    assertEquals(1525824000, data.getSegments(0).getStart().getEpoch());
    assertEquals(0, data.getSegments(0).getStart().getNanos());
    assertEquals("UTC", data.getSegments(0).getStart().getZoneId());
    assertEquals(1525827600, data.getSegments(0).getEnd().getEpoch());
    assertEquals(0, data.getSegments(0).getEnd().getNanos());
    assertEquals("UTC", data.getSegments(0).getEnd().getZoneId());
    assertTrue(data.getSegments(0).getData().is(NumericSummarySegment.class));
    
    PBufNumericSummaryIterator iterator = new PBufNumericSummaryIterator(data);
    assertFalse(iterator.hasNext());
    // NOTE: Unlike the numeric packing, we don't have a way to null in
    // the pbuf without knowing which summary to add nulls to.
  }

  @Test
  public void serializeResolutionScrewUp() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824000250L));
    v.resetValue(0, 42.5);
    v.resetValue(2, 4);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824000500L));
    v.resetValue(0, 8);
    v.resetValue(2, 1);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(1525824000750L));
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824180001L));
    v.resetValue(0, Double.NaN);
    v.resetValue(2, 0);
    ts.addValue(v);
    
    try {
      factory.serialize(ctx, options, result, 
          ts.iterator(NumericSummaryType.TYPE).get());
      fail("Expected SerdesException");
    } catch (SerdesException e) { }
  }

  @Test
  public void serializeStaggered() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824000000L));
    //v.resetValue(0, 42.5);
    v.resetValue(2, 4);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824060000L));
    v.resetValue(0, 8);
    //v.resetValue(2, 1);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824120000L));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    ts.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1525824180000L));
    v.resetValue(0, Double.NaN);
    //v.resetValue(2, 0);
    ts.addValue(v);
    
    TimeSeriesData data = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    assertEquals(1, data.getSegmentsCount());
    assertEquals(1525824000, data.getSegments(0).getStart().getEpoch());
    assertEquals(0, data.getSegments(0).getStart().getNanos());
    assertEquals("UTC", data.getSegments(0).getStart().getZoneId());
    assertEquals(1525827600, data.getSegments(0).getEnd().getEpoch());
    assertEquals(0, data.getSegments(0).getEnd().getNanos());
    assertEquals("UTC", data.getSegments(0).getEnd().getZoneId());
    assertTrue(data.getSegments(0).getData().is(NumericSummarySegment.class));
    
    PBufNumericSummaryIterator iterator = new PBufNumericSummaryIterator(data);
    assertTrue(iterator.hasNext());
    
    TimeSeriesValue<NumericSummaryType> value = iterator.next();
    assertEquals(1525824000, value.timestamp().epoch());
    assertNull(value.value().value(0));
    assertEquals(4, value.value().value(2).longValue());
    
    value = iterator.next();
    assertEquals(1525824060, value.timestamp().epoch());
    assertEquals(8, value.value().value(0).longValue());
    assertNull(value.value().value(2));
    
    value = iterator.next();
    assertEquals(1525824120, value.timestamp().epoch());
    assertEquals(24, value.value().value(0).longValue());
    assertEquals(5, value.value().value(2).longValue());
    
    value = iterator.next();
    assertEquals(1525824180, value.timestamp().epoch());
    assertTrue(Double.isNaN(value.value().value(0).doubleValue()));
    assertNull(value.value().value(2));
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iteratorEmpty() throws Exception {
    TimeSeriesData source = TimeSeriesData.newBuilder()
        .build();
    PBufNumericSummaryIterator iterator = 
        new PBufNumericSummaryIterator(source);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void iteratorCtor() throws Exception {
    try {
      new PBufNumericSummaryIterator(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void iteratorMultipleSegments() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    long time = 1525824000000L;
    int integer = 0;
    for (int i = 0; i < 6; i++) {
      MutableNumericSummaryValue v = new MutableNumericSummaryValue();
      v.resetTimestamp(new MillisecondTimeStamp(time + (i * 600000)));
      v.resetValue(0, integer * 2);
      v.resetValue(2, integer++);
      ts.addValue(v);
    }
        
    TimeSeriesData data = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    assertEquals(1, data.getSegmentsCount());
    assertEquals(1525824000, data.getSegments(0).getStart().getEpoch());
    assertEquals(0, data.getSegments(0).getStart().getNanos());
    assertEquals("UTC", data.getSegments(0).getStart().getZoneId());
    assertEquals(1525827600, data.getSegments(0).getEnd().getEpoch());
    assertEquals(0, data.getSegments(0).getEnd().getNanos());
    assertEquals("UTC", data.getSegments(0).getEnd().getZoneId());
    assertTrue(data.getSegments(0).getData().is(NumericSummarySegment.class));
    
    options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525827600000L))
        .setEnd(new MillisecondTimeStamp(1525831200000L))
        .setId("pbuf")
        .build();
    
    ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    time = 1525827600000L;
    for (int i = 0; i < 6; i++) {
      MutableNumericSummaryValue v = new MutableNumericSummaryValue();
      v.resetTimestamp(new MillisecondTimeStamp(time + (i * 600000)));
      v.resetValue(0, integer * 2);
      v.resetValue(2, integer++);
      ts.addValue(v);
    }
    
    TimeSeriesData.Builder builder = TimeSeriesData.newBuilder(data);
    TimeSeriesData data2 = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    builder.addSegments(data2.getSegments(0));
    
    options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525831200000L))
        .setEnd(new MillisecondTimeStamp(1525834800000L))
        .setId("pbuf")
        .build();
    
    ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    time = 1525831200000L;
    for (int i = 0; i < 6; i++) {
      MutableNumericSummaryValue v = new MutableNumericSummaryValue();
      v.resetTimestamp(new MillisecondTimeStamp(time + (i * 600000)));
      v.resetValue(0, integer * 2);
      v.resetValue(2, integer++);
      ts.addValue(v);
    }
    
    data2 = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    builder.addSegments(data2.getSegments(0));
    
    PBufNumericSummaryIterator iterator = 
        new PBufNumericSummaryIterator(builder.build());
    
    time = 1525824000000L;
    integer = 0;
    while(iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> value = iterator.next();
      assertEquals(time, value.timestamp().msEpoch());
      assertEquals(integer * 2, value.value().value(0).longValue());
      assertEquals(integer++, value.value().value(2).longValue());
      time += 600000;
    }
    assertEquals(1525834800000L, time);
  }
  
  @Test
  public void iteratorMultipleSegmentsEmptyMiddle() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    long time = 1525824000000L;
    int integer = 0;
    for (int i = 0; i < 6; i++) {
      MutableNumericSummaryValue v = new MutableNumericSummaryValue();
      v.resetTimestamp(new MillisecondTimeStamp(time + (i * 600000)));
      v.resetValue(0, integer * 2);
      v.resetValue(2, integer++);
      ts.addValue(v);
    }
        
    TimeSeriesData data = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    assertEquals(1, data.getSegmentsCount());
    assertEquals(1525824000, data.getSegments(0).getStart().getEpoch());
    assertEquals(0, data.getSegments(0).getStart().getNanos());
    assertEquals("UTC", data.getSegments(0).getStart().getZoneId());
    assertEquals(1525827600, data.getSegments(0).getEnd().getEpoch());
    assertEquals(0, data.getSegments(0).getEnd().getNanos());
    assertEquals("UTC", data.getSegments(0).getEnd().getZoneId());
    assertTrue(data.getSegments(0).getData().is(NumericSummarySegment.class));
    
    options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525827600000L))
        .setEnd(new MillisecondTimeStamp(1525831200000L))
        .setId("pbuf")
        .build();
    
    // no data!
    
    TimeSeriesData.Builder builder = TimeSeriesData.newBuilder(data);
    TimeSeriesData data2 = factory.serialize(ctx, options, result, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class));
    builder.addSegments(data2.getSegments(0));
    
    options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525831200000L))
        .setEnd(new MillisecondTimeStamp(1525834800000L))
        .setId("pbuf")
        .build();
    
    ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    time = 1525831200000L;
    integer = 12;
    for (int i = 0; i < 6; i++) {
      MutableNumericSummaryValue v = new MutableNumericSummaryValue();
      v.resetTimestamp(new MillisecondTimeStamp(time + (i * 600000)));
      v.resetValue(0, integer * 2);
      v.resetValue(2, integer++);
      ts.addValue(v);
    }
    
    data2 = factory.serialize(ctx, options, result, 
        ts.iterator(NumericSummaryType.TYPE).get());
    builder.addSegments(data2.getSegments(0));
    
    PBufNumericSummaryIterator iterator = 
        new PBufNumericSummaryIterator(builder.build());
    
    time = 1525824000000L;
    integer = 0;
    while(iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> value = iterator.next();
      assertEquals(time, value.timestamp().msEpoch());
      assertEquals(integer * 2, value.value().value(0).longValue());
      assertEquals(integer++, value.value().value(2).longValue());
      if (value.timestamp().msEpoch() == 1525827000000L) {
        time = 1525831200000L;
        integer = 12;
      } else {
        time += 600000;
      }
    }
    assertEquals(1525834800000L, time);
  }
  
  @Test
  public void iteratorMultipleSegmentsAllEmpty() throws Exception {
    PBufNumericSummaryTimeSeriesSerdes factory = new PBufNumericSummaryTimeSeriesSerdes();
        
    TimeSeriesData data = factory.serialize(ctx, options, result, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class));
    assertEquals(1, data.getSegmentsCount());
    assertEquals(1525824000, data.getSegments(0).getStart().getEpoch());
    assertEquals(0, data.getSegments(0).getStart().getNanos());
    assertEquals("UTC", data.getSegments(0).getStart().getZoneId());
    assertEquals(1525827600, data.getSegments(0).getEnd().getEpoch());
    assertEquals(0, data.getSegments(0).getEnd().getNanos());
    assertEquals("UTC", data.getSegments(0).getEnd().getZoneId());
    assertTrue(data.getSegments(0).getData().is(NumericSummarySegment.class));
    
    options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525827600000L))
        .setEnd(new MillisecondTimeStamp(1525831200000L))
        .setId("pbuf")
        .build();
    
    // no data!
    
    TimeSeriesData.Builder builder = TimeSeriesData.newBuilder(data);
    TimeSeriesData data2 = factory.serialize(ctx, options, result, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class));
    builder.addSegments(data2.getSegments(0));
    
    options = BaseSerdesOptions.newBuilder()
        .setStart(new MillisecondTimeStamp(1525831200000L))
        .setEnd(new MillisecondTimeStamp(1525834800000L))
        .setId("pbuf")
        .build();
    
    // no data!
    
    data2 = factory.serialize(ctx, options, result, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class));
    builder.addSegments(data2.getSegments(0));
    
    PBufNumericSummaryIterator iterator = 
        new PBufNumericSummaryIterator(builder.build());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void iteratorWrongSegmentType() throws Exception {
    TimeSeriesData source = TimeSeriesData.newBuilder()
        .addSegments(TimeSeriesDataSegment.newBuilder())
        .build();
    try {
      new PBufNumericSummaryIterator(source);
      fail("Expected SerdesException");
    } catch (SerdesException e) { }
  }
}
