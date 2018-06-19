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
package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.ZoneId;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.DefaultInterpolatorFactory;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestDownsampleNumericSummaryIterator {
  private DownsampleConfig config;
  private QueryNode node;
  private QueryContext query_context;
  private QueryPipelineContext pipeline_context;
  private MockTimeSeries source;
  private QueryResult result;
  private DefaultRollupConfig rollup_config;
  
  private static final long BASE_TIME = 1356998400000L;
  //30 minute offset
  final static ZoneId AF = ZoneId.of("Asia/Kabul");
  // 12h offset w/o DST
  final static ZoneId TV = ZoneId.of("Pacific/Funafuti");
  // 12h offset w DST
  final static ZoneId FJ = ZoneId.of("Pacific/Fiji");
  // Tue, 15 Dec 2015 04:02:25.123 UTC
  final static long DST_TS = 1450137600000L;
  
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
    
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
  }
  
  @Test
  public void downsampler1hAligned() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 24, 36, 2 };
    long[] counts = new long[] { 2, 5, 1, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void downsampler1hAlignedAvg() throws Exception {
    setConfig("avg", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 24, 36, 2 };
    long[] counts = new long[] { 2, 5, 1, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
      assertEquals(1, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void downsampler1hWithinQueryBounds() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME - (3600 * 2L * 1000L), 
        BASE_TIME + (3600 * 6L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 24, 36, 2 };
    long[] counts = new long[] { 2, 5, 1, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void downsampler1hWithinQueryBoundsAvg() throws Exception {
    setConfig("avg", "1h", false, BASE_TIME - (3600 * 2L * 1000L), 
        BASE_TIME + (3600 * 6L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 24, 36, 2 };
    long[] counts = new long[] { 2, 5, 1, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
      assertEquals(1, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void downsampler1hFilteredByQueryBounds() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME + (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    assertTrue(ds.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) ds.next();
    assertEquals(ts, tsv.timestamp().msEpoch());
    assertEquals(24, tsv.value().value(0).longValue());
    assertEquals(5, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler1hFilteredByQueryBoundsAvg() throws Exception {
    setConfig("avg", "1h", false, BASE_TIME + (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    assertTrue(ds.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) ds.next();
    assertEquals(ts, tsv.timestamp().msEpoch());
    assertNull(tsv.value().value(0));
    assertNull(tsv.value().value(2));
    assertEquals(4.8, tsv.value().value(5).doubleValue(), 0.001);
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler1hDataAfterStart() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME - (3600 * 5L * 1000L), 
        BASE_TIME - (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
   assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler1hDataAfterStartAvg() throws Exception {
    setConfig("avg", "1h", false, BASE_TIME - (3600 * 5L * 1000L), 
        BASE_TIME - (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
   assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler1hDataBeforeStart() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME + (3600 * 5L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
   assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler1hDataBeforeStartAvg() throws Exception {
    setConfig("avg", "1h", false, BASE_TIME + (3600 * 5L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
   assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler1hGaps() throws Exception {
    setGappyData(false);
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36, 15, 6 };
    long[] counts = new long[] { 2, 1, 5, 3 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      if (ts == BASE_TIME + (3600 * 4L * 1000L)) {
        ts += 3600 * 1000L;
      } else {
        ts += 3600 * 2 * 1000L;
      }
    }
    assertEquals(4, i);
  }
  
  @Test
  public void downsampler1hGapsAvg() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
//    v.resetValue(0, 24);
//    v.resetValue(2, 5);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
//    v.resetValue(0, 2);
//    v.resetValue(2, 4);
//    source.addValue(v);
    setConfig("avg", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
      assertEquals(1, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
      i++;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hGapsStart() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
//    v.resetValue(0, 42);
//    v.resetValue(2, 2);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
//    v.resetValue(0, 36);
//    v.resetValue(2, 1);
//    source.addValue(v);
//    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1000L);
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2L * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hGapsStartAvg() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
//    v.resetValue(0, 42);
//    v.resetValue(2, 2);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
//    v.resetValue(0, 36);
//    v.resetValue(2, 1);
//    source.addValue(v);
//    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("avg", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1000L);
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
      assertEquals(1, tsv.value().summariesAvailable().size());
      ts += 3600 * 2L * 1000L;
      i++;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hGapsStaggeredAvg() throws Exception {
    setGappyData(true);
    setConfig("avg", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36, -1, 6 };
    long[] counts = new long[] { -1, 1, -1, 3 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      
      if (sums[i] < 0 || counts[i] < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
        assertEquals(1, tsv.value().summariesAvailable().size());
      }
      if (ts == BASE_TIME + (3600L * 4L * 1000L)) {
        ts += 3600 * 1000L;
      } else {
        ts += 3600 * 2 * 1000L;
      }
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void downsampler1hNullValues() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hNullValuesAvg() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    setConfig("avg", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
      assertEquals(1, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
      i++;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hNullValuesStart() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hNullValuesStartAvg() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("avg", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
      assertEquals(1, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
      i++;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hNoSummaries() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hNoSummariesAvg() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    setConfig("avg", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
      assertEquals(1, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
      i++;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hNoSummariesStart() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hNulls() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hNullsStart() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler1hEmpty() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler2hAligned() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 66, 38 };
    long[] counts = new long[] { 7, 5 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler2hFilteredByQueryBounds() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME + (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 4L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 38 };
    long[] counts = new long[] { 5 };
    long ts = BASE_TIME + (3600 * 2L * 1000L);
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(1, i);
  }
  
  @Test
  public void downsampler2hGaps() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
//    v.resetValue(0, 24);
//    v.resetValue(2, 5);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
//    v.resetValue(0, 2);
//    v.resetValue(2, 4);
//    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler2hGapsStart() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
//    v.resetValue(0, 42);
//    v.resetValue(2, 2);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
//    v.resetValue(0, 36);
//    v.resetValue(2, 1);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler2hNullValues() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler2hNullValuesStart() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler2hNoSummaries() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler2hNoSummariesAvg() throws Exception {
    setConfig("avg", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertNull(tsv.value().value(0));
      assertNull(tsv.value().value(2));
      assertEquals((double) sums[i] / (double) counts[i], tsv.value().value(5).doubleValue(), 0.001);
      assertEquals(1, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
      i++;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler2hNoSummariesStart() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }

  @Test
  public void downsampler2hNulls() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler2hNullsStart() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (ds.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) ds.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void downsampler3h() throws Exception {
    setConfig("sum", "3h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    // only one value as the second is truncated by the config end.
    assertTrue(ds.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) ds.next();
    assertEquals(BASE_TIME, tsv.timestamp().msEpoch());
    assertEquals(102, tsv.value().value(0).longValue());
    assertEquals(8, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler3hGaps() throws Exception {
    setConfig("sum", "3h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
//    v.resetValue(0, 24);
//    v.resetValue(2, 5);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
//    v.resetValue(0, 2);
//    v.resetValue(2, 4);
//    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    // only one value as the second is truncated by the config end.
    assertTrue(ds.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) ds.next();
    assertEquals(BASE_TIME, tsv.timestamp().msEpoch());
    assertEquals(78, tsv.value().value(0).longValue());
    assertEquals(3, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsampler3hGapsStart() throws Exception {
    setConfig("sum", "3h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
//    v.resetValue(0, 42);
//    v.resetValue(2, 2);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
//    v.resetValue(0, 36);
//    v.resetValue(2, 1);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    // only one value as the second is truncated by the config end.
    assertTrue(ds.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) ds.next();
    assertEquals(BASE_TIME, tsv.timestamp().msEpoch());
    assertEquals(24, tsv.value().value(0).longValue());
    assertEquals(5, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(ds.hasNext());
  }

  @Test
  public void downsamplerAll() throws Exception {
    setConfig("sum", "0all", true, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    // only one value as the second is truncated by the config end.
    assertTrue(ds.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) ds.next();
    assertEquals(BASE_TIME, tsv.timestamp().msEpoch());
    assertEquals(104, tsv.value().value(0).longValue());
    assertEquals(12, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsamplerAllFilterOnQuery() throws Exception {
    setConfig("sum", "0all", true, BASE_TIME + (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    // only one value as the second is truncated by the config end.
    assertTrue(ds.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) ds.next();
    assertEquals(BASE_TIME + (3600 * 1L * 1000L), tsv.timestamp().msEpoch());
    assertEquals(60, tsv.value().value(0).longValue());
    assertEquals(6, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsamplerAllDataAfterStart() throws Exception {
    setConfig("sum", "0all", true, BASE_TIME - (3600 * 5L * 1000L), 
        BASE_TIME - (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void downsamplerAllDataBeforeStart() throws Exception {
    setConfig("sum", "0all", true, BASE_TIME + (3600 * 5L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    net.opentsdb.query.processor.downsample.DownsampleNumericSummaryIterator
      .Downsampler ds = it.new Downsampler();
    
    assertFalse(ds.hasNext());
  }
  
  @Test
  public void iterator1hAligned() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 24, 36, 2 };
    long[] counts = new long[] { 2, 5, 1, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void iterator1hWithinQueryBounds() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME - (3600 * 2L * 1000L), 
        BASE_TIME + (3600 * 6L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 24, 36, 2 };
    long[] counts = new long[] { 2, 5, 1, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void iterator1hFilteredByQueryBounds() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME + (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    assertTrue(it.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(ts, tsv.timestamp().msEpoch());
    assertEquals(24, tsv.value().value(0).longValue());
    assertEquals(5, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iterator1hDataAfterStart() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME - (3600 * 5L * 1000L), 
        BASE_TIME - (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
   assertFalse(it.hasNext());
  }
  
  @Test
  public void iterator1hDataBeforeStart() throws Exception {
    setConfig("sum", "1h", false, BASE_TIME + (3600 * 5L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
   assertFalse(it.hasNext());
  }
  
  @Test
  public void iterator1hGaps() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
//    v.resetValue(0, 24);
//    v.resetValue(2, 5);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
//    v.resetValue(0, 2);
//    v.resetValue(2, 4);
//    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator1hGapsStart() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
//    v.resetValue(0, 42);
//    v.resetValue(2, 2);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
//    v.resetValue(0, 36);
//    v.resetValue(2, 1);
//    source.addValue(v);
//    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2L * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator1hNullValues() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator1hNullValuesStart() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator1hNoSummaries() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator1hNoSummariesStart() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator1hNulls() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator1hNullsStart() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME + (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator1hEmpty() throws Exception {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    setConfig("sum", "1h", false, BASE_TIME, BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iterator2hAligned() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, 
        BASE_TIME + (3600 * 4L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 66, 38 };
    long[] counts = new long[] { 7, 5 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator2hFilteredByQueryBounds() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME + (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 4L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 38 };
    long[] counts = new long[] { 5 };
    long ts = BASE_TIME + (3600 * 2L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(1, i);
  }
  
  @Test
  public void iterator2hGaps() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
//    v.resetValue(0, 24);
//    v.resetValue(2, 5);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
//    v.resetValue(0, 2);
//    v.resetValue(2, 4);
//    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator2hGapsStart() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
//    v.resetValue(0, 42);
//    v.resetValue(2, 2);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
//    v.resetValue(0, 36);
//    v.resetValue(2, 1);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator2hNullValues() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator2hNullValuesStart() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator2hNoSummaries() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator2hNoSummariesStart() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }

  @Test
  public void iterator2hNulls() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 36 };
    long[] counts = new long[] { 2, 1 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator2hNullsStart() throws Exception {
    setConfig("sum", "2h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 24, 2 };
    long[] counts = new long[] { 5, 4 };
    long ts = BASE_TIME;
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 2 * 1000L;
    }
    assertEquals(2, i);
  }
  
  @Test
  public void iterator3h() throws Exception {
    setConfig("sum", "3h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    // only one value as the second is truncated by the config end.
    assertTrue(it.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME, tsv.timestamp().msEpoch());
    assertEquals(102, tsv.value().value(0).longValue());
    assertEquals(8, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iterator3hGaps() throws Exception {
    setConfig("sum", "3h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    v.resetValue(2, 2);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
//    v.resetValue(0, 24);
//    v.resetValue(2, 5);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
//    v.resetValue(0, 2);
//    v.resetValue(2, 4);
//    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    // only one value as the second is truncated by the config end.
    assertTrue(it.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME, tsv.timestamp().msEpoch());
    assertEquals(78, tsv.value().value(0).longValue());
    assertEquals(3, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iterator3hGapsStart() throws Exception {
    setConfig("sum", "3h", false, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
//    v.resetValue(0, 42);
//    v.resetValue(2, 2);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
    v.resetValue(0, 24);
    v.resetValue(2, 5);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
//    v.resetValue(0, 36);
//    v.resetValue(2, 1);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
    v.resetValue(0, 2);
    v.resetValue(2, 4);
    source.addValue(v);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    // only one value as the second is truncated by the config end.
    assertTrue(it.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME, tsv.timestamp().msEpoch());
    assertEquals(24, tsv.value().value(0).longValue());
    assertEquals(5, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(it.hasNext());
  }

  @Test
  public void iteratorAll() throws Exception {
    setConfig("sum", "0all", true, BASE_TIME, BASE_TIME + (3600 * 4L * 1000L));
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    // only one value as the second is truncated by the config end.
    assertTrue(it.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME, tsv.timestamp().msEpoch());
    assertEquals(104, tsv.value().value(0).longValue());
    assertEquals(12, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iteratorAllFilterOnQuery() throws Exception {
    setConfig("sum", "0all", true, BASE_TIME + (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    // only one value as the second is truncated by the config end.
    assertTrue(it.hasNext());
    final TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + (3600 * 1L * 1000L), tsv.timestamp().msEpoch());
    assertEquals(60, tsv.value().value(0).longValue());
    assertEquals(6, tsv.value().value(2).longValue());
    assertEquals(2, tsv.value().summariesAvailable().size());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iteratorAllDataAfterStart() throws Exception {
    setConfig("sum", "0all", true, BASE_TIME - (3600 * 5L * 1000L), 
        BASE_TIME - (3600 * 2L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iteratorAllDataBeforeStart() throws Exception {
    setConfig("sum", "0all", true, BASE_TIME + (3600 * 5L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iteratorFillNaN() throws Exception {
    setGappyData(false);
    setConfig("sum", "1h", false, FillPolicy.NOT_A_NUMBER, 
        FillWithRealPolicy.NONE, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { -1, 42, -1, 36, -1, 15, 6, -1 };
    long[] counts = new long[] { -1, 2, -1, 1, -1, 5, 3, -1 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillNull() throws Exception {
    setGappyData(false);
    setConfig("sum", "1h", false, FillPolicy.NULL, 
        FillWithRealPolicy.NONE, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { -1, 42, -1, 36, -1, 15, 6, -1 };
    long[] counts = new long[] { -1, 2, -1, 1, -1, 5, 3, -1 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
        assertEquals(2, tsv.value().summariesAvailable().size());
      }
      if (counts[i] < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillZero() throws Exception {
    setGappyData(false);
    setConfig("sum", "1h", false, FillPolicy.ZERO, 
        FillWithRealPolicy.NONE, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 0, 42, 0, 36, 0, 15, 6, 0 };
    long[] counts = new long[] { 0, 2, 0, 1, 0, 5, 3, 0 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillPreviousOnly() throws Exception {
    setGappyData(false);
    setConfig("sum", "1h", false, FillPolicy.NONE, 
        FillWithRealPolicy.PREVIOUS_ONLY, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { -1, 42, 42, 36, 36, 15, 6, 6 };
    long[] counts = new long[] { -1, 2, 2, 1, 1, 5, 3, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
        assertEquals(2, tsv.value().summariesAvailable().size());
      }
      if (counts[i] < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillPreferPrevious() throws Exception {
    setGappyData(false);
    setConfig("sum", "1h", false, FillPolicy.NONE, 
        FillWithRealPolicy.PREFER_PREVIOUS, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 42, 36, 36, 15, 6, 6 };
    long[] counts = new long[] { 2, 2, 2, 1, 1, 5, 3, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillPreferNext() throws Exception {
    setGappyData(false);
    setConfig("sum", "1h", false, FillPolicy.NONE, 
        FillWithRealPolicy.PREFER_NEXT, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 36, 36, 15, 15, 6, 6 };
    long[] counts = new long[] { 2, 2, 1, 1, 5, 5, 3, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillNextOnly() throws Exception {
    setGappyData(false);
    setConfig("sum", "1h", false, FillPolicy.NONE, 
        FillWithRealPolicy.NEXT_ONLY, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 36, 36, 15, 15, 6, -1 };
    long[] counts = new long[] { 2, 2, 1, 1, 5, 5, 3, -1 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
        assertEquals(2, tsv.value().summariesAvailable().size());
      }
      if (counts[i] < 0) {
        assertNull(tsv.value());
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillNaNStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NOT_A_NUMBER, 
        FillWithRealPolicy.NONE, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { -1, 42, -1, 36, -1, -1, 6, -1 };
    long[] counts = new long[] { -1, -1, -1, 1, -1, 5, 3, -1 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillPreviousOnlyStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NONE, 
        FillWithRealPolicy.PREVIOUS_ONLY, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { -1, 42, 42, 36, 36, 36, 6, 6 };
    long[] counts = new long[] { -1, -1, -1, 1, 1, 5, 3, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0 && counts[i] < 0) {
        assertNull(tsv.value());
      } else if (sums[i] < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (sums[i] < 0 && counts[i] < 0) {
        assertNull(tsv.value());
      } else if (counts[i] < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillPreferPreviousStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NONE, 
        FillWithRealPolicy.PREFER_PREVIOUS, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 42, 36, 36, 36, 6, 6 };
    long[] counts = new long[] { -1, 1, 1, 1, 1, 5, 3, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillPreferNextStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NONE, 
        FillWithRealPolicy.PREFER_NEXT, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 6L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 36, 36, 36, 6, 6 };
    long[] counts = new long[] { -1, 1, 1, 1, 5, 5, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(7, i);
  }
  
  @Test
  public void iteratorFillNextOnlyStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NONE, 
        FillWithRealPolicy.NEXT_ONLY, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 36, 36, -1, 6, 6, -1 };
    long[] counts = new long[] { -1, 1, 1, 1, 5, 5, 3, -1 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0 && counts[i] < 0) {
        assertNull(tsv.value());
      } else if (sums[i] < 0) {
        assertNull(tsv.value().value(0));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (sums[i] < 0 && counts[i] < 0) {
        assertNull(tsv.value());
      } else if (counts[i] < 0) {
        assertNull(tsv.value().value(2));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillNaNPreviousOnlyStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NOT_A_NUMBER, 
        FillWithRealPolicy.PREVIOUS_ONLY, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { -1, 42, 42, 36, 36, 36, 6, 6 };
    long[] counts = new long[] { -1, -1, -1, 1, 1, 5, 3, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillNaNPreferPreviousStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NOT_A_NUMBER, 
        FillWithRealPolicy.PREFER_PREVIOUS, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 42, 36, 36, 36, 6, 6 };
    long[] counts = new long[] { -1, 1, 1, 1, 1, 5, 3, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillNaNPreferNextStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NOT_A_NUMBER, 
        FillWithRealPolicy.PREFER_NEXT, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 36, 36, 36, 6, 6, 6 };
    long[] counts = new long[] { -1, 1, 1, 1, 5, 5, 3, 3 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFillNaNNextOnlyStaggered() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NOT_A_NUMBER, 
        FillWithRealPolicy.NEXT_ONLY, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 36, 36, -1, 6, 6, -1 };
    long[] counts = new long[] { -1, 1, 1, 1, 5, 5, 3, -1 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void iteratorFromNumericInterpolatorConfig() throws Exception {
    setGappyData(true);
    setConfig("sum", "1h", false, FillPolicy.NOT_A_NUMBER, 
        FillWithRealPolicy.NEXT_ONLY, BASE_TIME - (3600 * 1L * 1000L), 
        BASE_TIME + (3600 * 7L * 1000L));
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("1h")
        .setRunAll(false)
        .setFill(true)
        .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                .setRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
                .setType(NumericType.TYPE.toString())
                .build())
        .build();
    when(node.config()).thenReturn(config);
    
    DownsampleNumericSummaryIterator it = 
        new DownsampleNumericSummaryIterator(node, result, source);
    
    long[] sums = new long[] { 42, 42, 36, 36, -1, 6, 6, -1 };
    long ts = BASE_TIME - (3600 * 1L * 1000L);
    int i = 0;
    while (it.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      // since we pull from the numeric, we only expect the sums, not the
      // counts.
      assertNull(tsv.value().value(2));
      
      ts += 3600 * 1000L;
      i++;
    }
    assertEquals(8, i);
  }
  
  void setConfig(final String agg, final String interval, boolean runall, long start, long end) throws Exception {
    setConfig(agg, interval, runall, FillPolicy.NONE, FillWithRealPolicy.NONE, start, end);
  }
  
  void setConfig(final String agg, final String interval, boolean runall, 
      FillPolicy fill, FillWithRealPolicy real, long start, long end) throws Exception {
    node = mock(QueryNode.class);
    query_context = mock(QueryContext.class);
    pipeline_context = mock(QueryPipelineContext.class);
    when(pipeline_context.queryContext()).thenReturn(query_context);
    when(node.pipelineContext()).thenReturn(pipeline_context);
    final TSDB tsdb = mock(TSDB.class);
    when(pipeline_context.tsdb()).thenReturn(tsdb);
    final Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    final QueryInterpolatorFactory interp_factory = new DefaultInterpolatorFactory();
    interp_factory.initialize(tsdb).join();
    when(registry.getPlugin(any(Class.class), anyString())).thenReturn(interp_factory);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator(agg)
        .setId("foo")
        .setInterval(interval)
        .setRunAll(runall)
        .setFill(fill != FillPolicy.NONE || real != FillWithRealPolicy.NONE)
        .addInterpolatorConfig(NumericSummaryInterpolatorConfig.newBuilder()
                .setDefaultFillPolicy(fill)
                .setDefaultRealFillPolicy(real)
                .addExpectedSummary(0)
                .addExpectedSummary(2)
                .setType(NumericSummaryType.TYPE.toString())
                .build())
        .build();
    when(node.config()).thenReturn(config);
    
    TimeSeriesDataSource downstream = mock(TimeSeriesDataSource.class);
    when(pipeline_context.downstreamSources(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(downstream));
    
    QuerySourceConfig source_config = (QuerySourceConfig) QuerySourceConfig.newBuilder()
        .setStart(Long.toString(start))
        .setEnd(Long.toString(end))
        .setMetric("sys.cpu.user")
        .setQuery(mock(TimeSeriesQuery.class))
        .setId("m1")
        .build();
    when(downstream.config()).thenReturn(source_config);
    
    Downsample ds = new Downsample(null, pipeline_context, null, config);
    ds.initialize(null);
    final QueryResult result = mock(Downsample.DownsampleResult.class);
    when(result.rollupConfig()).thenReturn(rollup_config);
    this.result = ds.new DownsampleResult(result);
  }
  
  void setGappyData(boolean stagger) {
    source = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME));
    v.resetValue(0, 42);
    if (!stagger) {
      v.resetValue(2, 2);
    }
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 1L * 1000L)));
//    v.resetValue(0, 24);
//    v.resetValue(2, 5);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 2L * 1000L)));
    v.resetValue(0, 36);
    v.resetValue(2, 1);
    source.addValue(v);
    
//    v = new MutableNumericSummaryValue();
//    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 3L * 1000L)));
//    v.resetValue(0, 2);
//    v.resetValue(2, 4);
//    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 4L * 1000L)));
    if (!stagger) {
      v.resetValue(0, 15);
    }
    v.resetValue(2, 5);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(BASE_TIME + (3600 * 5L * 1000L)));
    v.resetValue(0, 6);
    v.resetValue(2, 3);
    source.addValue(v);
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