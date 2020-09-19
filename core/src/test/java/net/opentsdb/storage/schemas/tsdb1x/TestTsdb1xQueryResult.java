// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.List;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.SumFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec.OffsetResolution;

public class TestTsdb1xQueryResult extends SchemaBase {

  // GMT: Monday, January 1, 2018 12:15:00 AM
  public static final int START_TS = 1514765700;
  
  // GMT: Monday, January 1, 2018 1:15:00 AM
  public static final int END_TS = 1514769300;
  
  private static final long BASE_TIME = 1514764800;
  private static final byte[] TSUID_A = 
      Bytes.concat(METRIC_BYTES, net.opentsdb.utils.Bytes.fromInt((int) BASE_TIME), 
          TAGK_BYTES, TAGV_BYTES);
  private static final byte[] TSUID_B = 
      Bytes.concat(METRIC_BYTES, net.opentsdb.utils.Bytes.fromInt((int) BASE_TIME), 
          TAGK_BYTES, TAGV_B_BYTES);
  private static final byte[] APPEND_Q = 
      new byte[] { Schema.APPENDS_PREFIX, 0, 0 };
  
  private MockTSDB tsdb;
  private QueryNode node;
  private QueryPipelineContext context;
  private TimeSeriesDataSourceConfig source_config;
  private Schema schema;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    when(tsdb.registry.getPlugin(eq(NumericAggregatorFactory.class), anyString()))
      .thenReturn(new SumFactory());
    context = mock(QueryPipelineContext.class);
    schema = schema();
    node = mock(QueryNode.class);
    source_config = (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setId("m1")
        .build();
    when(node.config()).thenReturn(source_config);
    when(node.pipelineContext()).thenReturn(context);
    when(context.tsdb()).thenReturn(tsdb);
    
    SemanticQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setAggregator("avg"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric(METRIC_STRING))
        .build()
        .convert(tsdb)
        .build();
    when(context.query()).thenReturn(query);
    
    tsdb.config.register(Schema.QUERY_BYTE_LIMIT_KEY, 
        Schema.QUERY_BYTE_LIMIT_DEFAULT, false, "UT");
    tsdb.config.register(Schema.QUERY_DP_LIMIT_KEY, 
        Schema.QUERY_DP_LIMIT_DEFAULT, false, "UT");
    tsdb.config.register(Schema.QUERY_REVERSE_KEY, false, false, "UT");
    tsdb.config.register(Schema.QUERY_KEEP_FIRST_KEY, false, false, "UT");
  }
  
  @Test
  public void ctorDefaults() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(0, node, schema);
    assertEquals(0, result.sequenceId());
    assertSame(node, result.source());
    assertSame(schema, result.schema);
    assertTrue(result.results.isEmpty());
    assertEquals(0, result.byte_limit);
    assertEquals(0, result.dp_limit);
    assertFalse(result.isFull());
    assertEquals(0, result.bytes.get());
    assertEquals(0, result.dps.get());
    assertNull(result.timeSpecification());
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(Const.TS_BYTE_ID, result.idType());
  }
  
  @Test
  public void ctorOverrides() throws Exception {
    source_config = (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addOverride(Schema.QUERY_BYTE_LIMIT_KEY, "42")
        .addOverride(Schema.QUERY_DP_LIMIT_KEY, "24")
        .setId("m1")
        .build();
    when(node.config()).thenReturn(source_config);
    
    SemanticQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setAggregator("avg"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric(METRIC_STRING))
        .build()
        .convert(tsdb)
        .build();
    when(context.query()).thenReturn(query);
    
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(9, node, schema);
    assertEquals(9, result.sequenceId());
    assertSame(node, result.source());
    assertSame(schema, result.schema);
    assertTrue(result.results.isEmpty());
    assertEquals(42, result.byte_limit);
    assertEquals(24, result.dp_limit);
    assertFalse(result.isFull());
    assertEquals(0, result.bytes.get());
    assertEquals(0, result.dps.get());
    assertNull(result.timeSpecification());
    assertTrue(result.timeSeries().isEmpty());
    assertEquals(Const.TS_BYTE_ID, result.idType());
  }

  @Test
  public void ctorExceptions() throws Exception {
    try {
      new Tsdb1xQueryResult(-42, node, schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xQueryResult(9, null, schema);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xQueryResult(9, node, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addSequence() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(9, node, schema);
    
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(tsdb, false, false);
    
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_A),
        TSUID_A, seq, ChronoUnit.MILLIS);
    assertEquals(1, result.results.size());
    assertEquals(48, result.bytes.get());
    assertEquals(4, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.MILLIS, result.resolution());
    
    // another TSUID
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_B),
        TSUID_B, seq, ChronoUnit.NANOS);
    assertEquals(2, result.results.size());
    assertEquals(96, result.bytes.get());
    assertEquals(8, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.NANOS, result.resolution());
    
    List<TimeSeries> series = Lists.newArrayList(result.timeSeries());
    assertEquals(2, series.size());
    for (final TimeSeries ts : series) {
      value = 0;
      base_time = BASE_TIME;
      TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = ts.iterator(NumericType.TYPE).get();
      while (it.hasNext()) {
        TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
        assertEquals(base_time, v.timestamp().epoch());
        assertEquals(value++, v.value().longValue());
        base_time += 900;
      }
      assertEquals(4, value);
    }
  }
  
  @Test
  public void addSequenceMultipleRows() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(9, node, schema);
    
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(tsdb, false, false);
    
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_A),
        TSUID_A, seq, ChronoUnit.SECONDS);
    assertEquals(1, result.results.size());
    assertEquals(48, result.bytes.get());
    assertEquals(4, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
    
    // another TSUID
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_B),
        TSUID_B, seq, ChronoUnit.SECONDS);
    assertEquals(2, result.results.size());
    assertEquals(96, result.bytes.get());
    assertEquals(8, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
    
    List<TimeSeries> series = Lists.newArrayList(result.timeSeries());
    assertEquals(2, series.size());
    
    // next row
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(tsdb, false, false);
    
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_A),
        TSUID_A, seq, ChronoUnit.MILLIS);
    assertEquals(2, result.results.size());
    assertEquals(144, result.bytes.get());
    assertEquals(12, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.MILLIS, result.resolution());
    
    // B
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_B),
        TSUID_B, seq, ChronoUnit.SECONDS);
    assertEquals(2, result.results.size());
    assertEquals(192, result.bytes.get());
    assertEquals(16, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.MILLIS, result.resolution());
    
    series = Lists.newArrayList(result.timeSeries());
    assertEquals(2, series.size());
    for (final TimeSeries ts : series) {
      value = 0;
      base_time = BASE_TIME;
      TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = ts.iterator(NumericType.TYPE).get();
      while (it.hasNext()) {
        TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
        assertEquals(base_time, v.timestamp().epoch());
        assertEquals(value++, v.value().longValue());
        base_time += 900;
      }
      assertEquals(8, value);
    }
  }
  
  @Test
  public void addSequenceMultipleRowsReversed() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(9, node, schema);
    Whitebox.setInternalState(result, "reversed", true);
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(tsdb, false, true);
    
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_A),
        TSUID_A, seq, ChronoUnit.SECONDS);
    assertEquals(1, result.results.size());
    assertEquals(48, result.bytes.get());
    assertEquals(4, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.SECONDS, result.resolution());
    
    // another TSUID
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_B),
        TSUID_B, seq, ChronoUnit.MILLIS);
    assertEquals(2, result.results.size());
    assertEquals(96, result.bytes.get());
    assertEquals(8, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.MILLIS, result.resolution());
    
    List<TimeSeries> series = Lists.newArrayList(result.timeSeries());
    assertEquals(2, series.size());
    
    // next row
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(tsdb, false, true);
    
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_A),
        TSUID_A, seq, ChronoUnit.SECONDS);
    assertEquals(2, result.results.size());
    assertEquals(144, result.bytes.get());
    assertEquals(12, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.MILLIS, result.resolution());
    
    // B
    result.addSequence(LongHashFunction.xx().hashBytes(TSUID_B),
        TSUID_B, seq, ChronoUnit.MILLIS);
    assertEquals(2, result.results.size());
    assertEquals(192, result.bytes.get());
    assertEquals(16, result.dps.get());
    assertFalse(result.isFull());
    assertEquals(ChronoUnit.MILLIS, result.resolution());
    
    series = Lists.newArrayList(result.timeSeries());
    assertEquals(2, series.size());
    for (final TimeSeries ts : series) {
      value = 7;
      base_time = BASE_TIME + (3600 * 2) - 900;
      TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = ts.iterator(NumericType.TYPE).get();
      while (it.hasNext()) {
        TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
        assertEquals(base_time, v.timestamp().epoch());
        assertEquals(value--, v.value().longValue());
        base_time -= 900;
      }
      assertEquals(-1, value);
    }
  }

  @Test
  public void resultIsFullErrorMessage() throws Exception {
    Tsdb1xQueryResult result = new Tsdb1xQueryResult(9, node, schema);
    
    // not full
    assertTrue(result.resultIsFullErrorMessage().contains("data points"));
    
    source_config = (TimeSeriesDataSourceConfig) DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .addOverride(Schema.QUERY_BYTE_LIMIT_KEY, "42")
        .addOverride(Schema.QUERY_DP_LIMIT_KEY, "24")
        .setId("m1")
        .build();
    when(node.config()).thenReturn(source_config);
    
    SemanticQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Integer.toString(START_TS))
            .setEnd(Integer.toString(END_TS))
            .setAggregator("avg"))
        .addMetric(Metric.newBuilder()
            .setId("m1")
            .setMetric(METRIC_STRING))
        .build()
        .convert(tsdb)
        .build();
    when(context.query()).thenReturn(query);
    
    // byte limit
    result = new Tsdb1xQueryResult(9, node, schema);
    result.bytes.set(1024);
    assertTrue(result.resultIsFullErrorMessage().contains("MB from storage"));
    
    // dp limit
    result.bytes.set(1);
    result.dps.set(42);
    assertTrue(result.resultIsFullErrorMessage().contains("data points"));
  }
}
