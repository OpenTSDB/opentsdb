// This file is part of OpenTSDB.
// Copyright (C) 2015-2019  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.time.temporal.ChronoUnit;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.primitives.Bytes;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.storage.schemas.tsdb1x.NumericSummaryRowSeq;

public class TestNumericSummaryRowSeq {
  private static final long BASE_TIME = 1514764800;
  private final static String TSDB_TABLE = "tsdb";
  private final static String ROLLUP_TABLE = "tsdb-rollup-10m";
  private final static String PREAGG_TABLE = "tsdb-rollup-agg-10m";
  private final static byte PREFIX = (byte) 0;
  
  private static DefaultRollupConfig CONFIG;
  private static RollupInterval RAW;
  private static RollupInterval TENMIN;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    RAW = RollupInterval.builder()
        .setTable(TSDB_TABLE)
        .setPreAggregationTable(TSDB_TABLE)
        .setInterval("1m")
        .setRowSpan("1h")
        .setDefaultInterval(true)
        .build();
    
    TENMIN = RollupInterval.builder()
        .setTable(ROLLUP_TABLE)
        .setPreAggregationTable(PREAGG_TABLE)
        .setInterval("10m")
        .setRowSpan("1d")
        .build();
    
    CONFIG = DefaultRollupConfig.newBuilder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 1)
        .addAggregationId("Count", 2)
        .addInterval(RAW)
        .addInterval(TENMIN)
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    assertEquals(BASE_TIME, seq.base_timestamp);
    assertSame(RAW, seq.interval);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE, seq.size());
    assertEquals(0, seq.dataPoints());
    assertTrue(seq.summary_data.isEmpty());
    
    seq = new NumericSummaryRowSeq(0, TENMIN);
    assertEquals(0, seq.base_timestamp);
    assertSame(TENMIN, seq.interval);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE, seq.size());
    assertEquals(0, seq.dataPoints());
    assertTrue(seq.summary_data.isEmpty());
    
    try {
      new NumericSummaryRowSeq(BASE_TIME, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new NumericSummaryRowSeq(BASE_TIME, mock(RollupInterval.class));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addRawSingleType() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 42 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 3, seq.size());
    assertEquals(1, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    byte[] extant = seq.summary_data.get(2);
    assertArrayEquals(buildExpected(
        null, qualifier, new byte[] { 42 }), extant);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 24 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 6, seq.size());
    assertEquals(2, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant, qualifier, new byte[] { 24 }), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 2, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 12, seq.size());
    assertEquals(3, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant, qualifier, value), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.751));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 2, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 22, seq.size());
    assertEquals(4, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant, qualifier, value), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    seq.dedupe(true, false);
    NumericSummarySpan span = new NumericSummarySpan(false);
    span.addSequence(seq, false);
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> it = span.iterator();
    assertTrue(it.hasNext());
    
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME, v.timestamp().epoch());
    assertEquals(1, v.value().summariesAvailable().size());
    NumericType s = v.value().value(2);
    assertEquals(42, s.longValue());
    
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 600, v.timestamp().epoch());
    assertEquals(1, v.value().summariesAvailable().size());
    s = v.value().value(2);
    assertEquals(24, s.longValue());
    
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 1200, v.timestamp().epoch());
    assertEquals(1, v.value().summariesAvailable().size());
    s = v.value().value(2);
    assertEquals(42.5, s.doubleValue(), 0.001);
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void addRawMixedTypes() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 3, seq.size());
    assertEquals(1, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    byte[] extant_count = seq.summary_data.get(2);
    assertArrayEquals(buildExpected(
        null, qualifier, new byte[] { 4 }), extant_count);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 12 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 6, seq.size());
    assertEquals(2, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    byte[] extant_sum = seq.summary_data.get(0);
    assertArrayEquals(buildExpected(
        null, qualifier, new byte[] { 12 }), seq.summary_data.get(0));

    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 9, seq.size());
    assertEquals(3, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant_count, qualifier, new byte[] { 3 }), seq.summary_data.get(2));
    extant_count = seq.summary_data.get(2);
    
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 15, seq.size());
    assertEquals(4, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant_sum, qualifier, value), seq.summary_data.get(0));
    extant_sum = seq.summary_data.get(0);

    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 18, seq.size());
    assertEquals(5, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant_count, qualifier, new byte[] { 5 }), seq.summary_data.get(2));
    extant_count = seq.summary_data.get(2);
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    assertEquals(6, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant_sum, qualifier, value), seq.summary_data.get(0));
    extant_sum = seq.summary_data.get(0);
  }
  
  @Test
  public void addRawSingleTypeStringPrefix() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    byte[] qualifier = buildStringQualifier(0, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 42 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 3, seq.size());
    assertEquals(1, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    byte[] extant = seq.summary_data.get(2);
    assertArrayEquals(buildExpectedStripString(
        null, qualifier, new byte[] { 42 }), extant);
    
    qualifier = buildStringQualifier(600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 24 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 6, seq.size());
    assertEquals(2, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant, qualifier, new byte[] { 24 }), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = buildStringQualifier(1200, flags, 2, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 12, seq.size());
    assertEquals(3, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant, qualifier, value), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.751));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = buildStringQualifier(1200, flags, 2, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 22, seq.size());
    assertEquals(4, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant, qualifier, value), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
  }
  
  @Test
  public void addRawMixedTypesStringPRefix() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    byte[] qualifier = buildStringQualifier(0, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 3, seq.size());
    assertEquals(1, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    byte[] extant_count = seq.summary_data.get(2);
    assertArrayEquals(buildExpectedStripString(
        null, qualifier, new byte[] { 4 }), extant_count);
    
    qualifier = buildStringQualifier(0, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 12 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 6, seq.size());
    assertEquals(2, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    byte[] extant_sum = seq.summary_data.get(0);
    assertArrayEquals(buildExpectedStripString(
        null, qualifier, new byte[] { 12 }), seq.summary_data.get(0));

    qualifier = buildStringQualifier(600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 9, seq.size());
    assertEquals(3, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant_count, qualifier, new byte[] { 3 }), seq.summary_data.get(2));
    extant_count = seq.summary_data.get(2);
    
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = buildStringQualifier(600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 15, seq.size());
    assertEquals(4, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant_sum, qualifier, value), seq.summary_data.get(0));
    extant_sum = seq.summary_data.get(0);

    qualifier = buildStringQualifier(1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 18, seq.size());
    assertEquals(5, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant_count, qualifier, new byte[] { 5 }), seq.summary_data.get(2));
    extant_count = seq.summary_data.get(2);
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = buildStringQualifier(1200, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    assertEquals(6, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant_sum, qualifier, value), seq.summary_data.get(0));
    extant_sum = seq.summary_data.get(0);
  }
  
  @Test
  public void addRawAppendSingleType() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    seq.addColumn(PREFIX, new byte[] { 0 }, Bytes.concat(
        getAppendValue(BASE_TIME, 4, RAW),
        getAppendValue(BASE_TIME + 600, 2000, RAW),
        getAppendValue(BASE_TIME + 1200, 42.5, RAW),
        getAppendValue(BASE_TIME + 1800, 24.751, RAW)));
    
    seq.addColumn(PREFIX, new byte[] { 2 }, Bytes.concat(
        getAppendValue(BASE_TIME, 2, RAW),
        getAppendValue(BASE_TIME + 600, 3, RAW),
        getAppendValue(BASE_TIME + 1200, 1, RAW),
        getAppendValue(BASE_TIME + 1800, 6, RAW)));
    
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 39, seq.size());
    assertEquals(8, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    seq.dedupe(true, false);
    NumericSummarySpan span = new NumericSummarySpan(false);
    span.addSequence(seq, false);
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> it = span.iterator();
    assertTrue(it.hasNext());
    
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME, v.timestamp().epoch());
    assertEquals(2, v.value().summariesAvailable().size());
    NumericType s = v.value().value(0);
    assertEquals(4, s.longValue());
    s = v.value().value(2);
    assertEquals(2, s.longValue());
    
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 600, v.timestamp().epoch());
    assertEquals(2, v.value().summariesAvailable().size());
    s = v.value().value(0);
    assertEquals(2000, s.longValue());
    s = v.value().value(2);
    assertEquals(3, s.longValue());
    
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 1200, v.timestamp().epoch());
    assertEquals(2, v.value().summariesAvailable().size());
    s = v.value().value(0);
    assertEquals(42.5, s.doubleValue(), 0.001);
    s = v.value().value(2);
    assertEquals(1, s.longValue());
    
    v = (TimeSeriesValue<NumericSummaryType>) it.next();
    assertEquals(BASE_TIME + 1800, v.timestamp().epoch());
    assertEquals(2, v.value().summariesAvailable().size());
    s = v.value().value(0);
    assertEquals(24.751, s.doubleValue(), 0.001);
    s = v.value().value(2);
    assertEquals(6, s.longValue());
  }
  
  @Test
  public void addTenMinSingleType() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, TENMIN);
    
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 42 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 3, seq.size());
    assertEquals(1, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    byte[] extant = seq.summary_data.get(2);
    assertArrayEquals(buildExpected(
        null, qualifier, new byte[] { 42 }), extant);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 24 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 6, seq.size());
    assertEquals(2, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant, qualifier, new byte[] { 24 }), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 12, seq.size());
    assertEquals(3, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant, qualifier, value), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.751));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 22, seq.size());
    assertEquals(4, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant, qualifier, value), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
  }
  
  @Test
  public void addTenMinMixedTypes() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, TENMIN);
    
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 3, seq.size());
    assertEquals(1, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    byte[] extant_count = seq.summary_data.get(2);
    assertArrayEquals(buildExpected(
        null, qualifier, new byte[] { 4 }), extant_count);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 12 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 6, seq.size());
    assertEquals(2, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    byte[] extant_sum = seq.summary_data.get(0);
    assertArrayEquals(buildExpected(
        null, qualifier, new byte[] { 12 }), seq.summary_data.get(0));

    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 9, seq.size());
    assertEquals(3, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant_count, qualifier, new byte[] { 3 }), seq.summary_data.get(2));
    extant_count = seq.summary_data.get(2);
    
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 15, seq.size());
    assertEquals(4, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant_sum, qualifier, value), seq.summary_data.get(0));
    extant_sum = seq.summary_data.get(0);

    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 18, seq.size());
    assertEquals(5, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant_count, qualifier, new byte[] { 5 }), seq.summary_data.get(2));
    extant_count = seq.summary_data.get(2);
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    assertEquals(6, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpected(
        extant_sum, qualifier, value), seq.summary_data.get(0));
    extant_sum = seq.summary_data.get(0);
  }
  
  @Test
  public void addTenMinSingleTypeStringPrefix() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, TENMIN);
    
    byte[] qualifier = buildStringQualifier(0, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 42 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 3, seq.size());
    assertEquals(1, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    byte[] extant = seq.summary_data.get(2);
    assertArrayEquals(buildExpectedStripString(
        null, qualifier, new byte[] { 42 }), extant);
    
    qualifier = buildStringQualifier(600, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 24 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 6, seq.size());
    assertEquals(2, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant, qualifier, new byte[] { 24 }), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = buildStringQualifier(1200, flags, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 12, seq.size());
    assertEquals(3, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant, qualifier, value), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.751));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = buildStringQualifier(1200, flags, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 22, seq.size());
    assertEquals(4, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant, qualifier, value), seq.summary_data.get(2));
    extant = seq.summary_data.get(2);
  }
  
  @Test
  public void addTenMinMixedTypesStringPRefix() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, TENMIN);
    
    byte[] qualifier = buildStringQualifier(0, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 3, seq.size());
    assertEquals(1, seq.dataPoints());
    assertEquals(1, seq.summary_data.size());
    byte[] extant_count = seq.summary_data.get(2);
    assertArrayEquals(buildExpectedStripString(
        null, qualifier, new byte[] { 4 }), extant_count);
    
    qualifier = buildStringQualifier(0, (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 12 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 6, seq.size());
    assertEquals(2, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    byte[] extant_sum = seq.summary_data.get(0);
    assertArrayEquals(buildExpectedStripString(
        null, qualifier, new byte[] { 12 }), seq.summary_data.get(0));

    qualifier = buildStringQualifier(600, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 9, seq.size());
    assertEquals(3, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant_count, qualifier, new byte[] { 3 }), seq.summary_data.get(2));
    extant_count = seq.summary_data.get(2);
    
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = buildStringQualifier(600, flags, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 15, seq.size());
    assertEquals(4, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant_sum, qualifier, value), seq.summary_data.get(0));
    extant_sum = seq.summary_data.get(0);

    qualifier = buildStringQualifier(1200, (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 18, seq.size());
    assertEquals(5, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant_count, qualifier, new byte[] { 5 }), seq.summary_data.get(2));
    extant_count = seq.summary_data.get(2);
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = buildStringQualifier(1200, flags, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, value);
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    assertEquals(6, seq.dataPoints());
    assertEquals(2, seq.summary_data.size());
    assertArrayEquals(buildExpectedStripString(
        extant_sum, qualifier, value), seq.summary_data.get(0));
    extant_sum = seq.summary_data.get(0);
  }
  
  @Test
  public void dedupeSorted() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    // add dps
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 12 });

    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
        
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    byte[] expected_sum = seq.summary_data.get(0);
    byte[] expected_count = seq.summary_data.get(2);
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, false));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    
    // test reverse without dedupe
    expected_sum = Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) (7 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) (3 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 12 })
        ); 
    expected_count= Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 0, RAW), 
            new byte[] { 5 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 0, RAW), 
            new byte[] { 3 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 })
        ); 
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, true));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
  }
  
  @Test
  public void dedupeOrder() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    // sums out of order
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    // counts out of order
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    
    byte[] expected_sum = Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) (3 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) (7 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75)))
        ); 
    byte[] expected_count= Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 0, RAW), 
            new byte[] { 3 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 0, RAW), 
            new byte[] { 5 })
        );
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, false));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    
    // re-do while reversing
    seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    // sums out of order
    value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    // counts out of order
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    
    expected_sum = Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) (7 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) (3 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 })
        ); 
    expected_count= Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 0, RAW), 
            new byte[] { 5 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 0, RAW), 
            new byte[] { 3 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 })
        ); 
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, true));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
  }
  
  @Test
  public void dedupe() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    // sums out of order and duped
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -4 });
    
    value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(-42.5F));
    flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    // counts out of order and duped
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -5 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -3 });
    
    byte[] expected_sum = Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { -4 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) (3 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(-42.5F))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) (7 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75)))
        ); 
    byte[] expected_count= Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 0, RAW), 
            new byte[] { -3 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 0, RAW), 
            new byte[] { -5 })
        ); 
    assertEquals(10, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 43, seq.size());
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, false));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    
    // re-do keep-earliest
    seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -4 });
    
    value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(-42.5F));
    flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    // counts out of order and duped
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -5 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -3 });
    
    expected_sum = Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) (3 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) (7 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75)))
        ); 
    expected_count= Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 0, RAW), 
            new byte[] { 3 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 0, RAW), 
            new byte[] { 5 })
        ); 
    assertEquals(10, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 43, seq.size());
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(true, false));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    
    // re-do while reversing
    seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -4 });
    
    value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(-42.5F));
    flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    // counts out of order and duped
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -5 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { -3 });
    
    expected_sum = Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) (7 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) (3 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(-42.5F))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { -4 })
        ); 
    expected_count= Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 0, RAW), 
            new byte[] { -5 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 0, RAW), 
            new byte[] { -3 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 })
        );
    assertEquals(10, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 43, seq.size());
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, true));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
  }
  
  @Test
  public void dedupeSingleValues() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    // sums
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    // counts
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
        
    byte[] expected_sum = buildExpected(null, 
        RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) (3 | NumericCodec.FLAG_FLOAT), 0, RAW), 
        net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F)));
    byte[] expected_count = buildExpected(null, 
        RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 0, RAW), 
        new byte[] { 5 });
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, false));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(2, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 9, seq.size());
    
    seq.reverse();
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(2, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 9, seq.size());
  }
  
  @Test
  public void appendSeq() throws Exception {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    // add dps
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 12 });

    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, flags, 0, RAW);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 2, RAW);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    
    assertEquals(4, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 15, seq.size());
    
    // now start the next one
    NumericSummaryRowSeq seq2 = new NumericSummaryRowSeq(BASE_TIME, RAW);
    
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 2, RAW);
    seq2.addColumn(PREFIX, qualifier, new byte[] { 3 });
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + 1200, flags, 0, RAW);
    seq2.addColumn(PREFIX, qualifier, value);
    
    assertEquals(2, seq2.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 13, seq2.size());
    
    // merge em
    seq.appendSeq(seq2);
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
    
    // test reverse without dedupe
    byte[] expected_sum = Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) (7 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) (3 | NumericCodec.FLAG_FLOAT), 0, RAW), 
            net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F))),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 12 })
        ); 
    byte[] expected_count= Bytes.concat(
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 1200, (short) 0, 0, RAW), 
            new byte[] { 5 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME + 600, (short) 0, 0, RAW), 
            new byte[] { 3 }),
        buildExpected(null, 
            RollupUtils.buildRollupQualifier(BASE_TIME, (short) 0, 0, RAW), 
            new byte[] { 4 })
        ); 
    
    assertEquals(ChronoUnit.SECONDS, seq.dedupe(false, true));
    assertArrayEquals(expected_sum, seq.summary_data.get(0));
    assertArrayEquals(expected_count, seq.summary_data.get(2));
    assertEquals(6, seq.dataPoints());
    assertEquals(NumericSummaryRowSeq.HEADER_SIZE + 28, seq.size());
  }
  
  byte[] buildExpected(final byte[] extant, final byte[] qualifier, final byte[] value) {
    byte[] expected = new byte[(extant == null ? 0 : extant.length) + 
                               qualifier.length - 1 + value.length];
    
    int idx = 0;
    if (extant != null) {
      System.arraycopy(extant, 0, expected, 0, extant.length);
      idx += extant.length;
    }
    System.arraycopy(qualifier, 1, expected, idx, qualifier.length - 1);
    idx += qualifier.length - 1;
    System.arraycopy(value, 0, expected, idx, value.length);
    return expected;
  }
  
  byte[] buildExpectedStripString(final byte[] extant, final byte[] qualifier, final byte[] value) {
    int colon = 0;
    for (int i = 0; i < qualifier.length; i++) {
      if (qualifier[i] == ':') {
        colon = i + 1;
        break;
      }
    }
    byte[] expected = new byte[(extant == null ? 0 : extant.length) + 
                               qualifier.length - colon + value.length];
    
    int idx = 0;
    if (extant != null) {
      System.arraycopy(extant, 0, expected, 0, extant.length);
      idx += extant.length;
    }
    System.arraycopy(qualifier, colon, expected, idx, qualifier.length - colon);
    idx += qualifier.length - colon;
    System.arraycopy(value, 0, expected, idx, value.length);
    return expected;
  }

  byte[] buildStringQualifier(int offset, short flags, int type, RollupInterval interval) {
    byte[] qualifier = RollupUtils.buildRollupQualifier(BASE_TIME + offset, flags, type, interval);
    String name = CONFIG.getAggregatorForId(type);
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("No agg for ID: " + type);
    }
    name = name.toUpperCase();
    byte[] q = new byte[qualifier.length - 1 + name.length() + 1];
    System.arraycopy(name.getBytes(), 0, q, 0, name.length());
    q[name.length()] = ':';
    System.arraycopy(qualifier, 1, q, name.length() + 1, qualifier.length - 1);
    return q;
  }
  
  private static byte[] getAppendValue(final long timestamp, 
                                       final long value, 
                                       final RollupInterval interval) {
    final byte[] val = NumericCodec.vleEncodeLong(value);
    final short flags = (short) (val.length - 1);  // Just the length.
    return RollupUtils.buildAppendRollupValue(timestamp, flags, interval, val);
  }
  
  private static byte[] getAppendValue(final long timestamp, 
                                       final double value, 
                                       final RollupInterval interval) {
    //final int base_time = RollupUtils.getRollupBasetime(timestamp, interval);
    final byte[] val = net.opentsdb.utils.Bytes.fromLong(Double.doubleToRawLongBits(value));
    final short flags = (short) ((short) (val.length - 1) | NumericCodec.FLAG_FLOAT); 
    return RollupUtils.buildAppendRollupValue(timestamp, flags, interval, val);
  }
}