// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.utils.Bytes;

public class TestNumericSummarySpan {
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
    
    CONFIG = DefaultRollupConfig.builder()
        .addAggregationId("Sum", 0)
        .addAggregationId("Max", 1)
        .addAggregationId("Count", 2)
        .addInterval(RAW)
        .addInterval(TENMIN)
        .build();
  }
  
  @Test
  public void addSequence() throws Exception {
    NumericSummarySpan span = new NumericSummarySpan(false);
    
    try {
      span.addSequence(null, false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    NumericSummaryRowSeq seq = newSeq(BASE_TIME + 3600, RAW);
    assertEquals(0, span.rows.size());
    span.addSequence(seq, false);
    assertEquals(1, span.rows.size());
    assertEquals(BASE_TIME + 3600, span.rows.get(0).base_timestamp);
    
    // empty rows are skipped
    seq = new NumericSummaryRowSeq(BASE_TIME + (3600 * 2), RAW);
    span.addSequence(seq, false);
    assertEquals(1, span.rows.size());
    assertEquals(BASE_TIME + 3600, span.rows.get(0).base_timestamp);
    
    // earlier row
    seq = newSeq(BASE_TIME, RAW);
    span.addSequence(seq, false);
    assertEquals(2, span.rows.size());
    assertEquals(BASE_TIME, span.rows.get(0).base_timestamp);
    assertEquals(BASE_TIME + 3600, span.rows.get(1).base_timestamp);
    
    // later row
    seq = newSeq(BASE_TIME + (3600 * 3), RAW);
    span.addSequence(seq, false);
    assertEquals(3, span.rows.size());
    assertEquals(BASE_TIME, span.rows.get(0).base_timestamp);
    assertEquals(BASE_TIME + 3600, span.rows.get(1).base_timestamp);
    assertEquals(BASE_TIME + (3600 * 3), span.rows.get(2).base_timestamp);
    
    // merge earlier
    seq = newSeq(BASE_TIME, RAW);
    span.addSequence(seq, false);
    assertEquals(3, span.rows.size());
    assertEquals(BASE_TIME, span.rows.get(0).base_timestamp);
    assertEquals(BASE_TIME + 3600, span.rows.get(1).base_timestamp);
    assertEquals(BASE_TIME + (3600 * 3), span.rows.get(2).base_timestamp);
    
    // insert earlier
    seq = newSeq(BASE_TIME + (3600 * 2), RAW);
    span.addSequence(seq, false);
    assertEquals(4, span.rows.size());
    assertEquals(BASE_TIME, span.rows.get(0).base_timestamp);
    assertEquals(BASE_TIME + 3600, span.rows.get(1).base_timestamp);
    assertEquals(BASE_TIME + (3600 * 2), span.rows.get(2).base_timestamp);
    assertEquals(BASE_TIME + (3600 * 3), span.rows.get(3).base_timestamp);
  }
  
  @Test
  public void iterateLongsRaw() throws Exception {
    int value = 0;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(false);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, RAW);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 900), (short) 0, 0, RAW);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericSummaryRowSeq(base_time, RAW);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 900), (short) 0, 0, RAW);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericSummaryRowSeq(base_time, RAW);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 900), (short) 0, 0, RAW);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(1, v.value().summariesAvailable().size());
      assertEquals(0, (int) v.value().summariesAvailable().iterator().next());
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().value(0).longValue());
      base_time += 900;
    }
  }
  
  @Test
  public void iterateLongsTenMin() throws Exception {
    int value = 0;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(false);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) 0, 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) 0, 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) 0, 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(1, v.value().summariesAvailable().size());
      assertEquals(0, (int) v.value().summariesAvailable().iterator().next());
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().value(0).longValue());
      base_time += 21600;
    }
  }
  
  @Test
  public void iterateLongsReversedTenMin() throws Exception {
    int value = 0;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(true);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) 0, 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) 0, 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) 0, 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 11;
    base_time = BASE_TIME + (86400 * 3) - 21600;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(1, v.value().summariesAvailable().size());
      assertEquals(0, (int) v.value().summariesAvailable().iterator().next());
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().value(0).longValue());
      base_time -= 21600;
    }
  }
  
  @Test
  public void iterateFloatsTenMin() throws Exception {
    double value = 0.5;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(false);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value++)));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value++)));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value++)));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0.5;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(1, v.value().summariesAvailable().size());
      assertEquals(0, (int) v.value().summariesAvailable().iterator().next());
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().value(0).doubleValue(), 0.001);
      base_time += 21600;
    }
  }
  
  @Test
  public void iterateFloatsReversedTenMin() throws Exception {
    double value = 0.5;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(true);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value++)));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value++)));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    for (int i = 0; i < 4; i++) {
      byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (i * 21600), (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
      seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value++)));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 11.5;
    base_time = BASE_TIME + (86400 * 3) - 21600;;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(1, v.value().summariesAvailable().size());
      assertEquals(0, (int) v.value().summariesAvailable().iterator().next());
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().value(0).doubleValue(), 0.001);
      base_time -= 21600;
    }
  }
  
  @Test
  public void iterateMixedTenMin() throws Exception {
    double value = 0.5;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(false);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0.5;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(1, v.value().summariesAvailable().size());
      assertEquals(0, (int) v.value().summariesAvailable().iterator().next());
      assertEquals(base_time, v.timestamp().epoch());
      if (v.value().value(0).isInteger()) {
        assertEquals((long) value, v.value().value(0).longValue());
      } else {
        assertEquals(value, v.value().value(0).doubleValue(), 0.001);
      }
      value += 1.5;
      base_time += 21600;
    }
  }
  
  @Test
  public void iterateMixedReversedTenMin() throws Exception {
    double value = 0.5;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(true);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) (7 | NumericCodec.FLAG_FLOAT), 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, Bytes.fromLong(Double.doubleToLongBits(value)));
    value += 1.5;
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) (long) value });
    value += 1.5;
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 17.0;
    base_time = BASE_TIME + (86400 * 3) - 21600;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(1, v.value().summariesAvailable().size());
      assertEquals(0, (int) v.value().summariesAvailable().iterator().next());
      assertEquals(base_time, v.timestamp().epoch());
      if (v.value().value(0).isInteger()) {
        assertEquals((long) value, v.value().value(0).longValue());
      } else {
        assertEquals(value, v.value().value(0).doubleValue(), 0.001);
      }
      value -= 1.5;
      base_time -= 21600;
    }
  }
  
  @Test
  public void iterateSumAndCountSyncedTenMin() throws Exception {
    long value = 0;
    int count = 10;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(false);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    count = 10;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(2, v.value().summariesAvailable().size());
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals((long) value++, v.value().value(0).longValue());
      assertEquals((long) count++, v.value().value(2).longValue());
      base_time += 21600;
    }
  }
  
  @Test
  public void iterateSumAndCountSyncedReversedTenMin() throws Exception {
    long value = 0;
    int count = 10;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(true);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 7;
    count = 17;
    base_time = BASE_TIME + (86400 * 2) - 21600;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(2, v.value().summariesAvailable().size());
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals((long) value--, v.value().value(0).longValue());
      assertEquals((long) count--, v.value().value(2).longValue());
      base_time -= 21600;
    }
  }
  
  @Test
  public void iterateSumAndCountOutOfSyncTenMin() throws Exception {
    long value = 0;
    int count = 10;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(false);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
//        (short) 0, 0, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
//        (short) 0, 2, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
//        (short) 0, 0, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
//        (short) 0, 0, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
//        (short) 0, 2, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    base_time = BASE_TIME;
    
    Integer[] values = new Integer[] { 0, null, 1, null, null, 2, 3, 4 };
    Integer[] counts = new Integer[] { 10, 11, null, 12, 13, 14, null, 15 };
    int i = 0;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(2, v.value().summariesAvailable().size());
      assertEquals(base_time, v.timestamp().epoch());
      if (values[i] == null) {
        assertNull(v.value().value(0));
      } else {
        assertEquals((long) values[i], v.value().value(0).longValue());
      }
      if (counts[i] == null) {
        assertNull(v.value().value(2));
      } else {
        assertEquals((long) counts[i], v.value().value(2).longValue());
      }
      i++;
      base_time += 21600;
    }
  }
  
  @Test
  public void iterateSumAndCountOutOfSyncReversedTenMin() throws Exception {
    long value = 0;
    int count = 10;
    long base_time = BASE_TIME;
    NumericSummarySpan span = new NumericSummarySpan(true);
    
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_time, TENMIN);
    byte[] qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
//        (short) 0, 0, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
//        (short) 0, 2, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
//        (short) 0, 0, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 86400;
    seq = new NumericSummaryRowSeq(base_time, TENMIN);
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
//        (short) 0, 0, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 0), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 1), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
//    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 2), 
//        (short) 0, 2, TENMIN);
//    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 0, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) value++ });
    qualifier = RollupUtils.buildRollupQualifier(base_time + (21600L * 3), 
        (short) 0, 2, TENMIN);
    seq.addColumn(PREFIX, qualifier, new byte[] { (byte) count++ });
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    base_time = BASE_TIME + (86400 * 2) - 21600;
    
    Integer[] values = new Integer[] { 0, null, 1, null, null, 2, 3, 4 };
    Integer[] counts = new Integer[] { 10, 11, null, 12, 13, 14, null, 15 };
    int i = values.length - 1;
    while (it.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) it.next();
      assertEquals(2, v.value().summariesAvailable().size());
      assertEquals(base_time, v.timestamp().epoch());
      if (values[i] == null) {
        assertNull(v.value().value(0));
      } else {
        assertEquals((long) values[i], v.value().value(0).longValue());
      }
      if (counts[i] == null) {
        assertNull(v.value().value(2));
      } else {
        assertEquals((long) counts[i], v.value().value(2).longValue());
      }
      i--;
      base_time -= 21600;
    }
  }
  
  NumericSummaryRowSeq newSeq(final long base_timestamp, final RollupInterval interval) {
    NumericSummaryRowSeq seq = new NumericSummaryRowSeq(base_timestamp, interval);
    
    // add dps
    byte[] qualifier = RollupUtils.buildRollupQualifier(base_timestamp, (short) 0, 2, interval);
    seq.addColumn(PREFIX, qualifier, new byte[] { 4 });
    
    qualifier = RollupUtils.buildRollupQualifier(base_timestamp, (short) 0, 0, interval);
    seq.addColumn(PREFIX, qualifier, new byte[] { 12 });

    qualifier = RollupUtils.buildRollupQualifier(base_timestamp + 600, (short) 0, 2, interval);
    seq.addColumn(PREFIX, qualifier, new byte[] { 3 });
        
    byte[] value = net.opentsdb.utils.Bytes.fromInt(Float.floatToIntBits(42.5F));
    short flags = 3 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(base_timestamp + 600, flags, 0, interval);
    seq.addColumn(PREFIX, qualifier, value);
    
    qualifier = RollupUtils.buildRollupQualifier(base_timestamp + 1200, (short) 0, 2, interval);
    seq.addColumn(PREFIX, qualifier, new byte[] { 5 });
    
    value = net.opentsdb.utils.Bytes.fromLong(Double.doubleToLongBits(24.75));
    flags = 7 | NumericCodec.FLAG_FLOAT;
    qualifier = RollupUtils.buildRollupQualifier(base_timestamp + 1200, flags, 0, interval);
    seq.addColumn(PREFIX, qualifier, value);
    return seq;
  }
}