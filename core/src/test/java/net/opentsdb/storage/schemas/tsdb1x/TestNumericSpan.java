// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.junit.Test;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec.OffsetResolution;

public class TestNumericSpan {
  private static final long BASE_TIME = 1514764800;
  private static final byte[] APPEND_Q = 
      new byte[] { Schema.APPENDS_PREFIX, 0, 0 };
  
  @Test
  public void addSequence() throws Exception {
    NumericSpan span = new NumericSpan(false);
    
    try {
      span.addSequence(null, false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME + 3600);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 0));
    
    assertEquals(0, span.rows.size());
    span.addSequence(seq, false);
    assertEquals(1, span.rows.size());
    
    // empty rows are skipped
    seq = new NumericRowSeq(BASE_TIME + (3600 * 2));
    span.addSequence(seq, false);
    assertEquals(1, span.rows.size());
    assertEquals(BASE_TIME + 3600, span.rows.get(0).base_timestamp);
    
    seq = new NumericRowSeq(BASE_TIME);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, 0));
    try {
      span.addSequence(seq, false);
      fail("Expected IllegalStateException e");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void addSequenceAppend() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(false);
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 2; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    assertEquals(1, span.rows.size());
    
    // simulate split row
    seq = new NumericRowSeq(base_time);
    for (int i = 2; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    assertEquals(1, span.rows.size());
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
    }
    assertEquals(4, value);
  }
  
  @Test
  public void addIterateLongsSeconds() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
    }
  }
  
  @Test
  public void addIterateLongsSecondsReversed() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(true);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 11;
    base_time = BASE_TIME + (3600 * 3) - 900;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 900;
    }
  }

  @Test
  public void addIterateFloatsSeconds() throws Exception {
    long base_time = BASE_TIME;
    double value = 0.5;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0.5;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().doubleValue(), 0.001);
      base_time += 900;
    }
  }
  
  @Test
  public void addIterateFloatsSecondsReversed() throws Exception {
    long base_time = BASE_TIME;
    double value = 0.5;
    
    NumericSpan span = new NumericSpan(true);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 11.5;
    base_time = BASE_TIME + (3600 * 3) - 900;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().doubleValue(), 0.001);
      base_time -= 900;
    }
  }
  
  @Test
  public void addIterateLongsMillis() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME * 1000;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().msEpoch());
      assertEquals(value++, v.value().longValue());
      base_time += 360000;
    }
  }
  
  @Test
  public void addIterateLongsMillisReversed() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(true);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 29;
    base_time = (BASE_TIME * 1000) + (3600000 * 3) - 360000;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().msEpoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 360000;
    }
  }

  @Test
  public void addIterateFloatsMillis() throws Exception {
    long base_time = BASE_TIME;
    double value = 0.5;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0.5;
    base_time = BASE_TIME * 1000;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().msEpoch());
      assertEquals(value++, v.value().doubleValue(), 0.001);
      base_time += 360000;
    }
  }
  
  @Test
  public void addIterateFloatsMillisReversed() throws Exception {
    long base_time = BASE_TIME;
    double value = 0.5;
    
    NumericSpan span = new NumericSpan(true);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 360000 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 29.5;
    base_time = (BASE_TIME * 1000) + (3600000 * 3) - 360000;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().msEpoch());
      assertEquals(value--, v.value().doubleValue(), 0.001);
      base_time -= 360000;
    }
  }
  
  @Test
  public void addIterateLongsNanos() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 360;
    }
  }
  
  @Test
  public void addIterateLongsNanosReversed() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(true);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 29;
    base_time = BASE_TIME + (3600 * 3) - 360;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 360;
    }
  }

  @Test
  public void addIterateFloatsNanos() throws Exception {
    long base_time = BASE_TIME;
    double value = 0.5;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0.5;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().doubleValue(), 0.001);
      base_time += 360;
    }
  }
  
  @Test
  public void addIterateFloatsNanosReversed() throws Exception {
    long base_time = BASE_TIME;
    double value = 0.5;
    
    NumericSpan span = new NumericSpan(true);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 10; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 360000000000L * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 29.5;
    base_time = BASE_TIME + (3600 * 3) - 360;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().doubleValue(), 0.001);
      base_time -= 360;
    }
  }
  
  @Test
  public void addIterateLongsMixed() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 900000000000L, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2700, value++));
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 0, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 900000, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 2700000000000L, value++));
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 0, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 900000, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 2700000000000L, value++));
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
    }
  }
  
  @Test
  public void addIterateFloatsMixed() throws Exception {
    long base_time = BASE_TIME;
    double value = 0.5;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 900000000000L, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2700, value++));
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 0, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 900000, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 2700000000000L, value++));
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 0, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 900000, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, value++));
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 2700000000000L, value++));
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0.5;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().doubleValue(), 0.001);
      base_time += 900;
    }
  }
  
  @Test
  public void addIterateMixedMixed() throws Exception {
    long base_time = BASE_TIME;
    double value = 0;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 0, (long) value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 900000000000L, value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, (long) value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 2700, value));
    value += 1.5;
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 0, (long) value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 900000, value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, (long) value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 2700000000000L, value));
    value += 1.5;
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 0, (long) value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 900000, value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 1800000L, (long) value));
    value += 1.5;
    seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
        NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 2700000000000L, value));
    value += 1.5;
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 00;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      if (v.value().isInteger()) {
        assertEquals((long) value, v.value().longValue());
      } else {
        assertEquals(value, v.value().doubleValue(), 0.001);
      }
      
      value += 1.5;
      base_time += 900;
    }
  }

  @Test
  public void addIterateSingleSegment() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
        
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
    }
  }
  
  @Test
  public void addIterateSingleSegmentReversed() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(true);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
        
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 3;
    base_time = BASE_TIME + 3600 - 900;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 900;
    }
  }
  
  @Test
  public void addIterateLongsSecondsGap() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(false);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    base_time += 3600;
    // nope!
//    seq = new NumericRowSeq(base_time);
//    for (int i = 0; i < 4; i++) {
//      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
//          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
//    }
//    seq.dedupe(false, false);
//    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
      if (base_time == BASE_TIME + 3600) {
        base_time = BASE_TIME + 3600 + 3600;
      }
    }
  }
  
  @Test
  public void addIterateLongsSecondsGapReversed() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericSpan span = new NumericSpan(true);
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    base_time += 3600;
    // nope!
//    seq = new NumericRowSeq(base_time);
//    for (int i = 0; i < 4; i++) {
//      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
//          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
//    }
//    seq.dedupe(false, false);
//    span.addSequence(seq, false);
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    span.addSequence(seq, false);
    
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    value = 7;
    base_time = BASE_TIME + (3600 * 3) - 900;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 900;
      if (base_time == BASE_TIME + 3600 + 3600 - 900) {
        base_time = BASE_TIME + 3600 - 900;
      }
    }
  }
  
  @Test
  public void iterateNoSegments() throws Exception {
    NumericSpan span = new NumericSpan(false);
    Iterator<TimeSeriesValue<?>> it = span.iterator();
    assertFalse(it.hasNext());
  }
}
