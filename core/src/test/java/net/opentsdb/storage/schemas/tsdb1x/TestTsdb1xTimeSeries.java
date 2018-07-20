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
package net.opentsdb.storage.schemas.tsdb1x;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.primitives.Bytes;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec.OffsetResolution;

public class TestTsdb1xTimeSeries extends SchemaBase {
  private static final byte[] TSUID = 
      Bytes.concat(METRIC_BYTES, TAGK_BYTES, TAGV_BYTES);
  private static final long BASE_TIME = 1514764800;
  private static final byte[] APPEND_Q = 
      new byte[] { Schema.APPENDS_PREFIX, 0, 0 };
  
  @Test
  public void ctor() throws Exception {
    try {
      new Tsdb1xTimeSeries(null, schema());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xTimeSeries(new byte[0], schema());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new Tsdb1xTimeSeries(TSUID, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    Tsdb1xTimeSeries series = new Tsdb1xTimeSeries(TSUID, schema());
    assertArrayEquals(METRIC_BYTES, ((TimeSeriesByteId) series.id()).metric());
    assertArrayEquals(TAGV_BYTES, ((TimeSeriesByteId) series.id()).tags().get(TAGK_BYTES));
    assertTrue(series.data.isEmpty());
    assertTrue(series.types().isEmpty());
  }
  
  @Test
  public void addSequenceOnce() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    
    Tsdb1xTimeSeries series = new Tsdb1xTimeSeries(TSUID, schema());
    assertFalse(series.iterator(NumericType.TYPE).isPresent());
    assertEquals(0, series.data.size());
    
    series.addSequence(seq, false, false, schema());
    assertEquals(1, series.data.size());
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
    }
    assertEquals(4, value);
    
    Collection<Iterator<TimeSeriesValue<?>>> iterators = series.iterators();
    assertEquals(1, iterators.size());
    it = iterators.iterator().next();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
    }
    assertEquals(4, value);
    assertEquals(1, series.types().size());
    assertTrue(series.types().contains(NumericType.TYPE));
  }
  
  @Test
  public void addSequenceOnceReversed() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    
    Tsdb1xTimeSeries series = new Tsdb1xTimeSeries(TSUID, schema());
    assertFalse(series.iterator(NumericType.TYPE).isPresent());
    assertEquals(0, series.data.size());
    
    series.addSequence(seq, true, false, schema());
    assertEquals(1, series.data.size());
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    value = 3;
    base_time = BASE_TIME + 3600 - 900;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 900;
    }
    assertEquals(-1, value);
    
    Collection<Iterator<TimeSeriesValue<?>>> iterators = series.iterators();
    assertEquals(1, iterators.size());
    it = iterators.iterator().next();
    value = 3;
    base_time = BASE_TIME + 3600 - 900;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 900;
    }
    assertEquals(-1, value);
    assertEquals(1, series.types().size());
    assertTrue(series.types().contains(NumericType.TYPE));
  }
  
  @Test
  public void addSequenceTwice() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    
    Tsdb1xTimeSeries series = new Tsdb1xTimeSeries(TSUID, schema());
    series.addSequence(seq, false, false, schema());
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, false);
    series.addSequence(seq, false, false, schema());
    
    assertEquals(1, series.data.size());
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
    }
    assertEquals(8, value);
    
    Collection<Iterator<TimeSeriesValue<?>>> iterators = series.iterators();
    assertEquals(1, iterators.size());
    it = iterators.iterator().next();
    value = 0;
    base_time = BASE_TIME;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value++, v.value().longValue());
      base_time += 900;
    }
    assertEquals(8, value);
  }
  
  @Test
  public void addSequenceTwiceReversed() throws Exception {
    long base_time = BASE_TIME;
    int value = 0;
    
    NumericRowSeq seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    
    Tsdb1xTimeSeries series = new Tsdb1xTimeSeries(TSUID, schema());
    series.addSequence(seq, true, false, schema());
    
    base_time += 3600;
    seq = new NumericRowSeq(base_time);
    for (int i = 0; i < 4; i++) {
      seq.addColumn(Schema.APPENDS_PREFIX, APPEND_Q, 
          NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 900 * i, value++));
    }
    seq.dedupe(false, true);
    series.addSequence(seq, true, false, schema());
    
    assertEquals(1, series.data.size());
    Iterator<TimeSeriesValue<?>> it = series.iterator(NumericType.TYPE).get();
    value = 7;
    base_time = BASE_TIME + (3600 * 2) - 900;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 900;
    }
    assertEquals(-1, value);
    
    Collection<Iterator<TimeSeriesValue<?>>> iterators = series.iterators();
    assertEquals(1, iterators.size());
    it = iterators.iterator().next();
    value = 7;
    base_time = BASE_TIME + (3600 * 2) - 900;
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(base_time, v.timestamp().epoch());
      assertEquals(value--, v.value().longValue());
      base_time -= 900;
    }
    assertEquals(-1, value);
  }
  
  @Test
  public void addSequenceNoCodec() throws Exception {
    class DummyType implements TimeSeriesDataType {
      @Override
      public TypeToken<? extends TimeSeriesDataType> type() {
        return TypeToken.of(DummyType.class);
      } 
    }
    
    TypeToken<? extends TimeSeriesDataType> type = 
        TypeToken.of(DummyType.class);
    RowSeq seq = mock(RowSeq.class);
    when(seq.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type;
      }
    });
    
    Tsdb1xTimeSeries series = new Tsdb1xTimeSeries(TSUID, schema());
    try {
      series.addSequence(seq, false, false, schema());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    Schema mock_schema = mock(Schema.class);
    try {
      series.addSequence(seq, false, false, mock_schema);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
  }
  
  @Test
  public void addSequenceExceptions() throws Exception {
    Tsdb1xTimeSeries series = new Tsdb1xTimeSeries(TSUID, schema());
    try {
      series.addSequence(null, false, false, schema());
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    NumericRowSeq seq = new NumericRowSeq(BASE_TIME);
    try {
      series.addSequence(seq, false, false, null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
  
  // TODO - test other data types like annotations when ready
}
