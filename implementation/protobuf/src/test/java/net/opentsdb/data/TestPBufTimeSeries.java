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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;

import net.opentsdb.data.pbuf.TimeSeriesIdPB;
import net.opentsdb.data.pbuf.TimeSeriesPB;
import net.opentsdb.data.pbuf.TimeSeriesDataPB.TimeSeriesData;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.serdes.PBufIteratorSerdesFactory;

public class TestPBufTimeSeries {

  private PBufIteratorSerdesFactory factory;
  private byte[] numeric_data;
  private byte[] summary_data;
  
  @Before
  public void before() throws Exception {
    factory = new PBufIteratorSerdesFactory();
    numeric_data = new byte[] { 10, 43, 110, 101, 116, 46, 111, 112, 101, 
        110, 116, 115, 100, 98, 46, 100, 97, 116, 97, 46, 116, 121, 112, 
        101, 115, 46, 110, 117, 109, 101, 114, 105, 99, 46, 78, 117, 109, 
        101, 114, 105, 99, 84, 121, 112, 101, 18, 101, 10, 11, 8, -128, 
        -12, -56, -41, 5, 26, 3, 85, 84, 67, 18, 11, 8, -112, -112, -55, 
        -41, 5, 26, 3, 85, 84, 67, 42, 73, 10, 34, 116, 121, 112, 101, 
        46, 103, 111, 111, 103, 108, 101, 97, 112, 105, 115, 46, 99, 111, 
        109, 47, 78, 117, 109, 101, 114, 105, 99, 83, 101, 103, 109, 101,
        110, 116, 18, 35, 10, 29, 0, 0, 0, 42, 0, 3, -49, 64, 57, -103, 
        -103, -103, -103, -103, -102, 0, 7, -120, 0, 11, 79, 127, -8, 0, 
        0, 0, 0, 0, 0, 16, 3, 24, 3 };
    summary_data = new byte[] { 10, 50, 110, 101, 116, 46, 111, 112, 101, 
        110, 116, 115, 100, 98, 46, 100, 97, 116, 97, 46, 116, 121, 112, 
        101, 115, 46, 110, 117, 109, 101, 114, 105, 99, 46, 78, 117, 109, 
        101, 114, 105, 99, 83, 117, 109, 109, 97, 114, 121, 84, 121, 112,
        101, 18, 127, 10, 11, 8, -128, -12, -56, -41, 5, 26, 3, 85, 84, 
        67, 18, 11, 8, -112, -112, -55, -41, 5, 26, 3, 85, 84, 67, 42, 
        99, 10, 41, 116, 121, 112, 101, 46, 103, 111, 111, 103, 108, 101, 
        97, 112, 105, 115, 46, 99, 111, 109, 47, 78, 117, 109, 101, 114, 
        105, 99, 83, 117, 109, 109, 97, 114, 121, 83, 101, 103, 109, 101, 
        110, 116, 18, 54, 10, 27, 10, 25, 0, 0, 11, 66, 42, 0, 0, 0, 3, 
        -64, 8, 0, 7, -120, 0, 11, 79, 127, -8, 0, 0, 0, 0, 0, 0, 10, 19, 
        10, 15, 0, 0, 0, 4, 0, 3, -64, 1, 0, 7, -120, 0, 11, 64, 0, 16, 
        2, 16, 3, 24, 3 };
  }
  
  @Test
  public void ctor() throws Exception {
    PBufTimeSeries time_series = new PBufTimeSeries(factory, getSeries(true, true));
    assertEquals(2, time_series.types().size());
    assertTrue(time_series.types().contains(NumericType.TYPE));
    assertTrue(time_series.types().contains(NumericSummaryType.TYPE));
    assertEquals("sys.cpu", ((TimeSeriesStringId) time_series.id()).metric());
    assertEquals("web01", ((TimeSeriesStringId) time_series.id()).tags().get("host"));
    
    // empty series is fine
    time_series = new PBufTimeSeries(factory, getSeries(false, false));
    assertTrue(time_series.types().isEmpty());
    assertEquals("sys.cpu", ((TimeSeriesStringId) time_series.id()).metric());
    assertEquals("web01", ((TimeSeriesStringId) time_series.id()).tags().get("host"));
    
    try {
      new PBufTimeSeries(factory, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new PBufTimeSeries(null, getSeries(true, true));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void iterator() throws Exception {
    PBufTimeSeries time_series = new PBufTimeSeries(factory, getSeries(true, true));
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = 
        time_series.iterator(NumericType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericType> nv = (TimeSeriesValue<NumericType>) iterator.next();
    assertEquals(1525824000000L, nv.timestamp().msEpoch());
    assertEquals(42, nv.value().longValue());
    
    iterator = time_series.iterator(NumericSummaryType.TYPE).get();
    TimeSeriesValue<NumericSummaryType> sv = 
        (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertTrue(iterator.hasNext());
    assertEquals(1525824000000L, sv.timestamp().msEpoch());
    assertEquals(42.5, sv.value().value(0).doubleValue(), 0.001);
    assertEquals(4, sv.value().value(2).longValue());
    
    TypeToken<?> string_type = TypeToken.of(String.class);
    assertFalse(time_series.iterator(string_type).isPresent());
    assertFalse(time_series.iterator(null).isPresent());
    
    // empty
    time_series = new PBufTimeSeries(factory, getSeries(false, false));
    assertFalse(time_series.iterator(NumericType.TYPE).isPresent());
    assertFalse(time_series.iterator(NumericSummaryType.TYPE).isPresent());
    assertFalse(time_series.iterator(string_type).isPresent());
    assertFalse(time_series.iterator(null).isPresent());
  }
  
  @Test
  public void iterators() throws Exception {
    PBufTimeSeries time_series = new PBufTimeSeries(factory, getSeries(true, true));
    Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = 
        time_series.iterators();
    assertEquals(2, iterators.size());
    for (final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> it : iterators) {
      assertTrue(it.hasNext());
      TimeSeriesValue<? extends TimeSeriesDataType> value = it.next();
      assertEquals(1525824000000L, value.timestamp().msEpoch());
      if (value.type() == NumericType.TYPE) {
        assertEquals(42, ((NumericType) value.value()).longValue());
      } else {
        assertEquals(42.5, ((NumericSummaryType) value.value()).value(0).doubleValue(), 0.001);
        assertEquals(4, ((NumericSummaryType) value.value()).value(2).longValue());
      }
    }

    // empty
    time_series = new PBufTimeSeries(factory, getSeries(false, false));
    assertTrue(time_series.iterators().isEmpty());
  }
  
  TimeSeriesPB.TimeSeries getSeries(final boolean include_numeric, 
      final boolean include_summary) throws InvalidProtocolBufferException {
    TimeSeriesPB.TimeSeries.Builder builder = TimeSeriesPB.TimeSeries.newBuilder()
        .setId(TimeSeriesIdPB.TimeSeriesId.newBuilder()
            .setMetric("sys.cpu")
            .putTags("host", "web01"));
    if (include_numeric) {
      builder.addData(TimeSeriesData.parseFrom(numeric_data));
    }
    if (include_summary) {
      builder.addData(TimeSeriesData.parseFrom(summary_data));
    }
    return builder.build();
  }
}
