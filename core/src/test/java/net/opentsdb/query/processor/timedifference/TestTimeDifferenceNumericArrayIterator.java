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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;

public class TestTimeDifferenceNumericArrayIterator {

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
    
    TimeSpecification spec = mock(TimeSpecification.class);
    when(spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(spec.start()).thenReturn(new SecondTimeStamp(0L));
    when(context.query()).thenReturn(query);
    when(context.tsdb()).thenReturn(TSDB);
    when(result.timeSpecification()).thenReturn(spec);
    
    when(query.startTime()).thenReturn(new SecondTimeStamp(60L * 5));
  }
  
  @Test
  public void longs() throws Exception {
    NumericArrayTimeSeries ts = new NumericArrayTimeSeries(id, 
        new SecondTimeStamp(0));
    ((NumericArrayTimeSeries) ts).add(8L);
    ((NumericArrayTimeSeries) ts).add(6L);
    ((NumericArrayTimeSeries) ts).add(48L);
    ((NumericArrayTimeSeries) ts).add(2L);
    ((NumericArrayTimeSeries) ts).add(9L);
    
    setNode(ChronoUnit.SECONDS);
    TimeDifferenceNumericArrayIterator iterator = 
        new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 60, 60, 60, 60 }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
    
    setNode(ChronoUnit.MILLIS);
    iterator = new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 60_000, 60_000, 60_000, 60_000 }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
    
    setNode(ChronoUnit.NANOS);
    iterator = new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 60_000_000_000L, 60_000_000_000L, 60_000_000_000L, 60_000_000_000L }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
    
    setNode(ChronoUnit.MINUTES);
    iterator = new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 1, 1, 1, 1 }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
    
    setNode(ChronoUnit.HOURS);
    iterator = new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 0.016, 0.016, 0.016, 0.016 }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }

  @Test
  public void doubles() throws Exception {
    NumericArrayTimeSeries ts = new NumericArrayTimeSeries(id, 
        new SecondTimeStamp(0));
    ((NumericArrayTimeSeries) ts).add(8d);
    ((NumericArrayTimeSeries) ts).add(6d);
    ((NumericArrayTimeSeries) ts).add(48d);
    ((NumericArrayTimeSeries) ts).add(2d);
    ((NumericArrayTimeSeries) ts).add(9d);
    
    setNode(ChronoUnit.SECONDS);
    TimeDifferenceNumericArrayIterator iterator = 
        new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 60, 60, 60, 60 }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
    
    // test NaNs. The resolution is covered in longs.
    ts = new NumericArrayTimeSeries(id, new SecondTimeStamp(0));
    ((NumericArrayTimeSeries) ts).add(8d);
    ((NumericArrayTimeSeries) ts).add(6d);
    ((NumericArrayTimeSeries) ts).add(Double.NaN);
    ((NumericArrayTimeSeries) ts).add(Double.NaN);
    ((NumericArrayTimeSeries) ts).add(9d);
    
    setNode(ChronoUnit.SECONDS);
    iterator = new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 60, Double.NaN, Double.NaN, 180 }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
    
    // start nan
    ts = new NumericArrayTimeSeries(id, new SecondTimeStamp(0));
    ((NumericArrayTimeSeries) ts).add(Double.NaN);
    ((NumericArrayTimeSeries) ts).add(Double.NaN);
    ((NumericArrayTimeSeries) ts).add(48L);
    ((NumericArrayTimeSeries) ts).add(2L);
    ((NumericArrayTimeSeries) ts).add(9d);
    
    setNode(ChronoUnit.SECONDS);
    iterator = new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, 60, 60 }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
    
    // end Nans
    ts = new NumericArrayTimeSeries(id, new SecondTimeStamp(0));
    ((NumericArrayTimeSeries) ts).add(8d);
    ((NumericArrayTimeSeries) ts).add(6d);
    ((NumericArrayTimeSeries) ts).add(48d);
    ((NumericArrayTimeSeries) ts).add(Double.NaN);
    ((NumericArrayTimeSeries) ts).add(Double.NaN);
    
    setNode(ChronoUnit.SECONDS);
    iterator = new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
    assertTrue(iterator.hasNext());
    value = iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 60, 60, Double.NaN, Double.NaN }, 
        value.value().doubleArray(), 0.001);
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void noData() throws Exception {
    MockTimeSeries ts = new MockTimeSeries(id);
    
    setNode(ChronoUnit.SECONDS);
    TimeDifferenceNumericArrayIterator iterator = 
        new TimeDifferenceNumericArrayIterator(node, result, Lists.newArrayList(ts));
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
