/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  The OpenTSDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.query.processor.merge;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import net.opentsdb.data.*;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericTestUtils;
import net.opentsdb.data.types.numeric.aggregators.*;
import net.opentsdb.utils.DateTime;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestSplitNumericArrayIterator {

  private static final long BASE_TS = 1609459200;
  private Merger node;
  private MergerResult result;
  private NumericArrayAggregator aggregator;

  @Before
  public void before() throws Exception {
    node = mock(Merger.class);
  }

  @Test
  public void ctorAndClosed() throws Exception {
    setupQuery(BASE_TS, BASE_TS + (60 * 11), "1m", "avg");
    SplitNumericArrayIterator nai = new SplitNumericArrayIterator(node, result, overlapped());

    assertEquals(NumericArrayType.TYPE, nai.getType());
    assertTrue(nai.hasNext());

    TimeSeriesValue<NumericArrayType> value = nai.next();
    assertFalse(value.value().isInteger());
    assertArrayEquals(new double[]{1, 5, 2, 1, 4, 8, 9, 0, 2, 3, 4},
            value.value().doubleArray(), 0.001);

    assertFalse(nai.hasNext());
    assertEquals(nai.timestamp(), result.timeSpecification().start());

    nai.close();
    assertNull(nai.value());
  }

  @Test
  public void overlappedAvg() throws Exception {
    setupQuery(BASE_TS, BASE_TS + (60 * 11), "1m", "avg");
    SplitNumericArrayIterator nai = new SplitNumericArrayIterator(node, result, overlapped());
    assertFalse(nai.value().isInteger());
    assertArrayEquals(new double[]{1, 5, 2, 1, 4, 8, 9, 0, 2, 3, 4},
            nai.value().doubleArray(), 0.001);
  }

  @Test
  public void overlappedMax() throws Exception {
    setupQuery(BASE_TS, BASE_TS + (60 * 11), "1m", "max");
    SplitNumericArrayIterator nai = new SplitNumericArrayIterator(node, result, overlapped());
    // TODO support longs
    assertFalse(nai.value().isInteger());
    assertArrayEquals(new double[] { 1, 5, 2, 1, 4, 8, 9, 0, 2, 3, 4},
            nai.value().doubleArray(), 0.0001);
  }

  @Test
  public void overlappedMissingMiddle() throws Exception {
    setupQuery(BASE_TS, BASE_TS + (60 * 11), "1m", "max");
    List<TimeSeries> series = overlapped();
    series.remove(1);
    SplitNumericArrayIterator nai = new SplitNumericArrayIterator(node, result, series);
    assertFalse(nai.value().isInteger());
    NumericTestUtils.assertArrayEqualsNaNs(
            new double[] { 1, 5, 2, 1, 4, Double.NaN, 9, 0, 2, 3, 4},
            nai.value().doubleArray(), 0.0001);
  }

  @Test
  public void paddingBothEndsAvg() throws Exception {
    setupQuery(BASE_TS + (60 * 2), BASE_TS + (60 * 8), "1m", "avg");
    SplitNumericArrayIterator nai = new SplitNumericArrayIterator(node, result, overlapped());
    assertFalse(nai.value().isInteger());
    assertArrayEquals(new double[] { 2, 1, 4, 8, 9, 0 },
            nai.value().doubleArray(), 0.001);
  }

  void setupQuery(long start, long end, String interval, String agg) throws Exception {
    result = mock(MergerResult.class);
    TimeSpecification timeSpecification = mock(TimeSpecification.class);
    when(result.timeSpecification()).thenReturn(timeSpecification);
    when(timeSpecification.start()).thenReturn(new SecondTimeStamp(start));
    when(timeSpecification.end()).thenReturn(new SecondTimeStamp(end));
    when(timeSpecification.interval()).thenReturn(DateTime.parseDuration2(interval));

    long duration = DateTime.parseDuration(interval) / 1000;
    int intervals = (int) ((end - start) / duration);
    NumericArrayAggregatorConfig naac = DefaultArrayAggregatorConfig.newBuilder()
            .setArraySize(intervals)
            .build();
    if (agg.equals("max")) {
      when(node.getNAI()).thenReturn(new ArrayMaxFactory().newAggregator(naac));
    } else {
      when(node.getNAI()).thenReturn(new ArrayAverageFactory().newAggregator(naac));
    }
  }

  List<TimeSeries> overlapped() {
    NumericArrayTimeSeries ts1 = new NumericArrayTimeSeries(
            BaseTimeSeriesStringId.newBuilder()
                    .setMetric("a")
                    .build(), new SecondTimeStamp(BASE_TS));
    ts1.add(1, 5, 2, 1, 4);

    NumericArrayTimeSeries ts2 = new NumericArrayTimeSeries(
            BaseTimeSeriesStringId.newBuilder()
                    .setMetric("a")
                    .build(), new SecondTimeStamp(BASE_TS + (60 * 3)));
    ts2.add(1, 4, 8, 9, 0);

    NumericArrayTimeSeries ts3 = new NumericArrayTimeSeries(
            BaseTimeSeriesStringId.newBuilder()
                    .setMetric("a")
                    .build(), new SecondTimeStamp(BASE_TS + (60 * 6)));
    ts3.add(9, 0, 2, 3, 4);
    return Lists.newArrayList(ts1, ts2, ts3);
  }

}
