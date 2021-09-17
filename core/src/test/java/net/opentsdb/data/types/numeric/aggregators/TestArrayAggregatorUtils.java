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
package net.opentsdb.data.types.numeric.aggregators;

import com.google.common.reflect.TypeToken;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.ArrayAggregatorUtils.AccumulateState;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static net.opentsdb.data.types.numeric.NumericTestUtils.assertArrayEqualsNaNs;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestArrayAggregatorUtils {

  private static int BASE_TIMESTAMP = 1610496000;
  private static BaseArrayFactory FACTORY;

  @BeforeClass
  public static void beforeClass() {
    FACTORY = mock(BaseArrayFactory.class);
  }

  @Test
  public void accumulateInAggregatorNAIOverlap() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mockArrayTS(BASE_TIMESTAMP - 60, 0, 6);
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockArrayTS(BASE_TIMESTAMP + (60 * 4), 6, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockArrayTS(BASE_TIMESTAMP + (60 * 8), 11, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    assertEquals(0, agg.offset());
    assertEquals(12, agg.end());
    double[] expected = new double[] {
            1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14
    };
    assertArrayEquals(expected, agg.doubleArray(), 0.001);
  }

  @Test
  public void accumulateInAggregatorNAIOverlapDoubles() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mockArrayTS(BASE_TIMESTAMP - 60, .5, 6);
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockArrayTS(BASE_TIMESTAMP + (60 * 4), 6.5, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockArrayTS(BASE_TIMESTAMP + (60 * 8), 11.5, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    assertEquals(0, agg.offset());
    assertEquals(12, agg.end());
    double[] expected = new double[] {
            1.5, 2.5, 3.5, 4.5, 6.5, 7.5, 8.5, 9.5, 11.5, 12.5, 13.5, 14.5
    };
    assertArrayEquals(expected, agg.doubleArray(), 0.001);
  }

  @Test
  public void accumulateInAggregatorNAIOverlapMissing() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mockArrayTS(BASE_TIMESTAMP, 1, 5);
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    // skip middle

    ts = mockArrayTS(BASE_TIMESTAMP + (60 * 8), 11, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    assertEquals(0, agg.offset());
    assertEquals(12, agg.end());
    double[] expected = new double[] {
            1, 2, 3, 4, 5, Double.NaN, Double.NaN, Double.NaN, 11, 12, 13, 14
    };
    assertArrayEqualsNaNs(expected, agg.doubleArray(), 0.001);
  }

  @Test
  public void accumulateInAggregatorNAIOutOfBounds() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mockArrayTS(BASE_TIMESTAMP - (60 * 12), 1, 5);
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.OUT_OF_BOUNDS, state);

    // middle is good
    ts = mockArrayTS(BASE_TIMESTAMP + (60 * 4), 6, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockArrayTS(BASE_TIMESTAMP + (60 * 12), 11, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.OUT_OF_BOUNDS, state);

    assertEquals(0, agg.offset());
    assertEquals(12, agg.end());
    double[] expected = new double[] {
            Double.NaN, Double.NaN, Double.NaN, Double.NaN,
            6, 7, 8, 9, 10, Double.NaN, Double.NaN, Double.NaN
    };
    assertArrayEqualsNaNs(expected, agg.doubleArray(), 0.001);
  }

  @Test
  public void accumulateInAggregatorNAIBadData() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mock(TimeSeries.class);
    when(ts.iterator(any(TypeToken.class)))
            .thenReturn(Optional.empty());
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.NOT_PRESENT, state);

    TypedTimeSeriesIterator it = mock(TypedTimeSeriesIterator.class);
    when(ts.iterator(NumericArrayAggregator.TYPE)).thenReturn(Optional.of(it));
    state = ArrayAggregatorUtils.accumulateInAggregatorArray(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.NO_VALUE, state);
  }

  @Test
  public void accumulateInAggregatorNumericOverlap() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mockNumericTS(BASE_TIMESTAMP - 60, 0, 6);
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockNumericTS(BASE_TIMESTAMP + (60 * 4), 6, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockNumericTS(BASE_TIMESTAMP + (60 * 8), 11, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    assertEquals(0, agg.offset());
    assertEquals(12, agg.end());
    double[] expected = new double[] {
            1, 2, 3, 4, 6, 7, 8, 9, 11, 12, 13, 14
    };
    assertArrayEquals(expected, agg.doubleArray(), 0.001);
  }

  @Test
  public void accumulateInAggregatorNumericOverlapDoubles() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mockNumericTS(BASE_TIMESTAMP - 60, 0.5, 6);
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockNumericTS(BASE_TIMESTAMP + (60 * 4), 6.5, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockNumericTS(BASE_TIMESTAMP + (60 * 8), 11.5, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    assertEquals(0, agg.offset());
    assertEquals(12, agg.end());
    double[] expected = new double[] {
            1.5, 2.5, 3.5, 4.5, 6.5, 7.5, 8.5, 9.5, 11.5, 12.5, 13.5, 14.5
    };
    assertArrayEquals(expected, agg.doubleArray(), 0.001);
  }

  @Test
  public void accumulateInAggregatorNumericOverlapMissing() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mockNumericTS(BASE_TIMESTAMP, 1, 5);
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    // skip middle

    ts = mockNumericTS(BASE_TIMESTAMP + (60 * 8), 11, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    assertEquals(0, agg.offset());
    assertEquals(12, agg.end());
    double[] expected = new double[] {
            1, 2, 3, 4, 5, Double.NaN, Double.NaN, Double.NaN, 11, 12, 13, 14
    };
    assertArrayEqualsNaNs(expected, agg.doubleArray(), 0.001);
  }

  @Test
  public void accumulateInAggregatorNumericOutOfBounds() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mockNumericTS(BASE_TIMESTAMP - (60 * 12), 1, 5);
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.OUT_OF_BOUNDS, state);

    // middle is good
    ts = mockNumericTS(BASE_TIMESTAMP + (60 * 4), 6, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.SUCCESS, state);

    ts = mockNumericTS(BASE_TIMESTAMP + (60 * 12), 11, 5);
    state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.OUT_OF_BOUNDS, state);

    assertEquals(0, agg.offset());
    assertEquals(12, agg.end());
    double[] expected = new double[] {
            Double.NaN, Double.NaN, Double.NaN, Double.NaN,
            6, 7, 8, 9, 10, Double.NaN, Double.NaN, Double.NaN
    };
    assertArrayEqualsNaNs(expected, agg.doubleArray(), 0.001);
  }

  @Test
  public void accumulateInAggregatorNumericBadData() throws Exception {
    NumericArrayAggregator agg = new ArrayMaxFactory.ArrayMax(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(12)
                    .build(),
            FACTORY);
    TimeStamp start = new SecondTimeStamp(BASE_TIMESTAMP);
    TimeStamp end = new SecondTimeStamp(BASE_TIMESTAMP + (60 * 12));

    TimeSeries ts = mock(TimeSeries.class);
    when(ts.iterator(any(TypeToken.class)))
            .thenReturn(Optional.empty());
    AccumulateState state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.NOT_PRESENT, state);

    TypedTimeSeriesIterator it = mock(TypedTimeSeriesIterator.class);
    when(ts.iterator(NumericType.TYPE)).thenReturn(Optional.of(it));
    state = ArrayAggregatorUtils.accumulateInAggregatorNumeric(
            agg, start, end, Duration.of(60, ChronoUnit.SECONDS), ts);
    assertEquals(AccumulateState.NO_VALUE, state);
  }

  TimeSeries mockArrayTS(int timestamp, long startingValue, int count) {
    NumericArrayTimeSeries ts = new NumericArrayTimeSeries(
            mock(TimeSeriesId.class),
            new SecondTimeStamp(timestamp));
    for (int i = 0; i < count; i++) {
      ts.add(startingValue++);
    }
    return ts;
  }

  TimeSeries mockArrayTS(int timestamp, double startingValue, int count) {
    NumericArrayTimeSeries ts = new NumericArrayTimeSeries(
            mock(TimeSeriesId.class),
            new SecondTimeStamp(timestamp));
    for (int i = 0; i < count; i++) {
      ts.add(startingValue++);
    }
    return ts;
  }

  TimeSeries mockNumericTS(int timestamp, long startingValue, int count) {
    NumericMillisecondShard ts = new NumericMillisecondShard(
            mock(TimeSeriesId.class),
            new SecondTimeStamp(timestamp),
            new SecondTimeStamp(timestamp + (60 * 12)));
    for (int i = 0; i < count; i++) {
      ts.add(timestamp * 1000L, startingValue++);
      timestamp += 60;
    }
    return ts;
  }

  TimeSeries mockNumericTS(int timestamp, double startingValue, int count) {
    NumericMillisecondShard ts = new NumericMillisecondShard(
            mock(TimeSeriesId.class),
            new SecondTimeStamp(timestamp),
            new SecondTimeStamp(timestamp + (60 * 12)));
    for (int i = 0; i < count; i++) {
      ts.add(timestamp * 1000L, startingValue++);
      timestamp += 60;
    }
    return ts;
  }
}
