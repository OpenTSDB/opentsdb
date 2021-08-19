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

package net.opentsdb.query.anomaly;

import com.google.common.reflect.TypeToken;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertType;
import net.opentsdb.data.types.alert.AlertType.State;
import net.opentsdb.data.types.alert.AlertValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.ArrayMaxFactory;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAnomalyPredictionTimeSeries {
  private static final int BASE_TIME = 1356998400;

  private BaseAnomalyNode node;
  private AnomalyQueryResult result;
  private TimeSeriesId id;

  @Before
  public void before() throws Exception {
    node = mock(BaseAnomalyNode.class);
    result = mock(AnomalyQueryResult.class);
    id = BaseTimeSeriesStringId.newBuilder()
            .setMetric("sys.cpu.user")
            .addTags("host", "web01")
            .build();
  }

  @Test
  public void aggregatorOnly() throws Exception {
    NumericArrayAggregator aggregator = newAgg();
    for (int i = 0; i < 60; i++) {
      aggregator.accumulate((double) i, i);
    }

    AnomalyPredictionTimeSeries prediction = new AnomalyPredictionTimeSeries(
            id, aggregator, new SecondTimeStamp(BASE_TIME));
    assertSame(id, prediction.id());
    Collection<TypeToken<? extends TimeSeriesDataType>> types = prediction.types();
    assertEquals(1, types.size());
    assertTrue(types.contains(NumericArrayType.TYPE));

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            prediction.iterator(NumericType.TYPE);
    assertFalse(op.isPresent());
    op = prediction.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());

    TypedTimeSeriesIterator<NumericArrayType> iterator =
            (TypedTimeSeriesIterator<NumericArrayType>) op.get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> seriesValue = iterator.next();
    assertEquals(BASE_TIME, seriesValue.timestamp().epoch());
    assertEquals(0, seriesValue.value().offset());
    assertEquals(60, seriesValue.value().end());
    double[] results = seriesValue.value().doubleArray();
    for (int i = 0; i < 60; i++) {
      assertEquals(i, results[i], 0.001);
    }

    Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
            prediction.iterators();
    assertEquals(1, iterators.size());
    iterator = (TypedTimeSeriesIterator<NumericArrayType>) iterators.iterator().next();
    assertTrue(iterator.hasNext());
    seriesValue = iterator.next();
    assertEquals(BASE_TIME, seriesValue.timestamp().epoch());
    assertEquals(0, seriesValue.value().offset());
    assertEquals(60, seriesValue.value().end());
    results = seriesValue.value().doubleArray();
    for (int i = 0; i < 60; i++) {
      assertEquals(i, results[i], 0.001);
    }

    prediction.close();
    verify(aggregator, times(1)).close();
  }

  @Test
  public void emptyAggregatorOnly() throws Exception {
    NumericArrayAggregator aggregator = newAgg();

    AnomalyPredictionTimeSeries prediction = new AnomalyPredictionTimeSeries(
            id, aggregator, new SecondTimeStamp(BASE_TIME));
    assertSame(id, prediction.id());
    Collection<TypeToken<? extends TimeSeriesDataType>> types = prediction.types();
    assertEquals(1, types.size());
    assertTrue(types.contains(NumericArrayType.TYPE));

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            prediction.iterator(NumericType.TYPE);
    assertFalse(op.isPresent());
    op = prediction.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());

    TypedTimeSeriesIterator<NumericArrayType> iterator =
            (TypedTimeSeriesIterator<NumericArrayType>) op.get();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void nullAggregatorOnly() throws Exception {
    AnomalyPredictionTimeSeries prediction = new AnomalyPredictionTimeSeries(
            id, null, new SecondTimeStamp(BASE_TIME));
    assertSame(id, prediction.id());
    Collection<TypeToken<? extends TimeSeriesDataType>> types = prediction.types();
    assertEquals(1, types.size());
    assertTrue(types.contains(NumericArrayType.TYPE));

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            prediction.iterator(NumericType.TYPE);
    assertFalse(op.isPresent());
    op = prediction.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());

    TypedTimeSeriesIterator<NumericArrayType> iterator =
            (TypedTimeSeriesIterator<NumericArrayType>) op.get();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void aggregatorWithAlerts() throws Exception {
    NumericArrayAggregator aggregator = newAgg();
    for (int i = 0; i < 60; i++) {
      aggregator.accumulate((double) i, i);
    }

    AnomalyPredictionTimeSeries prediction = new AnomalyPredictionTimeSeries(
            id, aggregator, new SecondTimeStamp(BASE_TIME));
    prediction.addAlert(AlertValue.newBuilder()
            .setDataPoint(3)
            .setTimestamp(new SecondTimeStamp(BASE_TIME + (60 * 3)))
            .setThreshold(1.5)
            .setState(State.BAD)
            .setMessage("Test A")
            .build());
    prediction.addAlert(AlertValue.newBuilder()
            .setDataPoint(42)
            .setTimestamp(new SecondTimeStamp(BASE_TIME + (60 * 42)))
            .setThreshold(16)
            .setState(State.BAD)
            .setMessage("Test B")
            .build());

    assertSame(id, prediction.id());
    Collection<TypeToken<? extends TimeSeriesDataType>> types = prediction.types();
    assertEquals(2, types.size());
    assertTrue(types.contains(NumericArrayType.TYPE));
    assertTrue(types.contains(AlertType.TYPE));

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            prediction.iterator(NumericType.TYPE);
    assertFalse(op.isPresent());
    op = prediction.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());

    TypedTimeSeriesIterator<NumericArrayType> iterator =
            (TypedTimeSeriesIterator<NumericArrayType>) op.get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> seriesValue = iterator.next();
    assertEquals(BASE_TIME, seriesValue.timestamp().epoch());
    assertEquals(0, seriesValue.value().offset());
    assertEquals(60, seriesValue.value().end());
    double[] results = seriesValue.value().doubleArray();
    for (int i = 0; i < 60; i++) {
      assertEquals(i, results[i], 0.001);
    }

    op = prediction.iterator(AlertType.TYPE);
    assertTrue(op.isPresent());
    TypedTimeSeriesIterator<AlertType> alertIterator =
            (TypedTimeSeriesIterator<AlertType>) op.get();
    assertTrue(alertIterator.hasNext());
    TimeSeriesValue<AlertType> alert = alertIterator.next();
    assertEquals(3, alert.value().dataPoint().longValue());
    assertEquals(1.5, alert.value().threshold().doubleValue(), 0.001);

    assertTrue(alertIterator.hasNext());
    alert = alertIterator.next();
    assertEquals(42, alert.value().dataPoint().longValue());
    assertEquals(16, alert.value().threshold().longValue());
    assertFalse(alertIterator.hasNext());

    Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
            prediction.iterators();
    assertEquals(2, iterators.size());
    iterator = (TypedTimeSeriesIterator<NumericArrayType>) iterators.iterator().next();
    assertTrue(iterator.hasNext());
    seriesValue = iterator.next();
    assertEquals(BASE_TIME, seriesValue.timestamp().epoch());
    assertEquals(0, seriesValue.value().offset());
    assertEquals(60, seriesValue.value().end());
    results = seriesValue.value().doubleArray();
    for (int i = 0; i < 60; i++) {
      assertEquals(i, results[i], 0.001);
    }

    alertIterator = (TypedTimeSeriesIterator<AlertType>)
            ((List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>>) iterators).get(1);
    assertTrue(alertIterator.hasNext());
    alert = alertIterator.next();
    assertEquals(3, alert.value().dataPoint().longValue());
    assertEquals(1.5, alert.value().threshold().doubleValue(), 0.001);

    assertTrue(alertIterator.hasNext());
    alert = alertIterator.next();
    assertEquals(42, alert.value().dataPoint().longValue());
    assertEquals(16, alert.value().threshold().longValue());
    assertFalse(alertIterator.hasNext());
  }

  @Test
  public void joinAllPresent() throws Exception {
    setupNode(BASE_TIME + (60 * 30)); // 30m offset
    TimeSeries[] series = new TimeSeries[] {
            mockSeries(BASE_TIME, 0),
            mockSeries(BASE_TIME + 3600, 60)
    };

    AnomalyPredictionTimeSeries prediction = new AnomalyPredictionTimeSeries(series,
            node, result, "prediction");
    assertNotSame(id, prediction.id());
    Collection<TypeToken<? extends TimeSeriesDataType>> types = prediction.types();
    assertEquals(1, types.size());
    assertTrue(types.contains(NumericArrayType.TYPE));

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            prediction.iterator(NumericType.TYPE);
    assertFalse(op.isPresent());
    op = prediction.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());

    TypedTimeSeriesIterator<NumericArrayType> iterator =
            (TypedTimeSeriesIterator<NumericArrayType>) op.get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> seriesValue = iterator.next();
    assertEquals(BASE_TIME + (60 * 30), seriesValue.timestamp().epoch());
    assertEquals(0, seriesValue.value().offset());
    assertEquals(60, seriesValue.value().end());
    double[] results = seriesValue.value().doubleArray();
    double val = 30;
    for (int i = 0; i < 60; i++) {
      assertEquals(val++, results[i], 0.001);
    }

    TimeSeriesStringId id = (TimeSeriesStringId) prediction.id();
    assertEquals("sys.cpu.user.prediction", id.metric());
  }

  @Test
  public void joinMissingFirst() throws Exception {
    setupNode(BASE_TIME + (60 * 30)); // 30m offset
    TimeSeries[] series = new TimeSeries[] {
            //mockSeries(BASE_TIME, 0),
            mockSeries(BASE_TIME + 3600, 60)
    };

    AnomalyPredictionTimeSeries prediction = new AnomalyPredictionTimeSeries(series,
            node, result, "prediction");
    assertNotSame(id, prediction.id());
    Collection<TypeToken<? extends TimeSeriesDataType>> types = prediction.types();
    assertEquals(1, types.size());
    assertTrue(types.contains(NumericArrayType.TYPE));

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            prediction.iterator(NumericType.TYPE);
    assertFalse(op.isPresent());
    op = prediction.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());

    TypedTimeSeriesIterator<NumericArrayType> iterator =
            (TypedTimeSeriesIterator<NumericArrayType>) op.get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> seriesValue = iterator.next();
    assertEquals(BASE_TIME + (60 * 30), seriesValue.timestamp().epoch());
    assertEquals(0, seriesValue.value().offset());
    assertEquals(60, seriesValue.value().end());
    double[] results = seriesValue.value().doubleArray();
    double val = 60;
    for (int i = 0; i < 60; i++) {
      if (i < 30) {
       assertTrue(Double.isNaN(results[i]));
      } else {
        assertEquals(val++, results[i], 0.001);
      }
    }

    TimeSeriesStringId id = (TimeSeriesStringId) prediction.id();
    assertEquals("sys.cpu.user.prediction", id.metric());
  }

  @Test
  public void joinMissingSecond() throws Exception {
    setupNode(BASE_TIME + (60 * 30)); // 30m offset
    TimeSeries[] series = new TimeSeries[] {
            mockSeries(BASE_TIME, 0),
            //mockSeries(BASE_TIME + 3600, 60)
    };

    AnomalyPredictionTimeSeries prediction = new AnomalyPredictionTimeSeries(series,
            node, result, "prediction");
    assertNotSame(id, prediction.id());
    Collection<TypeToken<? extends TimeSeriesDataType>> types = prediction.types();
    assertEquals(1, types.size());
    assertTrue(types.contains(NumericArrayType.TYPE));

    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            prediction.iterator(NumericType.TYPE);
    assertFalse(op.isPresent());
    op = prediction.iterator(NumericArrayType.TYPE);
    assertTrue(op.isPresent());

    TypedTimeSeriesIterator<NumericArrayType> iterator =
            (TypedTimeSeriesIterator<NumericArrayType>) op.get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> seriesValue = iterator.next();
    assertEquals(BASE_TIME + (60 * 30), seriesValue.timestamp().epoch());
    assertEquals(0, seriesValue.value().offset());
    assertEquals(60, seriesValue.value().end());
    double[] results = seriesValue.value().doubleArray();
    double val = 30;
    for (int i = 0; i < 60; i++) {
      if (i >= 30) {
        assertTrue(Double.isNaN(results[i]));
      } else {
        assertEquals(val++, results[i], 0.001);
      }
    }

    TimeSeriesStringId id = (TimeSeriesStringId) prediction.id();
    assertEquals("sys.cpu.user.prediction", id.metric());
  }

  TimeSeries mockSeries(int timestamp, int startValue) {
    NumericArrayTimeSeries timeseries = new NumericArrayTimeSeries(id,
            new SecondTimeStamp(timestamp));
    for (int i = 0; i < 60; i++) {
      timeseries.add(startValue++);
    }
    return timeseries;
  }

  NumericArrayAggregator newAgg() {
    return spy(new ArrayMaxFactory().newAggregator(
            DefaultArrayAggregatorConfig.newBuilder().setArraySize(60).build()));
  }

  void setupNode(int resultStart) {
    when(node.newAggregator()).thenReturn(newAgg());
    when(node.modelId()).thenReturn("UTModel");

    TimeSpecification spec = mock(TimeSpecification.class);
    when(spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(spec.start()).thenReturn(new SecondTimeStamp(resultStart));
    when(spec.end()).thenReturn(new SecondTimeStamp(resultStart + 3600));
    when(result.timeSpecification()).thenReturn(spec);
  }
}
