// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly.egads.olympicscoring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Properties;

import net.opentsdb.data.types.numeric.aggregators.ArrayMaxFactory;
import net.opentsdb.data.types.numeric.aggregators.ArrayMaxFactory.ArrayMax;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.query.QueryResult;

public class TestOlympicScoringBaseline {
  private static final long BASE_TIME = 1356998400L;
  private static final TimeSeriesId ID = BaseTimeSeriesStringId.newBuilder()
      .setMetric("a")
      .build();
  
  private OlympicScoringNode node;
  private QueryResult result;
  
  @Before
  public void before() throws Exception {
    node = mock(OlympicScoringNode.class);
    result = mock(QueryResult.class);
    ArrayMaxFactory factory = new ArrayMaxFactory();
    when(node.newAggregator()).thenReturn(factory.newAggregator(
            DefaultArrayAggregatorConfig.newBuilder()
                    .setArraySize(60)
                    .build()
    ));
    
    when(node.getAggregatorFactory()).thenReturn(factory);
  }
  
  @Test
  public void appendNumericType() throws Exception {
    TimeSeries source = new NumericMillisecondShard(ID, 
      new MillisecondTimeStamp(BASE_TIME * 1000), 
      new MillisecondTimeStamp(BASE_TIME * 1000 + (3600 * 1000)));
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000), 0);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 60_000, 50);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 120_000, 75);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 180_000, 50);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 240_000, 25);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 300_000, 0);
    
    OlympicScoringBaseline baseline = new OlympicScoringBaseline(node, ID);
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + 300, baseline.baseline.lastTime());
    assertEquals(6, baseline.baseline.size());
    
    // test overlap
    source = new NumericMillisecondShard(ID, 
      new MillisecondTimeStamp(BASE_TIME * 1000), 
      new MillisecondTimeStamp(BASE_TIME * 1000 + (3600 * 1000)));
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 240_000, 25);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 300_000, 0);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 360_000, 25);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 420_000, 50);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 480_000, 75);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 540_000, 50);
    
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + 540, baseline.baseline.lastTime());
    assertEquals(10, baseline.baseline.size());
    
    // empty
    source = new NumericMillisecondShard(ID, 
      new MillisecondTimeStamp(BASE_TIME * 1000), 
      new MillisecondTimeStamp(BASE_TIME * 1000 + (3600 * 1000)));
    
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + 540, baseline.baseline.lastTime());
    assertEquals(10, baseline.baseline.size());
  }
  
  @Test
  public void appendNumericArrayType() throws Exception {
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + 300));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.timeSpecification()).thenReturn(time_spec);
    
    TimeSeries source = new NumericArrayTimeSeries(ID, time_spec.start());
    ((NumericArrayTimeSeries) source).add(0);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(0);
    
    OlympicScoringBaseline baseline = new OlympicScoringBaseline(node, ID);
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + 300, baseline.baseline.lastTime());
    assertEquals(6, baseline.baseline.size());
    
    // test overlap
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 240));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + 540));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.timeSpecification()).thenReturn(time_spec);
    source = new NumericArrayTimeSeries(ID, time_spec.start());
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(0);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + 540, baseline.baseline.lastTime());
    assertEquals(10, baseline.baseline.size());
    
    // empty
    source = new NumericArrayTimeSeries(ID, time_spec.start());
    
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + 540, baseline.baseline.lastTime());
    assertEquals(10, baseline.baseline.size());
  }
  
  @Test
  public void appendNumericSummaryType() throws Exception {
    MockTimeSeries source = new MockTimeSeries((TimeSeriesStringId) ID);
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME));
    v.resetValue(0, 0);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + 3600));
    v.resetValue(0, 25);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 2)));
    v.resetValue(0, 50);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    v.resetValue(0, 75);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 4)));
    v.resetValue(0, 50);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 5)));
    v.resetValue(0, 25);
    source.addValue(v);
    
    OlympicScoringBaseline baseline = new OlympicScoringBaseline(node, ID);
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + (3600 * 5), baseline.baseline.lastTime());
    assertEquals(6, baseline.baseline.size());
    
    // test overlap
    source = new MockTimeSeries((TimeSeriesStringId) ID);
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 4)));
    v.resetValue(0, 50);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 5)));
    v.resetValue(0, 25);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 6)));
    v.resetValue(0, 0);
    source.addValue(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new SecondTimeStamp(BASE_TIME + (3600 * 7)));
    v.resetValue(0, 25);
    source.addValue(v);
    
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + (3600 * 7), baseline.baseline.lastTime());
    assertEquals(8, baseline.baseline.size());
    
    // empty
    source = new MockTimeSeries((TimeSeriesStringId) ID);
    
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + (3600 * 7), baseline.baseline.lastTime());
    assertEquals(8, baseline.baseline.size());
  }

  @Test
  public void appendMixed() throws Exception {
    TimeSeries source = new NumericMillisecondShard(ID, 
      new MillisecondTimeStamp(BASE_TIME * 1000), 
      new MillisecondTimeStamp(BASE_TIME * 1000 + (3600 * 1000)));
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000), 0);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 60_000, 50);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 120_000, 75);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 180_000, 50);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 240_000, 25);
    ((NumericMillisecondShard) source).add((BASE_TIME * 1000) + 300_000, 0);
    
    OlympicScoringBaseline baseline = new OlympicScoringBaseline(node, ID);
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + 300, baseline.baseline.lastTime());
    assertEquals(6, baseline.baseline.size());
    
    // test overlap
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME + 240));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + 540));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.timeSpecification()).thenReturn(time_spec);
    source = new NumericArrayTimeSeries(ID, time_spec.start());
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(0);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    
    baseline.append(source, result);
    
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + 540, baseline.baseline.lastTime());
    assertEquals(10, baseline.baseline.size());
  }
  
  @Test
  public void predict() throws Exception {
    when(node.predictionIntervals()).thenReturn(60L);
    when(node.predictionInterval()).thenReturn(60L);
    
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.timeSpecification()).thenReturn(time_spec);
    
    TimeSeries source = new NumericArrayTimeSeries(ID, time_spec.start());
    long ts = BASE_TIME;
    for (int x = 0; x < 3; x++) {
      for (int i = 0; i < 60; i++) {
        double value = Math.sin((ts % 3600) / 100) + x;
        ((NumericArrayTimeSeries) source).add(value);
        ts += 60;
      }
    }
    
    OlympicScoringBaseline baseline = new OlympicScoringBaseline(node, ID);
    baseline.append(source, result);
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + (3600 * 3) - 60, baseline.baseline.lastTime());
    assertEquals(180, baseline.baseline.size());
    
    Properties properties = new Properties();
    properties.setProperty("TS_MODEL", "OlympicModel2");
    properties.setProperty("INTERVAL", "1");
    properties.setProperty("INTERVAL_UNITS", "MINUTES");
    properties.setProperty("WINDOW_SIZE", "1");
    properties.setProperty("WINDOW_SIZE_UNITS", "HOURS");
    properties.setProperty("WINDOW_DISTANCE", "1");
    properties.setProperty("WINDOW_DISTANCE_UNITS", "HOURS");
    properties.setProperty("HISTORICAL_WINDOWS", "3");
    properties.setProperty("WINDOW_AGGREGATOR", "AVG");
    properties.setProperty("MODEL_START", Long.toString(BASE_TIME + (3600 * 3)));
    properties.setProperty("ENABLE_WEIGHTING", "TRUE");
    properties.setProperty("AGGREGATOR", "AVG");
    properties.setProperty("NUM_TO_DROP_LOWEST", "0");
    properties.setProperty("NUM_TO_DROP_HIGHEST","0");
    properties.setProperty("PERIOD", "3600");
    
    TimeSeries result = baseline.predict(properties, BASE_TIME + (3600 * 3));
    
    TypedTimeSeriesIterator iterator = result.iterator(NumericArrayType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(60, v.value().end());
    ts = BASE_TIME + (3600 * 3);
    for (int i = v.value().offset(); i < v.value().end(); i++) {
      assertTrue(Double.isFinite(v.value().doubleArray()[i]));
    }
    
    TimeSeriesStringId id = (TimeSeriesStringId) result.id();
    assertEquals(((TimeSeriesStringId) ID).metric(), id.metric());
    assertEquals(0, id.tags().size());
  }
  
  @Test
  public void predictWithRightIndex() throws Exception {
    when(node.predictionIntervals()).thenReturn(60L);
    when(node.predictionInterval()).thenReturn(60L);
    
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.timeSpecification()).thenReturn(time_spec);
    
    TimeSeries source = new NumericArrayTimeSeries(ID, time_spec.start());
    long ts = BASE_TIME;
    for (int x = 0; x < 3; x++) {
      for (int i = 0; i < 60; i++) {
        double value = Math.sin((ts % 3600) / 100) + x;
        ((NumericArrayTimeSeries) source).add(value);
        ts += 60;
      }
    }
    
    OlympicScoringBaseline baseline = new OlympicScoringBaseline(node, ID);
    baseline.append(source, result);
    assertEquals(BASE_TIME, baseline.baseline.startTime());
    assertEquals(BASE_TIME + (3600 * 3) - 60, baseline.baseline.lastTime());
    assertEquals(180, baseline.baseline.size());
    
    Properties properties = new Properties();
    properties.setProperty("TS_MODEL", "OlympicModel2");
    properties.setProperty("INTERVAL", "1");
    properties.setProperty("INTERVAL_UNITS", "MINUTES");
    properties.setProperty("WINDOW_SIZE", "1");
    properties.setProperty("WINDOW_SIZE_UNITS", "HOURS");
    properties.setProperty("WINDOW_DISTANCE", "1");
    properties.setProperty("WINDOW_DISTANCE_UNITS", "HOURS");
    properties.setProperty("HISTORICAL_WINDOWS", "3");
    properties.setProperty("WINDOW_AGGREGATOR", "AVG");
    properties.setProperty("MODEL_START", Long.toString(BASE_TIME + (3600 * 3)));
    properties.setProperty("ENABLE_WEIGHTING", "TRUE");
    properties.setProperty("AGGREGATOR", "AVG");
    properties.setProperty("NUM_TO_DROP_LOWEST", "0");
    properties.setProperty("NUM_TO_DROP_HIGHEST","0");
    properties.setProperty("PERIOD", "3600");
    
    TimeSeries result = baseline.predict(properties, BASE_TIME + (3600 * 3));
    
    TypedTimeSeriesIterator iterator = result.iterator(NumericArrayType.TYPE).get();
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertEquals(60, v.value().end());
    ts = BASE_TIME + (3600 * 3);
    for (int i = v.value().offset(); i < v.value().end(); i++) {
      assertTrue(Double.isFinite(v.value().doubleArray()[i]));
    }
    
    TimeSeriesStringId id = (TimeSeriesStringId) result.id();
    assertEquals(((TimeSeriesStringId) ID).metric(), id.metric());
    assertEquals(0, id.tags().size());
  }
  
  @Test
  public void predictNoBaseline() throws Exception {
    when(node.predictionIntervals()).thenReturn(60L);
    when(node.predictionInterval()).thenReturn(60L);
    
    TimeSpecification time_spec = mock(TimeSpecification.class);
    when(time_spec.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(time_spec.end()).thenReturn(new SecondTimeStamp(BASE_TIME + (3600 * 3)));
    when(time_spec.interval()).thenReturn(Duration.ofSeconds(60));
    when(result.timeSpecification()).thenReturn(time_spec);
    
    TimeSeries source = new NumericArrayTimeSeries(ID, time_spec.start());
    
    OlympicScoringBaseline baseline = new OlympicScoringBaseline(node, ID);
    baseline.append(source, result);
    assertEquals(0, baseline.baseline.size());
    
    Properties properties = new Properties();
    properties.setProperty("TS_MODEL", "OlympicModel2");
    properties.setProperty("INTERVAL", "1");
    properties.setProperty("INTERVAL_UNITS", "MINUTES");
    properties.setProperty("WINDOW_SIZE", "1");
    properties.setProperty("WINDOW_SIZE_UNITS", "HOURS");
    properties.setProperty("WINDOW_DISTANCE", "1");
    properties.setProperty("WINDOW_DISTANCE_UNITS", "HOURS");
    properties.setProperty("HISTORICAL_WINDOWS", "3");
    properties.setProperty("WINDOW_AGGREGATOR", "AVG");
    properties.setProperty("MODEL_START", Long.toString(BASE_TIME + (3600 * 3)));
    properties.setProperty("ENABLE_WEIGHTING", "TRUE");
    properties.setProperty("AGGREGATOR", "AVG");
    properties.setProperty("NUM_TO_DROP_LOWEST", "0");
    properties.setProperty("NUM_TO_DROP_HIGHEST","0");
    properties.setProperty("PERIOD", "3600");
    
    assertNull(baseline.predict(properties, BASE_TIME + (3600 * 3)));
  }
  
}
