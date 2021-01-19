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
package net.opentsdb.query.anomaly;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.MockNumericTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestAnomalyThresholdEvaluator {
  private static MockConfig CONFIG;
  private static NumericInterpolatorConfig INTERPOLATOR;
  private static final int BASE_TIME = 1356998400;
  private static final TimeSeriesId ID = BaseTimeSeriesStringId.newBuilder()
      .setMetric("a")
      .build();
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    INTERPOLATOR = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
    CONFIG = MockConfig.newBuilder()
        .setSerializeObserved(true)
        .setSerializeThresholds(true)
        .setSerializeDeltas(true)
        .setLowerThresholdBad(25)
        .setUpperThresholdBad(25)
        .setMode(ExecutionMode.EVALUATE)
        .addInterpolatorConfig(INTERPOLATOR)
        .addSource("ds")
        .setId("egads")
        .build();
  }
  
  @Test
  public void ctor() throws Exception {
    MockConfig config = MockConfig.newBuilder()
        .setSerializeObserved(true)
        .setSerializeThresholds(true)
        .setSerializeDeltas(true)
        .setLowerThresholdBad(25)
        .setUpperThresholdBad(25)
        .setMode(ExecutionMode.EVALUATE)
        .addInterpolatorConfig(INTERPOLATOR)
        .addSource("ds")
        .setId("egads")
        .build();
    
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(config, 42, 
        mock(TimeSeries.class),
        mock(QueryResult.class),
        new TimeSeries[0],
        new QueryResult[0]);
    assertSame(config, eval.config);
    assertEquals(42, eval.upper_bad_thresholds.length);
    assertNull(eval.upper_warn_thresholds);
    assertEquals(42, eval.lower_bad_thresholds.length);
    assertNull(eval.lower_warn_thresholds);
    assertEquals(42, eval.deltas.length);
    assertNull(eval.alerts());
    
    config = MockConfig.newBuilder()
        .setLowerThresholdBad(25)
        .setUpperThresholdBad(25)
        .setMode(ExecutionMode.EVALUATE)
        .addInterpolatorConfig(INTERPOLATOR)
        .addSource("ds")
        .setId("egads")
        .build();
    
    eval = new AnomalyThresholdEvaluator(config, 42, 
        mock(TimeSeries.class),
        mock(QueryResult.class),
        new TimeSeries[0],
        new QueryResult[0]);
    assertSame(config, eval.config);
    assertNull(eval.upper_bad_thresholds);
    assertNull(eval.upper_warn_thresholds);
    assertNull(eval.lower_bad_thresholds);
    assertNull(eval.lower_warn_thresholds);
    assertNull(eval.deltas);
    assertNull(eval.alerts());
  }

  @Test
  public void numericOneAligned() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 60, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 240, 25);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    QueryResult result = mockResult(BASE_TIME, BASE_TIME + 300);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 5, 
        source,
        result,
        new TimeSeries[] { prediction },
        new QueryResult[] { result });
    eval.evaluate();
    assertArrayEquals(new double[] { 31.25, 62.5, 93.75, 62.5, 31.25 }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { 18.75, 37.5, 56.25, 37.5, 18.75 }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[5], eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(63);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    ((NumericArrayTimeSeries) source).add(25);
    
    eval = new AnomalyThresholdEvaluator(CONFIG, 5, 
        source,
        result,
        new TimeSeries[] { prediction },
        new QueryResult[] { result });
    eval.evaluate();
    assertArrayEquals(new double[] { 31.25, 62.5, 93.75, 62.5, 31.25 }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { 18.75, 37.5, 56.25, 37.5, 18.75 }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { 0, 13, 0, 49, 0 }, eval.deltas, 0.001);
    assertEquals(2, eval.alerts().size());
  }
  
  @Test
  public void numericOneUnAligned() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 300);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 5, 
        source,
        result,
        new TimeSeries[] { prediction },
        new QueryResult[] { prediction_result });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    
    eval = new AnomalyThresholdEvaluator(CONFIG, 5, 
        source,
        result,
        new TimeSeries[] { prediction },
        new QueryResult[] { prediction_result });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 49, Double.NaN }, 
        eval.deltas, 0.001);
    assertEquals(1, eval.alerts().size());
  }
  
  @Test
  public void numericTwoUnAligned() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 240, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 300, 1);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 360, 25);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction2 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 300));
    ((NumericArrayTimeSeries) prediction2).add(1);
    ((NumericArrayTimeSeries) prediction2).add(25);
    ((NumericArrayTimeSeries) prediction2).add(50);
    ((NumericArrayTimeSeries) prediction2).add(75);
    ((NumericArrayTimeSeries) prediction2).add(50);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 480);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    QueryResult prediction_result2 = mockResult(BASE_TIME + 300, BASE_TIME + 600);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 10, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2 },
        new QueryResult[] { prediction_result, prediction_result2 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, Double.NaN, Double.NaN, Double.NaN}, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(83);
    
    eval = new AnomalyThresholdEvaluator(CONFIG, 10, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2 },
        new QueryResult[] { prediction_result, prediction_result2 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, Double.NaN, Double.NaN, Double.NaN}, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 49, 0, 
        0, 58, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertEquals(2, eval.alerts().size());
  }
  
  @Test
  public void numericThreeUnAligned() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 240, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 300, 1);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 360, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 420, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 480, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 540, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 600, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 660, 1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction2 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 300));
    ((NumericArrayTimeSeries) prediction2).add(1);
    ((NumericArrayTimeSeries) prediction2).add(25);
    ((NumericArrayTimeSeries) prediction2).add(50);
    ((NumericArrayTimeSeries) prediction2).add(75);
    ((NumericArrayTimeSeries) prediction2).add(50);
    
    TimeSeries prediction3 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 600));
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(1);
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(50);
    ((NumericArrayTimeSeries) prediction3).add(75);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    QueryResult prediction_result2 = mockResult(BASE_TIME + 300, BASE_TIME + 600);
    QueryResult prediction_result3 = mockResult(BASE_TIME + 600, BASE_TIME + 900);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2, prediction3 },
        new QueryResult[] { prediction_result, prediction_result2, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, 62.5, 93.75, 62.5, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, 37.5, 56.25, 37.5,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        0, 0, 0, 0, 0, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(0);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(108);
    ((NumericArrayTimeSeries) source).add(1);
    
    eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2, prediction3 },
        new QueryResult[] { prediction_result, prediction_result2, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, 62.5, 93.75, 62.5, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, 37.5, 56.25, 37.5,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 49, 0, 
        0, 0, -50, 0, 0, 
        83, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertEquals(3, eval.alerts().size());
  }
  
  @Test
  public void numericThreeUnAlignedFirstNull() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 240, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 300, 1);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 360, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 420, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 480, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 540, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 600, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 660, 1);
    
    TimeSeries prediction2 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 300));
    ((NumericArrayTimeSeries) prediction2).add(1);
    ((NumericArrayTimeSeries) prediction2).add(25);
    ((NumericArrayTimeSeries) prediction2).add(50);
    ((NumericArrayTimeSeries) prediction2).add(75);
    ((NumericArrayTimeSeries) prediction2).add(50);
    
    TimeSeries prediction3 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 600));
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(1);
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(50);
    ((NumericArrayTimeSeries) prediction3).add(75);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result2 = mockResult(BASE_TIME + 300, BASE_TIME + 600);
    QueryResult prediction_result3 = mockResult(BASE_TIME + 600, BASE_TIME + 900);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { null, prediction2, prediction3 },
        new QueryResult[] { null, prediction_result2, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        1.25, 31.25, 62.5, 93.75, 62.5, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        .75, 18.75, 37.5, 56.25, 37.5,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        0, 0, 0, 0, 0, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
  }
  
  @Test
  public void numericThreeUnAlignedFirstTwoNull() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 240, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 300, 1);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 360, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 420, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 480, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 540, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 600, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 660, 1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction3 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 600));
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(1);
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(50);
    ((NumericArrayTimeSeries) prediction3).add(75);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result3 = mockResult(BASE_TIME + 600, BASE_TIME + 900);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { null, null, prediction3 },
        new QueryResult[] { null, null, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(0);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(108);
    ((NumericArrayTimeSeries) source).add(1);
  }
  
  @Test
  public void numericThreeUnAlignedMiddleNull() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 240, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 300, 1);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 360, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 420, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 480, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 540, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 600, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 660, 1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction3 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 600));
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(1);
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(50);
    ((NumericArrayTimeSeries) prediction3).add(75);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    QueryResult prediction_result3 = mockResult(BASE_TIME + 600, BASE_TIME + 900);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, null, prediction3 },
        new QueryResult[] { prediction_result, null, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
  }
  
  @Test
  public void numericThreeUnAlignedMiddleAndEndNull() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 240, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 300, 1);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 360, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 420, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 480, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 540, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 600, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 660, 1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, null, null },
        new QueryResult[] { prediction_result, null, null });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
  }
  
  @Test
  public void numericThreeUnAlignedEndNull() throws Exception {
    TimeSeries source = new MockNumericTimeSeries(ID);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 120, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 180, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 240, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 300, 1);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 360, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 420, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 480, 75);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 540, 50);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 600, 25);
    ((MockNumericTimeSeries) source).add(BASE_TIME + 660, 1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction2 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 300));
    ((NumericArrayTimeSeries) prediction2).add(1);
    ((NumericArrayTimeSeries) prediction2).add(25);
    ((NumericArrayTimeSeries) prediction2).add(50);
    ((NumericArrayTimeSeries) prediction2).add(75);
    ((NumericArrayTimeSeries) prediction2).add(50);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    QueryResult prediction_result2 = mockResult(BASE_TIME + 300, BASE_TIME + 600);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2, null },
        new QueryResult[] { prediction_result, prediction_result2, null });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, 62.5, 93.75, 62.5, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, 37.5, 56.25, 37.5,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        0, 0, 0, 0, 0, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
  }
  
  @Test
  public void arrayOneAligned() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    QueryResult result = mockResult(BASE_TIME, BASE_TIME + 300);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 5, 
        source,
        result,
        new TimeSeries[] { prediction },
        new QueryResult[] { result });
    eval.evaluate();
    assertArrayEquals(new double[] { 31.25, 62.5, 93.75, 62.5, 31.25 }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { 18.75, 37.5, 56.25, 37.5, 18.75 }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[5], eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(63);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    ((NumericArrayTimeSeries) source).add(25);
    
    eval = new AnomalyThresholdEvaluator(CONFIG, 5, 
        source,
        result,
        new TimeSeries[] { prediction },
        new QueryResult[] { result });
    eval.evaluate();
    assertArrayEquals(new double[] { 31.25, 62.5, 93.75, 62.5, 31.25 }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { 18.75, 37.5, 56.25, 37.5, 18.75 }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { 0, 13, 0, 49, 0 }, eval.deltas, 0.001);
    assertEquals(2, eval.alerts().size());
  }
  
  @Test
  public void arrayOneUnAligned() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 300);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 5, 
        source,
        result,
        new TimeSeries[] { prediction },
        new QueryResult[] { prediction_result });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    
    eval = new AnomalyThresholdEvaluator(CONFIG, 5, 
        source,
        result,
        new TimeSeries[] { prediction },
        new QueryResult[] { prediction_result });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 49, Double.NaN }, 
        eval.deltas, 0.001);
    assertEquals(1, eval.alerts().size());
  }
  
  @Test
  public void arrayTwoUnAligned() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction2 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 300));
    ((NumericArrayTimeSeries) prediction2).add(1);
    ((NumericArrayTimeSeries) prediction2).add(25);
    ((NumericArrayTimeSeries) prediction2).add(50);
    ((NumericArrayTimeSeries) prediction2).add(75);
    ((NumericArrayTimeSeries) prediction2).add(50);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 480);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    QueryResult prediction_result2 = mockResult(BASE_TIME + 300, BASE_TIME + 600);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 10, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2 },
        new QueryResult[] { prediction_result, prediction_result2 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, Double.NaN, Double.NaN, Double.NaN}, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(83);
    
    eval = new AnomalyThresholdEvaluator(CONFIG, 10, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2 },
        new QueryResult[] { prediction_result, prediction_result2 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, Double.NaN, Double.NaN, Double.NaN}, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 49, 0, 
        0, 58, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertEquals(2, eval.alerts().size());
  }
  
  @Test
  public void arrayThreeUnAligned() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction2 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 300));
    ((NumericArrayTimeSeries) prediction2).add(1);
    ((NumericArrayTimeSeries) prediction2).add(25);
    ((NumericArrayTimeSeries) prediction2).add(50);
    ((NumericArrayTimeSeries) prediction2).add(75);
    ((NumericArrayTimeSeries) prediction2).add(50);
    
    TimeSeries prediction3 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 600));
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(1);
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(50);
    ((NumericArrayTimeSeries) prediction3).add(75);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    QueryResult prediction_result2 = mockResult(BASE_TIME + 300, BASE_TIME + 600);
    QueryResult prediction_result3 = mockResult(BASE_TIME + 600, BASE_TIME + 900);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2, prediction3 },
        new QueryResult[] { prediction_result, prediction_result2, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, 62.5, 93.75, 62.5, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, 37.5, 56.25, 37.5,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        0, 0, 0, 0, 0, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(0);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(108);
    ((NumericArrayTimeSeries) source).add(1);
    
    eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2, prediction3 },
        new QueryResult[] { prediction_result, prediction_result2, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, 62.5, 93.75, 62.5, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, 37.5, 56.25, 37.5,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 49, 0, 
        0, 0, -50, 0, 0, 
        83, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertEquals(3, eval.alerts().size());
  }
  
  @Test
  public void arrayThreeUnAlignedFirstNull() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    
    TimeSeries prediction2 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 300));
    ((NumericArrayTimeSeries) prediction2).add(1);
    ((NumericArrayTimeSeries) prediction2).add(25);
    ((NumericArrayTimeSeries) prediction2).add(50);
    ((NumericArrayTimeSeries) prediction2).add(75);
    ((NumericArrayTimeSeries) prediction2).add(50);
    
    TimeSeries prediction3 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 600));
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(1);
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(50);
    ((NumericArrayTimeSeries) prediction3).add(75);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result2 = mockResult(BASE_TIME + 300, BASE_TIME + 600);
    QueryResult prediction_result3 = mockResult(BASE_TIME + 600, BASE_TIME + 900);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { null, prediction2, prediction3 },
        new QueryResult[] { null, prediction_result2, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        1.25, 31.25, 62.5, 93.75, 62.5, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        .75, 18.75, 37.5, 56.25, 37.5,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        0, 0, 0, 0, 0, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
  }
  
  @Test
  public void arrayThreeUnAlignedFirstTwoNull() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction3 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 600));
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(1);
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(50);
    ((NumericArrayTimeSeries) prediction3).add(75);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result3 = mockResult(BASE_TIME + 600, BASE_TIME + 900);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { null, null, prediction3 },
        new QueryResult[] { null, null, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
    
    source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(99);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(0);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(108);
    ((NumericArrayTimeSeries) source).add(1);
  }
  
  @Test
  public void arrayThreeUnAlignedMiddleNull() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction3 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 600));
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(1);
    ((NumericArrayTimeSeries) prediction3).add(25);
    ((NumericArrayTimeSeries) prediction3).add(50);
    ((NumericArrayTimeSeries) prediction3).add(75);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    QueryResult prediction_result3 = mockResult(BASE_TIME + 600, BASE_TIME + 900);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, null, prediction3 },
        new QueryResult[] { prediction_result, null, prediction_result3 });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        31.25, 1.25, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        18.75, .75, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        0, 0, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
  }
  
  @Test
  public void arrayThreeUnAlignedMiddleAndEndNull() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, null, null },
        new QueryResult[] { prediction_result, null, null });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
  }
  
  @Test
  public void arrayThreeUnAlignedEndNull() throws Exception {
    TimeSeries source = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 120));
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(75);
    ((NumericArrayTimeSeries) source).add(50);
    ((NumericArrayTimeSeries) source).add(25);
    ((NumericArrayTimeSeries) source).add(1);
    
    TimeSeries prediction = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME));
    ((NumericArrayTimeSeries) prediction).add(25);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(75);
    ((NumericArrayTimeSeries) prediction).add(50);
    ((NumericArrayTimeSeries) prediction).add(25);
    
    TimeSeries prediction2 = new NumericArrayTimeSeries(ID, 
        new SecondTimeStamp(BASE_TIME + 300));
    ((NumericArrayTimeSeries) prediction2).add(1);
    ((NumericArrayTimeSeries) prediction2).add(25);
    ((NumericArrayTimeSeries) prediction2).add(50);
    ((NumericArrayTimeSeries) prediction2).add(75);
    ((NumericArrayTimeSeries) prediction2).add(50);
    
    QueryResult result = mockResult(BASE_TIME + 120, BASE_TIME + 720);
    QueryResult prediction_result = mockResult(BASE_TIME, BASE_TIME + 300);
    QueryResult prediction_result2 = mockResult(BASE_TIME + 300, BASE_TIME + 600);
    AnomalyThresholdEvaluator eval = new AnomalyThresholdEvaluator(CONFIG, 15, 
        source,
        result,
        new TimeSeries[] { prediction, prediction2, null },
        new QueryResult[] { prediction_result, prediction_result2, null });
    eval.evaluate();
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 93.75, 62.5, 31.25, 
        1.25, 31.25, 62.5, 93.75, 62.5, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.upper_bad_thresholds, 0.001);
    assertNull(eval.upper_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 56.25, 37.5, 18.75,
        .75, 18.75, 37.5, 56.25, 37.5,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.lower_bad_thresholds, 0.001);
    assertNull(eval.lower_warn_thresholds);
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, 0, 0, 0, 
        0, 0, 0, 0, 0, 
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN }, 
        eval.deltas, 0.001);
    assertNull(eval.alerts());
  }
  
  // TODO - summary tests
  // TODO - eval specific tests
 
  static class MockConfig extends BaseAnomalyConfig {

    protected MockConfig(Builder builder) {
      super(builder);
    }

    @Override
    public boolean pushDown() {
      return false;
    }

    @Override
    public boolean joins() {
      return false;
    }

    @Override
    public Builder toBuilder() {
      return null;
    }

    @Override
    public int compareTo(Object o) {
      return 0;
    }
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    public static class Builder extends BaseAnomalyConfig.Builder<Builder, MockConfig> {

      @Override
      public MockConfig build() {
        return new MockConfig(this);
      }

      @Override
      public Builder self() {
        return this;
      }
      
    }
  }
  
  QueryResult mockResult(final int start, final int end) {
    TimeSpecification spec = mock(TimeSpecification.class);
    when(spec.start()).thenReturn(new SecondTimeStamp(start));
    when(spec.end()).thenReturn(new SecondTimeStamp(end));
    when(spec.interval()).thenReturn(Duration.ofMinutes(1));
    QueryResult result = mock(QueryResult.class);
    when(result.timeSpecification()).thenReturn(spec);
    return result;
  }
}
