// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.interpolation.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Optional;

import org.junit.Test;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory.Default;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory.LERP;
import net.opentsdb.query.QueryInterpolator;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.rollup.RollupConfig;

public class TestNumericInterpolatorFactory {
  private static final RollupConfig CONFIG = mock(RollupConfig.class);
  
  @SuppressWarnings("unchecked")
  @Test
  public void defaultFactory() throws Exception {
    final TimeSeries source = mock(TimeSeries.class);
    when(source.iterator(NumericType.TYPE)).thenReturn(
        Optional.of(mock(Iterator.class)));
    Default factory = new Default();
    
    QueryInterpolator<NumericType> interpolator = 
        (QueryInterpolator<NumericType>)
          factory.newInterpolator(NumericType.TYPE, source, 
              NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NONE)
                .setRealFillPolicy(FillWithRealPolicy.NONE)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.NONE, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericType>)
          factory.newInterpolator(NumericType.TYPE, source, 
              NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NULL)
                .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            NumericInterpolatorConfig.newBuilder()
              .setFillPolicy(FillPolicy.NOT_A_NUMBER)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertTrue(Double.isNaN(interpolator.fillPolicy().fill().doubleValue()));
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            NumericInterpolatorConfig.newBuilder()
              .setFillPolicy(FillPolicy.ZERO)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertEquals(0, interpolator.fillPolicy().fill().longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            ScalarNumericInterpolatorConfig.newBuilder()
              .setValue(42)
              .setFillPolicy(FillPolicy.ZERO)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertEquals(42, interpolator.fillPolicy().fill().longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    // iterator creator
    interpolator = 
        (QueryInterpolator<NumericType>)
          factory.newInterpolator(NumericType.TYPE, 
              source.iterator(NumericType.TYPE).get(), 
              NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NONE)
                .setRealFillPolicy(FillWithRealPolicy.NONE)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.NONE, 
        interpolator.fillPolicy().realPolicy());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void defaultFactorySummary() throws Exception {
    final TimeSeries source = mock(TimeSeries.class);
    when(source.iterator(NumericSummaryType.TYPE)).thenReturn(
        Optional.of(mock(Iterator.class)));
    Default factory = new Default();
    
    QueryInterpolator<NumericSummaryType> interpolator = 
        (QueryInterpolator<NumericSummaryType>)
          factory.newInterpolator(NumericSummaryType.TYPE, source, 
              NumericSummaryInterpolatorConfig.newBuilder()
                .setDefaultFillPolicy(FillPolicy.NONE)
                .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
                .addExpectedSummary(0)
                .setRollupConfig(CONFIG)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.NONE, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericSummaryType>)
          factory.newInterpolator(NumericSummaryType.TYPE, source, 
              NumericSummaryInterpolatorConfig.newBuilder()
                .setDefaultFillPolicy(FillPolicy.NULL)
                .setDefaultRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                .addExpectedSummary(0)
                .setRollupConfig(CONFIG)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericSummaryType>)
        factory.newInterpolator(NumericSummaryType.TYPE, source, 
            NumericSummaryInterpolatorConfig.newBuilder()
              .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
              .setDefaultRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .addExpectedSummary(0)
              .setRollupConfig(CONFIG)
              .build());
    assertTrue(Double.isNaN(interpolator.fillPolicy().fill().value(0).doubleValue()));
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericSummaryType>)
        factory.newInterpolator(NumericSummaryType.TYPE, source, 
            NumericSummaryInterpolatorConfig.newBuilder()
              .setDefaultFillPolicy(FillPolicy.ZERO)
              .setDefaultRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .addExpectedSummary(0)
              .setRollupConfig(CONFIG)
              .build());
    assertEquals(0, interpolator.fillPolicy().fill().value(0).longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    // iterator creator
    interpolator = 
        (QueryInterpolator<NumericSummaryType>)
          factory.newInterpolator(NumericSummaryType.TYPE, 
              source.iterator(NumericSummaryType.TYPE).get(), 
              NumericSummaryInterpolatorConfig.newBuilder()
                .setDefaultFillPolicy(FillPolicy.NONE)
                .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
                .addExpectedSummary(0)
                .setRollupConfig(CONFIG)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.NONE, 
        interpolator.fillPolicy().realPolicy());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void lerpFactory() throws Exception {
    final TimeSeries source = mock(TimeSeries.class);
    when(source.iterator(NumericType.TYPE)).thenReturn(
        Optional.of(mock(Iterator.class)));
    LERP factory = new LERP();
    
    QueryInterpolator<NumericType> interpolator = 
        (QueryInterpolator<NumericType>)
          factory.newInterpolator(NumericType.TYPE, source, 
              NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NONE)
                .setRealFillPolicy(FillWithRealPolicy.NONE)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.NONE, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericType>)
          factory.newInterpolator(NumericType.TYPE, source, 
              NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NULL)
                .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            NumericInterpolatorConfig.newBuilder()
              .setFillPolicy(FillPolicy.NOT_A_NUMBER)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertTrue(Double.isNaN(interpolator.fillPolicy().fill().doubleValue()));
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            NumericInterpolatorConfig.newBuilder()
              .setFillPolicy(FillPolicy.ZERO)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertEquals(0, interpolator.fillPolicy().fill().longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            ScalarNumericInterpolatorConfig.newBuilder()
              .setValue(42)
              .setFillPolicy(FillPolicy.ZERO)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertEquals(42, interpolator.fillPolicy().fill().longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    // iterator creator
    interpolator = 
        (QueryInterpolator<NumericType>)
          factory.newInterpolator(NumericType.TYPE, 
              source.iterator(NumericType.TYPE).get(), 
              NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NONE)
                .setRealFillPolicy(FillWithRealPolicy.NONE)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.NONE, 
        interpolator.fillPolicy().realPolicy());
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void lerpFactorySummary() throws Exception {
    final TimeSeries source = mock(TimeSeries.class);
    when(source.iterator(NumericSummaryType.TYPE)).thenReturn(
        Optional.of(mock(Iterator.class)));
    LERP factory = new LERP();
    
    try {
      factory.newInterpolator(NumericSummaryType.TYPE, source, 
                NumericSummaryInterpolatorConfig.newBuilder()
                  .setDefaultFillPolicy(FillPolicy.NONE)
                  .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
                  .addExpectedSummary(0)
                  .setRollupConfig(CONFIG)
                  .build());
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
    
    try {
      factory.newInterpolator(NumericSummaryType.TYPE, 
          source.iterator(NumericSummaryType.TYPE).get(), 
                NumericSummaryInterpolatorConfig.newBuilder()
                  .setDefaultFillPolicy(FillPolicy.NONE)
                  .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
                  .addExpectedSummary(0)
                  .setRollupConfig(CONFIG)
                  .build());
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }
  
  @Test
  public void parse() throws Exception {
    NumericInterpolatorConfig config = NumericInterpolatorFactory.parse("1m-sum");
    assertEquals(FillPolicy.NONE, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("sum");
    assertEquals(FillPolicy.NONE, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-sum-nan");
    assertEquals(FillPolicy.NOT_A_NUMBER, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-sum-null");
    assertEquals(FillPolicy.NULL, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-sum-none");
    assertEquals(FillPolicy.NONE, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-sum-zero");
    assertEquals(FillPolicy.ZERO, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-zimsum");
    assertEquals(FillPolicy.ZERO, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-pfsum");
    assertEquals(FillPolicy.NONE, config.fillPolicy());
    assertEquals(FillWithRealPolicy.PREFER_PREVIOUS, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m");
    assertEquals(FillPolicy.NONE, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-mimmax");
    assertEquals(FillPolicy.MIN, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-max");
    assertEquals(FillPolicy.MIN, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-mimmin");
    assertEquals(FillPolicy.MAX, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    config = NumericInterpolatorFactory.parse("1m-min");
    assertEquals(FillPolicy.MAX, config.fillPolicy());
    assertEquals(FillWithRealPolicy.NONE, config.realFillPolicy());
    
    try {
      NumericInterpolatorFactory.parse(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericInterpolatorFactory.parse("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericInterpolatorFactory.parse("1m-sum-wtf");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
