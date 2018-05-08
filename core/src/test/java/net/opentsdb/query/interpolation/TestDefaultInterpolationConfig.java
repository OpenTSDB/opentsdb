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
package net.opentsdb.query.interpolation;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryInterpolator;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolator;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolator;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.rollup.RollupConfig;

public class TestDefaultInterpolationConfig {
  private static final RollupConfig CONFIG = mock(RollupConfig.class);
  
  private NumericInterpolatorConfig numeric_config;
  private NumericSummaryInterpolatorConfig summary_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .build();
    
    summary_config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
        .addExpectedSummary(0)
        .setRollupConfig(CONFIG)
        .build();
  }
  
  @Test
  public void builder() throws Exception {
    // empty is allowed.
    DefaultInterpolationConfig config = 
        DefaultInterpolationConfig.newBuilder()
        .build();
    assertTrue(config.configs.isEmpty());
    
    config = DefaultInterpolationConfig.newBuilder()
        .add(NumericType.TYPE, numeric_config, 
            new NumericInterpolatorFactory.Default())
        .add(NumericSummaryType.TYPE, summary_config, 
            new NumericInterpolatorFactory.Default())
        .build();
    
    try {
      DefaultInterpolationConfig.newBuilder()
        .add(null, numeric_config, 
            new NumericInterpolatorFactory.Default());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultInterpolationConfig.newBuilder()
        .add(NumericType.TYPE, null, 
            new NumericInterpolatorFactory.Default());
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      DefaultInterpolationConfig.newBuilder()
        .add(NumericType.TYPE, numeric_config, 
            null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void emptyConfig() throws Exception {
    DefaultInterpolationConfig config = 
        DefaultInterpolationConfig.newBuilder()
        .build();
    assertNull(config.config(NumericType.TYPE));
    assertNull(config.config(NumericSummaryType.TYPE));
    assertNull(config.newInterpolator(NumericType.TYPE, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class)));
    assertNull(config.newInterpolator(NumericType.TYPE, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class),
        numeric_config));
    assertNull(config.newInterpolator(NumericType.TYPE, mock(TimeSeries.class)));
    assertNull(config.newInterpolator(NumericType.TYPE, 
        mock(TimeSeries.class),
        numeric_config));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void newInterpolatorsAndConfigs() throws Exception {
    DefaultInterpolationConfig config = 
        DefaultInterpolationConfig.newBuilder()
        .add(NumericType.TYPE, numeric_config, 
            new NumericInterpolatorFactory.Default())
        .add(NumericSummaryType.TYPE, summary_config, 
            new NumericInterpolatorFactory.Default())
        .build();
    assertSame(numeric_config, config.config(NumericType.TYPE));
    assertSame(summary_config, config.config(NumericSummaryType.TYPE));
    QueryInterpolator<?> interpolator = config.newInterpolator(NumericType.TYPE, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class));
    assertTrue(interpolator instanceof NumericInterpolator);
    interpolator = config.newInterpolator(NumericType.TYPE, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class),
        numeric_config);
    assertTrue(interpolator instanceof NumericInterpolator);
    
    interpolator = config.newInterpolator(NumericSummaryType.TYPE, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class));
    assertTrue(interpolator instanceof NumericSummaryInterpolator);
    interpolator = config.newInterpolator(NumericSummaryType.TYPE, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) mock(Iterator.class),
        summary_config);
    assertTrue(interpolator instanceof NumericSummaryInterpolator);
    
    TimeSeries time_series = mock(TimeSeries.class);
    when(time_series.iterator(any(TypeToken.class))).thenReturn(Optional.empty());
    
    interpolator = config.newInterpolator(NumericType.TYPE, time_series);
    assertTrue(interpolator instanceof NumericInterpolator);
    interpolator = config.newInterpolator(NumericType.TYPE, time_series,
        numeric_config);
    assertTrue(interpolator instanceof NumericInterpolator);
    
    interpolator = config.newInterpolator(NumericSummaryType.TYPE, time_series);
    assertTrue(interpolator instanceof NumericSummaryInterpolator);
    interpolator = config.newInterpolator(NumericSummaryType.TYPE, time_series,
        summary_config);
    assertTrue(interpolator instanceof NumericSummaryInterpolator);
  }
}