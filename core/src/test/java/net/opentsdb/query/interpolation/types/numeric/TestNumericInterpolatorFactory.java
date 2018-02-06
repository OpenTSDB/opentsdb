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

import java.util.Optional;

import org.junit.Test;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory.Default;
import net.opentsdb.query.QueryIteratorInterpolator;
import net.opentsdb.query.pojo.FillPolicy;

public class TestNumericInterpolatorFactory {

  @Test
  public void defaultFactory() throws Exception {
    final TimeSeries source = mock(TimeSeries.class);
    when(source.iterator(NumericType.TYPE)).thenReturn(Optional.empty());
    Default factory = new Default();
    
    QueryIteratorInterpolator<NumericType> interpolator = 
        (QueryIteratorInterpolator<NumericType>)
          factory.newInterpolator(NumericType.TYPE, source, 
              NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NONE)
                .setRealFillPolicy(FillWithRealPolicy.NONE)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.NONE, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryIteratorInterpolator<NumericType>)
          factory.newInterpolator(NumericType.TYPE, source, 
              NumericInterpolatorConfig.newBuilder()
                .setFillPolicy(FillPolicy.NULL)
                .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                .build());
    assertNull(interpolator.fillPolicy().fill());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryIteratorInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            NumericInterpolatorConfig.newBuilder()
              .setFillPolicy(FillPolicy.NOT_A_NUMBER)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertTrue(Double.isNaN(interpolator.fillPolicy().fill().doubleValue()));
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryIteratorInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            NumericInterpolatorConfig.newBuilder()
              .setFillPolicy(FillPolicy.ZERO)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertEquals(0, interpolator.fillPolicy().fill().longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
    
    interpolator = (QueryIteratorInterpolator<NumericType>)
        factory.newInterpolator(NumericType.TYPE, source, 
            ScalarNumericInterpolatorConfig.newBuilder()
              .setValue(42)
              .setFillPolicy(FillPolicy.ZERO)
              .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
              .build());
    assertEquals(42, interpolator.fillPolicy().fill().longValue());
    assertEquals(FillWithRealPolicy.PREFER_NEXT, 
        interpolator.fillPolicy().realPolicy());
  }
  
  @Test
  public void parse() throws Exception {
    NumericInterpolatorConfig config = NumericInterpolatorFactory.parse("1m-sum");
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
