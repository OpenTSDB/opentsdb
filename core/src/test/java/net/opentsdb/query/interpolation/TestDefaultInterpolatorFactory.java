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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Optional;

import org.junit.Test;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolator;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolator;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;

public class TestDefaultInterpolatorFactory {

  @Test
  public void initialize() throws Exception {
    DefaultInterpolatorFactory factory = new DefaultInterpolatorFactory();
    assertTrue(factory.types.isEmpty());
    
    assertNull(factory.initialize(mock(TSDB.class)).join());
    assertEquals(2, factory.types.size());
    
    TimeSeries time_series = mock(TimeSeries.class);
    when(time_series.iterator(NumericType.TYPE)).thenReturn(Optional.empty());
    when(time_series.iterator(NumericSummaryType.TYPE)).thenReturn(Optional.empty());
    assertTrue(factory.newInterpolator(NumericType.TYPE, time_series, 
        mock(NumericInterpolatorConfig.class)) instanceof NumericInterpolator);
    assertTrue(factory.newInterpolator(NumericType.TYPE, mock(Iterator.class), 
        mock(NumericInterpolatorConfig.class)) instanceof NumericInterpolator);
    
    assertTrue(factory.newInterpolator(NumericSummaryType.TYPE, time_series, 
        mock(NumericSummaryInterpolatorConfig.class)) instanceof NumericSummaryInterpolator);
    assertTrue(factory.newInterpolator(NumericSummaryType.TYPE, mock(Iterator.class), 
        mock(NumericSummaryInterpolatorConfig.class)) instanceof NumericSummaryInterpolator);
    
    assertNull(factory.newInterpolator(AnnotationType.TYPE, time_series, 
        mock(NumericInterpolatorConfig.class)));
  }
}
