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
package net.opentsdb.data.types.numeric.aggregators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.exceptions.IllegalDataException;

public class TestMovingMedian {
  
  @Test
  public void factory() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);
    when(tsdb.getRegistry()).thenReturn(registry);
    NumericAggregatorFactory factory = new MovingMedianFactory();
    assertNull(factory.initialize(tsdb, null).join());
    assertEquals(MovingMedianFactory.TYPE, factory.id());
    assertNull(factory.shutdown().join());
  }
  
  @Test
  public void run() throws Exception {
    NumericAggregatorFactory factory = new MovingMedianFactory();
    NumericAggregator agg = factory.newAggregator(false);
    
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    agg.run(new long[] { 1, 2, 3 }, 0, 3, dp);
    assertEquals(2, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 0, 2, dp);
    assertEquals(1, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3 }, 0, 1, dp);
    assertEquals(1, dp.longValue());
    
    agg.run(new long[] { 1, 2, 3, 8 }, 0, 4, dp);
    assertEquals(2, dp.longValue());
    
    // sort
    agg.run(new long[] { 8, 2, -3, 1, -5 }, 0, 5, dp);
    assertEquals(1, dp.longValue());
    
    // subset
    agg.run(new long[] { 10, 2, 7, 1, -4 }, 1, 3, dp);
    assertEquals(2, dp.longValue());
    
    try {
      agg.run(new long[] { 1, 2, 3 }, 0, 0, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, 3, false, dp);
    assertEquals(2.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, 2, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, 1, false, dp);
    assertEquals(1.25, dp.doubleValue(), 0.001);
    
    try {
      agg.run(new double[] { 1.25, 2.25, 3.25 }, 0, 0, false, dp);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 0, 3, false, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { 1.25, Double.NaN, 3.25 }, 0, 3, true, dp);
    assertEquals(3.25, dp.doubleValue(), 0.001);
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 0, 3, false, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
    
    agg.run(new double[] { Double.NaN, Double.NaN, Double.NaN }, 0, 3, true, dp);
    assertTrue(Double.isNaN(dp.doubleValue()));
  }
  
}
