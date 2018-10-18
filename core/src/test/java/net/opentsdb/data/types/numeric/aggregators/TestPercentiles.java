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
package net.opentsdb.data.types.numeric.aggregators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import org.junit.Assert;
import org.junit.Test;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;

public class TestPercentiles {
  
  @Test
  public void factory() throws Exception {
    NumericAggregatorFactory factory = new PercentilesFactories.P999Factory();
    assertNull(factory.initialize(mock(TSDB.class), null).join());
    assertEquals(PercentilesFactories.P999Factory.TYPE, factory.id());
    assertNull(factory.shutdown().join());
    
    factory = new PercentilesFactories.EP50R3Factory();
    assertNull(factory.initialize(mock(TSDB.class), null).join());
    assertEquals(PercentilesFactories.EP50R3Factory.TYPE, factory.id());
    assertNull(factory.shutdown().join());
  }
  
  @Test
  public void run() throws Exception {
    final long[] longValues = new long[1000];
    for (int i = 0; i < longValues.length; i++) {
      longValues[i] = i+1;
    }
  
    Numbers values = new Numbers(longValues);
    assertAggregatorEquals(500, 
        new PercentilesFactories.P50Factory().newAggregator(false), values);
    assertAggregatorEquals(750, 
        new PercentilesFactories.P75Factory().newAggregator(false), values);
    assertAggregatorEquals(900, 
        new PercentilesFactories.P90Factory().newAggregator(false), values);
    assertAggregatorEquals(950, 
        new PercentilesFactories.P95Factory().newAggregator(false), values);
    assertAggregatorEquals(990, 
        new PercentilesFactories.P99Factory().newAggregator(false), values);
    assertAggregatorEquals(999, 
        new PercentilesFactories.P999Factory().newAggregator(false), values);
  
    assertAggregatorEquals(500, 
        new PercentilesFactories.EP50R3Factory().newAggregator(false), values);
    assertAggregatorEquals(750, 
        new PercentilesFactories.EP75R3Factory().newAggregator(false), values);
    assertAggregatorEquals(900, 
        new PercentilesFactories.EP90R3Factory().newAggregator(false), values);
    assertAggregatorEquals(950, 
        new PercentilesFactories.EP95R3Factory().newAggregator(false), values);
    assertAggregatorEquals(990, 
        new PercentilesFactories.EP99R3Factory().newAggregator(false), values);
    assertAggregatorEquals(999, 
        new PercentilesFactories.EP999R3Factory().newAggregator(false), values);
    
    assertAggregatorEquals(500, 
        new PercentilesFactories.EP50R7Factory().newAggregator(false), values);
    assertAggregatorEquals(750, 
        new PercentilesFactories.EP75R7Factory().newAggregator(false), values);
    assertAggregatorEquals(900, 
        new PercentilesFactories.EP90R7Factory().newAggregator(false), values);
    assertAggregatorEquals(950, 
        new PercentilesFactories.EP95R7Factory().newAggregator(false), values);
    assertAggregatorEquals(990, 
        new PercentilesFactories.EP99R7Factory().newAggregator(false), values);
    assertAggregatorEquals(999, 
        new PercentilesFactories.EP999R7Factory().newAggregator(false), values);
  }
  
  private void assertAggregatorEquals(long value, NumericAggregator agg, Numbers numbers) {
    MutableNumericValue dp = new MutableNumericValue(new MillisecondTimeStamp(1000), 0);
    if (numbers.isInteger()) {
      agg.run(numbers.longs, 0, numbers.longs.length, dp);
    } else {
      agg.run(numbers.doubles, 0, numbers.doubles.length, false, dp);
    }
    Assert.assertEquals((double) value, dp.toDouble(), 1.0);
    numbers.reset();
  }
  
  /** Helper class to hold a bunch of numbers we can iterate on.  */
  private static final class Numbers {
    private final long[] longs;
    private final double[] doubles;
    private int i = 0;

    public Numbers(final long[] numbers) {
      longs = numbers;
      doubles = null;
    }
    
    public Numbers(final double[] numbers) {
      longs = null;
      doubles = numbers;
    }

    public boolean isInteger() {
      return longs != null ? true : false;
    }
    
    void reset() {
      i = 0;
    }
  }
}
