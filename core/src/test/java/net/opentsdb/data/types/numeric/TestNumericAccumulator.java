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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.exceptions.IllegalDataException;

public class TestNumericAccumulator {

  @Test
  public void ctor() throws Exception {
    final NumericAccumulator acc = new NumericAccumulator();
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    assertEquals(0, acc.dp().timestamp().msEpoch());
  }
  
  @Test
  public void addAndRunLongs() throws Exception {
    NumericAccumulator acc = new NumericAccumulator();
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    try {
      acc.run(Aggregators.SUM, false);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    acc.add(42);
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(1, acc.valueIndex());
    assertEquals(42, acc.long_values[0]);
    acc.run(Aggregators.SUM, false);
    assertEquals(42, acc.dp().value().longValue());
    
    acc.add(24);
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(2, acc.valueIndex());
    assertEquals(42, acc.long_values[0]);
    assertEquals(24, acc.long_values[1]);
    acc.run(Aggregators.SUM, false);
    assertEquals(66, acc.dp().value().longValue());
    
    // now it doubles
    acc.add(1);
    assertEquals(4, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(3, acc.valueIndex());
    assertEquals(42, acc.long_values[0]);
    assertEquals(24, acc.long_values[1]);
    assertEquals(1, acc.long_values[2]);
    acc.run(Aggregators.SUM, false);
    assertEquals(67, acc.dp().value().longValue());
    
    acc.reset();
    assertEquals(4, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    // notice we don't clear the values, just reset the pointer.
    assertEquals(42, acc.long_values[0]);
    assertEquals(24, acc.long_values[1]);
    assertEquals(1, acc.long_values[2]);
    try {
      acc.run(Aggregators.SUM, false);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    for (int i = 0; i < 1025; i++) {
      acc.add(i);
    }
    assertEquals(2048, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(1025, acc.valueIndex());
    // notice we don't clear the values, just reset the pointer.
    assertEquals(0, acc.long_values[0]);
    assertEquals(1, acc.long_values[1]);
    assertEquals(2, acc.long_values[2]);
    acc.run(Aggregators.SUM, false);
    assertEquals(524800, acc.dp().value().longValue());
  }
  
  @Test
  public void addAndRunDouble() throws Exception {
    NumericAccumulator acc = new NumericAccumulator();
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    try {
      acc.run(Aggregators.SUM, false);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    acc.add(42.5);
    assertEquals(2, acc.long_values.length);
    assertEquals(2, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(1, acc.valueIndex());
    assertEquals(42.5, acc.double_values[0], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(42.5, acc.dp().value().doubleValue(), 0.001);
    
    acc.add(24.5);
    assertEquals(2, acc.long_values.length);
    assertEquals(2, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(2, acc.valueIndex());
    assertEquals(42.5, acc.double_values[0], 0.001);
    assertEquals(24.5, acc.double_values[1], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(67.0, acc.dp().value().doubleValue(), 0.001);
    
    // now it doubles
    acc.add(1.5);
    assertEquals(2, acc.long_values.length);
    assertEquals(4, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(3, acc.valueIndex());
    assertEquals(42.5, acc.double_values[0], 0.001);
    assertEquals(24.5, acc.double_values[1], 0.001);
    assertEquals(1.5, acc.double_values[2], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(68.5, acc.dp().value().doubleValue(), 0.001);
    
    acc.reset();
    assertEquals(2, acc.long_values.length);
    assertEquals(4, acc.double_values.length);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    // notice we don't clear the values, just reset the pointer.
    assertEquals(42.5, acc.double_values[0], 0.001);
    assertEquals(24.5, acc.double_values[1], 0.001);
    assertEquals(1.5, acc.double_values[2], 0.001);
    try {
      acc.run(Aggregators.SUM, false);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    for (double i = 0; i < 1025; i++) {
      acc.add(i);
    }
    assertEquals(2, acc.long_values.length);
    assertEquals(2048, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(1025, acc.valueIndex());
    // notice we don't clear the values, just reset the pointer.
    assertEquals(0, acc.double_values[0], 0.001);
    assertEquals(1, acc.double_values[1], 0.001);
    assertEquals(2, acc.double_values[2], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(524800, acc.dp().value().doubleValue(), 0.001);
  }
  
  @Test
  public void addAndRunDoubleWithNaNs() throws Exception {
    NumericAccumulator acc = new NumericAccumulator();
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    try {
      acc.run(Aggregators.SUM, false);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    acc.add(42.5);
    assertEquals(2, acc.long_values.length);
    assertEquals(2, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(1, acc.valueIndex());
    assertEquals(42.5, acc.double_values[0], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(42.5, acc.dp().value().doubleValue(), 0.001);
    acc.run(Aggregators.SUM, true);
    assertEquals(42.5, acc.dp().value().doubleValue(), 0.001);
    
    acc.add(Double.NaN);
    assertEquals(2, acc.long_values.length);
    assertEquals(2, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(2, acc.valueIndex());
    assertEquals(42.5, acc.double_values[0], 0.001);
    assertTrue(Double.isNaN(acc.double_values[1]));
    acc.run(Aggregators.SUM, false);
    assertEquals(42.5, acc.dp().value().doubleValue(), 0.001);
    acc.run(Aggregators.SUM, true);
    assertTrue(Double.isNaN(acc.dp().value().doubleValue()));
    
    // now it doubles
    acc.add(1.5);
    assertEquals(2, acc.long_values.length);
    assertEquals(4, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(3, acc.valueIndex());
    assertEquals(42.5, acc.double_values[0], 0.001);
    assertTrue(Double.isNaN(acc.double_values[1]));
    assertEquals(1.5, acc.double_values[2], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(44, acc.dp().value().doubleValue(), 0.001);
    acc.run(Aggregators.SUM, true);
    assertTrue(Double.isNaN(acc.dp().value().doubleValue()));
    
    acc.reset();
    assertEquals(2, acc.long_values.length);
    assertEquals(4, acc.double_values.length);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    // notice we don't clear the values, just reset the pointer.
    assertEquals(42.5, acc.double_values[0], 0.001);
    assertTrue(Double.isNaN(acc.double_values[1]));
    assertEquals(1.5, acc.double_values[2], 0.001);
    try {
      acc.run(Aggregators.SUM, false);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    for (double i = 0; i < 1025; i++) {
      if (i % 4 == 0) {
        acc.add(Double.NaN);
      } else {
        acc.add(i);
      }
    }
    assertEquals(2, acc.long_values.length);
    assertEquals(2048, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(1025, acc.valueIndex());
    // notice we don't clear the values, just reset the pointer.
    assertTrue(Double.isNaN(acc.double_values[0]));
    assertEquals(1, acc.double_values[1], 0.001);
    assertEquals(2, acc.double_values[2], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(393216, acc.dp().value().doubleValue(), 0.001);
    acc.run(Aggregators.SUM, true);
    assertTrue(Double.isNaN(acc.dp().value().doubleValue()));
  }
  
  @Test
  public void addAndRunLongsThenDoubles() throws Exception {
    NumericAccumulator acc = new NumericAccumulator();
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    try {
      acc.run(Aggregators.SUM, false);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    acc.add(42);
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(1, acc.valueIndex());
    assertEquals(42, acc.long_values[0]);
    acc.run(Aggregators.SUM, false);
    assertEquals(42, acc.dp().value().longValue());
    
    acc.add(24);
    assertEquals(2, acc.long_values.length);
    assertNull(acc.double_values);
    assertTrue(acc.longs);
    assertEquals(2, acc.valueIndex());
    assertEquals(42, acc.long_values[0]);
    assertEquals(24, acc.long_values[1]);
    acc.run(Aggregators.SUM, false);
    assertEquals(66, acc.dp().value().longValue());
    
    // now switch to doubles
    acc.add(42.5);
    assertEquals(2, acc.long_values.length);
    assertEquals(4, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(3, acc.valueIndex());
    assertEquals(42, acc.double_values[0], 0.001);
    assertEquals(24, acc.double_values[1], 0.001);
    assertEquals(42.5, acc.double_values[2], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(108.5, acc.dp().value().doubleValue(), 0.001);
    
    acc.add(24.5);
    assertEquals(2, acc.long_values.length);
    assertEquals(4, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(4, acc.valueIndex());
    assertEquals(42, acc.double_values[0], 0.001);
    assertEquals(24, acc.double_values[1], 0.001);
    assertEquals(42.5, acc.double_values[2], 0.001);
    assertEquals(24.5, acc.double_values[3], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(133, acc.dp().value().doubleValue(), 0.001);
    
    // another long at the inflection point
    acc.add(1);
    assertEquals(2, acc.long_values.length);
    assertEquals(8, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(5, acc.valueIndex());
    assertEquals(42, acc.double_values[0], 0.001);
    assertEquals(24, acc.double_values[1], 0.001);
    assertEquals(42.5, acc.double_values[2], 0.001);
    assertEquals(24.5, acc.double_values[3], 0.001);
    assertEquals(1, acc.double_values[4], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(134, acc.dp().value().doubleValue(), 0.001);
    
    // reset from this state
    acc.reset();
    assertEquals(2, acc.long_values.length);
    assertEquals(8, acc.double_values.length);
    assertTrue(acc.longs);
    assertEquals(0, acc.valueIndex());
    assertEquals(42, acc.double_values[0], 0.001);
    assertEquals(24, acc.double_values[1], 0.001);
    assertEquals(42.5, acc.double_values[2], 0.001);
    assertEquals(24.5, acc.double_values[3], 0.001);
    assertEquals(1, acc.double_values[4], 0.001);
    try {
      acc.run(Aggregators.SUM, false);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    acc.add(1);
    assertEquals(2, acc.long_values.length);
    assertEquals(8, acc.double_values.length);
    assertTrue(acc.longs);
    assertEquals(1, acc.valueIndex());
    assertEquals(1, acc.long_values[0]);
    assertEquals(42, acc.double_values[0], 0.001);
    assertEquals(24, acc.double_values[1], 0.001);
    assertEquals(42.5, acc.double_values[2], 0.001);
    assertEquals(24.5, acc.double_values[3], 0.001);
    assertEquals(1, acc.double_values[4], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(1, acc.dp().value().longValue());
    
    acc.add(2.5);
    assertEquals(2, acc.long_values.length);
    assertEquals(8, acc.double_values.length);
    assertFalse(acc.longs);
    assertEquals(2, acc.valueIndex());
    assertEquals(1, acc.long_values[0]);
    assertEquals(1, acc.double_values[0], 0.001);
    assertEquals(2.5, acc.double_values[1], 0.001);
    // these are still the same
    assertEquals(42.5, acc.double_values[2], 0.001);
    assertEquals(24.5, acc.double_values[3], 0.001);
    assertEquals(1, acc.double_values[4], 0.001);
    acc.run(Aggregators.SUM, false);
    assertEquals(3.5, acc.dp().value().doubleValue(), 0.001);
  }
}