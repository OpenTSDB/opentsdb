// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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

import org.junit.Test;

public class TestNumericType {

  @Test
  public void fitsInFloatSimple() {
    // deceiving eh?
    assertEquals(false, NumericType.fitsInFloat("12.3"));
    assertEquals(false, NumericType.fitsInFloat(12.3));
  }

  @Test
  public void fitsInFloatDoublePrecision() {
      assertEquals(false, NumericType.fitsInFloat("1.234556789123456"));
      assertEquals(false, NumericType.fitsInFloat(1.234556789123456));
  }

  @Test(expected=NumberFormatException.class)
  public void fitsInFloatMalformed() {
      assertEquals(false, NumericType.fitsInFloat("1.2abc34"));
  }
  
  @Test
  public void fitsInFloat() {
    assertEquals(true, NumericType.fitsInFloat("0.6116398572921753"));
    assertEquals(true, NumericType.fitsInFloat(0.6116398572921753));
    assertEquals(true, NumericType.fitsInFloat("1.01417856E9"));
    assertEquals(true, NumericType.fitsInFloat(1.01417856E9));
    assertEquals(false, NumericType.fitsInFloat("4.508277154265837E7"));
    assertEquals(false, NumericType.fitsInFloat(4.508277154265837E7));
    assertEquals(false, NumericType.fitsInFloat("8.208611994536002E8"));
    assertEquals(false, NumericType.fitsInFloat(8.208611994536002E8));
  }
  
  public void parseLongSimple() {
    assertEquals(0, NumericType.parseLong("0"));
    assertEquals(0, NumericType.parseLong("+0"));
    assertEquals(0, NumericType.parseLong("-0"));
    assertEquals(1, NumericType.parseLong("1"));
    assertEquals(1, NumericType.parseLong("+1"));
    assertEquals(-1, NumericType.parseLong("-1"));
    assertEquals(4242, NumericType.parseLong("4242"));
    assertEquals(4242, NumericType.parseLong("+4242"));
    assertEquals(-4242, NumericType.parseLong("-4242"));
  }

  @Test
  public void parseLongMaxValue() {
    assertEquals(Long.MAX_VALUE, NumericType.parseLong(Long.toString(Long.MAX_VALUE)));
  }

  @Test
  public void parseLongMinValue() {
    assertEquals(Long.MIN_VALUE, NumericType.parseLong(Long.toString(Long.MIN_VALUE)));
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongEmptyString() {
    NumericType.parseLong("");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformed() {
    NumericType.parseLong("42a51");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformedPlus() {
    NumericType.parseLong("+");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongMalformedMinus() {
    NumericType.parseLong("-");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooLarge() {
    NumericType.parseLong("18446744073709551616");
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooLargeSubtle() {
    NumericType.parseLong("9223372036854775808"); // MAX_VALUE + 1
  }

  @Test(expected=NumberFormatException.class)
  public void parseLongValueTooSmallSubtle() {
    NumericType.parseLong("-9223372036854775809"); // MIN_VALUE - 1
  }

}
