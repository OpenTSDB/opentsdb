// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.utils.Bytes;

public class TestNumericType {

  @Test
  public void vleEncodeLong0() throws Exception {
    final byte[] expected = new byte[1];
    assertArrayEquals(expected, NumericType.vleEncodeLong(0));
  }
  
  @Test
  public void vleEncodeLong1byte() throws Exception {
    final byte[] expected = new byte[] { 42 };
    assertArrayEquals(expected, NumericType.vleEncodeLong(42));
  }
  
  @Test
  public void vleEncodeLong1byteNegative() throws Exception {
    final byte[] expected = new byte[] { -42 };
    assertArrayEquals(expected, NumericType.vleEncodeLong(-42));
  }
  
  @Test
  public void vleEncodeLong2bytes() throws Exception {
    final byte[] expected = new byte[] { 1, 1 };
    assertArrayEquals(expected, NumericType.vleEncodeLong(257));
  }
  
  @Test
  public void vleEncodeLong2bytesNegative() throws Exception {
    final byte[] expected = new byte[] { (byte) 0xFE, (byte) 0xFF };
    assertArrayEquals(expected, NumericType.vleEncodeLong(-257));
  }
  
  @Test
  public void vleEncodeLong4bytes() throws Exception {
    final byte[] expected = new byte[] { 0, 1, 0, 1 };
    assertArrayEquals(expected, NumericType.vleEncodeLong(65537));
  }
  
  @Test
  public void vleEncodeLong4bytesNegative() throws Exception {
    final byte[] expected = 
        new byte[] { (byte) 0xFF, (byte) 0xFE, (byte) 0xFF, (byte) 0xFF };
    assertArrayEquals(expected, NumericType.vleEncodeLong(-65537));
  }
  
  @Test
  public void vleEncodeLong8bytes() throws Exception {
    final byte[] expected = new byte[] { 0, 0, 0, 1, 0, 0, 0, 0 };
    assertArrayEquals(expected, NumericType.vleEncodeLong(4294967296L));
  }
  
  @Test
  public void vleEncodeLong8bytesNegative() throws Exception {
    final byte[] expected = new byte[] { 
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0 };
    assertArrayEquals(expected, NumericType.vleEncodeLong(-4294967296L));
  }
  
  @Test
  public void getFlags() throws Exception {
    byte[] offsets = new byte[] { 7 };
    assertEquals(7, NumericType.getFlags(offsets, 0, (byte) 1));
    
    offsets = new byte[] { 0, 0, 0, 0, 7 };
    assertEquals(7, NumericType.getFlags(offsets, 0, (byte) 5));
    
    offsets = new byte[] { 0, 1, 0, 8, 0, 2 };
    assertEquals(1, NumericType.getFlags(offsets, 0, (byte) 2));
    assertEquals(8, NumericType.getFlags(offsets, 2, (byte) 2));
    assertEquals(2, NumericType.getFlags(offsets, 4, (byte) 2));
    
    try {
      NumericType.getFlags(offsets, 6, (byte) 2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericType.getFlags(null, 0, (byte) 2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericType.getFlags(offsets, -2, (byte) 2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericType.getFlags(offsets, 2, (byte) 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericType.getFlags(offsets, 2, (byte) 9);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getValueLength() throws Exception {
    assertEquals(8, NumericType.getValueLength((byte) 0x07));
    assertEquals(7, NumericType.getValueLength((byte) 0x06));
    assertEquals(6, NumericType.getValueLength((byte) 0x05));
    assertEquals(5, NumericType.getValueLength((byte) 0x04));
    assertEquals(4, NumericType.getValueLength((byte) 0x03));
    assertEquals(3, NumericType.getValueLength((byte) 0x02));
    assertEquals(2, NumericType.getValueLength((byte) 0x01));
    assertEquals(1, NumericType.getValueLength((byte) 0x00));
    // ignore the first bits
    assertEquals(8, NumericType.getValueLength((byte) 0xFF));
    assertEquals(7, NumericType.getValueLength((byte) 0xFE));
    assertEquals(6, NumericType.getValueLength((byte) 0xFD));
    assertEquals(5, NumericType.getValueLength((byte) 0xFC));
    assertEquals(4, NumericType.getValueLength((byte) 0xFB));
    assertEquals(3, NumericType.getValueLength((byte) 0xFA));
    assertEquals(2, NumericType.getValueLength((byte) 0xF9));
    assertEquals(1, NumericType.getValueLength((byte) 0xF8));
  }
  
  @Test
  public void encodeOn() throws Exception {
    int reserved = 7;

    assertEquals(1, NumericType.encodeOn(1, reserved));
    assertEquals(2, NumericType.encodeOn(2, reserved));
    assertEquals(2, NumericType.encodeOn(239, reserved));
    assertEquals(2, NumericType.encodeOn(511, reserved));
    assertEquals(3, NumericType.encodeOn(512, reserved));
    assertEquals(3, NumericType.encodeOn(131071, reserved));
    assertEquals(4, NumericType.encodeOn(131072, reserved));
    assertEquals(4, NumericType.encodeOn(33554431, reserved));
    assertEquals(5, NumericType.encodeOn(33554432, reserved));
    assertEquals(5, NumericType.encodeOn(8589934591L, reserved));
    assertEquals(6, NumericType.encodeOn(8589934592L, reserved));
    assertEquals(6, NumericType.encodeOn(2199023255551L, reserved));
    assertEquals(7, NumericType.encodeOn(2199023255552L, reserved));
    assertEquals(7, NumericType.encodeOn(562949953421311L, reserved));
    assertEquals(8, NumericType.encodeOn(562949953421312L, reserved));
    assertEquals(8, NumericType.encodeOn(72057594037927935L, reserved));
    assertEquals(8, NumericType.encodeOn(144115188075855871L, reserved));
    try {
      NumericType.encodeOn(144115188075855872L, reserved);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    reserved = 4;
    assertEquals(1, NumericType.encodeOn(1, reserved));
    assertEquals(1, NumericType.encodeOn(15, reserved));
    assertEquals(2, NumericType.encodeOn(16, reserved));
    assertEquals(2, NumericType.encodeOn(4095, reserved));
    assertEquals(3, NumericType.encodeOn(4096, reserved));
    assertEquals(3, NumericType.encodeOn(1048575, reserved));
    assertEquals(4, NumericType.encodeOn(1048576, reserved));
    assertEquals(4, NumericType.encodeOn(268435455, reserved));
    assertEquals(5, NumericType.encodeOn(268435456, reserved));
    assertEquals(5, NumericType.encodeOn(68719476735L, reserved));
    assertEquals(6, NumericType.encodeOn(68719476736L, reserved));
    assertEquals(6, NumericType.encodeOn(17592186044415L, reserved));
    assertEquals(7, NumericType.encodeOn(17592186044416L, reserved));
    assertEquals(7, NumericType.encodeOn(4503599627370495L, reserved));
    assertEquals(8, NumericType.encodeOn(4503599627370496L, reserved));
    assertEquals(8, NumericType.encodeOn(1152921504606846975L, reserved));
    try {
      NumericType.encodeOn(1152921504606846976L, reserved);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericType.encodeOn(-1, reserved);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    reserved = 0;
    try {
      NumericType.encodeOn(1, reserved);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void extractIntegerValue() throws Exception {
    byte[] values = new byte[] { 0 };
    assertEquals(0, NumericType.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { 1 };
    assertEquals(1, NumericType.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { (byte) 0x7F };
    assertEquals(127, NumericType.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { (byte) 0xFF };
    assertEquals(-1, NumericType.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { (byte) 0x80 };
    assertEquals(-128, NumericType.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { 1, 0 };
    assertEquals(256, NumericType.extractIntegerValue(values, 0, (byte) 1));
    
    values = new byte[] { 0x7F, (byte) 0xFF };
    assertEquals(32767, NumericType.extractIntegerValue(values, 0, (byte) 1));
    
    values = new byte[] { (byte) 0xFF, (byte) 0xFF };
    assertEquals(-1, NumericType.extractIntegerValue(values, 0, (byte) 1));
    
    values = new byte[] { (byte) 0x80, 0 };
    assertEquals(-32768, NumericType.extractIntegerValue(values, 0, (byte) 1));
    
    values = new byte[] { 1, 0, 0 };
    try {
      // has to be 4 bytes
      NumericType.extractIntegerValue(values, 0, (byte) 2);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    values = new byte[] { 0, 1, 0, 0 };
    assertEquals(65536, NumericType.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { 1, 0, 0, 0 };
    assertEquals(16777216, NumericType.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    assertEquals(2147483647, NumericType.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    assertEquals(-1, NumericType.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { (byte) 0x80, 0, 0, 0 };
    assertEquals(-2147483648, 
        NumericType.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { 1, 0, 0, 0, 0 };
    try {
      // has to be 8 bytes
      NumericType.extractIntegerValue(values, 0, (byte) 4);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    values = new byte[] { 0, 0, 0, 1, 0, 0, 0, 0 };
    assertEquals(4294967296L, 
        NumericType.extractIntegerValue(values, 0, (byte) 7));
    
    values = new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 };
    assertEquals(72057594037927936L, 
        NumericType.extractIntegerValue(values, 0, (byte) 7));
    
    values = new byte[] { 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    assertEquals(9223372036854775807L, 
        NumericType.extractIntegerValue(values, 0, (byte) 7));

    values = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    assertEquals(-1, NumericType.extractIntegerValue(values, 0, (byte) 7));
    
    values = new byte[] { (byte) 0x80, 0, 0, 0, 0, 0, 0, 0 };
    assertEquals(-9223372036854775808L, 
        NumericType.extractIntegerValue(values, 0, (byte) 7));
    
    values = new byte[] { (byte) 0x80, 1 };
    assertEquals(1, NumericType.extractIntegerValue(values, 1, (byte) 0));
  }

  @Test
  public void extractFloatingPointValue() throws Exception {
    byte[] values = new byte[] { 0, 0, 0, 0 };
    assertEquals(0, NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF3), 0.0001);
    
    values = Bytes.fromInt(Float.floatToIntBits(42.5f));
    assertEquals(42.5f, NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF3), 0.0001);
    
    values = Bytes.fromInt(Float.floatToIntBits(-42.5f));
    assertEquals(-42.5f, NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF3), 0.0001);
    try {
      // has to be 4 bytes
      NumericType.extractFloatingPointValue(values, 0, (byte) 0xF2);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    values = Bytes.fromInt(Float.floatToIntBits(Float.POSITIVE_INFINITY));
    assertTrue(Double.isInfinite(NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF3)));
    
    values = Bytes.fromInt(Float.floatToIntBits(Float.NEGATIVE_INFINITY));
    assertTrue(Double.isInfinite(NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF3)));
    
    values = Bytes.fromInt(Float.floatToIntBits(Float.NaN));
    assertTrue(Double.isNaN(NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF3)));
    
    values = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
    assertEquals(0, NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF7), 0.0001);
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(42.5d));
    assertEquals(42.5d, NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF7), 0.0001);
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(-42.5d));
    assertEquals(-42.5d, NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF7), 0.0001);
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(Double.POSITIVE_INFINITY));
    assertTrue(Double.isInfinite(NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF7)));
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY));
    assertTrue(Double.isInfinite(NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF7)));
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(Double.NaN));
    assertTrue(Double.isNaN(NumericType.extractFloatingPointValue(
        values, 0, (byte) 0xF7)));
  }
  
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
