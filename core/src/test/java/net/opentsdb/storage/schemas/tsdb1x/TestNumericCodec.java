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
package net.opentsdb.storage.schemas.tsdb1x;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec.OffsetResolution;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;

public class TestNumericCodec {

  @Test
  public void floatingPointValueToFix() throws Exception {
    assertFalse(NumericCodec.floatingPointValueToFix((byte) 0, 
        new byte[] { 42 }));
    assertFalse(NumericCodec.floatingPointValueToFix(
        (byte) (3 | NumericCodec.FLAG_FLOAT), 
        Bytes.fromInt(Float.floatToIntBits(42.5F))));
    assertFalse(NumericCodec.floatingPointValueToFix(
        (byte) (7 | NumericCodec.FLAG_FLOAT), 
        Bytes.fromLong(Double.doubleToLongBits(42.5))));
    assertTrue(NumericCodec.floatingPointValueToFix(
        (byte) (3 | NumericCodec.FLAG_FLOAT), 
        com.google.common.primitives.Bytes.concat(
            new byte[4],
            Bytes.fromInt(Float.floatToIntBits(42.5F)
            ))));
  }
  
  @Test
  public void fixFloatingPointValue() throws Exception {
    assertArrayEquals(new byte[] { 42 },
        NumericCodec.fixFloatingPointValue((byte) 0, 
        new byte[] { 42 }));
    assertArrayEquals(Bytes.fromInt(Float.floatToIntBits(42.5F)),
        NumericCodec.fixFloatingPointValue((byte) (3 | NumericCodec.FLAG_FLOAT), 
            Bytes.fromInt(Float.floatToIntBits(42.5F))));
    assertArrayEquals(Bytes.fromLong(Double.doubleToLongBits(42.5)),
        NumericCodec.fixFloatingPointValue((byte) (7 | NumericCodec.FLAG_FLOAT), 
            Bytes.fromLong(Double.doubleToLongBits(42.5))));
    assertArrayEquals(Bytes.fromInt(Float.floatToIntBits(42.5F)),
        NumericCodec.fixFloatingPointValue((byte) (3 | NumericCodec.FLAG_FLOAT), 
            com.google.common.primitives.Bytes.concat(
                new byte[4],
                Bytes.fromInt(Float.floatToIntBits(42.5F)
                ))));
    try {
      NumericCodec.fixFloatingPointValue((byte) (3 | NumericCodec.FLAG_FLOAT), 
          com.google.common.primitives.Bytes.concat(
              new byte[] { 0, 0, 1, 0 },
              Bytes.fromInt(Float.floatToIntBits(42.5F)
              )));
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
  }
  
  @Test
  public void buildNanoQualifier() throws Exception {
    // zero offset
    assertArrayEquals(new byte[] { (byte) 0xFF, 0, 0, 0, 0, 0, 0, 0 }, 
        NumericCodec.buildNanoQualifier(0, (short) 0));
    assertArrayEquals(new byte[] { (byte) 0xFF, 0, 0, 0, 0, 0, 0, 1 }, 
        NumericCodec.buildNanoQualifier(0, (short) 1));
    assertArrayEquals(new byte[] { (byte) 0xFF, 0, 0, 0, 0, 0, 0, 7 }, 
        NumericCodec.buildNanoQualifier(0, (short) 7));
    
    // max
    // 110100 01100011 00001011 10001001 11111111 1111 
    // shifted                                         ^^^^ flag bits
    assertArrayEquals(new byte[] { (byte) 0xFF, 0, 0x34, 0x63, 0x0B, 
        (byte) 0x89, (byte) 0xFF, (byte) 0xF0 }, 
        NumericCodec.buildNanoQualifier(3599999999999L, (short) 0));
    assertArrayEquals(new byte[] { (byte) 0xFF, 0, 0x34, 0x63, 0x0B, 
        (byte) 0x89, (byte) 0xFF, (byte) 0xF7 }, 
        NumericCodec.buildNanoQualifier(3599999999999L, (short) 7));
    
    try {
      NumericCodec.buildNanoQualifier(-1, (short) 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      NumericCodec.buildNanoQualifier(3600000000000L, (short) 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void buildMsQualifier() throws Exception {
    // zero offset
    assertArrayEquals(new byte[] { (byte) 0xF0, 0, 0, 0 }, 
        NumericCodec.buildMsQualifier(0, (short) 0));
    assertArrayEquals(new byte[] { (byte) 0xF0, 0, 0, 1 }, 
        NumericCodec.buildMsQualifier(0, (short) 1));
    assertArrayEquals(new byte[] { (byte) 0xF0, 0, 0, 7 }, 
        NumericCodec.buildMsQualifier(0, (short) 7));
    
    // max
    // 1101 10111011 10011111 11
    // shifted                  xx^^^^ flag bits
    assertArrayEquals(new byte[] { (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xC0 }, 
        NumericCodec.buildMsQualifier(3599999L, (short) 0));
    assertArrayEquals(new byte[] { (byte) 0xFD, (byte) 0xBB, (byte) 0x9F, (byte) 0xC7 }, 
        NumericCodec.buildMsQualifier(3599999L, (short) 7));
    
    try {
      NumericCodec.buildMsQualifier(-1, (short) 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      NumericCodec.buildMsQualifier(3600000, (short) 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void buildSecondQualifier() throws Exception {
    // zero offset
    assertArrayEquals(new byte[] { 0, 0 }, 
        NumericCodec.buildSecondQualifier(0, (short) 0));
    assertArrayEquals(new byte[] { 0, 1 }, 
        NumericCodec.buildSecondQualifier(0, (short) 1));
    assertArrayEquals(new byte[] { 0, 7 }, 
        NumericCodec.buildSecondQualifier(0, (short) 7));
    
    // max
    //11100000 1111
    // shifted     ^^^^ flag bits
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xF0 }, 
        NumericCodec.buildSecondQualifier(3599, (short) 0));
    assertArrayEquals(new byte[] { (byte) 0xE0, (byte) 0xF7 }, 
        NumericCodec.buildSecondQualifier(3599, (short) 7));
    
    try {
      NumericCodec.buildSecondQualifier(-1, (short) 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      NumericCodec.buildSecondQualifier(3600, (short) 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void offsetFromNanoQualifier() throws Exception {
    byte[] q = NumericCodec.buildNanoQualifier(0, (short) 0);
    assertEquals(0, NumericCodec.offsetFromNanoQualifier(q, 0));
    
    q = NumericCodec.buildNanoQualifier(3599999999999L, (short) 7);
    assertEquals(3599999999999L, NumericCodec.offsetFromNanoQualifier(q, 0));
    
    byte[] copy = new byte[16];
    System.arraycopy(q, 0, copy, 8, 8);
    assertEquals(3599999999999L, NumericCodec.offsetFromNanoQualifier(copy, 8));
    
    // I'd love to iterate through all possible values but it takes a 
    // looong time. Spot check randomly (mostly) generated set.
    long v = 4242424242L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 4), 0));
    v = 849571281116L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 3), 0));
    v = 564555677355L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 4), 0));
    v = 3147277940640L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 4), 0));
    v = 2578885577901L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 4), 0));
    v = 2800867488587L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 7), 0));
    v = 303539972779L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 4), 0));
    v = 136865874657L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 4), 0));
    v = 1660742495753L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 4), 0));
    v = 1992431869279L;
    assertEquals(v, NumericCodec.offsetFromNanoQualifier(
        NumericCodec.buildNanoQualifier(v, (short) 4), 0));
    
    try {
      NumericCodec.offsetFromNanoQualifier(null, 0);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      NumericCodec.offsetFromNanoQualifier(q, -1);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    
    try {
      NumericCodec.offsetFromNanoQualifier(q, 6);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
  }
  
  @Test
  public void offsetFromMilliQualifier() throws Exception {
    byte[] q = NumericCodec.buildMsQualifier(0, (short) 0);
    assertEquals(0, NumericCodec.offsetFromMsQualifier(q, 0));
    
    q = NumericCodec.buildMsQualifier(3599999L, (short) 7);
    assertEquals(3599999L * 1000 * 1000L, 
        NumericCodec.offsetFromMsQualifier(q, 0));
    
    byte[] copy = new byte[8];
    System.arraycopy(q, 0, copy, 4, 4);
    assertEquals(3599999L * 1000 * 1000L, 
        NumericCodec.offsetFromMsQualifier(copy, 4));
    
    for (int i = 0; i < 3599999; i++) {
      q = NumericCodec.buildMsQualifier(i, (short) 3);
      assertEquals((long) i * 1000L * 1000L, 
          NumericCodec.offsetFromMsQualifier(q, 0));
    }
    
    try {
      NumericCodec.offsetFromMsQualifier(null, 0);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      NumericCodec.offsetFromMsQualifier(q, -1);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    
    try {
      NumericCodec.offsetFromMsQualifier(q, 6);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
  }
  
  @Test
  public void offsetFromSecondQualifier() throws Exception {
    byte[] q = NumericCodec.buildSecondQualifier(0, (short) 0);
    assertEquals(0, NumericCodec.offsetFromSecondQualifier(q, 0));
    
    q = NumericCodec.buildSecondQualifier(3599, (short) 7);
    assertEquals(3599 * 1000L * 1000 * 1000L, 
        NumericCodec.offsetFromSecondQualifier(q, 0));
    
    byte[] copy = new byte[4];
    System.arraycopy(q, 0, copy, 2, 2);
    assertEquals(3599 * 1000L * 1000 * 1000L, 
        NumericCodec.offsetFromSecondQualifier(copy, 2));
    
    for (int i = 0; i < 3599; i++) {
      q = NumericCodec.buildSecondQualifier(i, (short) 3);
      assertEquals((long) i * 1000L * 1000L * 1000L, 
          NumericCodec.offsetFromSecondQualifier(q, 0));
    }
    
    try {
      NumericCodec.offsetFromSecondQualifier(null, 0);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    
    try {
      NumericCodec.offsetFromSecondQualifier(q, -1);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    
    try {
      NumericCodec.offsetFromSecondQualifier(q, 6);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
  }
  
  @Test
  public void getValueLengthFromQualifier() {
    assertEquals(8, NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 7 }));
    assertEquals(8, NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 0x0F }));
    assertEquals(1, NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 0 }));
    assertEquals(4, NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 0x4B }));
    assertEquals(4, NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 11 }));
    assertEquals(4, NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 0x1B }));
    assertEquals(8, NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 0x1F }));
    try {
      NumericCodec.getValueLengthFromQualifier(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    try {
      NumericCodec.getValueLengthFromQualifier(new byte[0]);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    try {
      NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 0x4B }, -42);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    try {
      NumericCodec.getValueLengthFromQualifier(new byte[] { 0, 0x4B }, 42);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
  }
  
  @Test
  public void vleEncodeLong() throws Exception {
    byte[] expected = new byte[1];
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(0));
    
    expected = new byte[] { 42 };
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(42));
    
    expected = new byte[] { -42 };
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(-42));
    
    expected = new byte[] { 1, 1 };
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(257));
    
    expected = new byte[] { (byte) 0xFE, (byte) 0xFF };
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(-257));
    
    expected = new byte[] { 0, 1, 0, 1 };
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(65537));
    
    expected = 
        new byte[] { (byte) 0xFF, (byte) 0xFE, (byte) 0xFF, (byte) 0xFF };
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(-65537));
    
    expected = new byte[] { 0, 0, 0, 1, 0, 0, 0, 0 };
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(4294967296L));
    
    expected = new byte[] { 
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0 };
    assertArrayEquals(expected, NumericCodec.vleEncodeLong(-4294967296L));
  }

  @Test
  public void getFlags() throws Exception {
    byte[] offsets = new byte[] { 7 };
    assertEquals(7, NumericCodec.getFlags(offsets, 0, (byte) 1));
    
    offsets = new byte[] { 0, 0, 0, 0, 7 };
    assertEquals(7, NumericCodec.getFlags(offsets, 0, (byte) 5));
    
    offsets = new byte[] { 0, 1, 0, 8, 0, 2, 0, 0x0F };
    assertEquals(1, NumericCodec.getFlags(offsets, 0, (byte) 2));
    assertEquals(8, NumericCodec.getFlags(offsets, 2, (byte) 2));
    assertEquals(2, NumericCodec.getFlags(offsets, 4, (byte) 2));
    assertEquals(15, NumericCodec.getFlags(offsets, 6, (byte) 2));
    
    try {
      NumericCodec.getFlags(offsets, 8, (byte) 2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericCodec.getFlags(null, 0, (byte) 2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericCodec.getFlags(offsets, -2, (byte) 2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericCodec.getFlags(offsets, 2, (byte) 0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericCodec.getFlags(offsets, 2, (byte) 9);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void getValueLength() throws Exception {
    assertEquals(8, NumericCodec.getValueLength((byte) 0x07));
    assertEquals(7, NumericCodec.getValueLength((byte) 0x06));
    assertEquals(6, NumericCodec.getValueLength((byte) 0x05));
    assertEquals(5, NumericCodec.getValueLength((byte) 0x04));
    assertEquals(4, NumericCodec.getValueLength((byte) 0x03));
    assertEquals(3, NumericCodec.getValueLength((byte) 0x02));
    assertEquals(2, NumericCodec.getValueLength((byte) 0x01));
    assertEquals(1, NumericCodec.getValueLength((byte) 0x00));
    // ignore the first bits
    assertEquals(8, NumericCodec.getValueLength((byte) 0xFF));
    assertEquals(7, NumericCodec.getValueLength((byte) 0xFE));
    assertEquals(6, NumericCodec.getValueLength((byte) 0xFD));
    assertEquals(5, NumericCodec.getValueLength((byte) 0xFC));
    assertEquals(4, NumericCodec.getValueLength((byte) 0xFB));
    assertEquals(3, NumericCodec.getValueLength((byte) 0xFA));
    assertEquals(2, NumericCodec.getValueLength((byte) 0xF9));
    assertEquals(1, NumericCodec.getValueLength((byte) 0xF8));
  }
  
  @Test
  public void encodeOn() throws Exception {
    int reserved = 7;

    assertEquals(1, NumericCodec.encodeOn(1, reserved));
    assertEquals(2, NumericCodec.encodeOn(2, reserved));
    assertEquals(2, NumericCodec.encodeOn(239, reserved));
    assertEquals(2, NumericCodec.encodeOn(511, reserved));
    assertEquals(3, NumericCodec.encodeOn(512, reserved));
    assertEquals(3, NumericCodec.encodeOn(131071, reserved));
    assertEquals(4, NumericCodec.encodeOn(131072, reserved));
    assertEquals(4, NumericCodec.encodeOn(33554431, reserved));
    assertEquals(5, NumericCodec.encodeOn(33554432, reserved));
    assertEquals(5, NumericCodec.encodeOn(8589934591L, reserved));
    assertEquals(6, NumericCodec.encodeOn(8589934592L, reserved));
    assertEquals(6, NumericCodec.encodeOn(2199023255551L, reserved));
    assertEquals(7, NumericCodec.encodeOn(2199023255552L, reserved));
    assertEquals(7, NumericCodec.encodeOn(562949953421311L, reserved));
    assertEquals(8, NumericCodec.encodeOn(562949953421312L, reserved));
    assertEquals(8, NumericCodec.encodeOn(72057594037927935L, reserved));
    assertEquals(8, NumericCodec.encodeOn(144115188075855871L, reserved));
    try {
      NumericCodec.encodeOn(144115188075855872L, reserved);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    reserved = 4;
    assertEquals(1, NumericCodec.encodeOn(1, reserved));
    assertEquals(1, NumericCodec.encodeOn(15, reserved));
    assertEquals(2, NumericCodec.encodeOn(16, reserved));
    assertEquals(2, NumericCodec.encodeOn(4095, reserved));
    assertEquals(3, NumericCodec.encodeOn(4096, reserved));
    assertEquals(3, NumericCodec.encodeOn(1048575, reserved));
    assertEquals(4, NumericCodec.encodeOn(1048576, reserved));
    assertEquals(4, NumericCodec.encodeOn(268435455, reserved));
    assertEquals(5, NumericCodec.encodeOn(268435456, reserved));
    assertEquals(5, NumericCodec.encodeOn(68719476735L, reserved));
    assertEquals(6, NumericCodec.encodeOn(68719476736L, reserved));
    assertEquals(6, NumericCodec.encodeOn(17592186044415L, reserved));
    assertEquals(7, NumericCodec.encodeOn(17592186044416L, reserved));
    assertEquals(7, NumericCodec.encodeOn(4503599627370495L, reserved));
    assertEquals(8, NumericCodec.encodeOn(4503599627370496L, reserved));
    assertEquals(8, NumericCodec.encodeOn(1152921504606846975L, reserved));
    try {
      NumericCodec.encodeOn(1152921504606846976L, reserved);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      NumericCodec.encodeOn(-1, reserved);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    reserved = 0;
    try {
      NumericCodec.encodeOn(1, reserved);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void extractIntegerValue() throws Exception {
    byte[] values = new byte[] { 0 };
    assertEquals(0, NumericCodec.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { 1 };
    assertEquals(1, NumericCodec.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { (byte) 0x7F };
    assertEquals(127, NumericCodec.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { (byte) 0xFF };
    assertEquals(-1, NumericCodec.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { (byte) 0x80 };
    assertEquals(-128, NumericCodec.extractIntegerValue(values, 0, (byte) 0));
    
    values = new byte[] { 1, 0 };
    assertEquals(256, NumericCodec.extractIntegerValue(values, 0, (byte) 1));
    
    values = new byte[] { 0x7F, (byte) 0xFF };
    assertEquals(32767, NumericCodec.extractIntegerValue(values, 0, (byte) 1));
    
    values = new byte[] { (byte) 0xFF, (byte) 0xFF };
    assertEquals(-1, NumericCodec.extractIntegerValue(values, 0, (byte) 1));
    
    values = new byte[] { (byte) 0x80, 0 };
    assertEquals(-32768, NumericCodec.extractIntegerValue(values, 0, (byte) 1));
    
    values = new byte[] { 1, 0, 0 };
    try {
      // has to be 4 bytes
      NumericCodec.extractIntegerValue(values, 0, (byte) 2);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    values = new byte[] { 0, 1, 0, 0 };
    assertEquals(65536, NumericCodec.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { 1, 0, 0, 0 };
    assertEquals(16777216, NumericCodec.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    assertEquals(2147483647, NumericCodec.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    assertEquals(-1, NumericCodec.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { (byte) 0x80, 0, 0, 0 };
    assertEquals(-2147483648, 
        NumericCodec.extractIntegerValue(values, 0, (byte) 3));
    
    values = new byte[] { 1, 0, 0, 0, 0 };
    try {
      // has to be 8 bytes
      NumericCodec.extractIntegerValue(values, 0, (byte) 4);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    values = new byte[] { 0, 0, 0, 1, 0, 0, 0, 0 };
    assertEquals(4294967296L, 
        NumericCodec.extractIntegerValue(values, 0, (byte) 7));
    
    values = new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 };
    assertEquals(72057594037927936L, 
        NumericCodec.extractIntegerValue(values, 0, (byte) 7));
    
    values = new byte[] { 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    assertEquals(9223372036854775807L, 
        NumericCodec.extractIntegerValue(values, 0, (byte) 7));

    values = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
    assertEquals(-1, NumericCodec.extractIntegerValue(values, 0, (byte) 7));
    
    values = new byte[] { (byte) 0x80, 0, 0, 0, 0, 0, 0, 0 };
    assertEquals(-9223372036854775808L, 
        NumericCodec.extractIntegerValue(values, 0, (byte) 7));
    
    values = new byte[] { (byte) 0x80, 1 };
    assertEquals(1, NumericCodec.extractIntegerValue(values, 1, (byte) 0));
  }

  @Test
  public void extractFloatingPointValue() throws Exception {
    byte[] values = new byte[] { 0, 0, 0, 0 };
    assertEquals(0, NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF3), 0.0001);
    
    values = Bytes.fromInt(Float.floatToIntBits(42.5f));
    assertEquals(42.5f, NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF3), 0.0001);
    
    values = Bytes.fromInt(Float.floatToIntBits(-42.5f));
    assertEquals(-42.5f, NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF3), 0.0001);
    try {
      // has to be 4 bytes
      NumericCodec.extractFloatingPointValue(values, 0, (byte) 0xF2);
      fail("Expected IllegalDataException");
    } catch (IllegalDataException e) { }
    
    values = Bytes.fromInt(Float.floatToIntBits(Float.POSITIVE_INFINITY));
    assertTrue(Double.isInfinite(NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF3)));
    
    values = Bytes.fromInt(Float.floatToIntBits(Float.NEGATIVE_INFINITY));
    assertTrue(Double.isInfinite(NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF3)));
    
    values = Bytes.fromInt(Float.floatToIntBits(Float.NaN));
    assertTrue(Double.isNaN(NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF3)));
    
    values = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
    assertEquals(0, NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF7), 0.0001);
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(42.5d));
    assertEquals(42.5d, NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF7), 0.0001);
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(-42.5d));
    assertEquals(-42.5d, NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF7), 0.0001);
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(Double.POSITIVE_INFINITY));
    assertTrue(Double.isInfinite(NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF7)));
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY));
    assertTrue(Double.isInfinite(NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF7)));
    
    values = Bytes.fromLong(Double.doubleToRawLongBits(Double.NaN));
    assertTrue(Double.isNaN(NumericCodec.extractFloatingPointValue(
        values, 0, (byte) 0xF7)));
  }
  
  @Test
  public void encodeDecodeAppendValue() throws Exception {
    long base = 1514764800;
    
    byte[] val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, 42);
    TimeSeriesValue<NumericType> value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 60, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(42, value.value().longValue());
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, Long.MIN_VALUE);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 60, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(Long.MIN_VALUE, value.value().longValue());
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 60, Long.MAX_VALUE);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 60, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(Long.MAX_VALUE, value.value().longValue());
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, 42.5F);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 120, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(42.5, value.value().doubleValue(), 0.001);
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, Float.MIN_VALUE);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 120, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(Float.MIN_VALUE, value.value().doubleValue(), 0.001);
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 120, Float.MAX_VALUE);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 120, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(Float.MAX_VALUE, value.value().doubleValue(), 0.001);
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, 42.5D);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 180, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(42.5, value.value().doubleValue(), 0.001);
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, Double.MIN_VALUE);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 180, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(Double.MIN_VALUE, value.value().doubleValue(), 0.001);
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.SECONDS, 180, Double.MAX_VALUE);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 180, value.timestamp().epoch());
    assertEquals(0, value.timestamp().nanos());
    assertEquals(Double.MAX_VALUE, value.value().doubleValue(), 0.001);
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 500, 24);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base, value.timestamp().epoch());
    assertEquals(500L * 1000L * 1000L, value.timestamp().nanos());
    assertEquals(24, value.value().longValue());
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.MILLIS, 60250, 24);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 60, value.timestamp().epoch());
    assertEquals(250L * 1000L * 1000L, value.timestamp().nanos());
    assertEquals(24, value.value().longValue());
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 32, 24);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base, value.timestamp().epoch());
    assertEquals(32L, value.timestamp().nanos());
    assertEquals(24, value.value().longValue());
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 1024, 24);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base, value.timestamp().epoch());
    assertEquals(1024L, value.timestamp().nanos());
    assertEquals(24, value.value().longValue());
    
    val = NumericCodec.encodeAppendValue(OffsetResolution.NANOS, 60000000001L, 24);
    value = NumericCodec.valueFromAppend(base, val, 0);
    assertEquals(base + 60, value.timestamp().epoch());
    assertEquals(1L, value.timestamp().nanos());
    assertEquals(24, value.value().longValue());
  }

  @Test
  public void encode() throws Exception {
    Codec codec = new NumericCodec();
    
    // SECONDS
    // top of the hour
    MutableNumericValue value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 42);
    Pair<byte[], byte[]> encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { 0, 0 }, encoded.getKey());
    assertArrayEquals(new byte[] { 42 }, encoded.getValue());
    
    // offset
    value = new MutableNumericValue(
        new SecondTimeStamp(1262305800), 42);
    encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { 0x70, (byte) 0x80 }, encoded.getKey());
    assertArrayEquals(new byte[] { 42 }, encoded.getValue());
    
    // VLE lengths
    value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 256);
    encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { 0, 1 }, encoded.getKey());
    assertArrayEquals(new byte[] { 1, 0 }, encoded.getValue());
    
    value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 65536);
    encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { 0, 3 }, encoded.getKey());
    assertArrayEquals(new byte[] { 0, 1, 0, 0 }, encoded.getValue());
    
    value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 4294967295L);
    encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { 0, 7 }, encoded.getKey());
    assertArrayEquals(new byte[] { 0, 0, 0, 0, -1, -1, -1, -1 }, encoded.getValue());
    
    // Float / Double
    value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 42.5D);
    encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { 0, 11 }, encoded.getKey());
    assertArrayEquals(new byte[] { 66, 42, 0, 0 }, encoded.getValue());
    
    value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 1.234556789123456);
    encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { 0, 15 }, encoded.getKey());
    assertArrayEquals(new byte[] { 63, -13, -64, -66, -98, -91, 112, -80 }, encoded.getValue());
    
    // milliseconds
    value = new MutableNumericValue(
        new MillisecondTimeStamp(1262304000000L), 42);
    encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { -16, 0, 0, 0 }, encoded.getKey());
    assertArrayEquals(new byte[] { 42 }, encoded.getValue());
    
    value = new MutableNumericValue(
        new MillisecondTimeStamp(1262304000250L), 42);
    encoded = codec.encode(value, false, 1262304000, null);
    assertArrayEquals(new byte[] { -16, 0, 62, -128 }, encoded.getKey());
    assertArrayEquals(new byte[] { 42 }, encoded.getValue());
    
    // TODO - nanos
    
    // appends
    value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 42);
    encoded = codec.encode(value, true, 1262304000, null);
    assertArrayEquals(NumericCodec.APPEND_QUALIFIER, encoded.getKey());
    assertArrayEquals(new byte[] { 0, 0, 42 }, encoded.getValue());
    
    value = new MutableNumericValue(
        new SecondTimeStamp(1262304000), 42.5D);
    encoded = codec.encode(value, true, 1262304000, null);
    assertArrayEquals(NumericCodec.APPEND_QUALIFIER, encoded.getKey());
    assertArrayEquals(new byte[] { 0, 11, 66, 42, 0, 0 }, encoded.getValue());
    
    value = new MutableNumericValue(
        new MillisecondTimeStamp(1262304000000L), 42);
    encoded = codec.encode(value, true, 1262304000, null);
    assertArrayEquals(NumericCodec.APPEND_QUALIFIER, encoded.getKey());
    assertArrayEquals(new byte[] { -16, 0, 0, 0, 42 }, encoded.getValue());
  }
}
