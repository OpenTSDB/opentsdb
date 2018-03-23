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
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestNumericCodec {

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
}
