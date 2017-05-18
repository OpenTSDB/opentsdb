// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestBytes {

  @Test
  public void memcmp() throws Exception {
    byte[] array_a = new byte[] { 0, 42, 12, 16 };
    byte[] array_b = new byte[] { 0, 42, 12, 16 };
    
    assertEquals(0, Bytes.memcmp(array_a, array_b));
    assertEquals(0, Bytes.memcmp(array_a, array_b, 0, array_a.length));
    assertEquals(0, Bytes.memcmp(array_a, array_b, 1, 2));
    assertEquals(0, Bytes.memcmp(array_a, array_b, 1, 3));
    try {
      Bytes.memcmp(array_a, array_b, 1, 4);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    
    try {
      Bytes.memcmp(array_a, array_b, -4, 2);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    
    // diff tail
    array_b = new byte[] { 0, 42, 12, 17 };
    assertEquals(-1, Bytes.memcmp(array_a, array_b));
    assertEquals(-1, Bytes.memcmp(array_a, array_b, 0, array_a.length));
    assertEquals(0, Bytes.memcmp(array_a, array_b, 1, 2));
    assertEquals(-1, Bytes.memcmp(array_a, array_b, 1, 3));
    
    // diff length
    array_b = new byte[] { 0, 42, 12 };
    assertEquals(1, Bytes.memcmp(array_a, array_b));
    try {
      Bytes.memcmp(array_a, array_b, 0, array_a.length);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    assertEquals(0, Bytes.memcmp(array_a, array_b, 1, 2));
    try {
      Bytes.memcmp(array_a, array_b, 1, 3);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    
    // diff length / order
    assertEquals(-1, Bytes.memcmp(array_b, array_a));
    try {
      Bytes.memcmp(array_b, array_a, 0, array_a.length);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    assertEquals(0, Bytes.memcmp(array_b, array_a, 1, 2));
    try {
      Bytes.memcmp(array_b, array_a, 1, 3);
      fail("Expected ArrayIndexOutOfBoundsException");
    } catch (ArrayIndexOutOfBoundsException e) { }
    
    // one null
    array_b = null;
    try {
      Bytes.memcmp(array_a, array_b);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
    try {
      Bytes.memcmp(array_b, array_a, 0, array_a.length);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }

  @Test
  public void arrayToStringToBytes() throws Exception {
    byte[] data = new byte[] { 42, -128, 24, 0 };
    String hex = Bytes.byteArrayToString(data);
    assertEquals("2A801800", hex);
    assertArrayEquals(data, Bytes.stringToByteArray(hex));
    
    data = new byte[] { };
    hex = Bytes.byteArrayToString(data);
    assertEquals("", hex);
    assertArrayEquals(data, Bytes.stringToByteArray(hex));
    
    try {
      Bytes.byteArrayToString(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      Bytes.stringToByteArray(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      Bytes.stringToByteArray("2A80180");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
