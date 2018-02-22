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
package net.opentsdb.utils;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

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
  public void memcmpnulls() throws Exception {
    byte[] array_a = new byte[] { 0, 42, 12, 16 };
    byte[] array_b = new byte[] { 0, 42, 12, 16 };
    
    assertEquals(0, Bytes.memcmpMaybeNull(array_a, array_b));
    array_b[1] = 2;
    assertEquals(40, Bytes.memcmpMaybeNull(array_a, array_b));
    assertEquals(1, Bytes.memcmpMaybeNull(array_a, null));
    assertEquals(-1, Bytes.memcmpMaybeNull(null, array_b));
    assertEquals(0, Bytes.memcmpMaybeNull(null, null));
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

  @Test
  public void isNullOrEmpty() throws Exception {
    assertTrue(Bytes.isNullOrEmpty(null));
    assertTrue(Bytes.isNullOrEmpty(new byte[] { }));
    assertFalse(Bytes.isNullOrEmpty(new byte[] { 0 }));
    assertFalse(Bytes.isNullOrEmpty(new byte[] { 'h', 'i', '!' }));
  }

  @Test
  public void byteListComparator() throws Exception {
    List<byte[]> a = Lists.newArrayList();
    List<byte[]> b = Lists.newArrayList();
    
    a.add(new byte[] { 'a', 'b' });
    a.add(new byte[] { 'a', 'c' });
    
    b.add(new byte[] { 'a', 'c' });
    b.add(new byte[] { 'a', 'b' });
    
    assertEquals(0, Bytes.BYTE_LIST_CMP.compare(null, null));
    assertEquals(0, Bytes.BYTE_LIST_CMP.compare(a, b));
    assertEquals(-1, Bytes.BYTE_LIST_CMP.compare(null, b));
    assertEquals(1, Bytes.BYTE_LIST_CMP.compare(a, null));
    assertEquals(0, Bytes.BYTE_LIST_CMP.compare(a, a));
    
    b.add(new byte[] { 'a', 'b' });
    assertEquals(1, Bytes.BYTE_LIST_CMP.compare(a, b));
    
    b.remove(2);
    b.set(1, null);
    assertEquals(1, Bytes.BYTE_LIST_CMP.compare(a, b));
  }
}
