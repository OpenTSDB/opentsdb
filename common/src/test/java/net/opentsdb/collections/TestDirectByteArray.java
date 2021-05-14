// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.collections;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDirectByteArray {
  @Test
  public void testEmptyArray() {
    DirectByteArray array = new DirectByteArray(0);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  public void testByte(final boolean encodeLength) {
    DirectByteArray array = new DirectByteArray(50, encodeLength);

    array.setByte(0, Byte.MAX_VALUE);
    array.setByte(1, Byte.MIN_VALUE);
    array.setByte(2, (byte) 123);
    array.setByte(3, (byte) -23);

    assertEquals(Byte.MAX_VALUE, array.getByte(0));
    assertEquals(Byte.MIN_VALUE, array.getByte(1));
    assertEquals((byte) 123, array.getByte(2));
    assertEquals((byte) -23, array.getByte(3));
  }

  @Test
  public void testShort() {
    DirectByteArray array = new DirectByteArray(50);

    array.setShort(0, Short.MAX_VALUE);
    array.setShort(2, Short.MIN_VALUE);
    array.setShort(4, (short) 256);
    array.setShort(6, (short) -342);

    assertEquals(Short.MAX_VALUE, array.getShort(0));
    assertEquals(Short.MIN_VALUE, array.getShort(2));
    assertEquals((short) 256, array.getShort(4));
    assertEquals((short) -342, array.getShort(6));
  }

  @Test
  public void testInt() {
    DirectByteArray array = new DirectByteArray(50);

    array.setInt(0, Integer.MAX_VALUE);
    array.setInt(4, Integer.MIN_VALUE);
    array.setInt(8, 12345);
    array.setInt(12, -12345);

    assertEquals(Integer.MAX_VALUE, array.getInt(0));
    assertEquals(Integer.MIN_VALUE, array.getInt(4));
    assertEquals(12345, array.getInt(8));
    assertEquals(-12345, array.getInt(12));
  }

  @Test
  public void testLong() {
    DirectByteArray array = new DirectByteArray(50);

    array.setLong(0, Long.MAX_VALUE);
    array.setLong(8, Long.MIN_VALUE);
    array.setLong(16, 1234567l);
    array.setLong(24, -1234567l);

    assertEquals(Long.MAX_VALUE, array.getLong(0));
    assertEquals(Long.MIN_VALUE, array.getLong(8));
    assertEquals(1234567l, array.getLong(16));
    assertEquals(-1234567l, array.getLong(24));
  }

  @Test
  public void testInitializationByCapacity() {
    DirectByteArray array = new DirectByteArray(5);
    assertEquals(5, array.getCapacity());
    assertTrue(array.getAddress() > 0);
  }

  @Test
  public void testInitializationByStartAddress() {
    DirectByteArray array = new DirectByteArray(2);
    array.setByte(0, (byte) 1);
    array.setByte(1, (byte) 2);

    DirectByteArray another = new DirectByteArray(array.getAddress());

    assertEquals(2, another.getCapacity());
    assertEquals(array.getAddress(), another.getAddress());
    assertEquals(1, another.getByte(0));
    assertEquals(2, another.getByte(1));
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  public void testInitializationByStartAddress(final boolean lengthEncoded) {
    DirectByteArray array = new DirectByteArray(2, lengthEncoded);
    array.setByte(0, (byte) 1);
    array.setByte(1, (byte) 2);

    DirectByteArray another = new DirectByteArray(array.getAddress(), lengthEncoded, 2);

    assertEquals(2, another.getCapacity());
    assertEquals(array.getAddress(), another.getAddress());
    assertEquals(1, another.getByte(0));
    assertEquals(2, another.getByte(1));
  }

  @Test
  void testInitializeByStartAddressZero() {
    DirectByteArray another = new DirectByteArray(0l);
    assertEquals(0, another.getCapacity());
    assertEquals(0, another.getAddress());
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  void testBoundaryCheck(boolean encodeLength) {
    DirectByteArray array = new DirectByteArray(2, encodeLength);
    assertThrows(IndexOutOfBoundsException.class, () -> array.setByte(2, (byte) 123));
    assertThrows(IndexOutOfBoundsException.class, () -> array.setByte(-10, (byte) 123));
    assertThrows(IndexOutOfBoundsException.class, () -> array.getByte(3));
    assertThrows(IndexOutOfBoundsException.class, () -> array.getInt(0)); // cannot read 4 bytes
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  void testResetHandleByCapacity(boolean encodeLength) {
    DirectByteArray array = new DirectByteArray(2, encodeLength);
    long address = array.getAddress();
    long oldAddress = array.init(5, encodeLength);
    assertEquals(address, oldAddress);
    assertEquals(5, array.getCapacity());
    assertTrue(array.getAddress() > 0);
  }

  @Test
  void testResetHandleByCapacityZero() {
    DirectByteArray array = new DirectByteArray(2);
    long address = array.getAddress();
    long oldAddress = array.init(0);
    assertEquals(address, oldAddress);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  void testResetHandleByStartAddress(boolean encodeLength) {
    DirectByteArray another = new DirectByteArray(3, encodeLength);
    DirectByteArray array = new DirectByteArray(2, encodeLength);
    long address = array.getAddress();

    long oldAddress = array.init(another.getAddress(), encodeLength, 3);

    assertEquals(address, oldAddress);
    assertEquals(3, array.getCapacity());
    assertEquals(another.getAddress(), array.getAddress());
  }

  @Test
  void testResetHandleByStartAddressZero() {
    DirectByteArray array = new DirectByteArray(2);
    long address = array.getAddress();

    long oldAddress = array.init(0l);

    assertEquals(address, oldAddress);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  void testResetHandleByStartAddressZero(boolean lengthEncoded) {
    DirectByteArray array = new DirectByteArray(2, lengthEncoded);
    long address = array.getAddress();

    long oldAddress = array.init(0l, lengthEncoded, 2);

    assertEquals(address, oldAddress);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
  }

  @Test
  void testFree() {
    DirectByteArray array = new DirectByteArray(1);
    array.free();
    assertEquals(0, array.getAddress());
    assertEquals(0, array.getCapacity());
    // can't verify the Unsafe method call.
  }

  @Test
  void testFreeEmptyArray() {
    DirectByteArray array = new DirectByteArray(0);
    array.free();
    assertEquals(0, array.getAddress());
    assertEquals(0, array.getCapacity());
    // can't verify the Unsafe method call.
  }

  @Test
  void testInitialValue() {
    int capacity = 10;
    DirectByteArray array = new DirectByteArray(capacity);
    for (int i = 0; i < capacity; i++) {
      assertEquals(0, array.getByte(i));
    }
  }

  @Test
  @Disabled
  public void perfTest() {
    int slotSize = 8 + 8 + 4;
    int n = 10_000_000;

    DirectByteArray array = new DirectByteArray(slotSize * n);
    Random random = new Random();
    for (int i = 0; i < n; i++) {
      int baseOffset = i * slotSize;
      array.setLong(baseOffset, random.nextLong());
      array.setLong(baseOffset + 8, random.nextLong());
      array.setInt(baseOffset + 16, random.nextInt());
    }

    int itr = 10;
    long sum = 0;
    for (int i = 0; i < itr; i++) {
      long start = System.nanoTime();
      for (int j = 0; j < n; j++) {
        int baseOffset = j * slotSize;
        long tsAddress = array.getLong(baseOffset);
        long tagAddress = array.getLong(baseOffset + 8);
        int tagLength = array.getInt(baseOffset + 16);
        sum += tsAddress + tagAddress + tagLength;
      }
      long end = System.nanoTime();
      System.out.println("Time to iterate " + n + " entries " + (end - start) + " ns");
      System.out.println("==========" + sum + "===============");
    }
  }
}
