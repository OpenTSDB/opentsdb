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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDirectIntArray {

  @Test
  void testEmptyArray() {
    DirectIntArray array = new DirectIntArray(0);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
  }

  @Test
  public void testInitializationByCapacity() {
    DirectIntArray array = new DirectIntArray(5);
    assertEquals(5, array.getCapacity());
    assertTrue(array.getAddress() > 0);
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  void testReadAndWriteIntegers(boolean encodeLength) {
    DirectIntArray array = new DirectIntArray(50, encodeLength);

    array.set(0, Integer.MAX_VALUE);
    array.set(1, Integer.MIN_VALUE);
    array.set(2, 12345);
    array.set(3, -12345);

    assertEquals(Integer.MAX_VALUE, array.get(0));
    assertEquals(Integer.MIN_VALUE, array.get(1));
    assertEquals(12345, array.get(2));
    assertEquals(-12345, array.get(3));

    assertEquals(0, array.get(4));
  }

  @Test
  public void testInitializationByStartAddress() {
    DirectIntArray array = new DirectIntArray(5);
    DirectIntArray another = new DirectIntArray(array.getAddress());

    assertEquals(5, another.getCapacity());
    assertEquals(array.getAddress(), another.getAddress());
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  public void testInitializationByStartAddress(boolean lengthEncoded) {
    DirectIntArray array = new DirectIntArray(5, lengthEncoded);
    DirectIntArray another = new DirectIntArray(array.getAddress(), lengthEncoded, 5);

    assertEquals(5, another.getCapacity());
    assertEquals(array.getAddress(), another.getAddress());
  }

  @Test
  void testInitializeByStartAddressZero() {
    DirectIntArray another = new DirectIntArray(0l);
    assertEquals(0, another.getCapacity());
    assertEquals(0, another.getAddress());
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  void testBoundaryCheck(boolean encodeLength) {
    DirectIntArray array = new DirectIntArray(2, encodeLength);
    assertThrows(IndexOutOfBoundsException.class, () -> array.set(2, 123));
    assertThrows(IndexOutOfBoundsException.class, () -> array.set(-10, 123));
    assertThrows(IndexOutOfBoundsException.class, () -> array.get(3));
    assertThrows(IndexOutOfBoundsException.class, () -> array.get(-1));
  }

  @Test
  void testResetHandleByCapacity() {
    DirectIntArray array = new DirectIntArray(2);
    long address = array.getAddress();
    long oldAddress = array.init(5);
    assertEquals(address, oldAddress);
    assertEquals(5, array.getCapacity());
    assertTrue(array.getAddress() > 0);
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  void testResetHandleByCapacity(boolean encodeLength) {
    DirectIntArray array = new DirectIntArray(2, encodeLength);
    long address = array.getAddress();
    long oldAddress = array.init(5, encodeLength);
    assertEquals(address, oldAddress);
    assertEquals(5, array.getCapacity());
    assertTrue(array.getAddress() > 0);
  }

  @Test
  void testResetHandleByCapacityZero() {
    DirectIntArray array = new DirectIntArray(2);
    long address = array.getAddress();
    long oldAddress = array.init(0);
    assertEquals(address, oldAddress);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
  }

  @Test
  void testResetHandleByStartAddress() {
    DirectIntArray another = new DirectIntArray(3);
    DirectIntArray array = new DirectIntArray(2);
    long address = array.getAddress();

    long oldAddress = array.init(another.getAddress());

    assertEquals(address, oldAddress);
    assertEquals(3, array.getCapacity());
    assertEquals(another.getAddress(), array.getAddress());
  }

  @ParameterizedTest(name = "[{index}] encode length: {0}")
  @CsvSource({"true", "false"})
  void testResetHandleByStartAddress(boolean lengthEncoded) {
    DirectIntArray another = new DirectIntArray(3, lengthEncoded);
    DirectIntArray array = new DirectIntArray(2, lengthEncoded);
    long address = array.getAddress();

    long oldAddress = array.init(another.getAddress(), lengthEncoded, 3);

    assertEquals(address, oldAddress);
    assertEquals(3, array.getCapacity());
    assertEquals(another.getAddress(), array.getAddress());
  }

  @Test
  void testResetHandleByStartAddressZero() {
    DirectIntArray array = new DirectIntArray(2);
    long address = array.getAddress();

    long oldAddress = array.init(0l);

    assertEquals(address, oldAddress);
    assertEquals(0, array.getCapacity());
    assertEquals(0, array.getAddress());
  }

  @Test
  void testFree() {
    DirectIntArray array = new DirectIntArray(1);
    array.free();
    assertEquals(0, array.getAddress());
    assertEquals(0, array.getCapacity());
    // can't verify the Unsafe method call.
  }

  @Test
  void testFreeEmptyArray() {
    DirectIntArray array = new DirectIntArray(0);
    array.free();
    assertEquals(0, array.getAddress());
    assertEquals(0, array.getCapacity());
    // can't verify the Unsafe method call.
  }

}
