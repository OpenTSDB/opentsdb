// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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
package net.opentsdb.uid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import net.opentsdb.utils.Bytes;

import org.junit.Test;

public final class TestRandomUniqueId {

  @Test
  public void getRandomUIDMetricWidth() throws Exception {
    generateAndTestUID(3, 100);
  }
  
  @Test
  public void getRandomUID1Byte() throws Exception {
    generateAndTestUID(1, 100);
  }
  
  @Test
  public void getRandomUID2Byte() throws Exception {
    generateAndTestUID(2, 100);
  }
  
  @Test
  public void getRandomUID3Byte() throws Exception {
    generateAndTestUID(3, 100);
  }
  
  @Test
  public void getRandomUID4Byte() throws Exception {
    generateAndTestUID(4, 100);
  }
  
  @Test
  public void getRandomUID5Byte() throws Exception {
    generateAndTestUID(5, 100);
  }
  
  @Test
  public void getRandomUID6Byte() throws Exception {
    generateAndTestUID(6, 100);
  }
  
  @Test
  public void getRandomUID7Byte() throws Exception {
    generateAndTestUID(7, 100);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidWidth() {
    RandomUniqueId.getRandomUID(8);
  }
  
  @Test(expected = NegativeArraySizeException.class)
  public void testNegativeWidth() {
    RandomUniqueId.getRandomUID(-1);
  }
  
  // if you pass in a width of 0 it will always return 1
  @Test
  public void testZeroWidth() {
    assertEquals(1L, RandomUniqueId.getRandomUID(0));
  }
  
  /**
   * Runs the test n times and makes sure it's greater than 0 and less than or
   * equal to the max value on {@link width} bytes.
   * @param width The number of bytes to generate a UID for
   * @param n How many times to run the tests
   */
  private void generateAndTestUID(final int width, final int n) {
    final long max_value = getMax(width);
    for (int i = 0; i < n; i++) {
      long uid = RandomUniqueId.getRandomUID(width);
      assertTrue(uid > 0 && uid <= max_value);
    }
  }
  
  /**
   * Simple helper to calculate the max value for any width of long
   * @param width The width of the byte array we're comparing
   * @return The maximum integer value on {@link width} bytes.
   */
  private long getMax(final int width) {
    if (width > 7) {
      throw new IllegalArgumentException("Can't use a width of [" + width + 
          "] in this unit test");
    }
    final byte[] value = new byte[8];
    for (int i = 0; i < width; i++) {
      value[8 - (i + 1)] = (byte) 0xFF;
    }
    return Bytes.getLong(value);
  }
}
