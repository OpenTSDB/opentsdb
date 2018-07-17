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

import java.security.SecureRandom;
import net.opentsdb.utils.Bytes;

/**
 * Generate Random UIDs to be used as unique ID.
 * Random metric IDs help to distribute hotspots evenly to region servers.
 * It is better to decide whether to use random or serial uid for one type when 
 * the hbase uid table is empty. If the logic to switch between random or serial
 * uid is changed in between writes it will cause frequent id collisions.
 * 
 * @since 2.2
 */
public class RandomUniqueId {
  /** Use the SecureRandom class to avoid blocking calls */
  private static SecureRandom random_generator = new SecureRandom(
      Bytes.fromLong(System.currentTimeMillis()));
  
  /** Used to limit UIDs to unsigned longs */
  public static final int MAX_WIDTH = 7;
  
  /**
   * Get the next random UID. It creates random bytes, then convert it to an
   * unsigned long.
   * @param width Number of bytes to randomize, it can not be larger 
   * than {@link MAX_WIDTH} bytes wide
   * @return a randomly UID
   * @throws IllegalArgumentException if the width is larger than 
   * {@link MAX_WIDTH} bytes
   */
  public static long getRandomUID(final int width) {
    if (width > MAX_WIDTH) {
      throw new IllegalArgumentException("Expecting to return an unsigned long "
          + "random integer, it can not be larger than " + MAX_WIDTH + 
          " bytes wide");
    }
    final byte[] bytes = new byte[width];
    random_generator.nextBytes(bytes);

    long value = 0;
    for (int i = 0; i<bytes.length; i++){
      value <<= 8;
      value |= bytes[i] & 0xFF;
    }

    // make sure we never return 0 as a UID
    return value != 0 ? value : value + 1;
  }
}