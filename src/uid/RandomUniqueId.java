// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.uid;

import java.security.SecureRandom;
import org.hbase.async.Bytes;
import net.opentsdb.core.TSDB;

/**
 * Generate Random UIDs to be used as unique ID.
 * Random metric IDs help to distribute hotspots evenly to region servers.
 * It is better to decide whether to use random or serial uid for one type when 
 * the hbase uid table is empty. If the logic to switch between random or serial
 * uid is changed in between writes it will cause frequent id collisions.
 * @since 2.2
 */
public class RandomUniqueId {
  /** Use the SecureRandom class to avoid blocking calls */
  private static SecureRandom random_generator = new SecureRandom(
      Bytes.fromLong(System.currentTimeMillis()));
  
  /** Used to limit UIDs to unsigned longs */
  public static final int MAX_WIDTH = 7;
  
  /**
   * Get the next random metric UID, a positive integer greater than zero. 
   * The default metric ID width is 3 bytes. If it is 3 then  it can return 
   * only up to the max value a 3 byte integer can return, which is 2^31-1. 
   * In that case, even though it is long, its range will be between 0 
   * and 2^31-1.
   * NOTE: The caller is responsible for assuring that the UID hasn't been
   * assigned yet.
   * @return a random UID up to {@link TSDB#metrics_width()} wide
   */
  public static long getRandomUID() {
    return getRandomUID(TSDB.metrics_width());
  }

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