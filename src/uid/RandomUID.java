// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
import net.opentsdb.core.TSDB;

/*
 * Generate Random UIDs to be used as unique id.
 * Random metric ids help to distribute hotspots evenly to region servers.
 * It is better to decide whether to use random or serial uid for one type when 
 * the hbase uid table is empty. If the logic to switch between random or serial
 * uid is changed in between writes it will cause frequent id collisions.
 */
public class RandomUID {
  private static SecureRandom random = new SecureRandom();
  public static final int MAX_WIDTH = 7;
  
  /**
   * Get the next random UID. Right now it uses 3 bytes as the metric with is 
   * 3. If it is 3 then it can return only upto the max value a 3 byte integer
   * can return, which is 2^31-1. In that case, even though it is long, 
   * its range will be between 0 and 2^31-1
   * @return random UID
   */
  public static long getRandomUID() {
    return getRandomUID(TSDB.metrics_width());
  }

  /**
   * Get the next random UID. It creates random bytes, then convert it to an
   * unsigned long
   * @param width number of bytes to randomize, it can not be larger 
   * than a long
   * @return random UID
   * @throws throws IllegalArgumentException if the width is larger than 8
   * bytes
   */
  public static long getRandomUID(final int width) {
    if (width > MAX_WIDTH) {
      throw new IllegalArgumentException("It expects an unsigned long "
          + "random integer, it can not be larger than " + MAX_WIDTH + 
          " bytes width");
    }
    byte[] bytes= new byte[width];
    random.nextBytes(bytes);
    return convertToUnsignedLong(bytes);
  }

  /**
   * Convert byte array with first the most significant to unsigned long
   * @return long storing unsigned integer value
   */
  private static long convertToUnsignedLong(byte[] bytes) {
    long value = 0;
    
    for (int i = 0; i<bytes.length;i++){
      value <<= 8;
      value |= bytes[i] & 0xFF;
    }
    
    return value;
  } 
}
