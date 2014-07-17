// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import java.nio.charset.Charset;

import javax.xml.bind.DatatypeConverter;

import org.junit.Ignore;

/**
 * Mock HBase implementation useful in testing calls to and from storage with
 * actual pretend data. The underlying data store is an incredibly ugly nesting
 * of ByteMaps from AsyncHbase so it stores and orders byte arrays similar to 
 * HBase. A MockBase instance represents a SINGLE table in HBase but it provides
 * support for column families and timestamped entries.
 * <p>
 * It's not a perfect mock but is useful for the majority of unit tests. Gets,
 * puts, cas, deletes and scans are currently supported. See notes for each
 * inner class below about what does and doesn't work.
 * <p>
 * Regarding timestamps, whenever you execute an RPC request, the 
 * {@code current_timestamp} will be incremented by one millisecond. By default
 * the timestamp starts at 1/1/2014 00:00:00 but you can set it to any value
 * at any time. If a PutRequest comes in with a specific time, that time will 
 * be stored and the timestamp will not be incremented.
 * <p>
 * <b>Warning:</b> To use this class, you need to prepare the classes for testing
 * with the @PrepareForTest annotation. The classes you need to prepare are:
 * <ul><li>TSDB</li>
 * <li>HBaseStore</li>
 * <li>GetRequest</li>
 * <li>PutRequest</li>
 * <li>KeyValue</li>
 * <li>Scanner</li>
 * <li>DeleteRequest</li>
 * <li>AtomicIncrementRequest</li></ul>
 * @since 2.0
 */
@Ignore
public final class MockBase {
  private static final Charset ASCII = Charset.forName("ISO-8859-1");

  /**
   * Helper to convert a hex encoded string into a byte array.
   * <b>Warning:</b> This method won't pad the string to make sure it's an
   * even number of bytes.
   * @param bytes The hex encoded string to convert
   * @return A byte array from the hex string
   * @throws IllegalArgumentException if the string contains illegal characters
   * or can't be converted.
   */
  public static byte[] stringToBytes(final String bytes) {
    return DatatypeConverter.parseHexBinary(bytes);
  }
  
  /** @return Returns the ASCII character set */
  public static Charset ASCII() {
    return ASCII;
  }
  
  /**
   * Concatenates byte arrays into one big array
   * @param arrays Any number of arrays to concatenate
   * @return The concatenated array
   */
  public static byte[] concatByteArrays(final byte[]... arrays) {
    int len = 0;
    for (final byte[] array : arrays) {
      len += array.length;
    }
    final byte[] result = new byte[len];
    len = 0;
    for (final byte[] array : arrays) {
      System.arraycopy(array, 0, result, len, array.length);
      len += array.length;
    }
    return result;
  }
}
