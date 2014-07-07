// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

/**
 * Various utilities shared amongst the CLI tools.
 * @since 2.1
 */
final class CliUtils {
  /** Function used to convert a String to a byte[]. */
  static final Method toBytes;
  /** Function used to convert a byte[] to a String. */
  static final Method fromBytes;
  /** Charset used to convert Strings to byte arrays and back. */
  static final Charset CHARSET;
  /** The single column family used by this class. */
  static final byte[] ID_FAMILY;
  /** The single column family used by this class. */
  static final byte[] NAME_FAMILY;
  /** Row key of the special row used to track the max ID already assigned. */
  static final byte[] MAXID_ROW;  
  static {
    final Class<UniqueId> uidclass = UniqueId.class;
    try {
      // Those are all implementation details so they're not part of the
      // interface.  We access them anyway using reflection.  I think this
      // is better than marking those and adding a javadoc comment
      // "THIS IS INTERNAL DO NOT USE".  If only Java had C++'s "friend" or
      // a less stupid notion of a package.
      Field f;
      f = uidclass.getDeclaredField("CHARSET");
      f.setAccessible(true);
      CHARSET = (Charset) f.get(null);
      f = uidclass.getDeclaredField("ID_FAMILY");
      f.setAccessible(true);
      ID_FAMILY = (byte[]) f.get(null);
      f = uidclass.getDeclaredField("NAME_FAMILY");
      f.setAccessible(true);
      NAME_FAMILY = (byte[]) f.get(null);
      f = uidclass.getDeclaredField("MAXID_ROW");
      f.setAccessible(true);
      MAXID_ROW = (byte[]) f.get(null);
      toBytes = uidclass.getDeclaredMethod("toBytes", String.class);
      toBytes.setAccessible(true);
      fromBytes = uidclass.getDeclaredMethod("fromBytes", byte[].class);
      fromBytes.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }
  /** Qualifier for metrics meta data */
  static final byte[] METRICS_META = "metric_meta".getBytes(CHARSET);
  /** Qualifier for tagk meta data */
  static final byte[] TAGK_META = "tagk_meta".getBytes(CHARSET);
  /** Qualifier for tagv meta data */
  static final byte[] TAGV_META = "tagv_meta".getBytes(CHARSET);
  /** Qualifier for metrics UIDs */
  static final byte[] METRICS = "metrics".getBytes(CHARSET);
  /** Qualifier for tagk UIDs */
  static final byte[] TAGK = "tagk".getBytes(CHARSET);
  /** Qualifier for tagv UIDs */
  static final byte[] TAGV = "tagv".getBytes(CHARSET);

  /**
   * Returns the max metric ID from the UID table
   * @param tsdb The TSDB to use for data access
   * @return The max metric ID as an integer value, may be 0 if the UID table
   * hasn't been initialized or is missing the UID row or metrics column.
   * @throws IllegalStateException if the UID column can't be found or couldn't
   * be parsed
   */
  static long getMaxMetricID(final TSDB tsdb) {
    // first up, we need the max metric ID so we can split up the data table
    // amongst threads.
    final GetRequest get = new GetRequest(tsdb.uidTable(), new byte[] { 0 });
    get.family("id".getBytes(CHARSET));
    get.qualifier("metrics".getBytes(CHARSET));
    ArrayList<KeyValue> row;
    try {
      row = tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return 0;
      }
      final byte[] id_bytes = row.get(0).value();
      if (id_bytes.length != 8) {
        throw new IllegalStateException("Invalid metric max UID, wrong # of bytes");
      }
      return Bytes.getLong(id_bytes);
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    }
  }
  
  /**
   * Returns a scanner set to iterate over a range of metrics in the main 
   * tsdb-data table.
   * @param tsdb The TSDB to use for data access
   * @param start_id A metric ID to start scanning on
   * @param end_id A metric ID to end scanning on
   * @return A scanner on the "t" CF configured for the specified range
   * @throws HBaseException if something goes pear shaped
   */
  static final Scanner getDataTableScanner(final TSDB tsdb, final long start_id, 
      final long end_id) throws HBaseException {
    final short metric_width = TSDB.metrics_width();
    final byte[] start_row = 
      Arrays.copyOfRange(Bytes.fromLong(start_id), 8 - metric_width, 8);
    final byte[] end_row = 
      Arrays.copyOfRange(Bytes.fromLong(end_id), 8 - metric_width, 8);

    final Scanner scanner = tsdb.getClient().newScanner(tsdb.dataTable());
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(TSDB.FAMILY());
    return scanner;
  }
  
  /**
   * Invokes the reflected {@code UniqueId.toBytes()} method with the given
   * string  using the UniqueId character set.
   * @param s The string to convert to a byte array
   * @return The byte array
   * @throws RuntimeException if reflection failed
   */
  static byte[] toBytes(final String s) {
    try {
      return (byte[]) toBytes.invoke(null, s);
    } catch (Exception e) {
      throw new RuntimeException("toBytes=" + toBytes, e);
    }
  }

  /**
   * Invokces the reflected {@code UnqieuiId.fromBytes()} method with the given
   * byte array using the UniqueId character set.
   * @param b The byte array to convert to a string
   * @return The string 
   * @throws RuntimeException if reflection failed
   */
  static String fromBytes(final byte[] b) {
    try {
      return (String) fromBytes.invoke(null, b);
    } catch (Exception e) {
      throw new RuntimeException("fromBytes=" + fromBytes, e);
    }
  }
}
