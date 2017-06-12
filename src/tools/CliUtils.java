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
import java.util.List;

import net.opentsdb.core.Const;
import net.opentsdb.core.Internal;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
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
  static final byte[] METRICS_META = "metric_meta".getBytes(Const.ASCII_CHARSET);
  /** Qualifier for tagk meta data */
  static final byte[] TAGK_META = "tagk_meta".getBytes(Const.ASCII_CHARSET);
  /** Qualifier for tagv meta data */
  static final byte[] TAGV_META = "tagv_meta".getBytes(Const.ASCII_CHARSET);
  /** Qualifier for metrics UIDs */
  static final byte[] METRICS = "metrics".getBytes(Const.ASCII_CHARSET);
  /** Qualifier for tagk UIDs */
  static final byte[] TAGK = "tagk".getBytes(Const.ASCII_CHARSET);
  /** Qualifier for tagv UIDs */
  static final byte[] TAGV = "tagv".getBytes(Const.ASCII_CHARSET);

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
    get.family("id".getBytes(Const.ASCII_CHARSET));
    get.qualifier("metrics".getBytes(Const.ASCII_CHARSET));
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
   * Generates a list of Scanners to use for iterating over the full TSDB 
   * data table. If salting is enabled then {@link Const.SaltBukets()} scanners
   * will be returned. If salting is disabled then {@link num_scanners} 
   * scanners will be returned.
   * @param tsdb The TSDB to generate scanners from
   * @param num_scanners The max number of scanners if salting is disabled
   * @return A list of scanners to use for scanning the table.
   */
  static final List<Scanner> getDataTableScanners(final TSDB tsdb, 
      final int num_scanners) {
    if (num_scanners < 1) {
      throw new IllegalArgumentException(
          "Number of scanners must be 1 or more: " + num_scanners);
    }
    // TODO - It would be neater to get a list of regions then create scanners
    // on those boundaries. We'll have to modify AsyncHBase for that to avoid
    // creating lots of custom HBase logic in here.
    final short metric_width = TSDB.metrics_width();
    final List<Scanner> scanners = new ArrayList<Scanner>();    
    
    if (Const.SALT_WIDTH() > 0) {
      // salting is enabled so we'll create one scanner per salt for now
      byte[] start_key = HBaseClient.EMPTY_ARRAY;
      byte[] stop_key = HBaseClient.EMPTY_ARRAY;
      
      for (int i = 1; i < Const.SALT_BUCKETS() + 1; i++) {
        // move stop key to start key
        if (i > 1) {
          start_key = Arrays.copyOf(stop_key, stop_key.length);
        }
        
        if (i >= Const.SALT_BUCKETS()) {
          stop_key = HBaseClient.EMPTY_ARRAY;
        } else {
          stop_key = RowKey.getSaltBytes(i);
        }
        final Scanner scanner = tsdb.getClient().newScanner(tsdb.dataTable());
        scanner.setStartKey(Arrays.copyOf(start_key, start_key.length));
        scanner.setStopKey(Arrays.copyOf(stop_key, stop_key.length));
        scanner.setFamily(TSDB.FAMILY());
        scanners.add(scanner);
      }
      
    } else {
      // No salt, just go by the max metric ID
      long max_id = CliUtils.getMaxMetricID(tsdb);
      if (max_id < 1) {
        max_id = Internal.getMaxUnsignedValueOnBytes(metric_width);
      }
      final long quotient = max_id % num_scanners == 0 ? max_id / num_scanners : 
        (max_id / num_scanners) + 1;
      
      byte[] start_key = HBaseClient.EMPTY_ARRAY;
      byte[] stop_key = new byte[metric_width];

      for (int i = 0; i < num_scanners; i++) {
        // move stop key to start key
        if (i > 0) {
          start_key = Arrays.copyOf(stop_key, stop_key.length);
        }
        
        // setup the next stop key
        final byte[] stop_id;
        if ((i +1) * quotient > max_id) {
          stop_id = null;
        } else {
          stop_id = Bytes.fromLong((i + 1) * quotient);
        }
        if ((i +1) * quotient >= max_id) {
          stop_key = HBaseClient.EMPTY_ARRAY;
        } else {
          System.arraycopy(stop_id, stop_id.length - metric_width, stop_key, 
              0, metric_width);
        }
        
        final Scanner scanner = tsdb.getClient().newScanner(tsdb.dataTable());
        scanner.setStartKey(Arrays.copyOf(start_key, start_key.length));
        if (stop_key != null) {
          scanner.setStopKey(Arrays.copyOf(stop_key, stop_key.length));
        }
        scanner.setFamily(TSDB.FAMILY());
        scanners.add(scanner);
      }
      
    }
    
    return scanners;
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
