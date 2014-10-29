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
package net.opentsdb.core;

import java.util.Arrays;
import java.util.Map;

import net.opentsdb.storage.hbase.HBaseStore;

import org.hbase.async.Bytes;

import com.stumbleupon.async.Deferred;

/** Helper functions to deal with the row key. */
final public class RowKey {
  private RowKey() {
    // Can't create instances of this utility class.
  }

  /** Extracts the metric id from a row key */
  public static byte[] metric(final byte[] row) {
    return Arrays.copyOfRange(row, 0, Const.METRICS_WIDTH);
  }

  /** Extracts the timestamp from a row key.  */
  public static long baseTime(final byte[] row) {
    return Bytes.getUnsignedInt(row, Const.METRICS_WIDTH);
  }

  /** Extracts the tag key and value ids from a row key */
  public static Map<byte[], byte[]> tags(final byte[] row) {
    final short name_width = Const.TAG_NAME_WIDTH;
    final short value_width = Const.TAG_VALUE_WIDTH;
    final short tag_bytes = (short) (name_width + value_width);
    final short metric_ts_bytes = Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES;

    final Map<byte[], byte[]> tags = new Bytes.ByteMap<byte[]>();

    for (short pos = metric_ts_bytes; pos < row.length; pos += tag_bytes) {
      final byte[] tmp_name = new byte[name_width];
      final byte[] tmp_value = new byte[value_width];

      System.arraycopy(row, pos, tmp_name, 0, name_width);
      System.arraycopy(row, pos + name_width, tmp_value, 0, value_width);

      tags.put(tmp_name, tmp_value);
    }

    return tags;
  }

  /**
   * Extracts the TSUID from a storage row key that includes the timestamp.
   * @param row_key The row key to process
   * @return The TSUID
   * @throws ArrayIndexOutOfBoundsException if the row_key is invalid
   */
  public static byte[] tsuid(final byte[] row_key) {
    final short metric_width = Const.METRICS_WIDTH;
    final short timestamp_width = Const.TIMESTAMP_BYTES;

    final byte[] tsuid = new byte[row_key.length - timestamp_width];
    System.arraycopy(row_key, 0, tsuid, 0, metric_width);
    System.arraycopy(row_key, metric_width + timestamp_width, tsuid,
            metric_width, row_key.length - metric_width - timestamp_width);

    return tsuid;
  }
  
  /**
   * Generates a row key given a TSUID and an absolute timestamp. The timestamp
   * will be normalized to an hourly base time.
   * @param tsdb The TSDB to use for fetching tag widths
   * @param tsuid The TSUID to use for the key
   * @param timestamp An absolute time from which we generate the row base time
   * @return A row key for use in fetching data from OpenTSDB
   * @since 2.0
   */
  public static byte[] rowKeyFromTSUID(final TSDB tsdb, final byte[] tsuid, 
      final long timestamp) {
    final long base_time = HBaseStore.buildBaseTime(timestamp);
    final byte[] row = new byte[tsuid.length + Const.TIMESTAMP_BYTES];
    System.arraycopy(tsuid, 0, row, 0, Const.METRICS_WIDTH);
    Bytes.setInt(row, (int) base_time, Const.METRICS_WIDTH);
    System.arraycopy(tsuid, Const.METRICS_WIDTH, row,
        Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES,
        tsuid.length - Const.METRICS_WIDTH);
    return row;
  }

  /**
   * Checks that the given row key matches the given metric.
   * @param key
   * @param metric
   * @throws net.opentsdb.core.IllegalDataException if the metric doesn't match.
   */
  public static void checkMetric(final byte[] key, final byte[] metric) {
    if (Bytes.memcmp(metric, key, 0, Const.METRICS_WIDTH) != 0) {
      throw new IllegalDataException(
              "The HBase row key " + Arrays.toString(key) + " does not match " +
                      "the required metric " + Arrays.toString(metric));
    }
  }
}
