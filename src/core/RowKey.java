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

import org.hbase.async.Bytes;

import com.stumbleupon.async.Deferred;

/** Helper functions to deal with the row key. */
final public class RowKey {

  private RowKey() {
    // Can't create instances of this utility class.
  }

  /**
   * Extracts the name of the metric ID contained in a row key.
   * @param tsdb The TSDB to use.
   * @param row The actual row key.
   * @return The name of the metric.
   */
  static String metricName(final TSDB tsdb, final byte[] row) {
    try {
      return metricNameAsync(tsdb, row).joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }

  /**
   * Extracts the name of the metric ID contained in a row key.
   * @param tsdb The TSDB to use.
   * @param row The actual row key.
   * @return The name of the metric.
   * @since 1.2
   */
  static Deferred<String> metricNameAsync(final TSDB tsdb, final byte[] row) {
    final byte[] id = Arrays.copyOfRange(row, 0, tsdb.metrics.width());
    return tsdb.metrics.getNameAsync(id);
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
    final long base_time;
    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((timestamp / 1000) - 
          ((timestamp / 1000) % Const.MAX_TIMESPAN));
    } else {
      base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
    }
    final byte[] row = new byte[tsuid.length + Const.TIMESTAMP_BYTES];
    System.arraycopy(tsuid, 0, row, 0, TSDB.metrics_width());
    Bytes.setInt(row, (int) base_time, TSDB.metrics_width());
    System.arraycopy(tsuid, TSDB.metrics_width(), row, 
        TSDB.metrics_width() + Const.TIMESTAMP_BYTES, 
        tsuid.length - TSDB.metrics_width());
    return row;
  }
}
