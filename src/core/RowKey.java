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
import java.util.Comparator;

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
   * @throws NoSuchUniqueId if the UID could not resolve to a string
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
   * @return A deferred to wait on that will return the name of the metric.
   * @throws IllegalArgumentException if the row key is too short due to missing
   * salt or metric or if it's null/empty.
   * @throws NoSuchUniqueId if the UID could not resolve to a string
   * @since 1.2
   */
  public static Deferred<String> metricNameAsync(final TSDB tsdb, 
      final byte[] row) {
    if (row == null || row.length < 1) {
      throw new IllegalArgumentException("Row key cannot be null or empty");
    }
    if (row.length < Const.SALT_WIDTH() + tsdb.metrics.width()) {
      throw new IllegalArgumentException("Row key is too short"); 
    }
    final byte[] id = Arrays.copyOfRange(
        row, Const.SALT_WIDTH(), tsdb.metrics.width() + Const.SALT_WIDTH());
    return tsdb.metrics.getNameAsync(id);
  }
  
  /**
   * Generates a row key given a TSUID and an absolute timestamp. The timestamp
   * will be normalized to an hourly base time. If salting is enabled then
   * empty salt bytes will be prepended to the key and must be filled in later.
   * @param tsdb The TSDB to use for fetching tag widths
   * @param tsuid The TSUID to use for the key
   * @param timestamp An absolute time from which we generate the row base time
   * @return A row key for use in fetching data from OpenTSDB
   * @throws IllegalArgumentException if the TSUID is too short, i.e. doesn't
   * contain a metric
   * @since 2.0
   */
  public static byte[] rowKeyFromTSUID(final TSDB tsdb, final byte[] tsuid, 
      final long timestamp) {
    if (tsuid.length < tsdb.metrics.width()) {
      throw new IllegalArgumentException("TSUID appears to be missing the metric");
    }
    final long base_time;
    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((timestamp / 1000) - 
          ((timestamp / 1000) % Const.MAX_TIMESPAN));
    } else {
      base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
    }
    final byte[] row = 
        new byte[Const.SALT_WIDTH() + tsuid.length + Const.TIMESTAMP_BYTES];
    System.arraycopy(tsuid, 0, row, Const.SALT_WIDTH(), tsdb.metrics.width());
    Bytes.setInt(row, (int) base_time, Const.SALT_WIDTH() + tsdb.metrics.width());
    System.arraycopy(tsuid, tsdb.metrics.width(), row, 
        Const.SALT_WIDTH() + tsdb.metrics.width() + Const.TIMESTAMP_BYTES, 
        tsuid.length - tsdb.metrics.width());
    RowKey.prefixKeyWithSalt(row);
    return row;
  }

  /**
   * Returns the byte array for the given salt id
   * WARNING: Don't use this one unless you know what you're doing. It's here
   * for unit testing.
   * @param bucket The ID of the bucket to get the salt for
   * @return The salt as a byte array based on the width in bytes
   * @since 2.2
   */
  public static byte[] getSaltBytes(final int bucket) {
    final byte[] bytes = new byte[Const.SALT_WIDTH()];
    int shift = 0;
    for (int i = 1;i <= Const.SALT_WIDTH(); i++) {
      bytes[Const.SALT_WIDTH() - i] = (byte) (bucket >>> shift);
      shift += 8;
    }
    return bytes;
  }

  /**
   * Calculates and writes an array of one or more salt bytes at the front of
   * the given row key. 
   * 
   * The salt is calculated by taking the Java hash code of the metric and 
   * tag UIDs and returning a modulo based on the number of salt buckets.
   * The result will always be a positive integer from 0 to salt buckets.
   * 
   * NOTE: The row key passed in MUST have allocated the {@link width} number of
   * bytes at the front of the row key or this call will overwrite data.
   * 
   * WARNING: If the width is set to a positive value, then the bucket must be
   * at least 1 or greater.
   * @param row_key The pre-allocated row key to write the salt to
   * @since 2.2
   */
  public static void prefixKeyWithSalt(final byte[] row_key) {
    if (Const.SALT_WIDTH() > 0) {
      if (row_key.length < (Const.SALT_WIDTH() + TSDB.metrics_width()) || 
        (Bytes.memcmp(row_key, new byte[Const.SALT_WIDTH() + TSDB.metrics_width()], 
            Const.SALT_WIDTH(), TSDB.metrics_width()) == 0)) {
        // ^ Don't salt the global annotation row, leave it at zero
        return;
      }
      final int tags_start = Const.SALT_WIDTH() + TSDB.metrics_width() + 
          Const.TIMESTAMP_BYTES;
      
      // we want the metric and tags, not the timestamp
      final byte[] salt_base = 
          new byte[row_key.length - Const.SALT_WIDTH() - Const.TIMESTAMP_BYTES];
      System.arraycopy(row_key, Const.SALT_WIDTH(), salt_base, 0, TSDB.metrics_width());
      System.arraycopy(row_key, tags_start,salt_base, TSDB.metrics_width(), 
          row_key.length - tags_start);
      int modulo = Arrays.hashCode(salt_base) % Const.SALT_BUCKETS();
      if (modulo < 0) {
        // make sure we return a positive salt.
        modulo = modulo * -1;
      }
    
      final byte[] salt = getSaltBytes(modulo);
      System.arraycopy(salt, 0, row_key, 0, Const.SALT_WIDTH());
    } // else salting is disabled so it's a no-op
  }

  /**
   * Checks a row key to determine if it contains the metric UID. If salting is
   * enabled, we skip the salt bytes.
   * @param metric The metric UID to match
   * @param row_key The row key to match on
   * @return 0 if the two arrays are identical, otherwise the difference
   * between the first two different bytes (treated as unsigned), otherwise
   * the different between their lengths.
   * @throws IndexOutOfBoundsException if either array isn't large enough.
   */
  public static int rowKeyContainsMetric(final byte[] metric, 
      final byte[] row_key) {
    int idx = Const.SALT_WIDTH();
    for (int i = 0; i < metric.length; i++, idx++) {
      if (metric[i] != row_key[idx]) {
        return (metric[i] & 0xFF) - (row_key[idx] & 0xFF);  // "promote" to unsigned.
      }
    }
    return 0;
  }
  
  /**
   * A comparator that ignores the salt in row keys
   */
  public static class SaltCmp implements Comparator<byte[]> {
    public int compare(final byte[] a, final byte[] b) {
      final int length = Math.min(a.length, b.length);
      if (a == b) {  // Do this after accessing a.length and b.length
        return 0;    // in order to NPE if either a or b is null.
      }
      // Skip salt
      for (int i = Const.SALT_WIDTH(); i < length; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      return a.length - b.length;
    }
  }
}
