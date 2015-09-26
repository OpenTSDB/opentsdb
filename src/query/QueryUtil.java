// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import net.opentsdb.core.Const;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.Scanner;

/**
 * A simple class with utility methods for executing queries against the storage
 * layer.
 * @since 2.2
 */
public class QueryUtil {
  
  /**
   * Crafts a regular expression for scanning over data table rows and filtering
   * time series that the user doesn't want. At least one of the parameters 
   * must be set and have values.
   * NOTE: This method will sort the group bys.
   * @param group_bys An optional list of tag keys that we want to group on. May
   * be null.
   * @param row_key_literals An optional list of key value pairs to filter on.
   * May be null.
   * @return A regular expression string to pass to the storage layer.
   */
  public static String getRowKeyUIDRegex(final List<byte[]> group_bys, 
      final ByteMap<byte[][]> row_key_literals) {
    if (group_bys != null) {
      Collections.sort(group_bys, Bytes.MEMCMP);
    }
    final short name_width = TSDB.tagk_width();
    final short value_width = TSDB.tagv_width();
    final short tagsize = (short) (name_width + value_width);
    // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
    // and { 4 5 6 9 8 7 }, the regexp will be:
    // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
    final StringBuilder buf = new StringBuilder(
        15  // "^.{N}" + "(?:.{M})*" + "$"
        + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
           * ((row_key_literals == null ? 0 : row_key_literals.size()) + 
               (group_bys == null ? 0 : group_bys.size() * 3))));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the salt, metric ID and timestamp.
       .append(Const.SALT_WIDTH() + TSDB.metrics_width() + Const.TIMESTAMP_BYTES)
       .append("}");

    final Iterator<Entry<byte[], byte[][]>> it = row_key_literals == null ? 
        new ByteMap<byte[][]>().iterator() : row_key_literals.iterator();

    while(it.hasNext()) {
      Entry<byte[], byte[][]> entry = it.hasNext() ? it.next() : null;
      // TODO - This look ahead may be expensive. We need to get some data around
      // whether it's faster for HBase to scan with a look ahead or simply pass
      // the rows back to the TSD for filtering.
      final boolean not_key = 
          entry.getValue() != null && entry.getValue().length == 0;
      
      // Skip any number of tags.
      buf.append("(?:.{").append(tagsize).append("})*");
      if (not_key) {
        // start the lookahead as we have a key we explicitly do not want in the
        // results
        buf.append("(?!");
      }
      buf.append("\\Q");
      
      addId(buf, entry.getKey(), true);
      if (entry.getValue() != null && entry.getValue().length > 0) {  // Add a group_by.
        // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
        buf.append("(?:");
        for (final byte[] value_id : entry.getValue()) {
          if (value_id == null) {
            continue;
          }
          buf.append("\\Q");
          addId(buf, value_id, true);
          buf.append('|');
        }
        // Replace the pipe of the last iteration.
        buf.setCharAt(buf.length() - 1, ')');
      } else {
        buf.append(".{").append(value_width).append('}');  // Any value ID.
      }
      
      if (not_key) {
        // be sure to close off the look ahead
        buf.append(")");
      }
    }
    // Skip any number of tags before the end.
    buf.append("(?:.{").append(tagsize).append("})*$");
    return buf.toString();
  }
  
  /**
   * Creates a regular expression with a list of or'd TUIDs to compare
   * against the rows in storage.
   * @param tsuids The list of TSUIDs to scan for
   * @return A regular expression string to pass to the storage layer.
   */
  public static String getRowKeyTSUIDRegex(final List<String> tsuids) {
    Collections.sort(tsuids);
    
    // first, convert the tags to byte arrays and count up the total length
    // so we can allocate the string builder
    final short metric_width = TSDB.metrics_width();
    int tags_length = 0;
    final ArrayList<byte[]> uids = new ArrayList<byte[]>(tsuids.size());
    for (final String tsuid : tsuids) {
      final String tags = tsuid.substring(metric_width * 2);
      final byte[] tag_bytes = UniqueId.stringToUid(tags);
      tags_length += tag_bytes.length;
      uids.add(tag_bytes);
    }
    
    // Generate a regexp for our tags based on any metric and timestamp (since
    // those are handled by the row start/stop) and the list of TSUID tagk/v
    // pairs. The generated regex will look like: ^.{7}(tags|tags|tags)$
    // where each "tags" is similar to \\Q\000\000\001\000\000\002\\E
    final StringBuilder buf = new StringBuilder(
        13  // "(?s)^.{N}(" + ")$"
        + (tsuids.size() * 11) // "\\Q" + "\\E|"
        + tags_length); // total # of bytes in tsuids tagk/v pairs
    
    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the metric ID and timestamp.
       .append(Const.SALT_WIDTH() + metric_width + Const.TIMESTAMP_BYTES)
       .append("}(");
    
    for (final byte[] tags : uids) {
       // quote the bytes
      buf.append("\\Q");
      addId(buf, tags, true);
      buf.append('|');
    }
    
    // Replace the pipe of the last iteration, close and set
    buf.setCharAt(buf.length() - 1, ')');
    buf.append("$");
    return buf.toString();
  }
  
  /**
   * Compiles an HBase scanner against the main data table
   * @param tsdb The TSDB with a configured HBaseClient
   * @param salt_bucket An optional salt bucket ID for salting the start/stop
   * keys.
   * @param metric The metric to scan for
   * @param start The start time stamp in seconds
   * @param stop The stop timestamp in seconds
   * @param table The table name to scan over
   * @param family The table family to scan over
   * @return A scanner ready for processing.
   */
  public static Scanner getMetricScanner(final TSDB tsdb, final int salt_bucket, 
      final byte[] metric, final int start, final int stop, 
      final byte[] table, final byte[] family) {
    final short metric_width = TSDB.metrics_width();
    final int metric_salt_width = metric_width + Const.SALT_WIDTH();
    final byte[] start_row = new byte[metric_salt_width + Const.TIMESTAMP_BYTES];
    final byte[] end_row = new byte[metric_salt_width + Const.TIMESTAMP_BYTES];
    
    if (Const.SALT_WIDTH() > 0) {
      final byte[] salt = RowKey.getSaltBytes(salt_bucket);
      System.arraycopy(salt, 0, start_row, 0, Const.SALT_WIDTH());
      System.arraycopy(salt, 0, end_row, 0, Const.SALT_WIDTH());
    }
    
    Bytes.setInt(start_row, start, metric_salt_width);
    Bytes.setInt(end_row, stop, metric_salt_width);
    
    System.arraycopy(metric, 0, start_row, Const.SALT_WIDTH(), metric_width);
    System.arraycopy(metric, 0, end_row, Const.SALT_WIDTH(), metric_width);
    
    final Scanner scanner = tsdb.getClient().newScanner(table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(family);
    return scanner;
  }
  
  /**
   * Appends the given UID to the given regular expression buffer
   * @param buf The String buffer to modify
   * @param id The UID to add
   * @param close Whether or not to append "\\E" to the end
   */
  public static void addId(final StringBuilder buf, final byte[] id, 
      final boolean close) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a litteral \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
    if (close) {
      buf.append("\\E");
    }
  }

  /**
   * Little helper to print out the regular expression by converting the UID
   * bytes to an array.
   * @param regexp The regex string to print to the debug log
   */
  public static String byteRegexToString(final String regexp) {
    final StringBuilder buf = new StringBuilder();
    for (int i = 0; i < regexp.length(); i++) {
      if (i > 0 && regexp.charAt(i - 1) == 'Q') {
        if (regexp.charAt(i - 3) == '*') {
          // tagk
          byte[] tagk = new byte[TSDB.tagk_width()];
          for (int x = 0; x < TSDB.tagk_width(); x++) {
            tagk[x] = (byte)regexp.charAt(i + x);
          }
          i += TSDB.tagk_width();
          buf.append(Arrays.toString(tagk));
        } else {
          // tagv
          byte[] tagv = new byte[TSDB.tagv_width()];
          for (int x = 0; x < TSDB.tagv_width(); x++) {
            tagv[x] = (byte)regexp.charAt(i + x);
          }
          i += TSDB.tagv_width();
          buf.append(Arrays.toString(tagv));
        }
      } else {
        buf.append(regexp.charAt(i));
      }
    }
    return buf.toString();
  }
}
