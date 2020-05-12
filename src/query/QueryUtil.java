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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import net.opentsdb.core.Const;
import net.opentsdb.core.Internal;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes;
import org.hbase.async.FuzzyRowFilter;
import org.hbase.async.FuzzyRowFilter.FuzzyFilterPair;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.FilterList.Operator;
import org.hbase.async.FilterList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.hbase.async.Scanner;

/**
 * A simple class with utility methods for executing queries against the storage
 * layer.
 * @since 2.2
 */
public class QueryUtil {
  private static final Logger LOG = LoggerFactory.getLogger(QueryUtil.class);
  
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
    return getRowKeyUIDRegex(group_bys, row_key_literals, false, null, null);
  }
  
  /**
   * Crafts a regular expression for scanning over data table rows and filtering
   * time series that the user doesn't want. Also fills in an optional fuzzy
   * mask and key as it builds the regex if configured to do so.
   * @param group_bys An optional list of tag keys that we want to group on. May
   * be null.
   * @param row_key_literals An optional list of key value pairs to filter on.
   * May be null.
   * @param explicit_tags Whether or not explicit tags are enabled so that the
   * regex only picks out series with the specified tags
   * @param fuzzy_key An optional fuzzy filter row key
   * @param fuzzy_mask An optional fuzzy filter mask
   * @return A regular expression string to pass to the storage layer.
   * @since 2.3
   */
  public static String getRowKeyUIDRegex(
      final List<byte[]> group_bys, 
      final ByteMap<byte[][]> row_key_literals, 
      final boolean explicit_tags,
      final byte[] fuzzy_key, 
      final byte[] fuzzy_mask) {
    if (group_bys != null) {
      Collections.sort(group_bys, Bytes.MEMCMP);
    }
    final int prefix_width = Const.SALT_WIDTH() + TSDB.metrics_width() + 
        Const.TIMESTAMP_BYTES;
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
    int fuzzy_offset = Const.SALT_WIDTH() + TSDB.metrics_width();
    if (fuzzy_mask != null) {
      // make sure to skip the timestamp when scanning
      while (fuzzy_offset < prefix_width) {
        fuzzy_mask[fuzzy_offset++] = 1;
      }
    }
    
    while(it.hasNext()) {
      Entry<byte[], byte[][]> entry = it.hasNext() ? it.next() : null;
      // TODO - This look ahead may be expensive. We need to get some data around
      // whether it's faster for HBase to scan with a look ahead or simply pass
      // the rows back to the TSD for filtering.
      final boolean not_key = 
          entry.getValue() != null && entry.getValue().length == 0;
      
      // Skip any number of tags.
      if (!explicit_tags) {
        buf.append("(?:.{").append(tagsize).append("})*");
      } else if (fuzzy_mask != null) {
        // TODO - see if we can figure out how to improve the fuzzy filter by
        // setting explicit tag values whenever we can. In testing there was
        // a conflict between the row key regex and fuzzy filter that prevented
        // results from returning properly.
        System.arraycopy(entry.getKey(), 0, fuzzy_key, fuzzy_offset, name_width);
        fuzzy_offset += name_width;
        for (int i = 0; i < value_width; i++) {
          fuzzy_mask[fuzzy_offset++] = 1;
        }
      }
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
    if (!explicit_tags) {
      buf.append("(?:.{").append(tagsize).append("})*");
    }
    buf.append("$");
    return buf.toString();
  }
  
  /**
   * Crafts a regular expression for scanning over data table rows and filtering
   * time series that the user doesn't want.
   * @param row_key_literals An optional list of key value pairs to filter on.
   * May be null.
   * @param explicit_tags Whether or not explicit tags are enabled so that the
   * regex only picks out series with the specified tags
   * @return A regular expression string to pass to the storage layer.
   */
  private static String getRowKeyUIDRegex(
      final ByteMap<byte[][]> row_key_literals, 
      final boolean explicit_tags) {
    final int prefix_width = Const.SALT_WIDTH() + TSDB.metrics_width() + 
        Const.TIMESTAMP_BYTES;
    final short name_width = TSDB.tagk_width();
    final short value_width = TSDB.tagv_width();
    final short tagsize = (short) (name_width + value_width);
    // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
    // and { 4 5 6 9 8 7 }, the regexp will be:
    // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
    final StringBuilder buf = new StringBuilder(
        15  // "^.{N}" + "(?:.{M})*" + "$"
        + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
           * ((row_key_literals == null ? 0 : row_key_literals.size()))));

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the salt, metric ID and timestamp.
       .append(prefix_width)
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
      if (!explicit_tags) {
        buf.append("(?:.{").append(tagsize).append("})*");
      }

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
    if (!explicit_tags) {
      buf.append("(?:.{").append(tagsize).append("})*");
    }
    buf.append("$");
    return buf.toString();
  }
  
  /**
   * Crafts a list of FuzzyFilters for scanning over data table rows and 
   * filtering time series that the user doesn't want.
   * Note: The caller has to restrict the scan to proper start and stop
   * for the filter to work correctly.
   * @param row_key_literals A list of key value pairs to filter on.
   * @param fuzzy_key The starting row key we'll adjust for proper filtering.
   * @return A sorted, non-empty list of FuzzyFilterPair
   */
  private static List<FuzzyFilterPair> buildFuzzyFilters(
      final ByteMap<byte[][]> row_key_literals,
      final byte[] fuzzy_key) {
    final int prefix_width = Const.SALT_WIDTH() + TSDB.metrics_width() + 
        Const.TIMESTAMP_BYTES;
    final short name_width = TSDB.tagk_width();
    final short value_width = TSDB.tagv_width();
    final short tag_width = (short) (name_width + value_width);
    int row_key_size = prefix_width;
    if (row_key_literals != null) {
      for(byte[][] v: row_key_literals.values()) {
        final boolean not_key = v!=null && v.length==0;
        if (!not_key) {
          row_key_size += tag_width;
        }
      }
    }
    final List<FuzzyFilterPair> fuzzy_filter_pairs =
        new ArrayList<FuzzyFilterPair>(row_key_literals.size());
    
    // Initialize first_fuzzy_key and first_fuzzy_mask
    // these will serve as model for the fuzzy filter list
    // generated for tags with multiple values (|)
    byte[] first_fuzzy_key  = Arrays.copyOf(fuzzy_key, fuzzy_key.length);
    byte[] first_fuzzy_mask = new byte[fuzzy_key.length];
    int fuzzy_offset = 0;
    
    // TODO - see if it's less expensive to skip the salt, timestamp and metric.
    // skip salt & timestamp (filtering should be done by start/stop
    // of the scanner)
    while(fuzzy_offset < prefix_width) {
      first_fuzzy_key[fuzzy_offset] = 0;
      first_fuzzy_mask[fuzzy_offset++] = 
          (row_key_literals != null) ? (byte)1 : (byte)0; 
    }
    
    // first pass to build the key and mask
    Iterator<Entry<byte[], byte[][]>> it = row_key_literals.iterator();
    while(it.hasNext()) {
      Entry<byte[], byte[][]> entry = it.next();
      final boolean not_key = 
          entry.getValue() != null && entry.getValue().length == 0;

      if (!not_key) {
        final byte[] tag_key = entry.getKey();
        System.arraycopy(tag_key, 0, 
            first_fuzzy_key, fuzzy_offset, name_width);
        for (int i=0; i<name_width; i++) {
          first_fuzzy_mask[fuzzy_offset++] = 0; 
        }

        final byte[] tag_value;
        if (entry.getValue()!=null && entry.getValue().length > 0) {
          tag_value = entry.getValue()[0];
        } else {
          tag_value = null;
        }
        
        if (tag_value!=null) {
          System.arraycopy(tag_value, 0, 
              first_fuzzy_key, fuzzy_offset, value_width);  
          for (int i=0; i<value_width; i++) {
            first_fuzzy_mask[fuzzy_offset++] = 0;
          }
        } else {
          // not filtered with fuzzy filter -> skip
          for (int i=0; i<value_width; i++) {
            first_fuzzy_key[fuzzy_offset]    = 0;
            first_fuzzy_mask[fuzzy_offset++] = 1;
          }
        }
      }
    }
    fuzzy_filter_pairs.add(new FuzzyFilterPair(first_fuzzy_key, first_fuzzy_mask));

    // generate filters for all combinations of tag values using the first key
    // as the template.
    fuzzy_offset = prefix_width;
    it = row_key_literals.iterator();
    while (it.hasNext()) {
      final Entry<byte[], byte[][]> entry = it.next();
      fuzzy_offset += name_width;

      // if multiple values value, generate a new combination of filters
      // for each value
      if (entry.getValue()!=null && entry.getValue().length > 1) {
        for (int i=1; i<entry.getValue().length; i++) {
          final byte[] tag_value = entry.getValue()[i];
          byte[] local_fuzzy_key = 
              Arrays.copyOf(first_fuzzy_key, row_key_size);
          System.arraycopy(tag_value, 0, 
              local_fuzzy_key, fuzzy_offset, value_width);

          fuzzy_filter_pairs.add(
              new FuzzyFilterPair(local_fuzzy_key, first_fuzzy_mask));
        }
      }
      fuzzy_offset += value_width;
    }
    
    // Sort filters list over rowkey
    Collections.sort(fuzzy_filter_pairs, FUZZY_FILTER_CMP);
    return fuzzy_filter_pairs;
  }
  
  /**
   * Comparator that sorts the fuzzy filter list ascending based on the row
   * key.
   */
  private static class FuzzyFilterComparator implements Comparator<FuzzyFilterPair> {
    @Override
    public int compare(FuzzyFilterPair pair1, FuzzyFilterPair pair2) {
      return Bytes.memcmp(pair2.getRowKey(), pair1.getRowKey());
    }
  }
  private static FuzzyFilterComparator FUZZY_FILTER_CMP = new FuzzyFilterComparator();
  
  /**
   * Sets a filter or filter list on the scanner based on whether or not the
   * query had tags it needed to match.
   * NOTE: This method will sort the group bys.
   * @param scanner The scanner to modify.
   * @param group_bys An optional list of tag keys that we want to group on. May
   * be null.
   * @param row_key_literals An optional list of key value pairs to filter on.
   * May be null.
   * @param explicit_tags Whether or not explicit tags are enabled so that the
   * regex only picks out series with the specified tags
   * @param enable_fuzzy_filter Whether or not a fuzzy filter should be used
   * in combination with the explicit tags param. If explicit tags is disabled
   * then this param is ignored. 
   * @param end_time The end of the query time so the fuzzy filter knows when
   * to stop scanning.
   */
  public static void setDataTableScanFilter(
      final Scanner scanner, 
      final List<byte[]> group_bys, 
      final ByteMap<byte[][]> row_key_literals,
      final boolean explicit_tags,
      final boolean enable_fuzzy_filter,
      final int end_time) {
    
    // no-op
    if ((group_bys == null || group_bys.isEmpty()) 
        && (row_key_literals == null || row_key_literals.isEmpty())) {
      return;
    }
    
    if (group_bys != null) {
      Collections.sort(group_bys, Bytes.MEMCMP);
    }
    
    final int prefix_width = Const.SALT_WIDTH() + TSDB.metrics_width() + 
        Const.TIMESTAMP_BYTES;
    
    final FuzzyRowFilter fuzzy_filter;
    if (explicit_tags && 
        enable_fuzzy_filter && 
        row_key_literals != null && 
        !row_key_literals.isEmpty()) {
      
      final byte[] fuzzy_key = new byte[prefix_width + (row_key_literals.size() * 
          (TSDB.tagk_width() + TSDB.tagv_width()))];
      System.arraycopy(scanner.getCurrentKey(), 0, fuzzy_key, 0, 
          scanner.getCurrentKey().length);
      
      final List<FuzzyFilterPair> fuzzy_filter_pairs = 
          buildFuzzyFilters(row_key_literals, fuzzy_key);
      
      // The Fuzzy Filter list is sorted: the first and last filters row key
      // can be used to build the stop key for the scanner
      final byte[] stop_key = Arrays.copyOf(
          fuzzy_filter_pairs.get(fuzzy_filter_pairs.size() - 1).getRowKey(),
          fuzzy_key.length);
      System.arraycopy(scanner.getCurrentKey(), 0, stop_key, 0, prefix_width);
      Internal.setBaseTime(stop_key, end_time);
      int idx = prefix_width + TSDB.tagk_width();
      // max out the tag values
      while (idx < stop_key.length) {
        for (int i = 0; i < TSDB.tagv_width(); i++) {
          stop_key[idx++] = (byte) 0xFF;
        }
        idx += TSDB.tagk_width();
      }

      scanner.setStartKey(fuzzy_key);
      scanner.setStopKey(stop_key);
      fuzzy_filter = new FuzzyRowFilter(fuzzy_filter_pairs);
    } else {
      fuzzy_filter = null;
    }
    
    final String regex = getRowKeyUIDRegex(row_key_literals, explicit_tags);
    final KeyRegexpFilter regex_filter;
    if (!Strings.isNullOrEmpty(regex)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Regex for scanner: " + scanner + ": " + 
            byteRegexToString(regex));
      }
      regex_filter = new KeyRegexpFilter(regex.toString(), 
          Const.ASCII_CHARSET);
    } else {
      regex_filter = null;
    }
    
    if (fuzzy_filter != null && !Strings.isNullOrEmpty(regex)) {
      final FilterList filter = new FilterList(Lists.newArrayList(fuzzy_filter, 
          regex_filter),Operator.MUST_PASS_ALL);
      scanner.setFilter(filter);
    } else if (fuzzy_filter != null) {
      scanner.setFilter(fuzzy_filter);
    } else if (!Strings.isNullOrEmpty(regex)) {
      scanner.setFilter(regex_filter);
    }
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
    scanner.setMaxNumRows(tsdb.getConfig().scanner_maxNumRows());
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
