// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import net.opentsdb.core.Const;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Bytes.ByteMap;

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
   * @param row_key_literals An optional list of key value pairs to filter on.
   * May be null.
   * @return A regular expression string to pass to the storage layer.
   */
  public static String getRowKeyUIDRegex(
      final Schema schema,
      final ByteMap<List<byte[]>> row_key_literals) {
    return getRowKeyUIDRegex(schema, row_key_literals, false, null, null);
  }
  
  /**
   * Crafts a regular expression for scanning over data table rows and filtering
   * time series that the user doesn't want. Also fills in an optional fuzzy
   * mask and key as it builds the regex if configured to do so.
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
      final Schema schema,
      final ByteMap<List<byte[]>> row_key_literals, 
      final boolean explicit_tags,
      final byte[] fuzzy_key, 
      final byte[] fuzzy_mask) {
    final int prefix_width = schema.saltWidth() + schema.metricWidth() + 
        Const.TIMESTAMP_BYTES;
    final int name_width = schema.tagkWidth();
    final int value_width = schema.tagvWidth();
    final int tagsize = name_width + value_width;
    // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
    // and { 4 5 6 9 8 7 }, the regexp will be:
    // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
    final StringBuilder buf = new StringBuilder(
        15  // "^.{N}" + "(?:.{M})*" + "$"
        + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
           * ((row_key_literals == null ? 0 : row_key_literals.size()))));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the salt, metric ID and timestamp.
       .append(schema.saltWidth() + schema.metricWidth() + Schema.TIMESTAMP_BYTES)
       .append("}");

    final Iterator<Entry<byte[], List<byte[]>>> it = row_key_literals == null ? 
        new ByteMap<List<byte[]>>().iterator() : row_key_literals.iterator();
    int fuzzy_offset = schema.saltWidth() + schema.metricWidth();
    if (fuzzy_mask != null) {
      // make sure to skip the timestamp when scanning
      while (fuzzy_offset < prefix_width) {
        fuzzy_mask[fuzzy_offset++] = 1;
      }
    }
    
    while(it.hasNext()) {
      Entry<byte[], List<byte[]>> entry = it.hasNext() ? it.next() : null;
      // TODO - This look ahead may be expensive. We need to get some data around
      // whether it's faster for HBase to scan with a look ahead or simply pass
      // the rows back to the TSD for filtering.
      final boolean not_key = 
          entry.getValue() != null && entry.getValue().size() == 0;
      
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
      if (entry.getValue() != null && entry.getValue().size() > 0) {  // Add a group_by.
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
   * Creates a regular expression with a list of or'd TUIDs to compare
   * against the rows in storage.
   * @param tsuids The list of TSUIDs to scan for
   * @return A regular expression string to pass to the storage layer.
   */
  public static String getRowKeyTSUIDRegex(final Schema schema, 
                                           final List<String> tsuids) {
    Collections.sort(tsuids);
    
    // first, convert the tags to byte arrays and count up the total length
    // so we can allocate the string builder
    final int metric_width = schema.metricWidth();
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
       .append(schema.saltWidth() + metric_width + Const.TIMESTAMP_BYTES)
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
  public static String byteRegexToString(final Schema schema, final String regexp) {
    final StringBuilder buf = new StringBuilder();
    for (int i = 0; i < regexp.length(); i++) {
      if (i > 0 && regexp.charAt(i - 1) == 'Q') {
        if (regexp.charAt(i - 3) == '*') {
          // tagk
          byte[] tagk = new byte[schema.tagkWidth()];
          for (int x = 0; x < schema.tagkWidth(); x++) {
            tagk[x] = (byte)regexp.charAt(i + x);
          }
          i += schema.tagkWidth();
          buf.append(Arrays.toString(tagk));
        } else {
          // tagv
          byte[] tagv = new byte[schema.tagvWidth()];
          for (int x = 0; x < schema.tagvWidth(); x++) {
            tagv[x] = (byte)regexp.charAt(i + x);
          }
          i += schema.tagvWidth();
          buf.append(Arrays.toString(tagv));
        }
      } else {
        buf.append(regexp.charAt(i));
      }
    }
    return buf.toString();
  }
}
