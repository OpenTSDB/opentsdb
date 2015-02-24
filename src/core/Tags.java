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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.hbase.async.Bytes;

import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Pair;

/** Helper functions to deal with tags. */
public final class Tags {

  private static final Logger LOG = LoggerFactory.getLogger(Tags.class);

  private Tags() {
    // Can't create instances of this utility class.
  }

  /**
   * Optimized version of {@code String#split} that doesn't use regexps.
   * This function works in O(5n) where n is the length of the string to
   * split.
   * @param s The string to split.
   * @param c The separator to use to split the string.
   * @return A non-null, non-empty array.
   */
  public static String[] splitString(final String s, final char c) {
    final char[] chars = s.toCharArray();
    int num_substrings = 1;
    for (final char x : chars) {
      if (x == c) {
        num_substrings++;
      }
    }
    final String[] result = new String[num_substrings];
    final int len = chars.length;
    int start = 0;  // starting index in chars of the current substring.
    int pos = 0;    // current index in chars.
    int i = 0;      // number of the current substring.
    for (; pos < len; pos++) {
      if (chars[pos] == c) {
        result[i++] = new String(chars, start, pos - start);
        start = pos + 1;
      }
    }
    result[i] = new String(chars, start, pos - start);
    return result;
  }

  /**
   * Parses a tag into a HashMap.
   * @param tags The HashMap into which to store the tag.
   * @param tag A String of the form "tag=value".
   * @throws IllegalArgumentException if the tag is malformed.
   * @throws IllegalArgumentException if the tag was already in tags with a
   * different value.
   */
  public static void parse(final HashMap<String, String> tags,
                           final String tag) {
    final String[] kv = splitString(tag, '=');
    if (kv.length != 2 || kv[0].length() <= 0 || kv[1].length() <= 0) {
      throw new IllegalArgumentException("invalid tag: " + tag);
    }
    if (kv[1].equals(tags.get(kv[0]))) {
        return;
    }
    if (tags.get(kv[0]) != null) {
      throw new IllegalArgumentException("duplicate tag: " + tag
                                         + ", tags=" + tags);
    }
    tags.put(kv[0], kv[1]);
  }

  /**
   * Parses a tag into a list of key/value pairs, allowing nulls for either
   * value.
   * @param tags The list into which the parsed tag should be stored
   * @param tag A string of the form "tag=value" or "=value" or "tag="
   * @throws IllegalArgumentException if the tag is malformed.
   * @since 2.1
   */
  public static void parse(final List<Pair<String, String>> tags,
      final String tag) {
    if (tag == null || tag.isEmpty() || tag.length() < 2) {
      throw new IllegalArgumentException("Missing tag pair");
    }
    if (tag.charAt(0) == '=') {
      tags.add(new Pair<String, String>(null, tag.substring(1)));
      return;
    } else if (tag.charAt(tag.length() - 1) == '=') {
      tags.add(new Pair<String, String>(tag.substring(0, tag.length() - 1), null));
      return;
    }
    
    final String[] kv = splitString(tag, '=');
    if (kv.length != 2 || kv[0].length() <= 0 || kv[1].length() <= 0) {
      throw new IllegalArgumentException("invalid tag: " + tag);
    }
    tags.add(new Pair<String, String>(kv[0], kv[1]));
  }
    
  /**
   * Parses the metric and tags out of the given string.
   * @param metric A string of the form "metric" or "metric{tag=value,...}".
   * @param tags The map to populate with the tags parsed out of the first
   * argument.
   * @return The name of the metric.
   * @throws IllegalArgumentException if the metric is malformed.
   */
  public static String parseWithMetric(final String metric,
                                       final HashMap<String, String> tags) {
    final int curly = metric.indexOf('{');
    if (curly < 0) {
      return metric;
    }
    final int len = metric.length();
    if (metric.charAt(len - 1) != '}') {  // "foo{"
      throw new IllegalArgumentException("Missing '}' at the end of: " + metric);
    } else if (curly == len - 2) {  // "foo{}"
      return metric.substring(0, len - 2);
    }
    // substring the tags out of "foo{a=b,...,x=y}" and parse them.
    for (final String tag : splitString(metric.substring(curly + 1, len - 1),
                                        ',')) {
      try {
        parse(tags, tag);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("When parsing tag '" + tag
                                           + "': " + e.getMessage());
      }
    }
    // Return the "foo" part of "foo{a=b,...,x=y}"
    return metric.substring(0, curly);
  }

  /**
   * Parses an optional metric and tags out of the given string, any of
   * which may be null. Requires at least one metric, tagk or tagv.
   * @param metric A string of the form "metric" or "metric{tag=value,...}"
   * or even "{tag=value,...}" where the metric may be missing.
   * @param tags The list to populate with parsed tag pairs
   * @return The name of the metric if it exists, null otherwise
   * @throws IllegalArgumentException if the metric is malformed.
   * @since 2.1
   */
  public static String parseWithMetric(final String metric,
      final List<Pair<String, String>> tags) {
    final int curly = metric.indexOf('{');
    if (curly < 0) {
      if (metric.isEmpty()) {
        throw new IllegalArgumentException("Metric string was empty");
      }
      return metric;
    }
    final int len = metric.length();
    if (metric.charAt(len - 1) != '}') {  // "foo{"
      throw new IllegalArgumentException("Missing '}' at the end of: " + metric);
    } else if (curly == len - 2) {  // "foo{}"
      if (metric.charAt(0) == '{') {
        throw new IllegalArgumentException("Missing metric and tags: " + metric);
      }
      return metric.substring(0, len - 2);
    }
    // substring the tags out of "foo{a=b,...,x=y}" and parse them.
    for (final String tag : splitString(metric.substring(curly + 1, len - 1),
           ',')) {
    try {
      parse(tags, tag);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("When parsing tag '" + tag
                + "': " + e.getMessage());
      }
    }
    // Return the "foo" part of "foo{a=b,...,x=y}"
    if (metric.charAt(0) == '{') {
      return null;
    }
    return metric.substring(0, curly);
  }
  
  /**
   * Parses an integer value as a long from the given character sequence.
   * <p>
   * This is equivalent to {@link Long#parseLong(String)} except it's up to
   * 100% faster on {@link String} and always works in O(1) space even with
   * {@link StringBuilder} buffers (where it's 2x to 5x faster).
   * @param s The character sequence containing the integer value to parse.
   * @return The value parsed.
   * @throws NumberFormatException if the value is malformed or overflows.
   */
  public static long parseLong(final CharSequence s) {
    final int n = s.length();  // Will NPE if necessary.
    if (n == 0) {
      throw new NumberFormatException("Empty string");
    }
    char c = s.charAt(0);  // Current character.
    int i = 1;  // index in `s'.
    if (c < '0' && (c == '+' || c == '-')) {  // Only 1 test in common case.
      if (n == 1) {
        throw new NumberFormatException("Just a sign, no value: " + s);
      } else if (n > 20) {  // "+9223372036854775807" or "-9223372036854775808"
          throw new NumberFormatException("Value too long: " + s);
      }
      c = s.charAt(1);
      i = 2;  // Skip over the sign.
    } else if (n > 19) {  // "9223372036854775807"
      throw new NumberFormatException("Value too long: " + s);
    }
    long v = 0;  // The result (negated to easily handle MIN_VALUE).
    do {
      if ('0' <= c && c <= '9') {
        v -= c - '0';
      } else {
        throw new NumberFormatException("Invalid character '" + c
                                        + "' in " + s);
      }
      if (i == n) {
        break;
      }
      v *= 10;
      c = s.charAt(i++);
    } while (true);
    if (v > 0) {
      throw new NumberFormatException("Overflow in " + s);
    } else if (s.charAt(0) == '-') {
      return v;  // Value is already negative, return unchanged.
    } else if (v == Long.MIN_VALUE) {
      throw new NumberFormatException("Overflow in " + s);
    } else {
      return -v;  // Positive value, need to fix the sign.
    }
  }

  /**
   * Extracts the value of the given tag name from the given row key.
   * @param tsdb The TSDB instance to use for UniqueId lookups.
   * @param row The row key in which to search the tag name.
   * @param name The name of the tag to search in the row key.
   * @return The value associated with the given tag name, or null if this tag
   * isn't present in this row key.
   */
  static String getValue(final TSDB tsdb, final byte[] row,
                         final String name) throws NoSuchUniqueName {
    validateString("tag name", name);
    final byte[] id = tsdb.tag_names.getId(name);
    final byte[] value_id = getValueId(tsdb, row, id);
    if (value_id == null) {
      return null;
    }
    // This shouldn't throw a NoSuchUniqueId.
    try {
      return tsdb.tag_values.getName(value_id);
    } catch (NoSuchUniqueId e) {
      LOG.error("Internal error, NoSuchUniqueId unexpected here!", e);
      throw e;
    }
  }

  /**
   * Extracts the value ID of the given tag UD name from the given row key.
   * @param tsdb The TSDB instance to use for UniqueId lookups.
   * @param row The row key in which to search the tag name.
   * @param name The name of the tag to search in the row key.
   * @return The value ID associated with the given tag ID, or null if this
   * tag ID isn't present in this row key.
   */
  static byte[] getValueId(final TSDB tsdb, final byte[] row,
                           final byte[] tag_id) {
    final short name_width = tsdb.tag_names.width();
    final short value_width = tsdb.tag_values.width();
    // TODO(tsuna): Can do a binary search.
    for (short pos = (short) (tsdb.metrics.width() + Const.TIMESTAMP_BYTES);
         pos < row.length;
         pos += name_width + value_width) {
      if (rowContains(row, pos, tag_id)) {
        pos += name_width;
        return Arrays.copyOfRange(row, pos, pos + value_width);
      }
    }
    return null;
  }

  /**
   * Checks whether or not the row key contains the given byte array at the
   * given offset.
   * @param row The row key in which to search.
   * @param offset The offset in {@code row} at which to start searching.
   * @param bytes The bytes to search that the given offset.
   * @return true if {@code bytes} are present in {@code row} at
   * {@code offset}, false otherwise.
   */
  private static boolean rowContains(final byte[] row,
                                     short offset, final byte[] bytes) {
    for (int pos = bytes.length - 1; pos >= 0; pos--) {
      if (row[offset + pos] != bytes[pos]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the tags stored in the given row key.
   * @param tsdb The TSDB instance to use for Unique ID lookups.
   * @param row The row key from which to extract the tags.
   * @return A map of tag names (keys), tag values (values).
   * @throws NoSuchUniqueId if the row key contained an invalid ID (unlikely).
   */
  static Map<String, String> getTags(final TSDB tsdb,
                                     final byte[] row) throws NoSuchUniqueId {
    try {
      return getTagsAsync(tsdb, row).joinUninterruptibly();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Returns the tags stored in the given row key.
   * @param tsdb The TSDB instance to use for Unique ID lookups.
   * @param row The row key from which to extract the tags.
   * @return A map of tag names (keys), tag values (values).
   * @throws NoSuchUniqueId if the row key contained an invalid ID (unlikely).
   * @since 1.2
   */
  static Deferred<Map<String, String>> getTagsAsync(final TSDB tsdb,
                                     final byte[] row) throws NoSuchUniqueId {
    final short name_width = tsdb.tag_names.width();
    final short value_width = tsdb.tag_values.width();
    final short tag_bytes = (short) (name_width + value_width);
    final short metric_ts_bytes = (short) (tsdb.metrics.width()
                                           + Const.TIMESTAMP_BYTES);
    
    final ArrayList<Deferred<String>> deferreds = 
      new ArrayList<Deferred<String>>((row.length - metric_ts_bytes) / tag_bytes);
    
    for (short pos = metric_ts_bytes; pos < row.length; pos += tag_bytes) {
      final byte[] tmp_name = new byte[name_width];
      final byte[] tmp_value = new byte[value_width];

      System.arraycopy(row, pos, tmp_name, 0, name_width);
      deferreds.add(tsdb.tag_names.getNameAsync(tmp_name));

      System.arraycopy(row, pos + name_width, tmp_value, 0, value_width);
      deferreds.add(tsdb.tag_values.getNameAsync(tmp_value));
    }
    
    class NameCB implements Callback<Map<String, String>, ArrayList<String>> {
      public Map<String, String> call(final ArrayList<String> names) 
        throws Exception {
        final HashMap<String, String> result = new HashMap<String, String>(
            (row.length - metric_ts_bytes) / tag_bytes);
        String tagk = "";
        for (String name : names) {
          if (tagk.isEmpty()) {
            tagk = name;
          } else {
            result.put(tagk, name);
            tagk = "";
          }
        }
        return result;
      }
    }
    
    return Deferred.groupInOrder(deferreds).addCallback(new NameCB());
  }

  /**
   * Ensures that a given string is a valid metric name or tag name/value.
   * @param what A human readable description of what's being validated.
   * @param s The string to validate.
   * @throws IllegalArgumentException if the string isn't valid.
   */
  public static void validateString(final String what, final String s) {
    if (s == null) {
      throw new IllegalArgumentException("Invalid " + what + ": null");
    } else if ("".equals(s)) {
      throw new IllegalArgumentException("Invalid " + what + ": empty string");
    }
    final int n = s.length();
    for (int i = 0; i < n; i++) {
      final char c = s.charAt(i);
      if (!(('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') 
          || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' 
          || c == '/' || Character.isLetter(c))) {
        throw new IllegalArgumentException("Invalid " + what
            + " (\"" + s + "\"): illegal character: " + c);
      }
    }
  }

  /**
   * Resolves all the tags (name=value) into the a sorted byte arrays.
   * This function is the opposite of {@link #resolveIds}.
   * @param tsdb The TSDB to use for UniqueId lookups.
   * @param tags The tags to resolve.
   * @return an array of sorted tags (tag id, tag name).
   * @throws NoSuchUniqueName if one of the elements in the map contained an
   * unknown tag name or tag value.
   */
  public static ArrayList<byte[]> resolveAll(final TSDB tsdb,
                                      final Map<String, String> tags)
    throws NoSuchUniqueName {
    try {
      return resolveAllInternal(tsdb, tags, false);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never happen!", e);
    }
  }

  /**
  * Resolves (and creates, if necessary) all the tags (name=value) into the a
  * sorted byte arrays.
  * @param tsdb The TSDB to use for UniqueId lookups.
  * @param tags The tags to resolve. If a new tag name or tag value is
  * seen, it will be assigned an ID.
  * @return an array of sorted tags (tag id, tag name).
  */
  static ArrayList<byte[]> resolveOrCreateAll(final TSDB tsdb,
                                              final Map<String, String> tags) {
    return resolveAllInternal(tsdb, tags, true);
  }
  
  private
  static ArrayList<byte[]> resolveAllInternal(final TSDB tsdb,
                                              final Map<String, String> tags,
                                              final boolean create)
    throws NoSuchUniqueName {
    final ArrayList<byte[]> tag_ids = new ArrayList<byte[]>(tags.size());
    for (final Map.Entry<String, String> entry : tags.entrySet()) {
      final byte[] tag_id = (create && tsdb.getConfig().auto_tagk()
                             ? tsdb.tag_names.getOrCreateId(entry.getKey())
                             : tsdb.tag_names.getId(entry.getKey()));
      final byte[] value_id = (create && tsdb.getConfig().auto_tagv()
                               ? tsdb.tag_values.getOrCreateId(entry.getValue())
                               : tsdb.tag_values.getId(entry.getValue()));
      final byte[] thistag = new byte[tag_id.length + value_id.length];
      System.arraycopy(tag_id, 0, thistag, 0, tag_id.length);
      System.arraycopy(value_id, 0, thistag, tag_id.length, value_id.length);
      tag_ids.add(thistag);
    }
    // Now sort the tags.
    Collections.sort(tag_ids, Bytes.MEMCMP);
    return tag_ids;
  }


  /**
   * Resolves (and creates, if necessary) all the tags (name=value) into the a
   * sorted byte arrays.
   * @param tsdb The TSDB to use for UniqueId lookups.
   * @param tags The tags to resolve.  If a new tag name or tag value is
   * seen, it will be assigned an ID.
   * @return an array of sorted tags (tag id, tag name).
   * @since 2.0
   */
  static Deferred<ArrayList<byte[]>>
    resolveOrCreateAllAsync(final TSDB tsdb, final Map<String, String> tags) {
    return resolveAllInternalAsync(tsdb, tags, true);
  }
  
  private static Deferred<ArrayList<byte[]>>
    resolveAllInternalAsync(final TSDB tsdb,
                            final Map<String, String> tags,
                            final boolean create) {
    final ArrayList<Deferred<byte[]>> tag_ids =
      new ArrayList<Deferred<byte[]>>(tags.size());

    // For each tag, start resolving the tag name and the tag value.
    for (final Map.Entry<String, String> entry : tags.entrySet()) {
      final Deferred<byte[]> name_id = create
        ? tsdb.tag_names.getOrCreateIdAsync(entry.getKey())
        : tsdb.tag_names.getIdAsync(entry.getKey());
      final Deferred<byte[]> value_id = create
        ? tsdb.tag_values.getOrCreateIdAsync(entry.getValue())
        : tsdb.tag_values.getIdAsync(entry.getValue());

      // Then once the tag name is resolved, get the resolved tag value.
      class TagNameResolvedCB implements Callback<Deferred<byte[]>, byte[]> {
        public Deferred<byte[]> call(final byte[] nameid) {
          // And once the tag value too is resolved, paste the two together.
          class TagValueResolvedCB implements Callback<byte[], byte[]> {
            public byte[] call(final byte[] valueid) {
              final byte[] thistag = new byte[nameid.length + valueid.length];
              System.arraycopy(nameid, 0, thistag, 0, nameid.length);
              System.arraycopy(valueid, 0, thistag, nameid.length, valueid.length);
              return thistag;
            }
          }

          return value_id.addCallback(new TagValueResolvedCB());
        }
      }

      // Put all the deferred tag resolutions in this list.
      final Deferred<byte[]> resolve = 
        name_id.addCallbackDeferring(new TagNameResolvedCB());
      tag_ids.add(resolve);
    }

    // And then once we have all the tags resolved, sort them.
    return Deferred.group(tag_ids).addCallback(SORT_CB);
  }

  /**
   * Sorts a list of tags.
   * Each entry in the list expected to be a byte array that contains the tag
   * name UID followed by the tag value UID.
   */
  private static class SortResolvedTagsCB
    implements Callback<ArrayList<byte[]>, ArrayList<byte[]>> {
    public ArrayList<byte[]> call(final ArrayList<byte[]> tags) {
      // Now sort the tags.
      Collections.sort(tags, Bytes.MEMCMP);
      return tags;
    }
  }
  private static final SortResolvedTagsCB SORT_CB = new SortResolvedTagsCB();

  /**
   * Resolves all the tags IDs (name followed by value) into the a map.
   * This function is the opposite of {@link #resolveAll}.
   * @param tsdb The TSDB to use for UniqueId lookups.
   * @param tags The tag IDs to resolve.
   * @return A map mapping tag names to tag values.
   * @throws NoSuchUniqueId if one of the elements in the array contained an
   * invalid ID.
   * @throws IllegalArgumentException if one of the elements in the array had
   * the wrong number of bytes.
   */
  public static HashMap<String, String> resolveIds(final TSDB tsdb,
                                            final ArrayList<byte[]> tags)
    throws NoSuchUniqueId {
    try {
      return resolveIdsAsync(tsdb, tags).joinUninterruptibly();
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    }
  }

  /**
   * Resolves all the tags IDs asynchronously (name followed by value) into a map.
   * This function is the opposite of {@link #resolveAll}.
   * @param tsdb The TSDB to use for UniqueId lookups.
   * @param tags The tag IDs to resolve.
   * @return A map mapping tag names to tag values.
   * @throws NoSuchUniqueId if one of the elements in the array contained an
   * invalid ID.
   * @throws IllegalArgumentException if one of the elements in the array had
   * the wrong number of bytes.
   * @since 2.0
   */
  public static Deferred<HashMap<String, String>> resolveIdsAsync(final TSDB tsdb,
                                            final List<byte[]> tags)
    throws NoSuchUniqueId {
    final short name_width = tsdb.tag_names.width();
    final short value_width = tsdb.tag_values.width();
    final short tag_bytes = (short) (name_width + value_width);
    final HashMap<String, String> result
      = new HashMap<String, String>(tags.size());
    final ArrayList<Deferred<String>> deferreds 
      = new ArrayList<Deferred<String>>(tags.size());
    
    for (final byte[] tag : tags) {
      final byte[] tmp_name = new byte[name_width];
      final byte[] tmp_value = new byte[value_width];
      if (tag.length != tag_bytes) {
        throw new IllegalArgumentException("invalid length: " + tag.length
            + " (expected " + tag_bytes + "): " + Arrays.toString(tag));
      }
      System.arraycopy(tag, 0, tmp_name, 0, name_width);
      deferreds.add(tsdb.tag_names.getNameAsync(tmp_name));
      System.arraycopy(tag, name_width, tmp_value, 0, value_width);
      deferreds.add(tsdb.tag_values.getNameAsync(tmp_value));
    }
    
    class GroupCB implements Callback<HashMap<String, String>, ArrayList<String>> {
      public HashMap<String, String> call(final ArrayList<String> names)
          throws Exception {
        for (int i = 0; i < names.size(); i++) {
          if (i % 2 != 0) {
            result.put(names.get(i - 1), names.get(i));
          }
        }
        return result;
      }
    }
    
    return Deferred.groupInOrder(deferreds).addCallback(new GroupCB());
  }
  
  /**
   * Returns true if the given string looks like an integer.
   * <p>
   * This function doesn't do any checking on the string other than looking
   * for some characters that are generally found in floating point values
   * such as '.' or 'e'.
   * @since 1.1
   */
  public static boolean looksLikeInteger(final String value) {
    final int n = value.length();
    for (int i = 0; i < n; i++) {
      final char c = value.charAt(i);
      if (c == '.' || c == 'e' || c == 'E') {
        return false;
      }
    }
    return true;
  }

}
