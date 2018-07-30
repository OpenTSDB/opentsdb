// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.HashMap;
import java.util.List;

import net.opentsdb.query.pojo.TagVFilter;
import net.opentsdb.utils.Pair;
import net.opentsdb.utils.StringUtils;

/** Helper functions to deal with tags. */
public final class Tags {

  private Tags() {
    // Can't create instances of this utility class.
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
    final String[] kv = StringUtils.splitString(tag, '=');
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
    
    final String[] kv = StringUtils.splitString(tag, '=');
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
    for (final String tag : StringUtils.splitString(metric.substring(curly + 1, len - 1),
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
    for (final String tag : StringUtils.splitString(metric.substring(curly + 1, len - 1),
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
   * Parses the metric and tags out of the given string.
   * @param metric A string of the form "metric" or "metric{tag=value,...}" or
   * now "metric{groupby=filter}{filter=filter}".
   * @param filters A list of filters to write the results to. May not be null
   * @return The name of the metric.
   * @throws IllegalArgumentException if the metric is malformed or the filter
   * list is null.
   * @since 2.2
   */
  public static String parseWithMetricAndFilters(final String metric, 
      final List<TagVFilter> filters) {
    if (metric == null || metric.isEmpty()) {
      throw new IllegalArgumentException("Metric cannot be null or empty");
    }
    if (filters == null) {
      throw new IllegalArgumentException("Filters cannot be null");
    }
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
    final int close = metric.indexOf('}');
    final HashMap<String, String> filter_map = new HashMap<String, String>();
    if (close != metric.length() - 1) { // "foo{...}{tagk=filter}" 
      final int filter_bracket = metric.lastIndexOf('{');
      for (final String filter : StringUtils.splitString(metric.substring(filter_bracket + 1, 
          metric.length() - 1), ',')) {
        if (filter.isEmpty()) {
          break;
        }
        filter_map.clear();
        try {
          parse(filter_map, filter);
          TagVFilter.mapToFilters(filter_map, filters, false);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("When parsing filter '" + filter
              + "': " + e.getMessage(), e);
        }
      }
    }
    
    // substring the tags out of "foo{a=b,...,x=y}" and parse them.
    for (final String tag : StringUtils.splitString(metric.substring(curly + 1, close), ',')) {
      try {
        if (tag.isEmpty() && close != metric.length() - 1){
          break;
        }
        filter_map.clear();
        parse(filter_map, tag);
        TagVFilter.tagsToFilters(filter_map, filters);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("When parsing tag '" + tag
                                           + "': " + e.getMessage(), e);
      }
    }
    // Return the "foo" part of "foo{a=b,...,x=y}"
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

}
