// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.filter;

import java.util.Arrays;
import java.util.Map;

import net.opentsdb.core.Tags;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

/**
 * Performs basic wild card searching. It supports prefix, postfix, infix,
 * multi-infix and case insensitive matching. The wildcard character is
 * an asterisk. If case insensitivity is enabled, we simply drop everything
 * to lower case.
 * @since 2.2
 */
public class TagVWildcardFilter extends TagVFilter {
  
  /** Name of this filter */
  final public static String FILTER_NAME = "wildcard";
  
  /** Whether or not the filter had a postfix asterisk */
  protected final boolean has_postfix;
  
  /** Whether or not the filter had a prefix asterisk */
  protected final boolean has_prefix;
  
  /** The individual components to match on */
  protected final String[] components;
  
  /** Whether or not we'll match case */
  protected boolean case_insensitive;

  /**
   * The default Ctor that disables case insensitivity
   * @param tagk The tag key to associate with this filter
   * @param filter The wildcard filter to match on
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   */
  public TagVWildcardFilter(final String tagk, final String filter) {
    this(tagk, filter, false);
  }
  
  /**
   * A ctor that allows enabling case insensitivity
   * @param tagk The tag key to associate with this filter
   * @param filter The wildcard filter to match on
   * @param case_insensitive Whether or not to match on case
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   */
  public TagVWildcardFilter(final String tagk, final String filter, 
      final boolean case_insensitive) {
    super(tagk, filter);
    this.case_insensitive = case_insensitive;
    
    if (filter == null || filter.length() < 1) {
      throw new IllegalArgumentException("Filter cannot be null or empty");
    }
    String actual = case_insensitive ? filter.toLowerCase() : filter;
    if (!actual.contains("*")) {
      throw new IllegalArgumentException("Filter must contain an asterisk");
    }
    
    if (actual.charAt(0) == '*') {
      has_postfix = true;
      while (actual.charAt(0) == '*') {
        if (actual.length() < 2) {
          break;
        }
        actual = actual.substring(1);
      }
    } else {
      has_postfix = false;
    }
    if (actual.charAt(actual.length() - 1) == '*') {
      has_prefix = true;
      while(actual.charAt(actual.length() - 1) == '*') {
        if (actual.length() < 2) {
          break;
        }
        actual = actual.substring(0, actual.length() - 1);
      }
    } else {
      has_prefix = false;
    }
    if (actual.indexOf('*') > 0) {
      components = Tags.splitString(actual, '*');
    } else {
      components = new String[1];
      components[0] = actual;
    }
    
    // avoid resolving UIDs at scan time
    if (components.length == 1 && components[0].equals("*")) {
      post_scan = false;
    }
  }

  @Override
  public boolean match(final Map<String, String> tags) {
    String tagv = tags.get(tagk);
    if (tagv == null) {
      return false;
    } else if (components.length == 1 && components[0].equals("*")) {
      // match all
      return true;
    } else if (case_insensitive) {
      tags.get(tagk).toLowerCase();
    }
    if (has_postfix && !has_prefix && 
        !tagv.endsWith(components[components.length-1])) {
      return false;
    }
    if (has_prefix && !has_postfix && !tagv.startsWith(components[0])) {
      return false;
    }
    int idx = 0;
    for (int i = 0; i < components.length; i++) {
      if (tagv.indexOf(components[i], idx) < 0) {
        return false;
      }
      idx += components[i].length();
    }
    return true;
  }

  @Override
  public String debugInfo() {
    return "{components=" + Arrays.toString(components) + ", case=" + 
        case_insensitive + "}";
  }
  
  /** @return Whether or not this filter has case insensitivity enabled */
  @JsonIgnore
  public boolean isCaseInsensitive() {
    return case_insensitive;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TagVWildcardFilter)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    final TagVWildcardFilter filter = (TagVWildcardFilter)obj;
    return Objects.equal(tagk, filter.tagk)
        && Arrays.equals(components, filter.components)
        && Objects.equal(case_insensitive, filter.case_insensitive);
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(tagk, Arrays.hashCode(components), case_insensitive);
  }

  @Override
  public String getType() {
    return FILTER_NAME;
  }
  
  /** @return a string describing the filter */
  public static String description() {
    return "Performs pre, post and in-fix glob matching of values. The globs "
        + "are case sensitive and multiple wildcards can be used. The wildcard "
        + "character is the * (asterisk). At least one wildcard must be "
        + "present in the filter value. A wildcard by itself can be used as "
        + "well to match on any value for the tag key.";
  }
  
  /** @return a list of examples showing how to use the filter */
  public static String examples() {
    return "host=wildcard(web*),  host=wildcard(web*.tsdb.net)  "
        + "{\"type\":\"wildcard\",\"tagk\":\"host\","
        + "\"filter\":\"web*.tsdb.net\",\"groupBy\":false}";
  }
  
  /**
   * Case insensitive version
   */
  public static class TagVIWildcardFilter extends TagVWildcardFilter {
    /** Name of this filter */
    final public static String FILTER_NAME = "iwildcard";
    
    public TagVIWildcardFilter(final String tagk, final String filter) {
      super(tagk, filter, true);
    }

    @Override
    public String getType() {
      return FILTER_NAME;
    }
    
    /** @return a string describing the filter */
    public static String description() {
      return "Performs pre, post and in-fix glob matching of values. The globs "
          + "are case insensitive and multiple wildcards can be used. The wildcard "
          + "character is the * (asterisk). Case insensitivity is achieved by "
          + "dropping all values to lower case. At least one wildcard must be "
          + "present in the filter value. A wildcard by itself can be used as "
          + "well to match on any value for the tag key.";
    }
    
    /** @return a list of examples showing how to use the filter */
    public static String examples() {
      return "host=iwildcard(web*),  host=iwildcard(web*.tsdb.net)  "
          + "{\"type\":\"iwildcard\",\"tagk\":\"host\","
          + "\"filter\":\"web*.tsdb.net\",\"groupBy\":false}";
    }
  }
}
