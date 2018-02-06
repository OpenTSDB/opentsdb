// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.query.filter;

import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;
import com.stumbleupon.async.Deferred;

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
      components = actual.split("\\*");
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
  public Deferred<Boolean> match(final Map<String, String> tags) {
    String tagv = tags.get(tagk);
    if (tagv == null) {
      return Deferred.fromResult(false);
    } else if (components.length == 1 && components[0].equals("*")) {
      // match all
      return Deferred.fromResult(true);
    } else if (case_insensitive) {
      tagv = tags.get(tagk).toLowerCase();
    }
    if (has_postfix && !has_prefix && 
        !tagv.endsWith(components[components.length-1])) {
      return Deferred.fromResult(false);
    }
    if (has_prefix && !has_postfix && !tagv.startsWith(components[0])) {
      return Deferred.fromResult(false);
    }
    int idx = 0;
    for (int i = 0; i < components.length; i++) {
      if (tagv.indexOf(components[i], idx) < 0) {
        return Deferred.fromResult(false);
      }
      idx += components[i].length();
    }
    return Deferred.fromResult(true);
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
