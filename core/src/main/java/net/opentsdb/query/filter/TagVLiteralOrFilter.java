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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.opentsdb.utils.Config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;
import com.stumbleupon.async.Deferred;

/**
 * A filter that lets the user list one or more explicit strings that should
 * be included in a result set for aggregation. 
 * @since 2.2
 */
public class TagVLiteralOrFilter extends TagVFilter {
  
  /** Name of this filter */
  final public static String FILTER_NAME = "literal_or";
  
  /** A list of strings to match on */
  final protected Set<String> literals;

  /** Whether or not the match should be case insensitive */
  final protected boolean case_insensitive;
  
  /**
   * The default Ctor that disables case insensitivity
   * @param tagk The tag key to associate with this filter
   * @param filter The filter to match on
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   */
  public TagVLiteralOrFilter(final String tagk, final String filter) {
    this(tagk, filter, false);
  }
  
  /**
   * A ctor that allows enabling case insensitivity
   * @param tagk The tag key to associate with this filter
   * @param filter The filter to match on
   * @param case_insensitive Whether or not to match on case
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   */
  public TagVLiteralOrFilter(final String tagk, final String filter, 
      final boolean case_insensitive) {
    super(tagk, filter);
    this.case_insensitive = case_insensitive;
    
    // we have to have at least one character.
    if (filter == null || filter.isEmpty()) {
      throw new IllegalArgumentException("Filter cannot be null or empty");
    }
    if (filter.length() == 1 && filter.charAt(0) == '|') {
      throw new IllegalArgumentException("Filter must contain more than just a pipe");
    }
    final String[] split = filter.split("\\|");
    if (case_insensitive) {
      for (int i = 0; i < split.length; i++) {
        split[i] = split[i].toLowerCase();
      }
    }
    literals = new HashSet<String>(Arrays.asList(split));
  }
  
  @Override
  public Deferred<Boolean> match(final Map<String, String> tags) {
    final String tagv = tags.get(tagk);
    if (tagv == null) {
      return Deferred.fromResult(false);
    }
    return Deferred.fromResult(
        literals.contains(case_insensitive ? tagv.toLowerCase() : tagv));
  }

  @Override
  public String debugInfo() {
    return "{literals=" + literals + ", case=" + case_insensitive + "}";
  }
  
  /**
   * Overridden here so that we can resolve the literal values if we don't have
   * too many of them AND we're not searching with case insensitivity.
   */
//  @Override
//  public Deferred<byte[]> resolveTagkName(final TSDB tsdb) {
//    final Config config = tsdb.getConfig();
//    
//    // resolve tag values if the filter is NOT case insensitive and there are 
//    // fewer literals than the expansion limit
//    if (!case_insensitive && 
//        literals.size() <= config.getInt("tsd.query.filter.expansion_limit")) {
//      return resolveTags(tsdb, literals);
//    } else {
//      return super.resolveTagkName(tsdb);
//    }
//  }
  
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
    if (!(obj instanceof TagVLiteralOrFilter)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    final TagVLiteralOrFilter filter = (TagVLiteralOrFilter)obj;
    return Objects.equal(tagk, filter.tagk)
        && Objects.equal(literals, filter.literals)
        && Objects.equal(case_insensitive, filter.case_insensitive);
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(tagk, literals, case_insensitive);
  }

  @Override
  public String getType() {
    return FILTER_NAME;
  }
  
  /** @return a string describing the filter */
  public static String description() {
    return "Accepts one or more exact values and matches if the series contains "
        + "any of them. Multiple values can be included and must be separated "
        + "by the | (pipe) character. The filter is case sensitive and will not "
        + "allow characters that TSDB does not allow at write time.";
  }
  
  /** @return a list of examples showing how to use the filter */
  public static String examples() {
    return "host=literal_or(web01),  host=literal_or(web01|web02|web03)  "
        + "{\"type\":\"literal_or\",\"tagk\":\"host\","
        + "\"filter\":\"web01|web02|web03\",\"groupBy\":false}";
  }
  
  /**
   * Case insensitive version
   */
  public static class TagVILiteralOrFilter extends TagVLiteralOrFilter {
    
    /** Name of this filter */
    final public static String FILTER_NAME = "iliteral_or";
    
    public TagVILiteralOrFilter(final String tagk, final String filter) {
      super(tagk, filter, true);
    }
    
    @Override
    public String getType() {
      return FILTER_NAME;
    }
    
    @Override
    public boolean equals(final Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof TagVILiteralOrFilter)) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      final TagVILiteralOrFilter filter = (TagVILiteralOrFilter)obj;
      return Objects.equal(tagk, filter.tagk)
          && Objects.equal(literals, filter.literals)
          && Objects.equal(case_insensitive, filter.case_insensitive);
    }
    
    /** @return a string describing the filter */
    public static String description() {
      return "Accepts one or more exact values and matches if the series contains "
          + "any of them. Multiple values can be included and must be separated "
          + "by the | (pipe) character. The filter is case insensitive and will not "
          + "allow characters that TSDB does not allow at write time.";
    }
    
    /** @return a list of examples showing how to use the filter */
    public static String examples() {
      return "host=iliteral_or(web01),  host=iliteral_or(web01|web02|web03)  "
          + "{\"type\":\"iliteral_or\",\"tagk\":\"host\","
          + "\"filter\":\"web01|web02|web03\",\"groupBy\":false}";
    }
  }
}
