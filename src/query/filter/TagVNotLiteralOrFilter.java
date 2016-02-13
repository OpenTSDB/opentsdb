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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;
import com.stumbleupon.async.Deferred;

/**
 * A filter that lets the user list one or more explicit strings that should
 * NOT be included in a result set for aggregation.
 * @since 2.2
 */
public class TagVNotLiteralOrFilter extends TagVFilter {

  /** Name of this filter */
  final public static String FILTER_NAME = "not_literal_or";
 
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
  public TagVNotLiteralOrFilter(final String tagk, final String filter) {
    this(tagk, filter, false);
  }
  
  /**
   * A ctor that allows enabling case insensitivity
   * @param tagk The tag key to associate with this filter
   * @param filter The filter to match on
   * @param case_insensitive Whether or not to match on case
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   */
  public TagVNotLiteralOrFilter(final String tagk, final String filter, 
      final boolean case_insensitive) {
    super(tagk, filter);
    this.case_insensitive = case_insensitive;
    
    // we have to have at least one character.
    if (filter == null || filter.length() < 2) {
      throw new IllegalArgumentException("Filter cannot be null or empty");
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
      return Deferred.fromResult(true);
    }
    return Deferred.fromResult(
        !(literals.contains(case_insensitive ? tagv.toLowerCase() : tagv)));
  }
  
  @Override
  public String debugInfo() {
    return "{literals=" + literals + ", case=" + case_insensitive + "}";
  }

  /** @return Whether or not this filter has case insensitivity enabled */
  @JsonIgnore
  public boolean isCaseInsensitive() {
    return case_insensitive;
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
    if (!(obj instanceof TagVNotLiteralOrFilter)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    final TagVNotLiteralOrFilter filter = (TagVNotLiteralOrFilter)obj;
    return Objects.equal(tagk, filter.tagk)
        && Objects.equal(literals, filter.literals)
        && Objects.equal(case_insensitive, filter.case_insensitive);
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(tagk, literals, case_insensitive);
  }
  
  /** @return a string describing the filter */
  public static String description() {
    return "Accepts one or more exact values and matches if the series does NOT "
        + "contain any of them. Multiple values can be included and must be "
        + "separated by the | (pipe) character. The filter is case sensitive "
        + "and will not allow characters that TSDB does not allow at write time.";
  }
  
  /** @return a list of examples showing how to use the filter */
  public static String examples() {
    return "host=not_literal_or(web01),  host=not_literal_or(web01|web02|web03)  "
        + "{\"type\":\"not_literal_or\",\"tagk\":\"host\","
        + "\"filter\":\"web01|web02|web03\",\"groupBy\":false}";
  }
  
  /**
   * Case insensitive version
   */
  public static class TagVNotILiteralOrFilter extends TagVNotLiteralOrFilter {
    
    /** Name of this filter */
    final public static String FILTER_NAME = "not_iliteral_or";
    
    public TagVNotILiteralOrFilter(final String tagk, final String filter) {
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
      if (!(obj instanceof TagVNotILiteralOrFilter)) {
        return false;
      }
      if (obj == this) {
        return true;
      }
      final TagVNotILiteralOrFilter filter = (TagVNotILiteralOrFilter)obj;
      return Objects.equal(tagk, filter.tagk)
          && Objects.equal(literals, filter.literals)
          && Objects.equal(case_insensitive, filter.case_insensitive);
    }
    
    /** @return a string describing the filter */
    public static String description() {
      return "Accepts one or more exact values and matches if the series does NOT "
          + "contain any of them. Multiple values can be included and must be "
          + "separated by the | (pipe) character. The filter is case insensitive "
          + "and will not allow characters that TSDB does not allow at write time.";
    }
    
    /** @return a list of examples showing how to use the filter */
    public static String examples() {
      return "host=not_iliteral_or(web01),  host=not_iliteral_or(web01|web02|web03)  "
          + "{\"type\":\"not_iliteral_or\",\"tagk\":\"host\","
          + "\"filter\":\"web01|web02|web03\",\"groupBy\":false}";
    }
  }
}
