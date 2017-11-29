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

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.google.common.base.Objects;
import com.stumbleupon.async.Deferred;

/**
 * A filter that allows for regular expression matching on tag values.
 * @since 2.2
 */
public class TagVRegexFilter extends TagVFilter {
  /** Name of this filter */
  final public static String FILTER_NAME = "regexp";
  
  /** The compiled pattern */
  final Pattern pattern;
  
  /**
   * The default Ctor that disables case insensitivity
   * @param tagk The tag key to associate with this filter
   * @param filter The filter to match on
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   * @throws PatternSyntaxException if the pattern was invalid
   */
  public TagVRegexFilter(final String tagk, final String filter) {
    super(tagk, filter);
    // we have to have at least one character.
    if (filter == null || filter.length() < 1) {
      throw new IllegalArgumentException("Filter cannot be null or empty");
    }
    pattern = Pattern.compile(filter);
  }
  
  @Override
  public Deferred<Boolean> match(final Map<String, String> tags) {
    final String tagv = tags.get(tagk);
    if (tagv == null) {
      return Deferred.fromResult(false);
    }
    return Deferred.fromResult(pattern.matcher(tagv).find());
  }

  @Override
  public String debugInfo() {
    return "{pattern=" + pattern.toString() + "}";
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TagVRegexFilter)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    final TagVRegexFilter filter = (TagVRegexFilter)obj;
    // NOTE: apparently different pattern objects with the SAME pattern will
    // return a different hash. *sigh*. So cast the pattern to a string, THEN
    // compare.
    return Objects.equal(tagk, filter.tagk)
        && Objects.equal(pattern.pattern(), filter.pattern.pattern());
  }
  
  @Override
  public int hashCode() {
    // NOTE: apparently different pattern objects with the SAME pattern will
    // return a different hash. *sigh*. So cast the pattern to a string, THEN
    // compare.
    return Objects.hashCode(tagk, pattern.pattern());
  }

  @Override
  public String getType() {
    return FILTER_NAME;
  }
  
  /** @return a string describing the filter */
  public static String description() {
    return "Provides full, POSIX compliant regular expression using the "
        + "built in Java Pattern class. Note that an expression containing "
        + "curly braces {} will not parse properly in URLs. If the pattern "
        + "is not a valid regular expression then an exception will be raised.";
  }
  
  /** @return a list of examples showing how to use the filter */
  public static String examples() {
    return "host=regexp(.*)  {\"type\":\"regexp\",\"tagk\":\"host\","
        + "\"filter\":\".*\",\"groupBy\":false}";
  }
}
