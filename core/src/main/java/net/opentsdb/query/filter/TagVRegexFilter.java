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
   * @throws java.util.regex.PatternSyntaxException if the pattern was invalid
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
