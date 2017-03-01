// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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

import com.google.common.base.Objects;
import com.stumbleupon.async.Deferred;

public class TagVNotKeyFilter extends TagVFilter {
  /** Name of this filter */
  final public static String FILTER_NAME = "not_key";
  
  public TagVNotKeyFilter(final String tagk, final String filter) {
    super(tagk, "");
    if (filter != null && filter.length() > 0) {
      throw new IllegalArgumentException("The filter must be empty for the " + 
          FILTER_NAME + " filter");
    }
    post_scan = true;
  }
  
  @Override
  public Deferred<Boolean> match(Map<String, String> tags) {
    if (tags.containsKey(tagk)) {
      return Deferred.fromResult(false);
    }
    return Deferred.fromResult(true);
  }

  @Override
  public String getType() {
    return FILTER_NAME;
  }

  @Override
  public String debugInfo() {
    return "{}";
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
    final TagVNotKeyFilter filter = (TagVNotKeyFilter)obj;
    return Objects.equal(tagk, filter.tagk);
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(tagk);
  }
  
  /** @return a string describing the filter */
  public static String description() {
    return "Skips any time series with the given tag key, regardless of the "
        + "value. This can be useful for situations where a metric has "
        + "inconsistent tag sets. NOTE: The filter value must be null or an "
        + "empty string.";
  }
  
  /** @return a list of examples showing how to use the filter */
  public static String examples() {
    return "host=not_key()  {\"type\":\"not_key\",\"tagk\":\"host\","
        + "\"filter\":\"\",\"groupBy\":false}";
  }
}
