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
