// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.Span;
import net.opentsdb.utils.StringUtils;

/**
 * Filters on a case sensitive tag key.
 * 
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TagKeyLiteralOrFilter.Builder.class)
public class TagKeyLiteralOrFilter implements TagKeyFilter {
  
  /** The original filter. */
  protected final String filter;
  
  /** A list of strings to match on */
  protected final List<String> literals;
  
  /**
   * Protected builder ctor
   * @param builder The non-null builder.
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   */
  protected TagKeyLiteralOrFilter(final Builder builder) {
    if (builder.filter.length() == 1 && builder.filter.charAt(0) == '|') {
      throw new IllegalArgumentException("Filter must contain more than just a pipe");
    }
    filter = builder.filter;
    final String[] split = StringUtils.splitString(filter, '|');
    
    // dedupe
    final Set<String> dedupe = Sets.newHashSetWithExpectedSize(split.length);
    literals = Lists.newArrayListWithCapacity(split.length);
    for (String value : split) {
      value = value.trim();
      if (Strings.isNullOrEmpty(value)) {
        continue;
      }
      if (!dedupe.contains(value)) {
        dedupe.add(value);
        literals.add(value);
      }
    }
  }
  
  @Override
  public String filter() {
    return filter;
  }
  
  @Override
  public boolean matches(final Map<String, String> tags) {
    for (final String tag_key : tags.keySet()) {
      if (literals.contains(tag_key)) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public boolean matches(final String tag_key) {
    return literals.contains(tag_key);
  }
  
  @Override
  public String getType() {
    return TagValueLiteralOrFactory.TYPE;
  }
  
  /** @return The collection of literal strings for resolution. */
  public List<String> literals() {
    return literals;
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("{type=")
        .append(getClass().getSimpleName())
        .append(", filter=")
        .append(filter)
        .append("}")
        .toString();
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    return INITIALIZED;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String filter;
    
    public Builder setFilter(final String filter) {
      this.filter = filter;
      return this;
    }
    
    public TagKeyLiteralOrFilter build() {
      return new TagKeyLiteralOrFilter(this);
    }
  }
}
