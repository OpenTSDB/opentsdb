// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.stats.Span;

/**
 * Regular expression filter to search for in any field for meta.
 *
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = AnyFieldRegexFilter.Builder.class)
public class AnyFieldRegexFilter implements QueryFilter {

  /** The filter. */
  private final String filter;
  
  /**
   * The compiled pattern
   */
  private final Pattern pattern;

  /** Whether or not the regex would match-all. */
  final boolean matches_all;
  
  /**
   * Protected ctor.
   *
   * @param builder The non-null builder.
   */
  protected AnyFieldRegexFilter(final Builder builder) {
    filter = builder.filter;
    if (Strings.isNullOrEmpty(filter)) {
      throw new IllegalArgumentException("Filter cannot be null or empty.");
    }
    pattern = Pattern.compile(builder.filter.trim());
    
    if (builder.filter.trim().equals(".*") || 
        builder.filter.trim().equals("^.*") || 
        builder.filter.trim().equals(".*$") || 
        builder.filter.trim().equals("^.*$")) {
      // yeah there are many more permutations but these are the most likely
      // to be encountered in the wild.
      matches_all = true;
    } else {
      matches_all = false;
    }
  }

  /** @return The original filter string. */
  public String getFilter() {
    return filter;
  }
  
  public boolean matches(final String value) {
    return pattern.matcher(value).find();
  }
  
  public boolean matches(final Map<String, String> tags) {
    for (final String key : tags.keySet()) {
      if (pattern.matcher(key).find()) {
        return true;
      }
    }
    for (final String value : tags.values()) {
      if (pattern.matcher(value).find()) {
        return true;
      }
    }
    return false;
  }

  /** Whether or not the regex would match all strings. */
  public boolean matchesAll() {
    return matches_all;
  }
  
  @Override
  public String getType() {
    return AnyFieldRegexFactory.TYPE;
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
            .append("{type=")
            .append(getClass().getSimpleName())
            .append(", filter=")
            .append(pattern.toString())
            .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final AnyFieldRegexFilter otherRegexFilter = (AnyFieldRegexFilter) o;

    return Objects.equal(filter, otherRegexFilter.getFilter());
  }


  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }


  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final HashCode hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(filter), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(getType()), Const.UTF8_CHARSET)
            .hash();

    return hc;
  }

  @Override
  public Deferred<Void> initialize(final Span span) {
    return INITIALIZED;
  }

  /** @return The compiled pattern. */
  public Pattern pattern() {
    return pattern;
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

    public AnyFieldRegexFilter build() {
      return new AnyFieldRegexFilter(this);
    }
  }

}
