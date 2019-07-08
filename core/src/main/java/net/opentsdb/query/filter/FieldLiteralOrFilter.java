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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.Const;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Comparators;
import net.opentsdb.utils.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Filters on a set of one or more case sensitive tag value strings.
 *
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = FieldLiteralOrFilter.Builder.class)
public class FieldLiteralOrFilter extends BaseFieldFilter {

  /** A list of strings to match on */
  protected final List<String> literals;

  /**
   * Protected builder ctor
   *
   * @param builder The non-null builder.
   * @throws IllegalArgumentException if the tagk or filter were empty or null
   */
  protected FieldLiteralOrFilter(final Builder builder) {
    super(builder.key, builder.filter);
    if (filter.length() == 1 && filter.charAt(0) == '|') {
      throw new IllegalArgumentException("Filter must contain more than just a pipe");
    }
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
  public String getType() {
    return FieldLiteralOrFactory.TYPE;
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
        .append(", key=")
        .append(key)
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

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    if (!super.equals(o)) {
      return false;
    }

    final FieldLiteralOrFilter otherFilter = (FieldLiteralOrFilter) o;

    // comparing literals
    if (!Comparators.ListComparison.equalLists(literals, otherFilter.literals())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final Hasher hc = Const.HASH_FUNCTION().newHasher()
            .putString(Strings.nullToEmpty(key), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(filter), Const.UTF8_CHARSET)
            .putString(Strings.nullToEmpty(getType()), Const.UTF8_CHARSET);

    if (literals != null) {
      final List<String> keys = Lists.newArrayList(literals);
      Collections.sort(keys);
      for (final String key : keys) {
        hc.putString(key, Const.UTF8_CHARSET);
      }
    }

    return hc.hash();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty private String key;
    @JsonProperty private String filter;

    public Builder setKey(final String key) {
      this.key = key;
      return this;
    }

    public Builder setFilter(final String filter) {
      this.filter = filter;
      return this;
    }

    public FieldLiteralOrFilter build() {
      return new FieldLiteralOrFilter(this);
    }
  }
}
