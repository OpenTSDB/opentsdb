// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.Const;
import net.opentsdb.stats.Span;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = PassThroughStringFilter.Builder.class)
public class PassThroughStringFilter implements PassThroughFilter {

  /** The metric. */
  protected final String filter;

  /**
   * Protected local ctor.
   * @param builder A non-null builder.
   */
  protected PassThroughStringFilter(final PassThroughStringFilter.Builder builder) {
    if (Strings.isNullOrEmpty(builder.filter)) {
      throw new IllegalArgumentException("Filter cannot be null or empty.");
    }
    this.filter = builder.filter.trim();
  }

  @Override
  public String getFilter() {
    return filter;
  }

  @Override
  public boolean matches(final String filter) {
    return this.filter.equals(filter);
  }

  @Override
  public String getType() {
    return PassThroughFilterFactory.TYPE;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final PassThroughStringFilter otherFreeTextFilter = (PassThroughStringFilter) o;

    return Objects.equal(filter, otherFreeTextFilter.getFilter());

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

  public static PassThroughStringFilter.Builder newBuilder() {
    return new PassThroughStringFilter.Builder();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String filter;

    public PassThroughStringFilter.Builder setFilter(final String filter) {
      this.filter = filter;
      return this;
    }

    public PassThroughStringFilter build() {
      return new PassThroughStringFilter(this);
    }
  }
}
