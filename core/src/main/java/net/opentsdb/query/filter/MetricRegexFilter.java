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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.stats.Span;

import java.util.regex.Pattern;

/**
 * Filters by matching a regex for metric
 *
 * @since 3.0
 */
@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = MetricRegexFilter.Builder.class)
public class MetricRegexFilter implements MetricFilter {

  /**
   * The metric.
   */
  protected final String metric;

  /**
   * The compiled pattern
   */
  final Pattern pattern;

  /**
   * Protected local ctor.
   *
   * @param builder A non-null builder.
   */
  public MetricRegexFilter(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.metric)) {
      throw new IllegalArgumentException("Filter cannot be null or empty.");
    }
    this.metric = builder.metric.trim();

    pattern = Pattern.compile(metric.trim());
  }

  @Override
  public String getMetric() {
    return metric;
  }

  @Override
  public boolean matches(final String metric) {
    return pattern.matcher(metric).find();
  }

  @Override
  public String getType() {
    return MetricRegexFactory.TYPE;
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
    private String metric;

    public Builder setMetric(final String metric) {
      this.metric = metric;
      return this;
    }

    public MetricRegexFilter build() {
      return new MetricRegexFilter(this);
    }
  }
}
