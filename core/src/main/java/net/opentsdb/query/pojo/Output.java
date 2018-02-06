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
package net.opentsdb.query.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;

import net.opentsdb.core.Const;

/**
 * Pojo builder class used for serdes of the output component of a query
 * @since 2.3
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Output.Builder.class)
public class Output extends Validatable implements Comparable<Output> {
  /** The ID of a metric or expression to emit */
  private String id;
  
  /** An alias to use as the metric name for the output */
  private String alias;

  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Output(final Builder builder) {
    this.id = builder.id;
    this.alias = builder.alias;
  }
  
  /** @return the ID of a metric or expression to emit */
  public String getId() {
    return id;
  }

  /** @return an alias to use as the metric name for the output */
  public String getAlias() {
    return alias;
  }

  /** @return A new builder for the output */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Clones an output into a new builder.
   * @param output A non-null output to pull values from
   * @return A new builder populated with values from the given output.
   * @throws IllegalArgumentException if the output was null.
   * @since 3.0
   */
  public static Builder newBuilder(final Output output) {
    if (output == null) {
      throw new IllegalArgumentException("Output cannot be null.");
    }
    return new Builder()
        .setId(output.id)
        .setAlias(output.alias);
  }

  /** Validates the output
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  @Override public void validate() { 
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("missing or empty id");
    }
    TimeSeriesQuery.validateId(id);
  }
  
  @Override
  public String toString() {
    return "var=" + id + ", alias=" + alias;
  }
    
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Output output = (Output) o;

    return Objects.equal(output.alias, alias)
        && Objects.equal(output.id, id);
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }

  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    return Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(id), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(alias), Const.UTF8_CHARSET)
        .hash();
  }

  @Override
  public int compareTo(final Output o) {
    return ComparisonChain.start()
        .compare(id, o.id, Ordering.natural().nullsFirst())
        .compare(alias, o.alias, Ordering.natural().nullsFirst())
        .result();
  }

  /**
   * A builder for the downsampler component of a query
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static final class Builder {
    @JsonProperty
    private String id;
    @JsonProperty
    private String alias;

    public Builder setId(String id) {
      TimeSeriesQuery.validateId(id);
      this.id = id;
      return this;
    }

    public Builder setAlias(String alias) {
      this.alias = alias;
      return this;
    }

    public Output build() {
      return new Output(this);
    }
  }
}
