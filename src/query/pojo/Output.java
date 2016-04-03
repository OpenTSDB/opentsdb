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
package net.opentsdb.query.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;

/**
 * Pojo builder class used for serdes of the output component of a query
 * @since 2.3
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Output.Builder.class)
public class Output extends Validatable {
  /** The ID of a metric or expression to emit */
  private String id;
  
  /** An alias to use as the metric name for the output */
  private String alias;

  /**
   * Default ctor
   * @param builder The builder to pull values from
   */
  public Output(Builder builder) {
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
  public static Builder Builder() {
    return new Builder();
  }

  /** Validates the output
   * @throws IllegalArgumentException if one or more parameters were invalid
   */
  @Override public void validate() { 
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("missing or empty id");
    }
    Query.validateId(id);
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
    return Objects.hashCode(id, alias);
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
      Query.validateId(id);
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
