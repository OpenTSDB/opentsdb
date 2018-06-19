// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;

import net.opentsdb.query.QueryNodeConfig;

/**
 * The base class used for configuring an executor. This class must be
 * serializable via Jackson for use in query overrides. Extend the Builder
 * class as needed.
 * <p>
 * <b>Note:</b> The implementations must include the {@link #executor_id} and
 * {@link #executor_type} in the {@link #equals(Object)}, {@link #hashCode()},
 * {@link #buildHashCode()} and {@link #compareTo(QueryExecutorConfig)} 
 * implementations. These are used to generate hashes.
 * <p>
 * <b>Note:</b> Make sure the implementing config calls 
 * {@code
 *   JsonDeserialize(builder = Config.Builder.class)
 * }
 * 
 * @since 3.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "executorType",
  visible = true)
public abstract class QueryExecutorConfig implements QueryNodeConfig {
  /** The class type of executor. */
  protected final String executor_type;
  
  /** The executor ID, a unique name within a graph. */
  protected final String executor_id;
  
  /** The factory this config is associated with. */
  protected QueryExecutorFactory<?> factory;
  
  /**
   * Default ctor.
   * @param builder A non-null builder for the config.
   * @throws IllegalArgumentException if the executor type or id were null or
   * empty.
   */
  protected QueryExecutorConfig(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.executorType)) {
      throw new IllegalArgumentException("Executor type cannot be null or "
          + "empty.");
    }
    if (Strings.isNullOrEmpty(builder.executorId)) {
      throw new IllegalArgumentException("Executor id cannot be null or empty.");
    }
    this.executor_type = builder.executorType;
    this.executor_id = builder.executorId;
  }
  
  /** @return The factory this config is associated with. */
  public QueryExecutorFactory<?> factory() {
    return factory;
  }
  
  /** @return The class of the executor (may not be fully qualified). 
   * Purposely not "get" as it conflicts with the Jackson name. */
  public String executorType() {
    return executor_type;
  }
  
  /** @return The unique ID of the executor within a graph. */
  public String getExecutorId() {
    return executor_id;
  }
  
  /** @param factory A factory to associate this config with. */
  public void setFactory(final QueryExecutorFactory<?> factory) {
    this.factory = factory;
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("executorType=")
        .append(executor_type)
        .append(", executorId=")
        .append(executor_id)
        .append(", factory=")
        .append(factory)
        .toString();
  }
  
  @Override
  public abstract boolean equals(final Object o);
  
  @Override
  public abstract int hashCode();
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public abstract HashCode buildHashCode();
  
  @Override
  public abstract int compareTo(final QueryNodeConfig config);
  
  /** Base builder for QueryExecutorConfigs. */
  public static abstract class Builder {
    @JsonProperty
    protected String executorType;
    @JsonProperty
    protected String executorId;
    
    /**
     * @param executorType The class type (full or simple) of the executor
     * this config pertains to.
     * @return The builder.
     */
    public Builder setExecutorType(final String executorType) {
      this.executorType = executorType;
      return this;
    }
    
    /**
     * @param executorId The unique ID of the executor within an execution
     * graph.
     * @return The builder.
     */
    public Builder setExecutorId(final String executorId) {
      this.executorId = executorId;
      return this;
    }
  
    /** @return A config object or exceptions if the config failed. */
    public abstract QueryExecutorConfig build();
  }
}
