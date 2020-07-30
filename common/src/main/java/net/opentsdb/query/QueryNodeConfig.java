// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
package net.opentsdb.query;

import com.google.common.hash.HashCode;
import net.opentsdb.configuration.Configuration;

import java.util.List;
import java.util.Map;

/**
 * The configuration interface for a particular query node. Queries will populate
 * the configs when instantiating a DAG.
 * 
 * @since 3.0
 */
public interface QueryNodeConfig<B extends QueryNodeConfig.Builder<B, C>, C extends QueryNodeConfig> extends Comparable<C> {

  /**
   * @return The ID of the node in this config.
   */
  String getId();
  
  /** @return The type of configuration registered with the TSDB registry.
   * This is used during parsing to determine what parser to use and 
   * also what kind of node to generate. */
  String getType();
  
  /** @return The list of {@link #getId()}s of other nodes that
   * feed into this node. This is used to build the query DAG. */
  List<String> getSources();
  
  /** @return A hash code for this configuration. */
  HashCode buildHashCode();
  
  @Override
  boolean equals(final Object o);
  
  @Override
  int hashCode();
  
  /** @return Whether or not the node config can be pushed down to 
   * the query source. */
  boolean pushDown();

  /** @return Whether or not this type of node joins results. E.g. an
   * binary expression node will take two results from downstream and 
   * combine them into one so this would be true, vs. a group by node
   * will group each result from downstream and pass it up. */
  boolean joins();
  
  /** @return Whether or not the data from this node is cachable. */
  boolean readCacheable();
  
  /** @return An optional map of query parameter overrides. May be null. */
  Map<String, String> getOverrides();
  
  /**
   * Retrieve a query-time override as a string.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The string or null if not set anywhere.
   */
  String getString(final Configuration config, final String key);
  
  /**
   * Retrieve a query-time override as an integer.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The value if parsed successfully.
   */
  int getInt(final Configuration config, final String key);
  
  /**
   * Retrieve a query-time override as an integer.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The value if parsed successfully.
   */
  long getLong(final Configuration config, final String key);
  
  /**
   * Retrieve a query-time override as a boolean. Only 'true', '1' or 'yes'
   * are considered true.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return True or false.
   */
  boolean getBoolean(final Configuration config, final String key);
  
  /**
   * Retrieve a query-time override as a double.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The value if parsed successfully.
   */
  double getDouble(final Configuration config, final String key);
  
  /**
   * Whether or not the key is present in the map, may have a null value.
   * @param key The non-null and non-empty key.
   * @return True if the key is present, false if not.
   */
  boolean hasKey(final String key);

  /**
   * Create a builder from the config
   * @return
   */
  B toBuilder();

  /**
   * The interface for a QueryNodeConfig builder implementation.
   */
  interface Builder<B extends Builder<B, C>, C extends QueryNodeConfig> {
    
    /**
     * @param id A non-null and non-empty unique Id for the node in query. 
     * @return The builder.
     */
    B setId(final String id);
    
    /**
     * @param type The class or type of the implementation if not set
     * in the ID.
     * @return The builder.
     */
    B setType(final String type);
    
    /**
     * @param sources An optional list of sources consisting of the IDs 
     * of a nodes in the graph.
     * @return The builder.
     */
    B setSources(final List<String> sources);
    
    /**
     * @param source A source to pull from for this node.
     * @return The builder.
     */
    B addSource(final String source);
    
    /**
     * @param overrides An override map to replace the existing map.
     * @return The builder.
     */
    B setOverrides(final Map<String, String> overrides);
    
    /**
     * @param key An override key to store in the override map.
     * @param value A value to store, overwriting existing values.
     * @return The builder.
     */
    B addOverride(final String key, final String value);

    /** @return The non-null config instance. */
    C build();

    /**
     * Concrete subclass builder should implement it and return it's current object.
     *
     * <pre>{@code
     * class Builder implements QueryNodeConfig.Builder {
     *    @Override
     *    public Builder self() {
     *      return this;
     *    }
     * }
     * }</pre>
     *
     * @return builder
     */
    B self();
  }
  
}
