// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import java.util.List;
import java.util.Map;

import com.google.common.hash.HashCode;

import net.opentsdb.configuration.Configuration;

/**
 * The configuration interface for a particular query node. Queries will populate
 * the configs when instantiating a DAG.
 * 
 * @since 3.0
 */
public interface QueryNodeConfig extends Comparable<QueryNodeConfig> {

  /**
   * @return The ID of the node in this config.
   */
  public String getId();
  
  /** @return The type of configuration registered with the TSDB registry.
   * This is used during parsing to determine what parser to use and 
   * also what kind of node to generate. */
  public String getType();
  
  /** @return The list of {@link #getId()}s of other nodes that
   * feed into this node. This is used to build the query DAG. */
  public List<String> getSources();
  
  /** @return A hash code for this configuration. */
  public HashCode buildHashCode();
  
  @Override
  public boolean equals(final Object o);
  
  @Override
  public int hashCode();
  
  /** @return Whether or not the node config can be pushed down to 
   * the query source. */
  public boolean pushDown();
  
  /** @return Whether or not this type of node joins results. E.g. an
   * binary expression node will take two results from downstream and 
   * combine them into one so this would be true, vs. a group by node
   * will group each result from downstream and pass it up. */
  public boolean joins();
  
  /** @return An optional map of query parameter overrides. May be null. */
  public Map<String, String> getOverrides();
  
  /**
   * Retrieve a query-time override as a string.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The string or null if not set anywhere.
   */
  public String getString(final Configuration config, final String key);
  
  /**
   * Retrieve a query-time override as an integer.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The value if parsed successfully.
   */
  public int getInt(final Configuration config, final String key);
  
  /**
   * Retrieve a query-time override as an integer.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The value if parsed successfully.
   */
  public long getLong(final Configuration config, final String key);
  
  /**
   * Retrieve a query-time override as a boolean. Only 'true', '1' or 'yes'
   * are considered true.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return True or false.
   */
  public boolean getBoolean(final Configuration config, final String key);
  
  /**
   * Retrieve a query-time override as a double.
   * @param config A non-null config object.
   * @param key The non-null and non-empty key.
   * @return The value if parsed successfully.
   */
  public double getDouble(final Configuration config, final String key);
  
  /**
   * Whether or not the key is present in the map, may have a null value.
   * @param key The non-null and non-empty key.
   * @return True if the key is present, false if not.
   */
  public boolean hasKey(final String key);

  /**
   * The interface for a QueryNodeConfig builder implementation.
   */
  public static interface Builder {
    
    /**
     * @param id A non-null and non-empty unique Id for the node in query. 
     * @return The builder.
     */
    public Builder setId(final String id);
    
    /**
     * @param type The class or type of the implementation if not set
     * in the ID.
     * @return The builder.
     */
    public Builder setType(final String type);
    
    /**
     * @param sources An optional list of sources consisting of the IDs 
     * of a nodes in the graph.
     * @return The builder.
     */
    public Builder setSources(final List<String> sources);
    
    /**
     * @param source A source to pull from for this node.
     * @return The builder.
     */
    public Builder addSource(final String source);
    
    /**
     * @param overrides An override map to replace the existing map.
     * @return The builder.
     */
    public Builder setOverrides(final Map<String, String> overrides);
    
    /**
     * @param key An override key to store in the override map.
     * @param value A value to store, overwriting existing values.
     * @return The builder.
     */
    public Builder addOverride(final String key, final String value);
  
    /** @return The non-null config instance. */
    public QueryNodeConfig build();
  }
  
}
