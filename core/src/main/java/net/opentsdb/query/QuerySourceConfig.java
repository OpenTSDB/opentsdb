//This file is part of OpenTSDB.
//Copyright (C) 2017-2018  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 2.1 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.
package net.opentsdb.query;

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.Const;

/**
 * A simple base config class for {@link TimeSeriesDataSource} nodes.
 * 
 * @since 3.0
 */
public class QuerySourceConfig implements QueryNodeConfig {
  
  /** The query from the caller. */
  protected final TimeSeriesQuery query;
  
  /** A unique name for this config. */
  private final String id;
  
  /** The configuration class in case we need to pull info out. */
  private final Configuration configuration;
  
  /**
   * Private ctor for the builder.
   * @param builder The non-null builder.
   */
  private QuerySourceConfig(final Builder builder) {
    if (builder.query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    if (builder.configuration == null) {
      throw new IllegalArgumentException("Configuration cannot be null.");
    }
    query = builder.query;
    id = builder.id;
    configuration = builder.configuration;
  }
  
  /** @return The query the node is executing. */
  public TimeSeriesQuery query() {
    return query;
  }
  
  @Override
  public String getId() {
    return id;
  }
  
  /** @return The master configuration class. */
  public Configuration configuration() {
    return configuration;
  }
  
  @Override
  public int compareTo(final QueryNodeConfig o) {
    if (!(o instanceof QuerySourceConfig)) {
      return -1;
    }
    
    return ComparisonChain.start()
        .compare(id, ((QuerySourceConfig) o).id, Ordering.natural().nullsFirst())
        .compare(query, ((QuerySourceConfig) o).query, Ordering.natural().nullsFirst())
        .result();
  }

  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  @Override
  public HashCode buildHashCode() {
    final List<HashCode> hashes = Lists.newArrayListWithCapacity(2);
    hashes.add(Const.HASH_FUNCTION().newHasher().putString(id, 
        Const.UTF8_CHARSET).hash());
    hashes.add(query.buildHashCode());
    return Hashing.combineOrdered(hashes);
  }
  
  /** @return A new builder. */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private TimeSeriesQuery query;
    private String id;
    private Configuration configuration;
    
    /** @param query The non-null query to execute. */
    public Builder setQuery(final TimeSeriesQuery query) {
      this.query = query;
      return this;
    }
    
    /** @param id The non-null and non-empty ID for this config. */
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    /** @param configuration The non-null master config. */
    public Builder setConfiguration(final Configuration configuration) {
      this.configuration = configuration;
      return this;
    }
    
    public QuerySourceConfig build() {
      return new QuerySourceConfig(this);
    }
  }

}
