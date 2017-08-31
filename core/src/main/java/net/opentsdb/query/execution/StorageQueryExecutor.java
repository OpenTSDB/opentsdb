// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashCode;
import com.stumbleupon.async.Callback;

import io.opentracing.Span;
import net.opentsdb.core.Const;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.storage.TimeSeriesDataStore;

/**
 * An executor that calls a Storage plugin to execute the query. Simple 
 * pass-through implementation that tracks the outstanding executions.
 * 
 * @param <T> The return type of data.
 */
public class StorageQueryExecutor<T> extends QueryExecutor<T>  {
  /** The data store to work with. */
  private TimeSeriesDataStore data_store;
  
  /**
   * Default ctor.
   * @param node A non-null node to pull the ID and config from.
   * @throws IllegalArgumentException if a config param was invalid.
   */
  public StorageQueryExecutor(final ExecutionGraphNode node) {
    super(node);
    if (Strings.isNullOrEmpty(((Config) node.getDefaultConfig()).getStorageId())) {
      throw new IllegalArgumentException("Default storage ID must be non-empty.");
    }
    try {
      data_store = (TimeSeriesDataStore) node.graph().tsdb().getRegistry()
          .getPlugin(TimeSeriesDataStore.class, 
              ((Config) node.getDefaultConfig()).getStorageId());
      if (data_store == null) {
        throw new IllegalStateException("No data store found for ID " 
            + ((Config) node.getDefaultConfig()).getStorageId());
      }
    } catch (RuntimeException e) {
      throw new RuntimeException("Unexpected exception initializing "
          + "storage query executor", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public QueryExecution<T> executeQuery(final QueryContext context,
                                        final TimeSeriesQuery query, 
                                        final Span upstream_span) {
    
    class Cleanup implements Callback<T, T> {
      private final QueryExecution<T> execution;
      public Cleanup(final QueryExecution<T> execution) {
        this.execution = execution;
      }
      @Override
      public T call(final T arg) throws Exception {
        outstanding_executions.remove(execution);
        return arg;
      }
    }
    
    final QueryExecution<T> execution = 
        (QueryExecution<T>) data_store.runTimeSeriesQuery(context, query, upstream_span);
    outstanding_executions.add(execution);
    execution.deferred().addBoth(new Cleanup(execution));
    return execution;
  }

  /** The configuration class for this executor. */
  @JsonInclude(Include.NON_NULL)
  @JsonDeserialize(builder = Config.Builder.class)
  public static class Config extends QueryExecutorConfig {
    private String storage_id;
    
    protected Config(Builder builder) {
      super(builder);
      storage_id = builder.storageId; 
    }
    
    public String getStorageId() {
      return storage_id;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Config config = (Config) o;
      return Objects.equal(executor_id, config.executor_id)
          && Objects.equal(executor_type, config.executor_type)
          && Objects.equal(storage_id, config.storage_id);
    }
    
    @Override
    public int hashCode() {
      return buildHashCode().asInt();
    }

    @Override
    public HashCode buildHashCode() {
      return Const.HASH_FUNCTION().newHasher()
        .putString(Strings.nullToEmpty(executor_id), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(executor_type), Const.UTF8_CHARSET)
        .putString(Strings.nullToEmpty(storage_id), Const.UTF8_CHARSET)
        .hash();
    }

    @Override
    public int compareTo(QueryExecutorConfig config) {
      return ComparisonChain.start()
          .compare(executor_id, config.executor_id, 
              Ordering.natural().nullsFirst())
          .compare(executor_type, config.executor_type, 
              Ordering.natural().nullsFirst())
          .compare(storage_id, ((Config) config).storage_id, 
              Ordering.natural().nullsFirst())
          .result();
    }
    
    public static Builder newBuilder() {
      return new Builder();
    }
    
    public static class Builder extends QueryExecutorConfig.Builder {
      @JsonProperty
      private String storageId;
      
      public Builder setStorageId(final String storageId) {
        this.storageId = storageId;
        return this;
      }
      
      @Override
      public Config build() {
        return new Config(this);
      }
    }
  }
}
