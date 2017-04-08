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
package net.opentsdb.core;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.DataMerger;
import net.opentsdb.data.DataShardMerger;
import net.opentsdb.data.DataShardsGroups;
import net.opentsdb.data.types.numeric.NumericMergeLargest;
import net.opentsdb.query.context.QueryExecutorContext;
import net.opentsdb.stats.TsdbTracer;

/**
 * A shared location for registering context, mergers, plugins, etc.
 *
 * @since 3.0
 */
public class Registry {
  
  /** The TSDB to which this registry belongs. Used for reading the config. */
  private final TSDB tsdb;
  
  /** The map of data mergers. */
  private final Map<TypeToken<?>, DataMerger<?>> data_mergers;
  
  /** Executor contexts container. Null key is the default. */
  private final Map<String, QueryExecutorContext> executor_contexts;
  
  /** The thread pool used for cleanup post query or other operations. */
  private final ExecutorService cleanup_pool;
  
  /** The loaded tracer plugin or null if disabled. */
  private TsdbTracer tracer_plugin;
  
  /**
   * Default Ctor. Sets up containers and initializes a cleanup pool but that's
   * all for now.
   * @param tsdb A non-null TSDB to load and pass to plugins.
   */
  public Registry(final TSDB tsdb) {
    if (tsdb == null) {
      throw new IllegalArgumentException("TSDB cannot be null.");
    }
    this.tsdb = tsdb;
    data_mergers = 
        Maps.<TypeToken<?>, DataMerger<?>>newHashMapWithExpectedSize(1);
    executor_contexts = 
        Maps.<String, QueryExecutorContext>newHashMapWithExpectedSize(1);
    cleanup_pool = Executors.newFixedThreadPool(1);
  }
  
  /**
   * Initializes plugins and registry types.
   * @return A non-null deferred to wait on for initialization to complete.
   */
  public Deferred<Object> initialize() {
    initDataMergers();
    if (tracer_plugin != null) {
      return tracer_plugin.initialize(tsdb);
    }
    return Deferred.fromResult(null);
  }
  
  /** @return An unmodifiable map of the data mergers. */
  public Map<TypeToken<?>, DataMerger<?>> dataMergers() {
    return Collections.unmodifiableMap(data_mergers);
  }
  
  /** @return The cleanup thread pool for post-query or other tasks. */
  public ExecutorService cleanupPool() {
    return cleanup_pool;
  }
  
  /**
   * Adds the given context to the registry.
   * @param context A non-null query executor context.
   * @param is_default Whether or not the context is the default.
   * @throws IllegalArgumentException if the context was null.
   */
  public void registerQueryExecutorContext(final QueryExecutorContext context,
      final boolean is_default) {
    if (context == null) {
      throw new IllegalArgumentException("Query executor context cannot "
          + "be null.");
    }
    if (Strings.isNullOrEmpty(context.id())) {
      throw new IllegalArgumentException("Query executor context returned a "
          + "null or empty ID");
    }
    if (is_default) {
      executor_contexts.put(null, context);
    }
    executor_contexts.put(context.id(), context);
  }
  
  /**
   * Returns the registered query executor context given an id or, if the id is
   * null or empty, the default.
   * @param id An optional ID to fetch the context for.
   * @return A query executor context or null if not found.
   */
  public QueryExecutorContext getQueryExecutorContext(final String id) {
    if (Strings.isNullOrEmpty(id)) {
      return executor_contexts.get(null);
    }
    return executor_contexts.get(id);
  }
  
  /**
   * Add the tracer implementation. Note that it must already be initialized.
   * @param tracer The tracer to pass to operations. May be null.
   */
  public void registerTracer(final TsdbTracer tracer) {
    this.tracer_plugin = tracer;
  }
  
  /** @return The tracer for use with operaitons. May be null. */
  public TsdbTracer tracer() {
    return tracer_plugin;
  }
  
  /** @return Package private shutdown returning the deferred to wait on. */
  Deferred<Object> shutdown() {
    cleanup_pool.shutdown();
    return Deferred.fromResult(null);
  }
  
  private void initDataMergers() {
    final DataShardMerger shards_merger = new DataShardMerger();
    shards_merger.registerStrategy(new NumericMergeLargest());
    data_mergers.put(DataShardsGroups.TYPE, shards_merger);
  }
  
}
