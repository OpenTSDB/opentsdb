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

import java.util.Collection;
import java.util.List;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;

/**
 * The API used to interact with a query pipeline. This should be given to the
 * caller by a query builder and the caller must call {@link #fetchNext()} to
 * start the query.
 * 
 * @since 3.0
 */
public interface QueryContext {

  /**
   * Returns the current sinks for this component.
   * @return A non-null and non empty collection of sinks. Note that the 
   * collection cannot be null or empty as a query context must not be created
   * without at least one valid sinks.
   */
  public Collection<QuerySink> sinks();
  
  /**
   * Returns the mode the query is executing in.
   * @return The non-null query mode.
   */
  public QueryMode mode();
  
  /**
   * Travels downstream the pipeline to fetch the next set of results. 
   * <b>WARNING:</b> Make sure to call {@link #initialize(Span)} before
   * calling this function.
   * @param span An optional tracing span.
   * @throws IllegalStateException if no sinks was set on this context.
   */
  public void fetchNext(final Span span);
  
  /**
   * Closes the pipeline and releases all resources.
   */
  public void close();
  
  /**
   * @return An optional stats collector for the query, may be null.
   */
  public QueryStats stats();
  
  /**
   * @return A list of zero or more sink configurations. If none are 
   * provided, default configs should be used.
   */
  public List<QuerySinkConfig> sinkConfigs();
  
  /** @return The original query. */
  public TimeSeriesQuery query();
  
  /** @return The TSDB to which we belong. */
  public TSDB tsdb();
  
  /** @return The optional auth state. May be null if auth is not enabled. */
  public AuthState authState();
  
  /**
   * Called after building the context but before calling 
   * {@link #fetchNext(Span)} so filters and such can be initialized.
   * @return A deferred resolving to a null if successful or an 
   * exception if something went wrong.
   */
  public Deferred<Void> initialize(final Span span);
  
  /**
   * Allows retrieval of the time series ID for the given hash and type from a
   * partial time series.
   * @param hash The hash to look up.
   * @param type The non-null type.
   * @return The time series ID if found, null if not.
   */
  public TimeSeriesId getId(final long hash, 
                            final TypeToken<? extends TimeSeriesId> type);
  
  /**
   * An optional list of entries from the execution to help users understand
   * what's happening under the hood.
   * @return A non-null, possibly empty, list of log entries.
   */
  public List<String> logs();
  
  /**
   * Adds an error level log entry.
   * @param log A non-null and non-empty log line.
   */
  public void logError(final String log);
  
  /**
   * Adds an error level log entry.
   * @param node An optional node.
   * @param log A non-null and non-empty log line.
   */
  public void logError(final QueryNode node, final String log);
 
  /**
   * Adds a warn level log entry.
   * @param log A non-null and non-empty log line.
   */
  public void logWarn(final String log);
  
  /**
   * Adds a warn level log entry.
   * @param node An optional node.
   * @param log A non-null and non-empty log line.
   */
  public void logWarn(final QueryNode node, final String log);
  
  /**
   * Adds an info level log entry.
   * @param log A non-null and non-empty log line.
   */
  public void logInfo(final String log);
  
  /**
   * Adds an info level log entry.
   * @param node An optional node.
   * @param log A non-null and non-empty log line.
   */
  public void logInfo(final QueryNode node, final String log);
  
  /**
   * Adds a debug level log entry.
   * @param log A non-null and non-empty log line.
   */
  public void logDebug(final String log);
  
  /**
   * Adds a debug level log entry.
   * @param node An optional node.
   * @param log A non-null and non-empty log line.
   */
  public void logDebug(final QueryNode node, final String log);
  
  /**
   * Adds a trace level log entry.
   * @param log A non-null and non-empty log line.
   */
  public void logTrace(final String log);
  
  /**
   * Adds a trace level log entry.
   * @param node An optional node.
   * @param log A non-null and non-empty log line.
   */
  public void logTrace(final QueryNode node, final String log);
}
