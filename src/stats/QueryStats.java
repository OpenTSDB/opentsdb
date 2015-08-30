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
package net.opentsdb.stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import net.opentsdb.core.QueryException;
import net.opentsdb.core.TSQuery;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * This class stores information about OpenTSDB queries executed through the 
 * HTTP API. It maintains a list of running queries as well as a cache of the 
 * last {@code COMPLETED_QUERY_CACHE_SIZE} number of queries executed. The
 * stats can be observed via /api/query/stats.
 * 
 * When a query is executed, it should instantiate an object of this class. 
 * Once the query is completed, make sure to call {@link markComplete}.
 * 
 * The cache will store each query based on the combination of the client, query
 * and the result code. If the same query was executed multiple times then it
 * will increment the "executed" counter for the query in the cache.
 * @since 2.2
 */
public class QueryStats {
  private static final Logger LOG = LoggerFactory.getLogger(QueryStats.class);
  
  /** Determines how many query stats to keep in the cache */
  private static int COMPLETED_QUERY_CACHE_SIZE = 256;
  
  /** Whether or not to allow duplicate queries from the same endpoint to
   * run simultaneously. */
  private static boolean ENABLE_DUPLICATES = false;
  
  /** Stores queries currently executing. If a thread doesn't call into 
   * markComplete then it's possible for this map to fill up.
   * Hash is the remote + query */
  private static ConcurrentHashMap<Integer, QueryStats> running_queries = 
      new ConcurrentHashMap<Integer, QueryStats>();
  
  /** Size limited cache of queries from the past. 
   * Hash is the remote + query + response code */
  private static Cache<Integer, QueryStats> completed_queries = 
      CacheBuilder.newBuilder().maximumSize(COMPLETED_QUERY_CACHE_SIZE).build();
  
  /** Start time for the query. Can be set post construction if necessary */
  private final long query_start;
  
  /** The remote address as <IP>:<port>, may be ipv6 */
  private final String remote_address;
  
  /** The TSQuery object that contains the query specification */
  private final TSQuery query;

  /** Amount of time taken for the query to complete, set on {@link markComplete} */
  private long time_total;
  
  /** Time it took to retrieve data from storage in ms*/
  private long time_storage;
  
  /** Time it took to aggregate over the data */
  private long time_aggregation;
  
  /** Time it took to serialize the data. Includes aggregation time and tag
   * lookups */
  private long time_serialization;
  
  /** Number of data points emitted, NOT the number of data points fetched */
  private long size;
  
  /** Total number of data points fetched from storage */
  private long aggregated_size;
  
  /** HTTP response when the query was completed, either successfully or failed */
  private HttpResponseStatus response;
  
  /** How many times this exact query was executed. Only updated on completion */
  private long executed;
  
  /** A possible exception if thrown when this query completes */
  private Throwable exception;
  
  /**
   * Default CTor
   * @param remote_address Remote address of the client
   * @param query Query being executed
   * @throws QueryException if the exact query is already running, e.g if the
   * client submitted the same query twice
   */
  public QueryStats(final String remote_address, final TSQuery query) {
    if (remote_address == null || remote_address.isEmpty()) {
      throw new IllegalArgumentException("Remote address was null or empty");
    }
    if (query == null) {
      throw new IllegalArgumentException("Query object was null");
    }
    this.remote_address = remote_address;
    this.query = query;
    executed = 1;
    query_start = DateTime.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("New query for remote " + remote_address + " with hash " + 
          hashCode() + " on thread " + Thread.currentThread().getId());
    }
    if (running_queries.putIfAbsent(this.hashCode(), this) != null) {
      if (ENABLE_DUPLICATES) {
        LOG.warn("Query " + query + " is already executing for endpoint: " + 
          remote_address);
      } else {
        throw new QueryException("Query is already executing for endpoint: " + 
            remote_address);
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Successfully put new query for remote " + remote_address + 
            " with hash " + hashCode() + " on thread " + 
            Thread.currentThread().getId() + " w q " + query.toString());
      }
    }
  }
  
  /**
   * Returns the hash based on the remote address and the query
   */
  public int hashCode() {
    return Objects.hashCode(remote_address, query.hashCode());
  }
  
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof QueryStats)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    final QueryStats stats = (QueryStats)obj;
    return Objects.equal(remote_address, stats.remote_address) 
        && Objects.equal(query, stats.query);
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder(256);
    buf.append("remote=")
       .append(remote_address)
       .append(", query=")
       .append(query)
       .append(", start=")
       .append(query_start)
       .append(", exception=")
       .append(exception == null ? "null" : exception.getMessage());
    return buf.toString();
  }
  
  /**
   * Marks a query as completed successfully with the 200 HTTP response code.
   * Moves it from the running map to the cache, updating the cache if it already
   * existed.
   */
  public void markComplete() {
    markComplete(HttpResponseStatus.OK, null);
  }
  
  /**
   * Marks a query as completed with the given HTTP code and moves it from the
   * running map to the cache, updating the cache if it already existed.
   * @param response The HttpStatus code to store
   * @param exception An optional exception
   */
  public void markComplete(final HttpResponseStatus response, 
      final Throwable exception) {
    this.exception = exception;
    LOG.debug("Marking query as complete for " + remote_address + " with hash " + 
        hashCode() + " on thread " + Thread.currentThread().getId() + " And q: " + 
        query.toString());
    this.response = response;
    time_total = DateTime.currentTimeMillis() - query_start;
    synchronized (running_queries) {
      if (!running_queries.containsKey(this.hashCode())) {
        if (!ENABLE_DUPLICATES) {
          LOG.warn("Query was already marked as complete: " + this);
        }
        return;
      }
      running_queries.remove(this.hashCode());
      LOG.debug("Removed completed query " + remote_address + " with hash " + 
          hashCode() + " on thread " + Thread.currentThread().getId());
    }
    
    final int cache_hash = this.hashCode() ^ response.toString().hashCode();
    synchronized (completed_queries) {
      final QueryStats old_query = completed_queries.getIfPresent(cache_hash);
      if (old_query == null) {
        completed_queries.put(cache_hash, this);
      } else {
        old_query.executed++;
      }
    }
    LOG.info("completed_query=" + JSON.serializeToString(this));
  }
  
  /**
   * Builds a serializable map from the running and cached query maps to be
   * returned to a caller.
   * @return A map for serialization
   */
  public static Map<String, List<Map<String, Object>>> buildStats() {
    Map<String, List<Map<String, Object>>> root = 
        new HashMap<String, List<Map<String, Object>>>();
    
    if (running_queries.isEmpty()) {
      root.put("running", Collections.<Map<String, Object>> emptyList());
    } else {
      final List<Map<String, Object>> running = 
          new ArrayList<Map<String, Object>>(running_queries.size());
      root.put("running", running);
      
      // don't need to lock the map beyond what the iterator will do implicitly
      for (final QueryStats stats : running_queries.values()) {
        final Map<String, Object> obj = new HashMap<String, Object>(10);
        obj.put("query", stats.query);
        obj.put("remote", stats.remote_address);
        obj.put("queryStart", stats.query_start);
        obj.put("timeTotal", stats.time_total);
        obj.put("elapsed", DateTime.currentTimeMillis() - stats.query_start);
        running.add(obj);
      }
    }
    
    final Map<Integer, QueryStats> completed = completed_queries.asMap();
    if (completed.isEmpty()) {
      root.put("completed", Collections.<Map<String, Object>> emptyList());
    } else {
      final List<Map<String, Object>> running = 
          new ArrayList<Map<String, Object>>(completed.size());
      root.put("completed", running);
      
      // don't need to lock the map beyond what the iterator will do implicitly
      for (final QueryStats stats : completed.values()) {
        final Map<String, Object> obj = new HashMap<String, Object>(10);
        obj.put("query", stats.query);
        obj.put("remote", stats.remote_address);
        obj.put("queryStart", stats.query_start);
        obj.put("timeTotal", stats.time_total);
        obj.put("executed", stats.executed);
        obj.put("datapoints", stats.size);
        obj.put("rawDatapoints", stats.aggregated_size);
        obj.put("status", stats.response.getCode());
        obj.put("timeStorage", stats.time_storage);
        obj.put("timeAggregation", stats.time_aggregation);
        obj.put("timeSerialization", stats.time_serialization);
        obj.put("exception", stats.exception);
        running.add(obj);
      }
    }
    
    return root;
  }
  
  /**
   * Fetches data about the running queries and status of queries in the cache
   * @param collector The collector to write to
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("query.count", running_queries.size(), "type=running");
    
    final Map<Integer, QueryStats> completed = completed_queries.asMap();
    int completed_success = 0;
    int completed_error = 0;
    for (final QueryStats stats : completed.values()) {
      if (stats.response == HttpResponseStatus.OK) {
        completed_success += stats.executed;
      } else {
        completed_error += stats.executed;
      }
    }
    
    collector.record("query.count", completed_success, "type=successful");
    collector.record("query.count", completed_error, "type=failed");
  }
  
  /** @return the start time of the query in ms */
  public long getQueryStart() {
    return query_start;
  }
  
  /** @return the total number of data points emitted for the query */
  public long getSize() {
    return size;
  }
  
  /** @param size increments the number of data points emitted */
  public void addSize(int size) {
    this.size += size;
  }
  
  /** @return the total number of data points retrieved from storage */
  public long getAggregatedSize() {
    return aggregated_size;
  }
  
  /** @param size increments the number of data points retrieved from storage */
  public void addAggregatedSize(int size) {
    aggregated_size += size;
  }
  
  /** @param time_storage the amount of time it took to fetch data from storage in ms */
  public void setTimeStorage(final long time_storage) {
    this.time_storage = time_storage;
  }
  
  /** @return the amount of time it took to fetch data from storage in ms */
  public long getTimeStorage() {
    return time_storage;
  }
  
  /** @param time_aggregation increments the amount of time spent aggregating in ms */
  public void addTimeAggregation(final long time_aggregation) {
    this.time_aggregation += time_aggregation;
  }
  
  /** @return the mount of time spent aggregating in ms */
  public long getTimeAggregation() {
    return time_aggregation;
  }
  
  /** @param time_serialization the amount of time spent serializing in ms */
  public void setTimeSerialization(final long time_serialization) {
    this.time_serialization = time_serialization;
  }
  
  /** @return the mount of time spent serializing in ms */
  public long getTimeSerialization() {
    return time_serialization;
  }
  
  /** @return the amount of time working on the query in ms */
  public long getTimeTotal() {
    return time_total;
  }

  /** @return the Http status code */
  public HttpResponseStatus getStatus() {
    return response;
  }
  
  /** @return an exception if it was associated with this query */
  public Throwable getException() {
    return exception;
  }

  /** @param whether or not to allow duplicate queries to run */
  public static void setEnableDuplicates(final boolean enable_dupes) {
    ENABLE_DUPLICATES = enable_dupes;
  }
}
