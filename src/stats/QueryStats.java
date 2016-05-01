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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import net.opentsdb.core.Const;
import net.opentsdb.core.QueryException;
import net.opentsdb.core.TSQuery;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

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
 * 
 * NOTE: Record everything in nano seconds, then convert to floating millis for 
 * serialization.
 * @since 2.2
 */
public class QueryStats {
  private static final Logger LOG = LoggerFactory.getLogger(QueryStats.class);
  private static final Logger QUERY_LOG = LoggerFactory.getLogger("QueryLog");
  
  /** Determines how many query stats to keep in the cache */
  private static int COMPLETED_QUERY_CACHE_SIZE = 256;
  
  /** Whether or not to allow duplicate queries from the same endpoint to
   * run simultaneously. */
  private static boolean ENABLE_DUPLICATES = true;
  
  /** Stores queries currently executing. If a thread doesn't call into 
   * markComplete then it's possible for this map to fill up.
   * Hash is the remote + query */
  private static ConcurrentHashMap<Integer, QueryStats> running_queries = 
      new ConcurrentHashMap<Integer, QueryStats>();
  
  /** Size limited cache of queries from the past. 
   * Hash is the remote + query + response code */
  private static Cache<Integer, QueryStats> completed_queries = 
      CacheBuilder.newBuilder().maximumSize(COMPLETED_QUERY_CACHE_SIZE).build();
  
  /** Start time for the query in nano seconds. Can be set post construction 
   * if necessary */
  private final long query_start_ns;
  
  /** Start timestamp for the query in millis for printing */
  private final long query_start_ms;
  
  /** When the query was marked completed in nanoseconds */
  private long query_completed_ts;
  
  /** The remote address as <IP>:<port>, may be ipv6 */
  private final String remote_address;
  
  /** The TSQuery object that contains the query specification */
  private final TSQuery query;
  
  /** HTTP response when the query was completed, either successfully or failed */
  private HttpResponseStatus response;
  
  /** Set if the query terminated with an exception */
  private Throwable exception;
  
  /** How many times this exact query was executed. Only updated on completion */
  private long executed;
  
  /** The users (if known) who executed this query (could be pulled from a header) */
  private String user;
  
  /** Stats for the entire query */
  private final Map<QueryStat, Long> overall_stats;
  
  /** Hold a list of stats for the sub queries */
  private final Map<Integer, Map<QueryStat, Long>> query_stats;
  
  /** Holds a list of stats for each scanner */
  private final Map<Integer, Map<Integer, Map<QueryStat, Long>>> scanner_stats;
  
  /** Hold a list of the region servers encountered for each scanner */
  private final Map<Integer, Map<Integer, Set<String>>> scanner_servers;
  
  /** Holds a lis tof the scanner IDs for each scanner */
  private final Map<Integer, Map<Integer, String>> scanner_ids;
  
  /** Holds a copy of the headers from the request */
  private final Map<String, String> headers;
  
  /** Whether or not the data was successfully sent to the client */
  private boolean sent_to_client;
  
  /**
   * A list of statistics surrounding individual queries
   */
  public enum QueryStat {
    // Query Setup stats
    STRING_TO_UID_TIME ("stringToUidTime", true),
    
    // Storage stats
    COLUMNS_FROM_STORAGE ("columnsFromStorage", false),
    ROWS_FROM_STORAGE ("rowsFromStorage", false),
    BYTES_FROM_STORAGE ("bytesFromStorage", false),
    SUCCESSFUL_SCAN ("successfulScan", false),
    
    // Single Scanner stats
    DPS_PRE_FILTER ("dpsPreFilter", false),
    ROWS_PRE_FILTER ("rowsPreFilter", false),
    DPS_POST_FILTER ("dpsPostFilter", false),
    ROWS_POST_FILTER ("rowsPostFilter", false),
    SCANNER_UID_TO_STRING_TIME ("scannerUidToStringTime", true),
    COMPACTION_TIME ("compactionTime", true),
    HBASE_TIME ("hbaseTime", true),
    UID_PAIRS_RESOLVED ("uidPairsResolved", false),
    SCANNER_TIME ("scannerTime", true),
    
    // Overall Salt Scanner stats
    SCANNER_MERGE_TIME ("saltScannerMergeTime", true),
    
    // Post Scan stats
    QUERY_SCAN_TIME ("queryScanTime", true),
    GROUP_BY_TIME ("groupByTime", true),
    
    // Serialization time stats
    UID_TO_STRING_TIME ("uidToStringTime", true),
    AGGREGATED_SIZE ("emittedDPs", false),
    NAN_DPS ("nanDPs", false),
    AGGREGATION_TIME ("aggregationTime", true),
    SERIALIZATION_TIME ("serializationTime", true),
    
    // Final stats
    PROCESSING_PRE_WRITE_TIME ("processingPreWriteTime", true),
    TOTAL_TIME ("totalTime", true),
    
    // MAX and Agg Times
    MAX_HBASE_TIME ("maxHBaseTime", true),
    AVG_HBASE_TIME ("avgHBaseTime", true),
    MAX_SALT_SCANNER_TIME ("maxScannerTime", true),
    AVG_SALT_SCANNER_TIME ("avgScannerTime", true),
    MAX_UID_TO_STRING ("maxUidToStringTime", true),
    AVG_UID_TO_STRING ("avgUidToStringTime", true),
    MAX_COMPACTION_TIME ("maxCompactionTime", true),
    AVG_COMPACTION_TIME ("avgCompactionTime", true),
    MAX_SCANNER_UID_TO_STRING_TIME ("maxScannerUidtoStringTime", true),
    AVG_SCANNER_UID_TO_STRING_TIME ("avgScannerUidToStringTime", true),
    MAX_SCANNER_MERGE_TIME ("maxSaltScannerMergeTime", true),
    AVG_SCANNER_MERGE_TIME ("avgSaltScannerMergeTime", true),
    MAX_SCAN_TIME ("maxQueryScanTime", true),
    AVG_SCAN_TIME ("avgQueryScanTime", true),
    MAX_AGGREGATION_TIME ("maxAggregationTime", true),
    AVG_AGGREGATION_TIME ("avgAggregationTime", true),
    MAX_SERIALIZATION_TIME ("maxSerializationTime", true),
    AVG_SERIALIZATION_TIME ("avgSerializationTime", true)
    ;
    
    /** The serializable name for this enum */
    private final String stat_name;
    /** Whether or not the stat is time based */
    private final boolean is_time;

    private QueryStat(final String stat_name, final boolean is_time) {
      this.stat_name = stat_name;
      this.is_time = is_time;
    }

    @Override
    public String toString() {
      return stat_name;
    }
  }

  // always AVG, MAX in the pair order
  static final Map<QueryStat, Pair<QueryStat, QueryStat>> AGG_MAP = 
      new HashMap<QueryStat, Pair<QueryStat, QueryStat>>();
  static {
    AGG_MAP.put(QueryStat.HBASE_TIME, new Pair<QueryStat, QueryStat>(
        QueryStat.AVG_HBASE_TIME, QueryStat.MAX_HBASE_TIME));
    AGG_MAP.put(QueryStat.SCANNER_TIME, new Pair<QueryStat, QueryStat>(
        QueryStat.AVG_SALT_SCANNER_TIME, QueryStat.MAX_HBASE_TIME));
    AGG_MAP.put(QueryStat.UID_TO_STRING_TIME, new Pair<QueryStat, QueryStat>(
        QueryStat.MAX_UID_TO_STRING, QueryStat.MAX_UID_TO_STRING));
    AGG_MAP.put(QueryStat.SCANNER_UID_TO_STRING_TIME, new Pair<QueryStat, QueryStat>(
        QueryStat.MAX_SCANNER_UID_TO_STRING_TIME, 
        QueryStat.AVG_SCANNER_UID_TO_STRING_TIME));
    AGG_MAP.put(QueryStat.QUERY_SCAN_TIME, new Pair<QueryStat, QueryStat>(
        QueryStat.MAX_SCAN_TIME, QueryStat.AVG_SCAN_TIME));
    AGG_MAP.put(QueryStat.AGGREGATION_TIME, new Pair<QueryStat, QueryStat>(
        QueryStat.MAX_AGGREGATION_TIME, QueryStat.AVG_AGGREGATION_TIME));
    AGG_MAP.put(QueryStat.SERIALIZATION_TIME, new Pair<QueryStat, QueryStat>(
        QueryStat.MAX_SERIALIZATION_TIME, 
        QueryStat.AVG_SERIALIZATION_TIME));
  }
  
  /**
   * Default CTor
   * @param remote_address Remote address of the client
   * @param query Query being executed
   * @param headers The HTTP headers passed with the query
   * @throws QueryException if the exact query is already running, e.g if the
   * client submitted the same query twice
   */
  public QueryStats(final String remote_address, final TSQuery query, 
      final Map<String, String> headers) {
    if (remote_address == null || remote_address.isEmpty()) {
      throw new IllegalArgumentException("Remote address was null or empty");
    }
    if (query == null) {
      throw new IllegalArgumentException("Query object was null");
    }
    this.remote_address = remote_address;
    this.query = query;
    this.headers = headers; // can be null
    executed = 1;
    query_start_ns = DateTime.nanoTime();
    query_start_ms = DateTime.currentTimeMillis();
    overall_stats = new HashMap<QueryStat, Long>();
    query_stats = new ConcurrentHashMap<Integer, Map<QueryStat, Long>>(1);
    scanner_stats = new ConcurrentHashMap<Integer, 
        Map<Integer, Map<QueryStat, Long>>>(1);
    scanner_servers = new ConcurrentHashMap<Integer, Map<Integer, Set<String>>>(1);
    scanner_ids = new ConcurrentHashMap<Integer, Map<Integer, String>>(1);
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
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Successfully put new query for remote " + remote_address + 
          " with hash " + hashCode() + " on thread " + 
          Thread.currentThread().getId() + " w q " + query.toString());
    }
    LOG.info("Executing new query=" + JSON.serializeToString(this));
  }
  
  /**
   * Returns the hash based on the remote address and the query
   */
  @Override
  public int hashCode() {
    return remote_address.hashCode() ^ query.hashCode();
  }
  
  /**
   * Equals is based solely on the endpoint and the original query
   */
  @Override
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
    // have to hack it to get the details. By default we dump just the highest
    // level of stats.
    final Map<String, Object> details = new HashMap<String, Object>();
    details.put("queryStartTimestamp", getQueryStartTimestamp());
    details.put("queryCompletedTimestamp", getQueryCompletedTimestamp());
    details.put("exception", getException());
    details.put("httpResponse", getHttpResponse());
    details.put("numRunningQueries", getNumRunningQueries());
    details.put("query", getQuery());
    details.put("user", getUser());
    details.put("requestHeaders", getRequestHeaders());
    details.put("executed", getExecuted());
    details.put("stats", getStats(true, true));
    return JSON.serializeToString(details);
  }
  
  /**
   * Marks a query as completed successfully with the 200 HTTP response code
   * without an exception.
   * Moves it from the running map to the cache, updating the cache if it already
   * existed.
   */
  public void markSerializationSuccessful() {
    markSerialized(HttpResponseStatus.OK, null);
  }
  
  /**
   * Marks a query as completed with the given HTTP code with exception and 
   * moves it from the running map to the cache, updating the cache if it 
   * already existed.
   * @param response The HTTP response to log
   * @param exception The exception thrown
   */
  public void markSerialized(final HttpResponseStatus response, 
      final Throwable exception) {
    this.exception = exception;
    this.response = response;
    
    query_completed_ts = DateTime.currentTimeMillis();
    overall_stats.put(QueryStat.PROCESSING_PRE_WRITE_TIME, DateTime.nanoTime() - query_start_ns);
    synchronized (running_queries) {
      if (!running_queries.containsKey(this.hashCode())) {
        if (!ENABLE_DUPLICATES) {
          LOG.warn("Query was already marked as complete: " + this);
        }
      }
      running_queries.remove(hashCode());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removed completed query " + remote_address + " with hash " + 
            hashCode() + " on thread " + Thread.currentThread().getId());
      }
    }
    
    aggQueryStats();
    
    final int cache_hash = this.hashCode() ^ response.toString().hashCode();
    synchronized (completed_queries) {
      final QueryStats old_query = completed_queries.getIfPresent(cache_hash);
      if (old_query == null) {
        completed_queries.put(cache_hash, this);
      } else {
        old_query.executed++;
      }
    }
  }
  
  /**
   * Marks the query as complete and logs it to the proper logs. This is called
   * after the data has been sent to the client.
   */
  public void markSent() {
    sent_to_client = true;
    overall_stats.put(QueryStat.TOTAL_TIME, DateTime.nanoTime() - query_start_ns);
    LOG.info("Completing query=" + JSON.serializeToString(this));
    QUERY_LOG.info(this.toString());
  }
  
  /** Leaves the sent_to_client field as false when we were unable to write to
   * the client end point. */
  public void markSendFailed() {
    overall_stats.put(QueryStat.TOTAL_TIME, DateTime.nanoTime() - query_start_ns);
    LOG.info("Completing query=" + JSON.serializeToString(this));
    QUERY_LOG.info(this.toString());
  }
  
  /**
   * Builds a serializable map from the running and cached query maps to be
   * returned to a caller.
   * @return A map for serialization
   */
  public static Map<String, Object> getRunningAndCompleteStats() {
    Map<String, Object> root = new TreeMap<String, Object>();
    
    if (running_queries.isEmpty()) {
      root.put("running", Collections.emptyList());
    } else {
      final List<Object> running = new ArrayList<Object>(running_queries.size());
      root.put("running", running);
      
      // don't need to lock the map beyond what the iterator will do implicitly
      for (final QueryStats stats : running_queries.values()) {
        final Map<String, Object> obj = new HashMap<String, Object>(10);
        obj.put("query", stats.query);
        obj.put("remote", stats.remote_address);
        obj.put("user", stats.user);
        obj.put("headers", stats.headers);;
        obj.put("queryStart", DateTime.msFromNano(stats.query_start_ns));
        obj.put("elapsed", DateTime.msFromNanoDiff(DateTime.nanoTime(), 
            stats.query_start_ns));
        running.add(obj);
      }
    }
    
    final Map<Integer, QueryStats> completed = completed_queries.asMap();
    if (completed.isEmpty()) {
      root.put("completed", Collections.emptyList());
    } else {
      root.put("completed", completed.values());
    }
    
    return root;
  }
  
  /**
   * Fetches data about the running queries and status of queries in the cache
   * @param collector The collector to write to
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("query.count", running_queries.size(), "type=running");
  }
  
  /**
   * Add an overall statistic for the query (i.e. not associated with a sub 
   * query or scanner)
   * @param name The name of the stat
   * @param value The value to store
   */
  public void addStat(final QueryStat name, final long value) {
    overall_stats.put(name, value);
  }
  
  /**
   * Adds a stat for a sub query, replacing it if it exists. Times must be
   * in nanoseconds.
   * @param query_index The index of the sub query to update 
   * @param name The name of the stat to update
   * @param value The value to set
   */
  public void addStat(final int query_index, final QueryStat name, 
      final long value) {
    Map<QueryStat, Long> qs = query_stats.get(query_index);
    if (qs == null) {
      qs = new HashMap<QueryStat, Long>();
      query_stats.put(query_index, qs);
    }
    qs.put(name, value);
  }

  /**
   * Aggregates the various stats from the lower to upper levels. This includes
   * calculating max and average time values for stats marked as time based.
   */
  public void aggQueryStats() {
    // These are overall aggregations
    final Map<QueryStat, Pair<Long, Long>> overall_cumulations = 
        new HashMap<QueryStat, Pair<Long, Long>>();
    
    // scanner aggs
    for (final Entry<Integer, Map<Integer, Map<QueryStat, Long>>> entry : 
      scanner_stats.entrySet()) {
      final int query_index = entry.getKey();
      
      final Map<QueryStat, Pair<Long, Long>> cumulations = 
          new HashMap<QueryStat, Pair<Long, Long>>();
      
      for (final Entry<Integer, Map<QueryStat, Long>> scanner : 
        entry.getValue().entrySet()) {
        
        for (final Entry<QueryStat, Long> stat : scanner.getValue().entrySet()) {
          if (stat.getKey().is_time) {
            if (!AGG_MAP.containsKey(stat.getKey())) {
              // we're not aggregating this value
              continue;
            }
            
            // per query aggs
            Pair<Long, Long> pair = cumulations.get(stat.getKey());
            if (pair == null) {
              pair = new Pair<Long, Long>(0L, Long.MIN_VALUE);
              cumulations.put(stat.getKey(), pair);
            }
            pair.setKey(pair.getKey() + stat.getValue());
            if (stat.getValue() > pair.getValue()) {
              pair.setValue(stat.getValue());
            }
            
            // overall aggs required here for proper time averaging
            pair = overall_cumulations.get(stat.getKey());
            if (pair == null) {
              pair = new Pair<Long, Long>(0L, Long.MIN_VALUE);
              overall_cumulations.put(stat.getKey(), pair);
            }
            pair.setKey(pair.getKey() + stat.getValue());
            if (stat.getValue() > pair.getValue()) {
              pair.setValue(stat.getValue());
            }
          } else {
            // only add counters for the per query maps as they'll be rolled
            // up below into the overall
            updateStat(query_index, stat.getKey(), stat.getValue());
          }
        }
      }
      
      // per query aggs
      for (final Entry<QueryStat, Pair<Long, Long>> cumulation : 
        cumulations.entrySet()) {
        // names can't be null as we validate above that it exists
        final Pair<QueryStat, QueryStat> names = AGG_MAP.get(cumulation.getKey());
        addStat(query_index, names.getKey(),  
            (cumulation.getValue().getKey() / entry.getValue().size()));
        addStat(query_index, names.getValue(), cumulation.getValue().getValue());
      }
    }
    
    // handle the per scanner aggs
    for (final Entry<QueryStat, Pair<Long, Long>> cumulation : 
      overall_cumulations.entrySet()) {
      // names can't be null as we validate above that it exists
      final Pair<QueryStat, QueryStat> names = AGG_MAP.get(cumulation.getKey());
      addStat(names.getKey(),  
          (cumulation.getValue().getKey() / 
              (scanner_stats.size() * 
                  Const.SALT_WIDTH() > 0 ? Const.SALT_BUCKETS() : 1)));
      addStat(names.getValue(), cumulation.getValue().getValue());
    }
    overall_cumulations.clear();
    
    // aggregate counters from the sub queries
    for (final Map<QueryStat, Long> sub_query : query_stats.values()) {
      for (final Entry<QueryStat, Long> stat : sub_query.entrySet()) {
        if (stat.getKey().is_time) {
          if (!AGG_MAP.containsKey(stat.getKey())) {
            // we're not aggregating this value
            continue;
          }
          Pair<Long, Long> pair = overall_cumulations.get(stat.getKey());
          if (pair == null) {
            pair = new Pair<Long, Long>(0L, Long.MIN_VALUE);
            overall_cumulations.put(stat.getKey(), pair);
          }
          pair.setKey(pair.getKey() + stat.getValue());
          if (stat.getValue() > pair.getValue()) {
            pair.setValue(stat.getValue());
          }
        } else if (overall_stats.containsKey(stat.getKey())) {
          overall_stats.put(stat.getKey(), 
              overall_stats.get(stat.getKey()) + stat.getValue());
        } else {
          overall_stats.put(stat.getKey(), stat.getValue());
        }
      }
    }
    
    for (final Entry<QueryStat, Pair<Long, Long>> cumulation : 
      overall_cumulations.entrySet()) {
      // names can't be null as we validate above that it exists
      final Pair<QueryStat, QueryStat> names = AGG_MAP.get(cumulation.getKey());
      overall_stats.put(names.getKey(),  
          (cumulation.getValue().getKey() / query_stats.size()));
      overall_stats.put(names.getValue(), cumulation.getValue().getValue());
    }
  }
  
  /**
   * Increments the cumulative value for a cumulative stat. If it's a time then
   * it must be in nanoseconds
   * @param query_index The index of the sub query
   * @param name The name of the stat
   * @param value The value to add to the existing value
   */
  public void updateStat(final int query_index, final QueryStat name, 
      final long value) {
    Map<QueryStat, Long> qs = query_stats.get(query_index);
    long cum_time = value;
    if (qs == null) {
      qs = new HashMap<QueryStat, Long>();
      query_stats.put(query_index, qs);
    }
    
    if (qs.containsKey(name)) {
      cum_time += qs.get(name);
    }
    qs.put(name, cum_time);
  }

  /**
   * Adds a value for a specific scanner for a specific sub query. If it's a time
   * then it must be in nanoseconds.
   * @param query_index The index of the sub query
   * @param id The numeric ID of the scanner 
   * @param name The name of the stat
   * @param value The value to add to the map
   */
  public void addScannerStat(final int query_index, final int id, 
      final QueryStat name, final long value) {
    Map<Integer, Map<QueryStat, Long>> qs = scanner_stats.get(query_index);
    if (qs == null) {
      qs = new ConcurrentHashMap<Integer, Map<QueryStat, Long>>(
              Const.SALT_WIDTH() > 0 ? Const.SALT_BUCKETS() : 1);
      scanner_stats.put(query_index, qs);
    }
    Map<QueryStat, Long> scanner_stat_map = qs.get(id);
    if (scanner_stat_map == null) {
      scanner_stat_map = new HashMap<QueryStat, Long>();
      qs.put(id, scanner_stat_map);
    }
    scanner_stat_map.put(name, value);
  }
  
  /**
   * Adds or overwrites the list of servers scanned by a scanner
   * @param query_index The index of the sub query
   * @param id The numeric ID of the scanner 
   * @param servers The list of servers encountered
   */
  public void addScannerServers(final int query_index, final int id, 
      final Set<String> servers) {
    Map<Integer, Set<String>> query_servers = scanner_servers.get(query_index);
    if (query_servers == null) {
      query_servers = new ConcurrentHashMap<Integer, Set<String>>(
          Const.SALT_WIDTH() > 0 ? Const.SALT_BUCKETS() : 1);
      scanner_servers.put(query_index, query_servers);
    }
    query_servers.put(id, servers);
  }
  
  /**
   * Updates or adds a stat for a specific scanner. IF it's a time it must
   * be in nanoseconds
   * @param query_index The index of the sub query
   * @param id The numeric ID of the scanner 
   * @param name The name of the stat
   * @param value The value to update to the map
   */
  public void updateScannerStat(final int query_index, final int id, 
      final QueryStat name, final long value) {
    Map<Integer, Map<QueryStat, Long>> qs = scanner_stats.get(query_index);
    long cum_time = value;
    if (qs == null) {
      qs = new ConcurrentHashMap<Integer, Map<QueryStat, Long>>();
      scanner_stats.put(query_index, qs);
    }
    Map<QueryStat, Long> scanner_stat_map = qs.get(id);
    if (scanner_stat_map == null) {
      scanner_stat_map = new HashMap<QueryStat, Long>();
      qs.put(id, scanner_stat_map);
    }

    if (scanner_stat_map.containsKey(name)) {
      cum_time += scanner_stat_map.get(name);
    }
    
    scanner_stat_map.put(name, cum_time);
  }

  /**
   * Adds a scanner for a sub query to the stats along with the description of
   * the scanner.
   * @param query_index The index of the sub query
   * @param id The numeric ID of the scanner 
   * @param string_id The description of the scanner
   */
  public void addScannerId(final int query_index, final int id, 
      final String string_id) {
    Map<Integer, String> scanners = scanner_ids.get(query_index);
    if (scanners == null) {
      scanners = new ConcurrentHashMap<Integer, String>();
      scanner_ids.put(query_index, scanners);
    }
    scanners.put(id, string_id);
  }
  
  /** @return the start time of the query in nano seconds */
  public long queryStart() {
    return query_start_ns;
  }

  /** @param user The user who executed the query */
  public void setUser(final String user) {
    this.user = user;
  }
  
  /** @return The user who executed the query if known */
  public String getUser() {
    return user;
  }
  
  /** @return The multi-mapped set of request headers associated with the query */
  public Map<String, String> getRequestHeaders() {
    return headers;
  }
  
  /** @return The number of currently running queries */
  public int getNumRunningQueries() {
    return running_queries.size();
  }
  
  /** @return An exception associated with the query or null if the query 
   * returned successfully. */
  public String getException() {
    if (exception == null) {
      return "null";
    }
    return exception.getMessage() + 
        (exception.getStackTrace() != null && exception.getStackTrace().length > 0
        ? "\n" + exception.getStackTrace()[0].toString() : "");
  }
  
  /** @return The HTTP status response for the query */
  public HttpResponseStatus getHttpResponse() {
    return response;
  }
  
  /** @return The number of times this query has been executed from the same
   * endpoint. */
  public long getExecuted() {
    return executed;
  }
  
  /** @return The full query */
  public TSQuery getQuery() {
    return query;
  }
  
  /** @return When the query was received and started executing, in ms */
  public long getQueryStartTimestamp() {
    return query_start_ms;
  }
  
  /** @return When the query was marked as completed, in ms */
  public long getQueryCompletedTimestamp() {
    return query_completed_ts;
  }
  
  /** @return Whether or not the data was successfully sent to the client */
  public boolean getSentToClient() {
    return sent_to_client;
  }
  
  /** @return A map with the subset of query measurements, not including scanners 
   * or sub queries */
  public Map<String, Object> getStats() {
    return getStats(false, false);
  }
  
  /**
   * Returns measurements of the given query
   * @param with_scanners Whether or not to dump individual scanner stats
   * @return A map with stats for the query
   */
  public Map<String, Object> getStats(final boolean with_sub_queries, 
      final boolean with_scanners) {
    final Map<String, Object> map = new TreeMap<String, Object>();
    
    for (final Entry<QueryStat, Long> entry : overall_stats.entrySet()) {
      if (entry.getKey().is_time) {
        map.put(entry.getKey().toString(), DateTime.msFromNano(entry.getValue()));
      } else {
        map.put(entry.getKey().toString(), entry.getValue());
      }
    }
    
    if (with_sub_queries) {
      final Iterator<Entry<Integer, Map<QueryStat, Long>>> it = 
          query_stats.entrySet().iterator();
      while (it.hasNext()) {
        final Entry<Integer, Map<QueryStat, Long>> entry = it.next();
        final Map<String, Object> qs = new HashMap<String, Object>(1);
        qs.put(String.format("queryIdx_%02d", entry.getKey()), 
            getQueryStats(entry.getKey(), with_scanners));
        map.putAll(qs);
      }
    }
    return map;
  }

  /**
   * Returns a map of stats for a single sub query
   * @param index The sub query to fetch
   * @param with_scanners Whether or not to print detailed stats for each scanner
   * @return A map with stats to print or null if the sub query didn't have any
   * data.
   */
  public Map<String, Object> getQueryStats(final int index, 
      final boolean with_scanners) {
    
    final Map<QueryStat, Long> qs = query_stats.get(index);
    if (qs == null) {
      return null;
    }
    
    final Map<String, Object> query_map = new TreeMap<String, Object>();
    query_map.put("queryIndex", index);
    
    final Iterator<Entry<QueryStat, Long>> stats_it = 
        qs.entrySet().iterator();
    while (stats_it.hasNext()) {
      final Entry<QueryStat, Long> stat = stats_it.next();
      if (stat.getKey().is_time) {
        query_map.put(stat.getKey().toString(), 
            DateTime.msFromNano(stat.getValue()));
      } else {
        query_map.put(stat.getKey().toString(), stat.getValue());
      }
    }
    
    if (with_scanners) {
      final Map<Integer, Map<QueryStat, Long>> scanner_stats_map = 
          scanner_stats.get(index);
      
      final Map<String, Object> scanner_maps = new TreeMap<String, Object>();
      
      query_map.put("scannerStats", scanner_maps);
      if (scanner_stats_map != null) {
        final Map<Integer, String> scanners = scanner_ids.get(index);
        final Iterator<Entry<Integer, Map<QueryStat, Long>>> scanner_it = 
            scanner_stats_map.entrySet().iterator();
        while (scanner_it.hasNext()) {
          final Entry<Integer, Map<QueryStat, Long>> scanner = scanner_it.next();
          final Map<String, Object> scanner_map = new TreeMap<String, Object>();
          scanner_maps.put(String.format("scannerIdx_%02d", scanner.getKey()), 
              scanner_map);
          final String id;
          if (scanners != null) {
            id = scanners.get(scanner.getKey());
          } else {
            id = null;
          }
          scanner_map.put("scannerId", id);
          /* Uncomment when AsyncHBase supports this
          final Map<Integer, Set<String>> servers = scanner_servers.get(index);
          if (servers != null) {
            scanner_map.put("regionServers", servers.get(scanner.getKey()));
          } else {
            scanner_map.put("regionServers", null);
          }
          */
          final Iterator<Entry<QueryStat, Long>> scanner_stats_it = 
              scanner.getValue().entrySet().iterator();
          while (scanner_stats_it.hasNext()) {
            final Entry<QueryStat, Long> scanner_stats = scanner_stats_it.next();
            if (!scanner_stats.getKey().is_time) {
              scanner_map.put(scanner_stats.getKey().toString(), 
                  scanner_stats.getValue());
            } else {
              scanner_map.put(scanner_stats.getKey().toString(), 
                  DateTime.msFromNano(scanner_stats.getValue()));
            }
          }
        }
      }
    }
    return query_map;
  }
  
  /** @return A stat for the overall query or -1 if the stat didn't exist. */
  public long getStat(final QueryStat stat) {
    if (!overall_stats.containsKey(stat)) {
      return -1;
    }
    return overall_stats.get(stat);
  }
  
  /** @return a timed stat for the overall query or NaN if the stat didn't exist */
  public double getTimeStat(final QueryStat stat) {
    if (!stat.is_time) {
      throw new IllegalArgumentException("The stat is not a time stat");
    }
    if (!overall_stats.containsKey(stat)) {
      return Double.NaN;
    }
    return DateTime.msFromNano(overall_stats.get(stat));
  }
  
  /** @param whether or not to allow duplicate queries to run */
  public static void setEnableDuplicates(final boolean enable_dupes) {
    ENABLE_DUPLICATES = enable_dupes;
  }
}
