// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.JSON;

import org.hbase.async.RegionClientStats;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * Handles fetching statistics from all over the code, collating them in a
 * string buffer or list, and emitting them to the caller. Stats are collected
 * lazily, i.e. only when this method is called.
 * This class supports the 1.x style HTTP call as well as the 2.x style API
 * calls.
 * @since 2.0
 */
public final class StatsRpc implements TelnetRpc, HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(StatsRpc.class);
  
  /**
   * Telnet RPC responder that returns the stats in ASCII style
   * @param tsdb The TSDB to use for fetching stats
   * @param chan The netty channel to respond on
   * @param cmd call parameters
   */
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
      final String[] cmd) {
    final boolean canonical = tsdb.getConfig().getBoolean("tsd.stats.canonical");
    final StringBuilder buf = new StringBuilder(1024);
    final ASCIICollector collector = new ASCIICollector("tsd", buf, null);
    doCollectStats(tsdb, collector, canonical);
    chan.write(buf.toString());
    return Deferred.fromResult(null);
  }

  /**
   * HTTP resposne handler
   * @param tsdb The TSDB to which we belong
   * @param query The query to parse and respond to
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    // only accept GET/POST
    if (query.method() != HttpMethod.GET && query.method() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    
    try {
      final String[] uri = query.explodeAPIPath();
      final String endpoint = uri.length > 1 ? uri[1].toLowerCase() : "";

      // Handle /threads and /regions.
      if ("threads".equals(endpoint)) {
        printThreadStats(query);
        return;
      } else if ("jvm".equals(endpoint)) {
        printJVMStats(tsdb, query);
        return;
      } else if ("query".equals(endpoint)) {
        printQueryStats(query);
        return;
      } else if ("region_clients".equals(endpoint)) {
        printRegionClientStats(tsdb, query);
        return;
      }
    } catch (IllegalArgumentException e) {
      // this is thrown if the url doesn't start with /api. To maintain backwards
      // compatibility with the /stats endpoint we can catch and continue here.
    }
    
    final boolean canonical = tsdb.getConfig().getBoolean("tsd.stats.canonical");
    
    // if we don't have an API request we need to respond with the 1.x version
    if (query.apiVersion() < 1) {
      final boolean json = query.hasQueryStringParam("json");
      final StringBuilder buf = json ? null : new StringBuilder(2048);
      final ArrayList<String> stats = json ? new ArrayList<String>(64) : null;
      final ASCIICollector collector = new ASCIICollector("tsd", buf, stats);
      doCollectStats(tsdb, collector, canonical);
      if (json) {
        query.sendReply(JSON.serializeToBytes(stats));
      } else {
        query.sendReply(buf);
      }
      return;
    }
    
    // we have an API version, so go newschool
    final List<IncomingDataPoint> dps = new ArrayList<IncomingDataPoint>(64);
    final SerializerCollector collector = new SerializerCollector("tsd", dps, 
        canonical);
    ConnectionManager.collectStats(collector);
    RpcHandler.collectStats(collector);
    RpcManager.collectStats(collector);
    tsdb.collectStats(collector);
    query.sendReply(query.serializer().formatStatsV1(dps));
  }
  
  /**
   * Helper to record the statistics for the current TSD
   * @param tsdb The TSDB to use for fetching stats
   * @param collector The collector class to call for emitting stats
   */
  private void doCollectStats(final TSDB tsdb, final StatsCollector collector, 
      final boolean canonical) {
    collector.addHostTag(canonical);
    ConnectionManager.collectStats(collector);
    RpcHandler.collectStats(collector);
    RpcManager.collectStats(collector);
    collectThreadStats(collector);
    tsdb.collectStats(collector);
  }
  
  /**
   * Display stats for each region client
   * @param tsdb The TSDB to use for fetching stats
   * @param query The query to respond to
   */
  private void printRegionClientStats(final TSDB tsdb, final HttpQuery query) {
    final List<RegionClientStats> region_stats = tsdb.getClient().regionStats();
    final List<Map<String, Object>> stats = 
        new ArrayList<Map<String, Object>>(region_stats.size());
    for (final RegionClientStats rcs : region_stats) {
      final Map<String, Object> stat_map = new HashMap<String, Object>(8);
      stat_map.put("rpcsSent", rcs.rpcsSent());
      stat_map.put("rpcsInFlight", rcs.inflightRPCs());
      stat_map.put("pendingRPCs", rcs.pendingRPCs());
      stat_map.put("pendingBatchedRPCs", rcs.pendingBatchedRPCs());
      stat_map.put("dead", rcs.isDead());
      stat_map.put("rpcid", rcs.rpcID());
      stat_map.put("endpoint", rcs.remoteEndpoint());
      stat_map.put("rpcsTimedout", rcs.rpcsTimedout());
      stat_map.put("rpcResponsesTimedout", rcs.rpcResponsesTimedout());
      stat_map.put("rpcResponsesUnknown", rcs.rpcResponsesUnknown());
      stat_map.put("inflightBreached", rcs.inflightBreached());
      stat_map.put("pendingBreached", rcs.pendingBreached());
      stat_map.put("writesBlocked", rcs.writesBlocked());
      
      stats.add(stat_map);
    }
    query.sendReply(query.serializer().formatRegionStatsV1(stats));
  }
  
  
  /**
   * Grabs a snapshot of all JVM thread states and formats it in a manner to
   * be displayed via API.
   * @param query The query to respond to
   */
  private void printThreadStats(final HttpQuery query) {
    final Set<Thread> threads = Thread.getAllStackTraces().keySet();
    final List<Map<String, Object>> output = 
        new ArrayList<Map<String, Object>>(threads.size());
    for (final Thread thread : threads) {
      final Map<String, Object> status = new HashMap<String, Object>();
      status.put("threadID", thread.getId());
      status.put("name", thread.getName());
      status.put("state", thread.getState().toString());
      status.put("interrupted", thread.isInterrupted());
      status.put("priority", thread.getPriority());

      final List<String> stack = 
          new ArrayList<String>(thread.getStackTrace().length);
      for (final StackTraceElement element: thread.getStackTrace()) {
        stack.add(element.toString());
      }
      status.put("stack", stack);
      output.add(status);
    }
    query.sendReply(query.serializer().formatThreadStatsV1(output));
  }
  
  /**
   * Yield (chiefly memory-related) stats about this OpenTSDB instance's JVM.
   * @param tsdb The TSDB from which to fetch stats.
   * @param query The query to which to respond.
   */
  private void printJVMStats(final TSDB tsdb, final HttpQuery query) {
    final Map<String, Map<String, Object>> map = 
        new HashMap<String, Map<String, Object>>();
    
    final RuntimeMXBean runtime_bean = ManagementFactory.getRuntimeMXBean();
    final Map<String, Object> runtime = new HashMap<String, Object>();
    map.put("runtime", runtime);
    
    runtime.put("startTime", runtime_bean.getStartTime());
    runtime.put("uptime", runtime_bean.getUptime());
    runtime.put("vmName", runtime_bean.getVmName());
    runtime.put("vmVendor", runtime_bean.getVmVendor());
    runtime.put("vmVersion", runtime_bean.getVmVersion());
    
    final MemoryMXBean mem_bean = ManagementFactory.getMemoryMXBean();
    final Map<String, Object> memory = new HashMap<String, Object>();
    map.put("memory", memory);
    
    memory.put("heapMemoryUsage", mem_bean.getHeapMemoryUsage());
    memory.put("nonHeapMemoryUsage", mem_bean.getNonHeapMemoryUsage());
    memory.put("objectsPendingFinalization",
        mem_bean.getObjectPendingFinalizationCount());
    
    final List<GarbageCollectorMXBean> gc_beans = 
        ManagementFactory.getGarbageCollectorMXBeans();
    final Map<String, Object> gc = new HashMap<String, Object>();
    map.put("gc", gc);
    
    for (final GarbageCollectorMXBean gc_bean : gc_beans) {
      final Map<String, Object> stats = new HashMap<String, Object>();
      final String name = formatStatName(gc_bean.getName());
      if (name == null) {
        LOG.warn("Null name for bean: " + gc_bean);
        continue;
      }
      
      gc.put(name, stats);
      stats.put("collectionCount", gc_bean.getCollectionCount());
      stats.put("collectionTime", gc_bean.getCollectionTime());
    }    
    
    final List<MemoryPoolMXBean> pool_beans = 
        ManagementFactory.getMemoryPoolMXBeans();
    final Map<String, Object> pools = new HashMap<String, Object>();
    map.put("pools", pools);
    
    for (final MemoryPoolMXBean pool_bean : pool_beans) {
      final Map<String, Object> stats = new HashMap<String, Object>();
      final String name = formatStatName(pool_bean.getName());
      if (name == null) {
        LOG.warn("Null name for bean: " + pool_bean);
        continue;
      }
      pools.put(name, stats);
      
      stats.put("collectionUsage", pool_bean.getCollectionUsage());
      stats.put("usage", pool_bean.getUsage());
      stats.put("peakUsage", pool_bean.getPeakUsage());
      stats.put("type", pool_bean.getType());
    }
    
    final OperatingSystemMXBean os_bean = 
        ManagementFactory.getOperatingSystemMXBean();
    final Map<String, Object> os = new HashMap<String, Object>();
    map.put("os", os);
    
    os.put("systemLoadAverage", os_bean.getSystemLoadAverage());
    
    query.sendReply(query.serializer().formatJVMStatsV1(map));
  }
  
  /**
   * Runs through the live threads and counts captures a coune of their
   * states for dumping in the stats page.
   * @param collector The collector to write to
   */
  private void collectThreadStats(final StatsCollector collector) {
    final Set<Thread> threads = Thread.getAllStackTraces().keySet();
    final Map<String, Integer> states = new HashMap<String, Integer>(6);
    states.put("new", 0);
    states.put("runnable", 0);
    states.put("blocked", 0);
    states.put("waiting", 0);
    states.put("timed_waiting", 0);
    states.put("terminated", 0);
    for (final Thread thread : threads) {
      int state_count = states.get(thread.getState().toString().toLowerCase());
      state_count++;
      states.put(thread.getState().toString().toLowerCase(), state_count);
    }
    for (final Map.Entry<String, Integer> entry : states.entrySet()) {
      collector.record("jvm.thread.states", entry.getValue(), "state=" + 
          entry.getKey());
    }
    collector.record("jvm.thread.count", threads.size());
  }
  
  /**
   * Little helper to convert the first character to lowercase and remove any
   * spaces
   * @param stat The name to cleanup
   * @return a clean name or null if the original string was null or empty
   */
  private static String formatStatName(final String stat) {
    if (stat == null || stat.isEmpty()) {
      return stat;
    }
    String name = stat.replace(" ", "");
    return name.substring(0, 1).toLowerCase() + name.substring(1);
  }
  
  /**
   * Print the detailed query stats to the caller using the proper serializer
   * @param query The query to answer to
   * @throws BadRequestException if the API version hasn't been implemented
   * yet
   */
  private void printQueryStats(final HttpQuery query) {
    switch (query.apiVersion()) {
    case 0:
    case 1:
      query.sendReply(query.serializer().formatQueryStatsV1(
          QueryStats.buildStats()));
      break;
    default: 
      throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
          "Requested API version not implemented", "Version " + 
          query.apiVersion() + " is not implemented");
    }
  }
  
  /**
   * Implements the StatsCollector with ASCII style output. Builds a string
   * buffer response to send to the caller
   */
  final class ASCIICollector extends StatsCollector {

    final StringBuilder buf;
    final ArrayList<String> stats;
    
    /**
     * Default constructor
     * @param prefix The prefix to prepend to all statistics
     * @param buf The buffer to store responses in
     * @param stats An array of strings to write for the old style JSON output
     * May be null. If that's the case, we'll try to write to the {@code buf}
     */
    public ASCIICollector(final String prefix, final StringBuilder buf, 
        final ArrayList<String> stats) {
      super(prefix);
      this.buf = buf;
      this.stats = stats;
    }
    
    /**
     * Called by the {@link #record} method after a source writes a statistic.
     */
    @Override
    public final void emit(final String line) {
      if (stats != null) {
        stats.add(line.substring(0, line.length() - 1));  // strip the '\n'
      } else {
        buf.append(line);
      }
    }
  }
  
  /**
   * Implements the StatsCollector with a list of IncomingDataPoint objects that
   * can be passed on to a serializer for output.
   */
  final class SerializerCollector extends StatsCollector {
    
    final boolean canonical;
    final List<IncomingDataPoint> dps;
    
    /**
     * Default constructor 
     * @param prefix The prefix to prepend to all statistics
     * @param dps The array to store objects in
     */
    public SerializerCollector(final String prefix, 
        final List<IncomingDataPoint> dps, final boolean canonical) {
      super(prefix);
      this.dps = dps;
      this.canonical = canonical;
    }

    /**
     * Override that records the stat to an IncomingDataPoint object and puts it
     * in the list
     * @param name Metric name
     * @param value The value to store
     * @param xtratag An optional extra tag in the format "tagk=tagv". Can only
     * have one extra tag
     */
    @Override
    public void record(final String name, final long value, 
        final String xtratag) {
      
      final IncomingDataPoint dp = new IncomingDataPoint();
      dp.setMetric(prefix + "." + name);
      dp.setTimestamp(System.currentTimeMillis() / 1000L);
      dp.setValue(Long.toString(value));
      
      String tagk = "";
      if (xtratag != null) {
        if (xtratag.indexOf('=') != xtratag.lastIndexOf('=')) {
          throw new IllegalArgumentException("invalid xtratag: " + xtratag
              + " (multiple '=' signs), name=" + name + ", value=" + value);
        } else if (xtratag.indexOf('=') < 0) {
          throw new IllegalArgumentException("invalid xtratag: " + xtratag
              + " (missing '=' signs), name=" + name + ", value=" + value);
        }
        final String[] pair = xtratag.split("=");
        tagk = pair[0];
        addExtraTag(tagk, pair[1]);
      }
      
      addHostTag(canonical);
     
      final HashMap<String, String> tags = 
        new HashMap<String, String>(extratags);
      dp.setTags(tags);
      dps.add(dp);
      
      if (!tagk.isEmpty()) {
        clearExtraTag(tagk);
      }
    }
    
  }
}
