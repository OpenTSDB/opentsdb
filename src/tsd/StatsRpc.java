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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.JSON;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

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
    tsdb.collectStats(collector);
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
      
      if (xtratag != null) {
        if (xtratag.indexOf('=') != xtratag.lastIndexOf('=')) {
          throw new IllegalArgumentException("invalid xtratag: " + xtratag
              + " (multiple '=' signs), name=" + name + ", value=" + value);
        } else if (xtratag.indexOf('=') < 0) {
          throw new IllegalArgumentException("invalid xtratag: " + xtratag
              + " (missing '=' signs), name=" + name + ", value=" + value);
        }
        final String[] pair = xtratag.split("=");
        if (extratags == null) {
          extratags = new HashMap<String, String>(1);
        }
        extratags.put(pair[0], pair[1]);
      }
      
      addHostTag(canonical);
     
      final HashMap<String, String> tags = 
        new HashMap<String, String>(extratags);
      dp.setTags(tags);
      dps.add(dp);
      
    }
    
  }
}
